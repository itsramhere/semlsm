package semlsm

import (
	"context"
	"fmt"
	"path/filepath"
	"time"

	"semlsm/internal/base"
	"semlsm/internal/compaction"
	"semlsm/internal/failpoint"
	"semlsm/internal/manifest"
	"semlsm/internal/merge"
	"semlsm/internal/semantic"
	"semlsm/internal/sstable"
	"semlsm/internal/version"
)

// Failpoint names for structural compaction (P6.2).
var (
	fpCompactBeforeManifest = failpoint.Define("compact.before_manifest")
	fpCompactAfterManifest  = failpoint.Define("compact.after_manifest")
)

// startCompactionWorkers builds the picker and launches the fast worker and
// the deep worker(s): semantic jobs when a Summarizer is configured (P10.2),
// else the raw deep loop (P6.3).
func (db *DB) startCompactionWorkers() {
	db.picker = compaction.NewPicker(
		db.opts.L0CompactTrigger, db.opts.L1TargetBytes,
		db.opts.LevelMultiplier, db.opts.TargetFileBytes)
	// Tier B pipeline: one Summarizer over the configured llm stack (P10).
	if db.opts.Summarizer != nil {
		db.semSum = semantic.NewSummarizer(db.opts.Summarizer,
			db.opts.MaxValueBytesInPrompt, db.opts.MaxChunkBytes,
			db.opts.MinCompressionRatio, max(1, db.opts.SemanticWorkers))
		db.semModel = db.opts.Summarizer.Model()
		db.semPV = db.opts.Summarizer.PromptVersion()
	}
	if db.opts.DisableAutoCompaction {
		return
	}
	db.compactWG.Add(1)
	go db.fastCompactLoop()
	if db.opts.Summarizer == nil {
		db.compactWG.Add(1)
		go db.deepRawLoop()
		return
	}
	for i := 0; i < max(1, db.opts.SemanticWorkers); i++ {
		db.compactWG.Add(1)
		go db.semanticLoop()
	}
}

// fastCompactLoop drives L0->L1 and L1->L2 (one worker; never waits on deep).
// Compaction errors back off with growing sleeps (100ms doubling to 5s) like
// the flush path, and each failure is counted in Stats (M-015 item 6). When
// its L1->L2 plan is blocked by files a semantic job holds and L1 is over
// HardScore, it preempts that job (anti-stall rule I11, P10.2).
func (db *DB) fastCompactLoop() {
	defer db.compactWG.Done()
	backoff := 100 * time.Millisecond
	for {
		if db.closed.Load() {
			return
		}
		db.drainObsolete()
		// Anti-stall (I11): while L1 is over HardScore, a semantic job
		// holding the L2 files the fast worker needs is preempted. Checked
		// every iteration: under constant writes the fast worker may never
		// go idle, but its L1->L2 duty stays blocked all the same.
		db.maybePreemptDeep()
		plan := db.pickFast()
		if plan == nil {
			select {
			case <-db.compactWake:
			case <-db.closeCh:
				return
			case <-time.After(20 * time.Millisecond):
			}
			continue
		}
		if err := db.runStructural(plan); err != nil {
			db.compactErrs.Add(1)
			db.logger.Warn("semlsm: fast compaction failed", "err", err)
			if !db.compactBackoff(&backoff) {
				return
			}
			continue
		}
		backoff = 100 * time.Millisecond
	}
}

// deepRawLoop moves L2+ data down raw when scores exceed 1 (P6.3 interim).
func (db *DB) deepRawLoop() {
	defer db.compactWG.Done()
	backoff := 100 * time.Millisecond
	for {
		if db.closed.Load() {
			return
		}
		db.drainObsolete()
		plan := db.pickDeepRaw()
		if plan == nil {
			select {
			case <-db.compactWake:
			case <-db.closeCh:
				return
			case <-time.After(50 * time.Millisecond):
			}
			continue
		}
		if err := db.runStructural(plan); err != nil {
			db.compactErrs.Add(1)
			db.logger.Warn("semlsm: deep compaction failed", "err", err)
			if !db.compactBackoff(&backoff) {
				return
			}
			continue
		}
		backoff = 100 * time.Millisecond
	}
}

// compactBackoff sleeps for the current backoff step, doubling it up to 5s.
// It returns false when the DB closed during the sleep (the worker exits
// instead of retrying forever).
func (db *DB) compactBackoff(backoff *time.Duration) bool {
	select {
	case <-db.closeCh:
		return false
	case <-time.After(*backoff):
	}
	if *backoff < 5*time.Second {
		*backoff *= 2
	}
	return true
}

// pickFast returns a plan from a consistent version snapshot.
func (db *DB) pickFast() *compaction.Plan {
	v := db.vs.Acquire()
	defer v.Unref()
	db.mu.Lock()
	busy := db.busySnapshotLocked()
	db.mu.Unlock()
	return db.picker.PickFast(v, busy)
}

func (db *DB) pickDeepRaw() *compaction.Plan {
	v := db.vs.Acquire()
	defer v.Unref()
	db.mu.Lock()
	busy := db.busySnapshotLocked()
	db.mu.Unlock()
	plan := db.picker.PickDeep(v, busy)
	if plan != nil {
		return plan
	}
	// bottom-level self-compaction placeholder: P10 implements the sweep;
	// without it L4 files with score >= 1 just wait.
	return nil
}

func (db *DB) busySnapshotLocked() map[uint64]bool {
	out := make(map[uint64]bool, len(db.busy))
	for num := range db.busy {
		out[num] = true
	}
	return out
}

// runStructural merges plan inputs into OutputLevel with an atomic manifest
// edit (P6.2). All versions and tombstones are retained.
func (db *DB) runStructural(plan *compaction.Plan) error {
	v := db.vs.Acquire()
	defer v.Unref()

	files := plan.AllFiles()
	db.mu.Lock()
	if db.rs == nil {
		db.mu.Unlock()
		return nil
	}
	// The plan was picked from an older version (M-015 item 5): another
	// compaction may have consumed its files in between. Re-check membership
	// against the current version while marking busy; a stale plan is dropped
	// with no error and no backoff.
	for _, f := range files {
		if !db.rs.version.HasFile(f.Num) {
			db.mu.Unlock()
			return nil
		}
	}
	for _, f := range files {
		if db.busy[f.Num] {
			// Another compaction owns the file: drop the plan silently and
			// re-pick later (the picker already avoids busy files; this is
			// the race window).
			db.mu.Unlock()
			return nil
		}
	}
	for _, f := range files {
		db.busy[f.Num] = true
	}
	db.compactionsInFlight++
	db.mu.Unlock()
	defer func() {
		db.mu.Lock()
		for _, f := range files {
			delete(db.busy, f.Num)
		}
		db.compactionsInFlight--
		db.stallCond.Broadcast()
		db.mu.Unlock()
		db.wakeFlush()
	}()

	// Merge sources: keep every version and tombstone.
	sources := make([]merge.Iter, 0, len(files))
	for _, f := range files {
		reader, rel, err := db.tableCache.Get(f.Num)
		if err != nil {
			return fmt.Errorf("semlsm: compact open %d: %w", f.Num, err)
		}
		defer rel()
		sources = append(sources, reader.NewIterator())
	}
	src := merge.New(sources...)

	ow := compaction.NewOutputWriter(db.opts.FS, db.dir, db.opts.TargetFileBytes, db.allocFile)
	outputLevel := plan.OutputLevel
	for src.First(); src.Valid(); src.Next() {
		if err := ow.Add(src.Key(), src.Value()); err != nil {
			ow.Abort()
			return fmt.Errorf("semlsm: compact output: %w", err)
		}
	}
	if err := src.Err(); err != nil {
		ow.Abort()
		return fmt.Errorf("semlsm: compact merge: %w", err)
	}
	outputs, err := ow.Finish()
	if err != nil {
		ow.Abort()
		return fmt.Errorf("semlsm: compact finish: %w", err)
	}
	// Empty output is impossible (inputs were non-empty), but guard anyway.
	if len(outputs) == 0 {
		ow.Abort()
		return fmt.Errorf("semlsm: compact produced no output")
	}
	for _, fm := range outputs {
		fm.Level = outputLevel
		if err := db.opts.FS.SyncDir(db.dir); err != nil {
			ow.Abort()
			return fmt.Errorf("semlsm: compact syncdir: %w", err)
		}
	}

	// Atomic manifest edit.
	if err := failpoint.Hit(fpCompactBeforeManifest); err != nil {
		ow.Abort()
		return err
	}
	db.manifestMu.Lock()
	e := &manifest.Edit{}
	for _, f := range files {
		e.DelFiles = append(e.DelFiles, manifest.DelFile{Level: f.Level, Num: f.Num})
	}
	for _, fm := range outputs {
		e.AddFiles = append(e.AddFiles, fm.FileMeta)
	}
	if err := db.appendManifestLocked(e, db.vs.CurrentLastTs()); err != nil {
		db.manifestMu.Unlock()
		ow.Abort()
		return fmt.Errorf("semlsm: compact manifest: %w", err)
	}

	// Install under mu nested inside manifestMu (B7): the Apply->readState
	// swap is atomic with respect to other installers, so the readState's
	// version ref is released exactly once.
	db.mu.Lock()
	newVersion := db.vs.Acquire()
	if db.rs != nil {
		old := db.rs.version
		db.rs = &readState{mem: db.mem, imms: db.rs.imms, version: newVersion}
		old.Unref()
		db.stallCond.Broadcast()
	}
	db.mu.Unlock()
	db.manifestMu.Unlock()

	if err := failpoint.Hit(fpCompactAfterManifest); err != nil {
		return err
	}

	// Obsolete deletion is driven by version death (drainObsolete); a
	// snapshot pinning this version keeps its files alive.
	db.drainObsolete()
	return nil
}

// WaitIdle blocks until all flush and compaction queues are drained (B5/P6.3,
// Pitfall 15: includes queued flushes, in-flight jobs and just-triggered
// picks).
func (db *DB) WaitIdle(ctx context.Context) error {
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if db.closed.Load() {
			return ErrClosed
		}
		// Drain obsolete first: an idle engine may still hold deletable
		// files whose versions died since the last drain.
		db.drainObsolete()
		db.mu.Lock()
		idle := db.rs != nil &&
			len(db.rs.imms) == 0 &&
			db.compactionsInFlight == 0 &&
			len(db.commitQ) == 0
		// a just-triggered pick: the flush queue is drained and the commit
		// queue is empty, but L0 may still exceed its trigger
		if idle && db.picker != nil && !db.opts.DisableAutoCompaction {
			scores := db.picker.Score(db.rs.version)
			if scores[0] >= 1 || scores[1] >= 1 || scores[2] >= 1 || scores[3] >= 1 {
				idle = false
			}
		}
		db.mu.Unlock()
		if idle {
			return nil
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// CompactLevel runs one compaction from level synchronously (manual trigger
// for tests and the CLI). Deep levels run semantic jobs when a healthy
// Summarizer is configured; the bottom level is the P10.4 sweep with the
// nextTry backoff ignored (manual intent).
func (db *DB) CompactLevel(ctx context.Context, level int) error {
	if db.closed.Load() {
		return ErrClosed
	}
	if level < 0 || level >= base.NumLevels {
		return fmt.Errorf("semlsm: bad level %d", level)
	}
	v := db.vs.Acquire()
	defer v.Unref()
	db.mu.Lock()
	busy := db.busySnapshotLocked()
	db.mu.Unlock()
	if level == base.NumLevels-1 {
		plan := db.pickSweep(v, busy, true)
		if plan == nil {
			return nil // no eligible file
		}
		if db.opts.Summarizer == nil || !db.llmAvailable() {
			return db.runStructural(plan)
		}
		return db.runSemantic(plan, plan.Inputs[0].Num)
	}
	plan := db.picker.PickLevel(v, level, busy)
	if plan == nil {
		return nil // nothing to compact
	}
	if level >= 2 && db.opts.Summarizer != nil && db.llmAvailable() {
		return db.runSemantic(plan, 0)
	}
	return db.runStructural(plan)
}

var (
	_ = sstable.OpenReader
	_ = base.NumLevels
)

// drainObsolete moves files whose versions died into the deleter queue.
// Files die only when their last referencing Version unrefs, so a snapshot
// keeps them alive (P6.4, M-014 item 4). A dying checkpoint takes its
// archive SSTs with it (best effort: an already-gone checkpoint leaves its
// archives for startup cleanup).
func (db *DB) drainObsolete() {
	if db.obsolete == nil {
		return
	}
	for _, d := range db.vs.TakeObsolete() {
		suffix := ".sst"
		isCkpt := false
		if d.Level < 0 {
			suffix = ".ckpt"
			isCkpt = true
		}
		if isCkpt {
			db.enqueueDyingArchives(d.Num)
		}
		db.obsolete.Enqueue(version.ObsoleteFile{
			Path:   filepath.Join(db.dir, fmt.Sprintf("%06d%s", d.Num, suffix)),
			Num:    d.Num,
			IsCkpt: isCkpt,
		})
	}
}

// enqueueDyingArchives queues the archive SSTs referenced by one dying
// checkpoint file for deletion.
func (db *DB) enqueueDyingArchives(ckptNum uint64) {
	if db.ckptCache == nil {
		return
	}
	r, rel, err := db.ckptCache.Get(ckptNum)
	if err != nil {
		return // the checkpoint is already unreadable; archives leak to P13
	}
	defer rel()
	for _, t := range r.Tuples() {
		if t.ArchiveFileNum != 0 {
			db.obsolete.Enqueue(version.ObsoleteFile{
				Path: filepath.Join(db.dir, "archive", fmt.Sprintf("%06d.sst", t.ArchiveFileNum)),
				Num:  t.ArchiveFileNum,
			})
		}
	}
}

// VerifyReport summarizes an integrity check (P7.5 minimal form; P11.3
// extends it with archive CRC and tuple-chain validation).
type VerifyReport struct {
	SSTFiles  int
	CkptFiles int
	Tuples    int
	Entries   int
	Problems  []string
}

// VerifyIntegrity reads every block of every SST and every section of every
// checkpoint referenced by the current version. Any CRC or structural
// failure is reported; it never returns wrong data silently (I12).
func (db *DB) VerifyIntegrity(ctx context.Context) (VerifyReport, error) {
	rep := VerifyReport{}
	v := db.vs.Acquire()
	defer v.Unref()
	for l := range v.Levels {
		for _, f := range v.Levels[l] {
			r, rel, err := db.tableCache.Get(f.Num)
			if err != nil {
				rep.Problems = append(rep.Problems, fmt.Sprintf("sst %d: open: %v", f.Num, err))
				continue
			}
			it := r.NewIterator()
			for it.First(); it.Valid(); it.Next() {
				rep.Entries++
			}
			if e := it.Err(); e != nil {
				rep.Problems = append(rep.Problems, fmt.Sprintf("sst %d: iterate: %v", f.Num, e))
			}
			it.Close()
			rel()
			rep.SSTFiles++
		}
	}
	for num := range v.Ckpts.ByNum {
		r, rel, err := db.ckptCache.Get(num)
		if err != nil {
			rep.Problems = append(rep.Problems, fmt.Sprintf("ckpt %d: open: %v", num, err))
			continue
		}
		for _, t := range r.Tuples() {
			rep.Tuples++
			keys, err := r.IterKeys(t.ID)
			if err != nil {
				rep.Problems = append(rep.Problems, fmt.Sprintf("ckpt %d tuple %d: index: %v", num, t.ID, err))
				continue
			}
			for range keys {
				rep.Entries++
			}
			if _, err := r.ReadSummary(t.ID); err != nil {
				rep.Problems = append(rep.Problems, fmt.Sprintf("ckpt %d tuple %d: summary: %v", num, t.ID, err))
			}
		}
		rel()
		rep.CkptFiles++
	}
	return rep, nil
}

// ----- Tier B: semantic compaction (P10) -----

// fpSemanticAfterManifest is hit after the semantic edit is durable and
// installed (P10.1 step 10); a crash here recovers from the manifest.
var fpSemanticAfterManifest = failpoint.Define("semantic.after_manifest")

// llmAvailable reports whether the configured Summarizer would currently
// admit a call (P10.2). Summarizers without a health capability are treated
// as available.
func (db *DB) llmAvailable() bool {
	if db.opts.Summarizer == nil {
		return false
	}
	if h, ok := db.opts.Summarizer.(interface{ LLMHealthy() bool }); ok {
		return h.LLMHealthy()
	}
	return true
}

// semanticLoop runs Tier B jobs (P10.2): deep plans at score >= 1, or the
// bottom-level sweep when its timer is due. While the LLM is unavailable
// (breaker open or budget exhausted), raw structural compaction runs only
// when a level's score reaches HardScore, so data keeps moving and later
// passes can still summarize.
func (db *DB) semanticLoop() {
	defer db.compactWG.Done()
	backoff := 100 * time.Millisecond
	for {
		if db.closed.Load() {
			return
		}
		db.drainObsolete()
		plan, sweepNum := db.pickSemantic()
		if plan == nil {
			select {
			case <-db.compactWake:
			case <-db.closeCh:
				return
			case <-time.After(20 * time.Millisecond):
			}
			continue
		}
		if !db.llmAvailable() {
			if db.levelScore(plan.Level) < db.opts.HardScore {
				db.noteSweepOutcome(sweepNum, false)
				continue // wait for the LLM or for HardScore
			}
			if err := db.runStructural(plan); err != nil {
				db.compactErrs.Add(1)
				db.logger.Warn("semlsm: raw deep compaction failed", "err", err)
				if !db.compactBackoff(&backoff) {
					return
				}
				continue
			}
			db.noteSweepOutcome(sweepNum, false)
			backoff = 100 * time.Millisecond
			continue
		}
		if err := db.runSemantic(plan, sweepNum); err != nil {
			db.compactErrs.Add(1)
			db.logger.Warn("semlsm: semantic job failed", "err", err)
			if !db.compactBackoff(&backoff) {
				return
			}
			continue
		}
		backoff = 100 * time.Millisecond
	}
}

// levelScore returns one level's current compaction score.
func (db *DB) levelScore(level int) float64 {
	v := db.vs.Acquire()
	defer v.Unref()
	return db.picker.Score(v)[level]
}

// pickSemantic returns the next Tier B plan: a deep plan at score >= 1, or a
// bottom-level sweep when the sweep timer is due (P10.2/P10.4). sweepNum is
// the L4 input file number for nextTry bookkeeping (0 for deep plans).
func (db *DB) pickSemantic() (*compaction.Plan, uint64) {
	v := db.vs.Acquire()
	defer v.Unref()
	db.mu.Lock()
	busy := db.busySnapshotLocked()
	db.mu.Unlock()
	if plan := db.picker.PickDeep(v, busy); plan != nil {
		return plan, 0
	}
	if db.sweepDue() {
		db.lastSweep.Store(db.opts.Clock.Now().UnixNano())
		if plan := db.pickSweep(v, busy, false); plan != nil {
			return plan, plan.Inputs[0].Num
		}
	}
	return nil, 0
}

// sweepDue reports whether SemanticSweepInterval has elapsed on the injected
// clock since the last sweep attempt.
func (db *DB) sweepDue() bool {
	last := db.lastSweep.Load()
	if last == 0 {
		return true
	}
	return db.opts.Clock.Now().Sub(time.Unix(0, last)) >= db.opts.SemanticSweepInterval
}

// pickSweep picks one L4 file holding eligible groups (P10.4): its MinTs is
// older than the smallest MinAge of any enabled policy, its count reaches
// the smallest MinBatch, and (unless manual) it is not in the nextTry
// backoff. Rewrites happen in place: inputs at L4, outputs at L4.
func (db *DB) pickSweep(v *version.Version, busy map[uint64]bool, manual bool) *compaction.Plan {
	minAge, minBatch, ok := db.smallestEligibility()
	if !ok {
		return nil
	}
	nowUnix := uint64(db.opts.Clock.Now().UnixNano())
	if uint64(minAge) >= nowUnix {
		return nil // nothing can be old enough yet
	}
	db.mu.Lock()
	nextTry := make(map[uint64]time.Time, len(db.nextTry))
	for k, t := range db.nextTry {
		nextTry[k] = t
	}
	db.mu.Unlock()
	for _, f := range v.Levels[base.NumLevels-1] {
		if busy[f.Num] {
			continue
		}
		if f.MinTs+uint64(minAge) > nowUnix {
			continue // the whole file is younger than the horizon
		}
		if f.Count < uint64(minBatch) {
			continue
		}
		if !manual {
			if nt, tried := nextTry[f.Num]; tried && db.opts.Clock.Now().Before(nt) {
				continue
			}
		}
		return &compaction.Plan{
			Level:       base.NumLevels - 1,
			OutputLevel: base.NumLevels - 1,
			Inputs:      []*version.FileMeta{f},
		}
	}
	return nil
}

// smallestEligibility returns the smallest MinAge and MinBatch over all
// enabled policies (P10.4's quick eligibility check). ok is false when no
// policy enables summarization.
func (db *DB) smallestEligibility() (minAge time.Duration, minBatch int, ok bool) {
	first := true
	consider := func(p base.Policy) {
		if p.Mode == base.ModeDisabled {
			return
		}
		if first || p.MinAge < minAge {
			minAge = p.MinAge
		}
		if first || p.MinBatch < minBatch {
			minBatch = p.MinBatch
		}
		first = false
	}
	for _, p := range db.opts.Policies {
		consider(p)
	}
	consider(db.opts.DefaultPolicy)
	return minAge, minBatch, !first
}

// noteSweepOutcome applies the sweep backoff (P10.4): a file that produced no
// tuples is not retried before a growing delay; a successful one is free.
func (db *DB) noteSweepOutcome(sweepNum uint64, summarized bool) {
	if sweepNum == 0 {
		return
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	if summarized {
		delete(db.nextTry, sweepNum)
		return
	}
	now := db.opts.Clock.Now()
	base := db.opts.SemanticSweepInterval
	if nt, tried := db.nextTry[sweepNum]; tried && nt.After(now) {
		if grown := nt.Sub(now) * 2; grown < 10*base {
			base = grown
		} else {
			base = 10 * base
		}
	}
	db.nextTry[sweepNum] = now.Add(base)
}

// maybePreemptDeep implements the anti-stall rule (I11, P10.2): when the
// fast worker cannot make L1->L2 progress because a semantic job holds L2
// files and L1's score is at least HardScore, the job is preempted. A
// preempted job keeps its successful segments and commits quickly.
func (db *DB) maybePreemptDeep() {
	v := db.vs.Acquire()
	defer v.Unref()
	if db.picker.Score(v)[1] < db.opts.HardScore {
		return
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	for _, f := range v.Levels[1] {
		if db.busy[f.Num] {
			continue
		}
		blocked := false
		for _, o := range v.Levels[2] {
			if db.busy[o.Num] && rangesIntersect(f, o) {
				blocked = true
				break
			}
		}
		if !blocked {
			continue
		}
		for h := range db.semJobs {
			h.cancel()
			db.semPreemptions.Add(1)
		}
		return
	}
}

// rangesIntersect reports whether two files' user-key ranges overlap.
func rangesIntersect(a, b *version.FileMeta) bool {
	return base.CompareUser(base.UserKey(a.Largest), base.UserKey(b.Smallest)) >= 0 &&
		base.CompareUser(base.UserKey(a.Smallest), base.UserKey(b.Largest)) <= 0
}

// semanticDeps assembles the job dependencies from the DB's facilities. No
// callback holds an engine lock when the job invokes it (I11).
func (db *DB) semanticDeps() compaction.SemanticDeps {
	return compaction.SemanticDeps{
		FS:             db.opts.FS,
		Dir:            db.dir,
		ArchiveDir:     filepath.Join(db.dir, "archive"),
		GroupDepth:     db.opts.GroupDepth,
		GroupDelimiter: db.opts.GroupDelimiter,
		PolicyFor: func(key []byte) base.Policy {
			return policyFor(db.opts, key)
		},
		Now:             db.opts.Clock.Now,
		AllocNum:        db.allocFile,
		PrevTupleID:     db.prevTupleID,
		TargetFileBytes: db.opts.TargetFileBytes,
		ArchiveRaw:      db.opts.ArchiveRaw,
		Model:           db.semModel,
		PromptVersion:   db.semPV,
		OpenInput: func(num uint64) (merge.Iter, func(), error) {
			r, rel, err := db.tableCache.Get(num)
			if err != nil {
				return nil, nil, err
			}
			return r.NewIterator(), rel, nil
		},
		Summarize: func(ctx context.Context, seg semantic.Segment, group string) (semantic.Summary, semantic.Usage, error) {
			return db.semSum.Summarize(ctx, seg, group)
		},
		Logger: db.logger,
	}
}

// prevTupleID returns the newest existing tuple id of the group from the
// current Version — the PrevTupleID chain head (P10.1). 0 when none.
func (db *DB) prevTupleID(group string) uint64 {
	v := db.vs.Acquire()
	defer v.Unref()
	refs := v.Ckpts.GroupRefs(group)
	if len(refs) == 0 {
		return 0
	}
	return refs[len(refs)-1].Ref.ID // sorted by TsFrom; the last is newest
}

// runSemantic drives one Tier B job through P10.1's order: the size guard
// (step 2) hands oversized plans to the raw structural path; the job runs
// steps 3-9 with no engine locks held (I11); the driver appends the one
// manifest edit (step 10), installs the new read state (step 11), and lets
// obsolete deletion happen through version death (step 12).
func (db *DB) runSemantic(plan *compaction.Plan, sweepNum uint64) error {
	files := plan.AllFiles()
	var totalBytes int64
	for _, f := range files {
		totalBytes += f.Size
	}
	if totalBytes > db.opts.MaxSemanticJobBytes {
		return db.runStructural(plan) // step 2: raw structural instead
	}

	v := db.vs.Acquire()
	defer v.Unref()
	db.mu.Lock()
	if db.rs == nil {
		db.mu.Unlock()
		return nil
	}
	// The plan was picked from an older version (M-015 item 5): re-check
	// membership against the current version while marking busy; stale or
	// already-owned plans are dropped silently.
	for _, f := range files {
		if !db.rs.version.HasFile(f.Num) || db.busy[f.Num] {
			db.mu.Unlock()
			return nil
		}
	}
	for _, f := range files {
		db.busy[f.Num] = true
	}
	db.compactionsInFlight++
	handle := &semJobHandle{files: make(map[uint64]bool, len(files))}
	for _, f := range files {
		handle.files[f.Num] = true
	}
	db.semJobs[handle] = struct{}{}
	db.mu.Unlock()
	unmark := func() {
		db.mu.Lock()
		for _, f := range files {
			delete(db.busy, f.Num)
		}
		delete(db.semJobs, handle)
		db.compactionsInFlight--
		db.stallCond.Broadcast()
		db.mu.Unlock()
		db.wakeFlush()
	}

	// Per-job context: SemanticJobTimeout plus a preempt cancel (P10.2).
	jctx, cancel := context.WithTimeout(context.Background(), db.opts.SemanticJobTimeout)
	defer cancel()
	handle.cancel = cancel

	res, jstats, err := compaction.RunSemantic(jctx, plan, db.semanticDeps())
	if err != nil {
		unmark()
		db.foldJobStats(nil, jstats, "failed")
		return err
	}
	outcome := "committed"
	if !res.HasTuples() {
		outcome = "raw-only"
	}
	db.foldJobStats(res, jstats, outcome)
	db.noteSweepOutcome(sweepNum, res.HasTuples())

	// Step 10: one manifest edit under manifestMu, fsynced before install.
	db.manifestMu.Lock()
	e := &manifest.Edit{}
	for _, f := range files {
		e.DelFiles = append(e.DelFiles, manifest.DelFile{Level: f.Level, Num: f.Num})
	}
	for _, fm := range res.Outputs {
		e.AddFiles = append(e.AddFiles, fm.FileMeta)
	}
	if res.HasTuples() {
		e.AddCkpts = append(e.AddCkpts, res.Ckpt)
	}
	if err := db.appendManifestLocked(e, db.vs.CurrentLastTs()); err != nil {
		db.manifestMu.Unlock()
		unmark()
		db.foldJobStats(nil, compaction.SemanticStats{}, "failed")
		return fmt.Errorf("semlsm: semantic manifest: %w", err)
	}

	// Step 11: install under mu nested inside manifestMu (B7).
	db.mu.Lock()
	newVersion := db.vs.Acquire()
	if db.rs != nil {
		old := db.rs.version
		db.rs = &readState{mem: db.mem, imms: db.rs.imms, version: newVersion}
		old.Unref()
		db.stallCond.Broadcast()
	}
	db.mu.Unlock()
	db.manifestMu.Unlock()

	if err := failpoint.Hit(fpSemanticAfterManifest); err != nil {
		unmark()
		return err
	}
	unmark()

	// Step 12: obsolete deletion is driven by version death; a snapshot
	// pinning this version keeps the inputs alive.
	db.drainObsolete()
	return nil
}

// foldJobStats folds one finished job's accounting into the Stats counters
// (P10.3).
func (db *DB) foldJobStats(res *compaction.SemanticResult, js compaction.SemanticStats, outcome string) {
	if res != nil && res.HasTuples() {
		db.semTuples.Add(uint64(len(res.Ckpt.Tuples)))
	}
	db.semRecs.Add(js.RecordsSummarized)
	db.semRawBytes.Add(js.RawBytesRemoved)
	db.semSumBytes.Add(js.SummaryBytes)
	db.semCalls.Add(uint64(js.LLMCalls))
	db.semInTokens.Add(uint64(js.InputTokens))
	db.semOutTokens.Add(uint64(js.OutputTokens))
	db.semStatsMu.Lock()
	for k, n := range js.Failures {
		db.semFail[k] += uint64(n)
	}
	db.semJobsOut[outcome]++
	db.semStatsMu.Unlock()
}
