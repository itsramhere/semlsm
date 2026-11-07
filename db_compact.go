package semlsm

import (
	"context"
	"fmt"
	"path/filepath"
	"sort"
	"time"

	"github.com/itsramhere/semlsm/internal/archive"
	"github.com/itsramhere/semlsm/internal/base"
	"github.com/itsramhere/semlsm/internal/checkpoint"
	"github.com/itsramhere/semlsm/internal/compaction"
	"github.com/itsramhere/semlsm/internal/failpoint"
	"github.com/itsramhere/semlsm/internal/manifest"
	"github.com/itsramhere/semlsm/internal/memtable"
	"github.com/itsramhere/semlsm/internal/merge"
	"github.com/itsramhere/semlsm/internal/semantic"
	"github.com/itsramhere/semlsm/internal/sstable"
	"github.com/itsramhere/semlsm/internal/version"
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
		retained, retentionErr := db.rewriteCheckpoints(context.Background(), true, true)
		if retentionErr != nil {
			if db.closed.Load() {
				return
			}
			db.compactErrs.Add(1)
			db.logger.Warn("semlsm: retention failed", "err", retentionErr)
			if !db.compactBackoff(&backoff) {
				return
			}
		}
		// One bounded checkpoint job per iteration, alongside (not in place
		// of) fast SST work. Tier B workers never wait for this copy's I/O.
		merged, mergeErr := db.compactCheckpoints(context.Background(), true)
		if mergeErr != nil {
			if db.closed.Load() {
				return
			}
			db.compactErrs.Add(1)
			db.logger.Warn("semlsm: checkpoint merge failed", "err", mergeErr)
			if !db.compactBackoff(&backoff) {
				return
			}
		}
		// Anti-stall (I11): while L1 is over HardScore, a semantic job
		// holding the L2 files the fast worker needs is preempted. Checked
		// every iteration: under constant writes the fast worker may never
		// go idle, but its L1->L2 duty stays blocked all the same.
		db.maybePreemptDeep()
		plan := db.pickFast()
		if plan == nil {
			if merged || retained {
				continue
			}
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
			if db.opts.CkptMergeThreshold >= 0 && len(db.rs.version.Ckpts.ByNum) > db.opts.CkptMergeThreshold {
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
// keeps them alive (P6.4, M-014 item 4). A dying checkpoint carries its
// archive numbers from the manifest; only legacy metadata needs a file read.
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
			db.enqueueDyingArchives(d.Ckpt)
			if db.ckptCache != nil {
				db.ckptCache.Evict(d.Num)
			}
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
func (db *DB) enqueueDyingArchives(cf *manifest.CkptFile) {
	if cf == nil {
		return
	}
	nums, err := checkpointArchives(db.opts.FS, db.dir, cf)
	if err != nil {
		db.logger.Warn("semlsm: cannot determine dying checkpoint archives", "file", cf.FileNum, "err", err)
		return
	}
	keep, err := checkpointArchiveSet(db.opts.FS, db.dir, db.vs.LiveCheckpoints())
	if err != nil {
		db.logger.Warn("semlsm: skipping archive deletion: live archive set unknown", "err", err)
		return
	}
	for _, num := range nums {
		if !keep[num] {
			db.obsolete.Enqueue(version.ObsoleteFile{
				Path: archive.Path(db.archiveDir(), num),
				Num:  num,
			})
		}
	}
}

// VerifyReport summarizes an integrity check (P7.5 minimal form; P11.3
// extends it with archive CRC and tuple-chain validation).
type VerifyReport struct {
	Stubs                int
	ArchiveRetiredTuples int
	SSTFiles             int
	CkptFiles            int
	Tuples               int
	Entries              int
	Problems             []string
}

// tupleRun is one key's dropped range within a tuple (I7 checking).
type tupleRun struct {
	min, max uint64
	tuple    uint64
}

// VerifyIntegrity reads every block of every SST and every section of every
// checkpoint referenced by the current version, walks I7 across all tuples
// and raw versions, recomputes SourceCRC32 from archives where they exist,
// and validates the PrevTupleID chains (P11.3). Any CRC or structural
// failure is reported; it never returns wrong data silently (I12).
func (db *DB) VerifyIntegrity(ctx context.Context) (VerifyReport, error) {
	rep := VerifyReport{}
	v := db.vs.Acquire()
	defer v.Unref()

	rawTs := map[string][]uint64{}
	tupleRuns := map[string][]tupleRun{}
	chains := map[string][]checkpoint.TupleMeta{}

	recordRaw := func(ikey []byte) {
		if u, ts, _, ok := base.SplitInternalKey(ikey); ok {
			rawTs[string(u)] = append(rawTs[string(u)], ts)
		}
	}

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
				recordRaw(it.Key())
			}
			if e := it.Err(); e != nil {
				rep.Problems = append(rep.Problems, fmt.Sprintf("sst %d: iterate: %v", f.Num, e))
			}
			it.Close()
			rel()
			rep.SSTFiles++
		}
	}
	// Raw versions in the memtables are not dropped and must not sit inside
	// any tuple range either.
	if rs, _ := db.acquireReadState(); rs != nil {
		addMem := func(mt *memtable.Memtable) {
			if mt == nil {
				return
			}
			it := mt.NewIterator()
			for it.First(); it.Valid(); it.Next() {
				recordRaw(it.Key())
			}
			it.Close()
		}
		addMem(rs.mem)
		for _, im := range rs.imms {
			addMem(im.mem)
		}
		releaseReadState(rs)
	}

	for num := range v.Ckpts.ByNum {
		r, rel, err := db.ckptCache.Get(num)
		if err != nil {
			rep.Problems = append(rep.Problems, fmt.Sprintf("ckpt %d: open: %v", num, err))
			continue
		}
		for _, ref := range v.Ckpts.ByNum[num].Tuples {
			t, err := r.ReadMeta(ref.ID)
			if err != nil {
				rep.Problems = append(rep.Problems, fmt.Sprintf("ckpt %d tuple %d: meta: %v", num, ref.ID, err))
				continue
			}
			rep.Tuples++
			if t.RetiredUnixNano != 0 {
				rep.Stubs++
			}
			if t.ArchiveRetiredUnixNano != 0 {
				rep.ArchiveRetiredTuples++
			}
			chains[t.Group] = append(chains[t.Group], t)
			keys, err := r.IterKeys(t.ID)
			if err != nil {
				rep.Problems = append(rep.Problems, fmt.Sprintf("ckpt %d tuple %d: index: %v", num, t.ID, err))
				continue
			}
			for _, ke := range keys {
				rep.Entries++
				tupleRuns[string(ke.Key)] = append(tupleRuns[string(ke.Key)],
					tupleRun{min: ke.MinTs, max: ke.MaxTs, tuple: t.ID})
			}
			if t.RetiredUnixNano == 0 {
				if _, err := r.ReadSummary(t.ID); err != nil {
					rep.Problems = append(rep.Problems, fmt.Sprintf("ckpt %d tuple %d: summary: %v", num, t.ID, err))
				}
				if t.ArchiveRetiredUnixNano == 0 {
					db.verifyArchiveCrc(&rep, t)
				}
			}
		}
		rel()
		rep.CkptFiles++
	}

	// I7: dropped ranges per key are disjoint and contain no raw version.
	for key, runs := range tupleRuns {
		sort.Slice(runs, func(a, b int) bool { return runs[a].min < runs[b].min })
		for i := 1; i < len(runs); i++ {
			if runs[i-1].max >= runs[i].min {
				rep.Problems = append(rep.Problems, fmt.Sprintf("I7: key %q tuple ranges overlap: [%d,%d] and [%d,%d]",
					key, runs[i-1].min, runs[i-1].max, runs[i].min, runs[i].max))
			}
		}
		lo, hi := runs[0].min, runs[len(runs)-1].max
		for _, ts := range rawTs[key] {
			if ts >= lo && ts <= hi {
				rep.Problems = append(rep.Problems, fmt.Sprintf("I7: key %q raw version ts %d inside dropped range [%d,%d]",
					key, ts, lo, hi))
			}
		}
	}

	// PrevTupleID chains: predecessors exist in the group, are used once,
	// and following them terminates (no cycles).
	for group, metas := range chains {
		ids := map[uint64]bool{}
		for _, t := range metas {
			ids[t.ID] = true
		}
		prevOwner := map[uint64]uint64{}
		for _, t := range metas {
			if t.PrevTupleID == 0 {
				continue
			}
			if !ids[t.PrevTupleID] {
				rep.Problems = append(rep.Problems, fmt.Sprintf("group %q tuple %d: PrevTupleID %d dangling", group, t.ID, t.PrevTupleID))
				continue
			}
			if owner, dup := prevOwner[t.PrevTupleID]; dup && owner != t.ID {
				rep.Problems = append(rep.Problems, fmt.Sprintf("group %q: tuples %d and %d share PrevTupleID %d", group, owner, t.ID, t.PrevTupleID))
			}
			prevOwner[t.PrevTupleID] = t.ID
		}
		for _, t := range metas {
			seen := map[uint64]bool{t.ID: true}
			cur := t.PrevTupleID
			for cur != 0 && !seen[cur] {
				seen[cur] = true
				var next uint64
				for _, m := range metas {
					if m.ID == cur {
						next = m.PrevTupleID
						break
					}
				}
				cur = next
			}
			if cur != 0 {
				rep.Problems = append(rep.Problems, fmt.Sprintf("group %q tuple %d: PrevTupleID chain has a cycle", group, t.ID))
			}
		}
	}
	return rep, nil
}

// verifyArchiveCrc recomputes one tuple's SourceCRC32 from its archive SST
// (P11.3). A missing or mismatching archive is reported, never fatal.
func (db *DB) verifyArchiveCrc(rep *VerifyReport, t checkpoint.TupleMeta) {
	if t.ArchiveFileNum == 0 {
		return
	}
	path := archive.Path(db.archiveDir(), t.ArchiveFileNum)
	r, err := sstable.OpenReader(db.opts.FS, path)
	if err != nil {
		rep.Problems = append(rep.Problems, fmt.Sprintf("tuple %d: archive %d missing or unreadable: %v", t.ID, t.ArchiveFileNum, err))
		return
	}
	defer r.Close()
	var recs []base.Record
	it := r.NewIterator()
	for it.First(); it.Valid(); it.Next() {
		u, ts, k, ok := base.SplitInternalKey(it.Key())
		if !ok {
			it.Close()
			rep.Problems = append(rep.Problems, fmt.Sprintf("archive %d: bad internal key", t.ArchiveFileNum))
			return
		}
		recs = append(recs, base.Record{Key: append([]byte(nil), u...), Ts: ts, Kind: k, Value: append([]byte(nil), it.Value()...)})
	}
	if e := it.Err(); e != nil {
		it.Close()
		rep.Problems = append(rep.Problems, fmt.Sprintf("archive %d: read: %v", t.ArchiveFileNum, e))
		return
	}
	it.Close()
	if uint64(len(recs)) != t.SourceCount {
		rep.Problems = append(rep.Problems, fmt.Sprintf("tuple %d: archive %d holds %d records, want %d", t.ID, t.ArchiveFileNum, len(recs), t.SourceCount))
		return
	}
	sort.Slice(recs, func(a, b int) bool {
		if recs[a].Ts != recs[b].Ts {
			return recs[a].Ts < recs[b].Ts
		}
		return string(recs[a].Key) < string(recs[b].Key)
	})
	if crc := base.SourceCRC(recs); crc != t.SourceCRC32 {
		rep.Problems = append(rep.Problems, fmt.Sprintf("tuple %d: archive %d CRC %08x, want %08x", t.ID, t.ArchiveFileNum, crc, t.SourceCRC32))
	}
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
		ArchiveDir:     db.archiveDir(),
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

// prevTupleID returns the most recently created tuple id of the group from
// the current Version — the PrevTupleID chain head (P10.1). 0 when none.
// "Newest" means newest created (largest id; ids come from the monotonic
// file allocator), not largest TsFrom: a deep job can drop an older range
// after a sweep dropped a newer one, so insertion order and ts order differ
// and the last-by-TsFrom tuple would be handed out twice (found by the
// P13.4 soak).
func (db *DB) prevTupleID(group string) uint64 {
	v := db.vs.Acquire()
	defer v.Unref()
	refs := v.Ckpts.GroupRefs(group)
	if len(refs) == 0 {
		return 0
	}
	best := refs[0].Ref.ID
	for _, loc := range refs[1:] {
		if loc.Ref.ID > best {
			best = loc.Ref.ID
		}
	}
	return best
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

	// Publish cancellation together with the job, before another worker or
	// Close can observe the handle.
	jctx, cancel := context.WithTimeout(context.Background(), db.opts.SemanticJobTimeout)
	defer cancel()
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
	handle := &semJobHandle{cancel: cancel, files: make(map[uint64]bool, len(files))}
	for _, f := range files {
		handle.files[f.Num] = true
	}
	db.semJobs[handle] = struct{}{}
	db.mu.Unlock()
	unmark := func() {
		db.mu.Lock()
		for num := range handle.files {
			delete(db.busy, num)
		}
		delete(db.semJobs, handle)
		db.compactionsInFlight--
		db.stallCond.Broadcast()
		db.mu.Unlock()
		db.wakeFlush()
	}

	deps := db.semanticDeps()
	deps.AllocNum = func() uint64 {
		num := db.allocFile()
		db.mu.Lock()
		db.busy[num] = true
		handle.files[num] = true
		db.mu.Unlock()
		return num
	}
	res, jstats, err := compaction.RunSemantic(jctx, plan, deps)
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
		// The edit never became durable: the job's files would otherwise
		// leak and grow the disk on every retry (M-027 item 1).
		db.removeSemanticFiles(res)
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

// removeSemanticFiles deletes the files a finished-but-uninstalled semantic
// job wrote (outputs, checkpoint, archives). Only for errors before the
// manifest edit is durable (M-027 item 1).
func (db *DB) removeSemanticFiles(res *compaction.SemanticResult) {
	if res == nil {
		return
	}
	for _, fm := range res.Outputs {
		_ = db.opts.FS.Remove(filepath.Join(db.dir, sstable.Name(fm.Num)))
	}
	if res.Ckpt.FileNum != 0 {
		_ = db.opts.FS.Remove(filepath.Join(db.dir, ckptName(res.Ckpt.FileNum)))
	}
	for _, num := range res.Archives {
		_ = db.opts.FS.Remove(archive.Path(db.archiveDir(), num))
	}
	_ = db.opts.FS.SyncDir(db.dir)
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
