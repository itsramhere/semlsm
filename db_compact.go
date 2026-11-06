package semlsm

import (
	"context"
	"fmt"
	"path/filepath"
	"runtime"
	"time"

	"semlsm/internal/base"
	"semlsm/internal/compaction"
	"semlsm/internal/failpoint"
	"semlsm/internal/manifest"
	"semlsm/internal/merge"
	"semlsm/internal/sstable"
	"semlsm/internal/version"
)

// Failpoint names for structural compaction (P6.2).
var (
	fpCompactBeforeManifest = failpoint.Define("compact.before_manifest")
	fpCompactAfterManifest  = failpoint.Define("compact.after_manifest")
)

// startCompactionWorkers builds the picker and launches the fast worker and
// (when no Summarizer is configured) the deep raw worker (P6.3).
func (db *DB) startCompactionWorkers() {
	if db.opts.DisableAutoCompaction {
		return
	}
	db.picker = compaction.NewPicker(
		db.opts.L0CompactTrigger, db.opts.L1TargetBytes,
		db.opts.LevelMultiplier, db.opts.TargetFileBytes)
	db.compactWG.Add(1)
	go db.fastCompactLoop()
	if db.opts.Summarizer == nil {
		// P6: until semantic compaction exists, deep levels move raw when
		// their score exceeds 1 (P10 replaces this with real semantic work).
		db.compactWG.Add(1)
		go db.deepRawLoop()
	}
}

// fastCompactLoop drives L0->L1 and L1->L2 (one worker; never waits on deep).
func (db *DB) fastCompactLoop() {
	defer db.compactWG.Done()
	for {
		if db.closed.Load() {
			return
		}
		db.drainObsolete()
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
			db.logger.Warn("semlsm: fast compaction failed", "err", err)
			time.Sleep(10 * time.Millisecond) // back off, then retry
		}
	}
}

// deepRawLoop moves L2+ data down raw when scores exceed 1 (P6.3 interim).
func (db *DB) deepRawLoop() {
	defer db.compactWG.Done()
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
			db.logger.Warn("semlsm: deep compaction failed", "err", err)
			time.Sleep(10 * time.Millisecond)
		}
	}
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
	if dbgLogCompaction != nil {
		buf := make([]byte, 2000)
		n := runtime.Stack(buf, false)
		dbgLogCompaction(fmt.Sprintf("runStructural L%d->L%d inputs=%d\n%s",
			plan.Level, plan.OutputLevel, len(plan.Inputs), buf[:n]))
	}
	v := db.vs.Acquire()
	defer v.Unref()

	files := plan.AllFiles()
	db.mu.Lock()
	for _, f := range files {
		if db.busy[f.Num] {
			db.mu.Unlock()
			return fmt.Errorf("semlsm: file %d became busy", f.Num)
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
		db.drainObsolete()
		time.Sleep(2 * time.Millisecond)
	}
}

// CompactLevel runs one compaction from level synchronously (manual trigger
// for tests and the CLI). The bottom level is a no-op until P10.
func (db *DB) CompactLevel(ctx context.Context, level int) error {
	if db.closed.Load() {
		return ErrClosed
	}
	if level < 0 || level >= base.NumLevels {
		return fmt.Errorf("semlsm: bad level %d", level)
	}
	if level == base.NumLevels-1 {
		return nil // bottom-level sweep arrives with P10
	}
	v := db.vs.Acquire()
	defer v.Unref()
	db.mu.Lock()
	busy := db.busySnapshotLocked()
	db.mu.Unlock()
	plan := db.picker.PickLevel(v, level, busy)
	if plan == nil {
		return nil // nothing to compact
	}
	return db.runStructural(plan)
}

var (
	_ = sstable.OpenReader
	_ = base.NumLevels
)

// drainObsolete moves files whose versions died into the deleter queue.
// Files die only when their last referencing Version unrefs, so a snapshot
// keeps them alive (P6.4, M-014 item 4).
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
		db.obsolete.Enqueue(version.ObsoleteFile{
			Path:   filepath.Join(db.dir, fmt.Sprintf("%06d%s", d.Num, suffix)),
			Num:    d.Num,
			IsCkpt: isCkpt,
		})
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

// dbgLogCompaction is a temporary test hook for compaction tracing.
var dbgLogCompaction func(msg string)
