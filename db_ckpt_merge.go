package semlsm

import (
	"context"
	"fmt"
	"path/filepath"
	"sort"

	"github.com/itsramhere/semlsm/internal/checkpoint"
	"github.com/itsramhere/semlsm/internal/failpoint"
	"github.com/itsramhere/semlsm/internal/manifest"
)

var (
	fpCkptMergeAfterWrite    = failpoint.Define("ckptmerge.after_write")
	fpCkptMergeAfterManifest = failpoint.Define("ckptmerge.after_manifest")
)

// CompactCheckpoints merges up to CkptMergeThreshold oldest available files
// (at least two). A negative threshold disables only automatic merges. A
// concurrent manual call waits for the current merge, respecting ctx.
func (db *DB) CompactCheckpoints(ctx context.Context) error {
	_, err := db.compactCheckpoints(ctx, false)
	return err
}

func (db *DB) compactCheckpoints(ctx context.Context, background bool) (bool, error) {
	return db.rewriteCheckpoints(ctx, background, false)
}

// rewriteCheckpoints shares admission, busy reservations, publication, rollback,
// and version-death cleanup between merging and retention.
func (db *DB) rewriteCheckpoints(ctx context.Context, background, retention bool) (merged bool, err error) {
	now := db.opts.Clock.Now()
	for {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		db.mu.Lock()
		if db.closed.Load() || db.rs == nil {
			db.mu.Unlock()
			return false, ErrClosed
		}
		if !db.ckptMergeRunning {
			break
		}
		done := db.ckptMergeDone
		db.mu.Unlock()
		if background {
			return false, nil
		}
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-done:
			now = db.opts.Clock.Now()
		}
	}
	// mu is held. A version reference protects every input through I/O and
	// installation. Busy reservations prevent overlapping file jobs.
	v := db.rs.version
	if retention {
		if db.opts.ArchiveRetention == 0 && db.opts.SummaryRetention == 0 {
			db.mu.Unlock()
			return false, nil
		}
		if background && !db.lastRetention.IsZero() && now.Sub(db.lastRetention) < db.opts.RetentionInterval {
			db.mu.Unlock()
			return false, nil
		}
		db.lastRetention = now
	} else if background && (db.opts.CkptMergeThreshold < 0 || len(v.Ckpts.ByNum) <= db.opts.CkptMergeThreshold) {
		db.mu.Unlock()
		return false, nil
	}
	var nums []uint64
	for num := range v.Ckpts.ByNum {
		if !db.busy[num] {
			nums = append(nums, num)
		}
	}
	sort.Slice(nums, func(i, j int) bool { return nums[i] < nums[j] })
	k := max(2, db.opts.CkptMergeThreshold)
	if !retention && len(nums) > k {
		nums = nums[:k]
	}
	if len(nums) == 0 || (!retention && len(nums) < 2) {
		db.mu.Unlock()
		return false, nil
	}
	reserved := append([]uint64(nil), nums...)
	v.Ref()
	for _, num := range nums {
		db.busy[num] = true
	}
	db.ckptMergeRunning = true
	db.ckptMergeDone = make(chan struct{})
	db.ckptMergeWG.Add(1)
	db.compactionsInFlight++
	db.mu.Unlock()
	var outputNum uint64
	defer func() {
		v.Unref()
		db.mu.Lock()
		for _, num := range reserved {
			delete(db.busy, num)
		}
		delete(db.busy, outputNum)
		db.ckptMergeRunning = false
		close(db.ckptMergeDone)
		db.compactionsInFlight--
		db.stallCond.Broadcast()
		db.mu.Unlock()
		db.drainObsolete()
		db.ckptMergeWG.Done()
	}()
	if retention {
		var affected []uint64
		for _, num := range nums {
			if err := ctx.Err(); err != nil {
				return false, err
			}
			if db.closed.Load() {
				return false, ErrClosed
			}
			cf := v.Ckpts.ByNum[num]
			if !db.retentionCandidate(cf, now) {
				continue
			}
			r, err := checkpoint.OpenReader(db.opts.FS, filepath.Join(db.dir, ckptName(num)))
			if err != nil {
				return false, err
			}
			changed := false
			for _, ref := range cf.Tuples {
				if err := ctx.Err(); err != nil {
					r.Close()
					return false, err
				}
				if db.closed.Load() {
					r.Close()
					return false, ErrClosed
				}
				meta, err := r.ReadMeta(ref.ID)
				if err != nil {
					r.Close()
					return false, err
				}
				if _, due := db.retainedMeta(meta, now); due {
					changed = true
					break
				}
			}
			r.Close()
			if changed {
				affected = append(affected, num)
			}
		}
		nums = affected
		if len(nums) == 0 {
			return false, nil
		}
	}
	outputNum = db.allocFile()
	db.mu.Lock()
	db.busy[outputNum] = true
	db.mu.Unlock()
	path := filepath.Join(db.dir, ckptName(outputNum))
	keepOutput := false
	defer func() {
		if !keepOutput {
			_ = db.opts.FS.Remove(path)
			_ = db.opts.FS.SyncDir(db.dir)
		}
	}()
	w, err := checkpoint.NewWriter(db.opts.FS, path)
	if err != nil {
		return false, err
	}
	finished := false
	defer func() {
		if !finished {
			w.Abort()
		}
	}()
	cf := manifest.CkptFile{FileNum: outputNum, ArchiveNums: []uint64{}}
	archives, ids := map[uint64]bool{}, map[uint64]bool{}
	for _, num := range nums {
		input := v.Ckpts.ByNum[num]
		r, err := checkpoint.OpenReader(db.opts.FS, filepath.Join(db.dir, ckptName(num)))
		if err != nil {
			return false, err
		}
		copyErr := func() error {
			defer r.Close()
			for _, ref := range input.Tuples {
				if err := ctx.Err(); err != nil {
					return err
				}
				if db.closed.Load() {
					return ErrClosed
				}
				if ids[ref.ID] {
					return fmt.Errorf("semlsm: checkpoint merge duplicate tuple %d: %w", ref.ID, ErrCorruption)
				}
				ids[ref.ID] = true
				var meta checkpoint.TupleMeta
				var loc checkpoint.DirEntry
				var err error
				if retention {
					meta, err = r.ReadMeta(ref.ID)
					if err != nil {
						return err
					}
					if changed, due := db.retainedMeta(meta, now); due {
						meta = changed
						loc, err = w.RewriteTuple(r, ref.ID, meta, meta.RetiredUnixNano != 0)
					} else {
						meta, loc, err = w.CopyTuple(r, ref.ID)
					}
				} else {
					meta, loc, err = w.CopyTuple(r, ref.ID)
				}
				if err != nil {
					return err
				}
				if meta.Group != ref.Group || meta.TsFrom != ref.TsFrom || meta.TsTo != ref.TsTo {
					return fmt.Errorf("semlsm: checkpoint merge tuple %d disagrees with manifest: %w", ref.ID, ErrCorruption)
				}
				ref.Offset, ref.Length = loc.Offset, loc.Length
				cf.Tuples = append(cf.Tuples, ref)
				if meta.ArchiveFileNum != 0 {
					archives[meta.ArchiveFileNum] = true
				}
			}
			// Preserve the manifest union too, including known-empty sets.
			if !retention {
				for _, n := range input.ArchiveNums {
					archives[n] = true
				}
			}
			return nil
		}()
		if copyErr != nil {
			return false, copyErr
		}
	}
	for num := range archives {
		cf.ArchiveNums = append(cf.ArchiveNums, num)
	}
	sort.Slice(cf.ArchiveNums, func(i, j int) bool { return cf.ArchiveNums[i] < cf.ArchiveNums[j] })
	if err := w.Finish(); err != nil {
		return false, err
	}
	finished = true
	st, err := db.opts.FS.Stat(path)
	if err != nil {
		return false, fmt.Errorf("semlsm: checkpoint merge stat: %w", err)
	}
	cf.Size = st.Size
	if err := db.opts.FS.SyncDir(db.dir); err != nil {
		return false, err
	}
	afterWrite, afterManifest := fpCkptMergeAfterWrite, fpCkptMergeAfterManifest
	if retention {
		afterWrite, afterManifest = fpRetentionAfterWrite, fpRetentionAfterManifest
	}
	if err := failpoint.Hit(afterWrite); err != nil {
		return false, err
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	db.manifestMu.Lock()
	defer db.manifestMu.Unlock()
	db.mu.Lock()
	if db.closed.Load() {
		db.mu.Unlock()
		return false, ErrClosed
	}
	for _, num := range nums {
		// Each busy entry must still be this merge's reservation. Other jobs
		// cannot acquire it while true; outputs of semantic jobs stay busy
		// until those jobs finish, so they cannot be selected prematurely.
		if db.rs.version.Ckpts.ByNum[num] != v.Ckpts.ByNum[num] || !db.busy[num] {
			db.mu.Unlock()
			return false, nil
		}
	}
	db.mu.Unlock()
	e := &manifest.Edit{AddCkpts: []manifest.CkptFile{cf}, DelCkpts: nums}
	appendErr := db.appendManifestLocked(e, db.vs.CurrentLastTs())
	// A threshold roll can fail after Apply: never remove an output already
	// referenced by the durable edit. Install that version even on roll error.
	cur := db.vs.Acquire()
	committed := cur.Ckpts.ByNum[outputNum] != nil
	keepOutput = committed
	if !committed {
		cur.Unref()
		// Append or Sync may fail after leaving a complete frame readable.
		// Publish the unchanged state in a fresh manifest before removing its
		// candidate output, including under TornTail's keep-every-byte case.
		if rollbackErr := db.rewriteManifestLocked(); rollbackErr != nil {
			// Durability is uncertain: keeping the output is safer than
			// deleting data a surviving manifest frame might reference.
			keepOutput = true
			return false, fmt.Errorf("semlsm: checkpoint merge manifest: %w; restore snapshot: %v", appendErr, rollbackErr)
		}
		return false, fmt.Errorf("semlsm: checkpoint merge manifest: %w", appendErr)
	}
	db.mu.Lock()
	old := db.rs
	db.rs = &readState{mem: old.mem, imms: old.imms, version: cur}
	old.release()
	db.mu.Unlock()
	if retention {
		db.retentionPasses.Add(1)
	} else {
		db.ckptMerges.Add(1)
		db.ckptFilesMerged.Add(uint64(len(nums)))
	}
	if appendErr != nil {
		return true, appendErr
	}
	if err := failpoint.Hit(afterManifest); err != nil {
		return true, err
	}
	return true, nil
}
