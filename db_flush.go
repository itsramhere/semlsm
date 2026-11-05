package semlsm

import (
	"fmt"
	"path/filepath"

	"semlsm/internal/base"
	"semlsm/internal/failpoint"
	"semlsm/internal/manifest"
	"semlsm/internal/sstable"
	"semlsm/internal/vfs"
)

// flushLoop drains immutable memtables into L0 SSTs (P5.4).
func (db *DB) flushLoop() {
	defer db.flushWG.Done()
	for {
		select {
		case <-db.flushWake:
		case <-db.closeCh:
			// drain any remaining immutables before exiting
		}
		for {
			db.mu.Lock()
			if db.rs == nil || len(db.rs.imms) == 0 {
				db.mu.Unlock()
				break
			}
			// oldest immutable is at the end (newest first)
			item := db.rs.imms[len(db.rs.imms)-1]
			db.mu.Unlock()
			// The edit may only declare this item's WAL obsolete: other immutables
			// may hold unflushed data. Advancing log_num to the active WAL here
			// would lose them (I1).
			if err := db.flushOne(item, item.walNum+1); err != nil {
				db.setFailed(err)
			}
		}
		select {
		case <-db.closeCh:
			return
		default:
		}
	}
}

// flushOne writes one immutable memtable to an L0 SST and installs it (B7).
func (db *DB) flushOne(item immEntry, logNumToSet uint64) error {
	sstNum := db.allocFile()
	w, err := sstable.NewWriter(db.opts.FS, filepath.Join(db.dir, sstName(sstNum)))
	if err != nil {
		return fmt.Errorf("semlsm: flush create sst %d: %w", sstNum, err)
	}
	var count uint64
	var minTs, maxTs uint64
	var smallest, largest []byte
	it := item.mem.NewIterator()
	for it.First(); it.Valid(); it.Next() {
		ikey := it.Key()
		user, ts, kind, ok := base.SplitInternalKey(ikey)
		if !ok {
			w.Abort()
			return fmt.Errorf("semlsm: flush sst %d: bad internal key", sstNum)
		}
		val := it.Value()
		if err := w.Add(ikey, val); err != nil {
			w.Abort()
			return fmt.Errorf("semlsm: flush sst %d: %w", sstNum, err)
		}
		count++
		if count == 1 || ts < minTs {
			minTs = ts
		}
		if ts > maxTs {
			maxTs = ts
		}
		if smallest == nil {
			smallest = append([]byte(nil), ikey...)
		}
		largest = append(largest[:0], ikey...)
		_ = user
		_ = kind
	}
	if count == 0 {
		// empty memtable: nothing to write; drop the file and install anyway
		w.Abort()
		db.finishFlush(item, sstNum, nil, true, logNumToSet)
		return nil
	}
	if err := w.Finish(); err != nil {
		w.Abort()
		return fmt.Errorf("semlsm: flush sst %d: %w", sstNum, err)
	}
	if err := db.opts.FS.SyncDir(db.dir); err != nil {
		return fmt.Errorf("semlsm: flush syncdir: %w", err)
	}
	if err := failpoint.Hit(fpFlushAfterSSTSync); err != nil {
		return err
	}

	meta := manifest.FileMeta{
		Level:    0,
		Num:      sstNum,
		Size:     fileSizeOnDisk(db.opts.FS, filepath.Join(db.dir, sstName(sstNum))),
		Smallest: smallest,
		Largest:  append([]byte(nil), largest...),
		MinTs:    minTs,
		MaxTs:    maxTs,
		Count:    count,
	}
	return db.finishFlush(item, sstNum, &meta, false, logNumToSet)
}

// finishFlush appends the manifest edit, installs the new version, and
// removes the flushed memtable and its WAL (B7 order). logNumToSet (if > 0)
// advances log_num past WALs whose data is fully durable in the SST.
func (db *DB) finishFlush(item immEntry, sstNum uint64, meta *manifest.FileMeta, empty bool, logNumToSet uint64) error {
	// manifest edit under manifestMu
	db.manifestMu.Lock()
	e := &manifest.Edit{}
	if !empty {
		e.AddFiles = []manifest.FileMeta{*meta}
	}
	if logNumToSet > 0 {
		e.LogNum = logNumToSet
	}
	if err := db.manifest.Append(e); err != nil {
		db.manifestMu.Unlock()
		return fmt.Errorf("semlsm: flush manifest: %w", err)
	}
	if err := db.manifest.Sync(); err != nil {
		db.manifestMu.Unlock()
		return fmt.Errorf("semlsm: flush manifest sync: %w", err)
	}
	if _, aerr := db.vs.Apply(e); aerr != nil {
		db.manifestMu.Unlock()
		return fmt.Errorf("semlsm: flush apply: %w", aerr)
	}
	db.manifestMu.Unlock()

	if err := failpoint.Hit(fpFlushAfterManifest); err != nil {
		return err
	}

	// install under mu: remove the memtable, swap the version (B7: the
	// readState is immutable once built, so build a fresh one)
	db.mu.Lock()
	newVersion := db.vs.Acquire()
	if db.rs != nil {
		kept := make([]immEntry, 0, len(db.rs.imms))
		for _, im := range db.rs.imms {
			if im.mem != item.mem {
				kept = append(kept, im)
			}
		}
		old := db.rs.version
		db.rs = &readState{mem: db.mem, imms: kept, version: newVersion}
		old.Unref()
		db.stallCond.Broadcast()
	}
	db.mu.Unlock()

	// close and delete the old WAL (best effort; I6)
	if item.wal != nil {
		_ = item.wal.Close()
	}
	if item.walNum > 0 {
		if err := db.opts.FS.Remove(walPath(db.dir, item.walNum)); err == nil {
			_ = db.opts.FS.SyncDir(db.dir)
		}
	}
	return nil
}

// allocFile allocates the next file number.
func (db *DB) allocFile() uint64 {
	db.manifestMu.Lock()
	defer db.manifestMu.Unlock()
	n := db.allocateFileNum()
	db.nextFileNumSnapshot()
	return n
}

// fileSizeOnDisk stats a file through the DB's FS.
func fileSizeOnDisk(fs vfs.FS, path string) int64 {
	st, err := fs.Stat(path)
	if err != nil {
		return 0
	}
	return st.Size
}
