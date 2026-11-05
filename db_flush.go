package semlsm

import (
	"fmt"
	"path/filepath"

	"semlsm/internal/base"
	"semlsm/internal/failpoint"
	"semlsm/internal/manifest"
	"semlsm/internal/sstable"
	"time"

	"semlsm/internal/vfs"
)

// flushLoop drains immutable memtables into L0 SSTs (P5.4). On a persistent
// error it backs off with growing sleeps (100ms doubling to 5s) instead of
// hot-looping (M-013 item 2).
func (db *DB) flushLoop() {
	defer db.flushWG.Done()
	backoff := 100 * time.Millisecond
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
				// flush failures must not fail writes (the data is safe in the
				// WAL): log, back off, and retry (P6.2, P13.3)
				db.logger.Warn("semlsm: flush failed", "err", err)
				select {
				case <-db.closeCh:
					return
				case <-time.After(backoff):
				}
				if backoff < 5*time.Second {
					backoff *= 2
				}
				continue
			}
			backoff = 100 * time.Millisecond
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
		db.finishFlush(item, sstNum, nil, true, logNumToSet, 0)
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
		_ = db.opts.FS.Remove(filepath.Join(db.dir, sstName(sstNum)))
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
	return db.finishFlush(item, sstNum, &meta, false, logNumToSet, maxTs)
}

// finishFlush appends the manifest edit, installs the new version, and
// removes the flushed memtable and its WAL (B7 order). logNumToSet (if > 0)
// advances log_num past WALs whose data is fully durable in the SST.
func (db *DB) finishFlush(item immEntry, sstNum uint64, meta *manifest.FileMeta, empty bool, logNumToSet uint64, itemMaxTs uint64) error {
	// manifest edit under manifestMu (M-013 item 1a: one stamped helper)
	db.manifestMu.Lock()
	e := &manifest.Edit{}
	if !empty {
		e.AddFiles = []manifest.FileMeta{*meta}
	}
	if logNumToSet > 0 {
		e.LogNum = logNumToSet
	}
	if err := db.appendManifestLocked(e, itemMaxTs); err != nil {
		db.manifestMu.Unlock()
		return fmt.Errorf("semlsm: flush manifest: %w", err)
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
