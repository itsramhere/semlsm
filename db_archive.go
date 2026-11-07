package semlsm

import (
	"fmt"
	"path/filepath"

	"semlsm/internal/archive"
	"semlsm/internal/base"
	"semlsm/internal/resolve"
)

// archiveDir returns the archive root: Options.ArchiveDir when set, else
// <dbdir>/archive (P12.1).
func (db *DB) archiveDir() string {
	if db.opts.ArchiveDir != "" {
		return db.opts.ArchiveDir
	}
	return filepath.Join(db.dir, "archive")
}

// GetArchived returns the exact raw version of key at ts out of the cold
// archive tier (P12.2): the covering tuple is found through the checkpoint
// index, then the archive segment is read for the newest version <= ts in
// the run. ErrNotArchived when no covering tuple exists, when the tuple was
// written without ArchiveRaw, or when the archive file is gone.
func (db *DB) GetArchived(key []byte, ts uint64) (Result, error) {
	if err := validateKeyForRead(key); err != nil {
		return Result{}, err
	}
	rs, visible := db.acquireReadState()
	if rs == nil {
		return Result{}, ErrClosed
	}
	defer releaseReadState(rs)
	return db.getArchivedAtState(key, ts, visible, rs)
}

// GetArchived reads an archived version through the snapshot's pinned state.
func (s *Snapshot) GetArchived(key []byte, ts uint64) (Result, error) {
	if s.released.Load() {
		return Result{}, ErrReleased
	}
	if err := validateKeyForRead(key); err != nil {
		return Result{}, err
	}
	if ts > s.ts {
		return Result{}, ErrFutureTimestamp
	}
	return s.db.getArchivedAtState(key, ts, s.ts, s.rs)
}

func (db *DB) getArchivedAtState(key []byte, ts, visible uint64, rs *readState) (Result, error) {

	group := string(base.GroupOf(key, db.opts.GroupDepth, db.opts.GroupDelimiter))
	var best resolve.Entry
	var bestNum uint64 // the covering tuple's archive segment (0 = none)
	var retired bool
	for _, loc := range rs.version.Ckpts.GroupRefs(group) {
		ref := loc.Ref
		if ref.TsFrom > ts {
			break // sorted by TsFrom
		}
		r, rel, err := db.ckptCache.Get(loc.FileNum)
		if err != nil {
			return Result{}, fmt.Errorf("semlsm: getarchived open ckpt %d: %w", loc.FileNum, err)
		}
		ke, ok, err := r.LookupKey(ref.ID, key)
		if err != nil {
			rel()
			return Result{}, fmt.Errorf("semlsm: getarchived ckpt %d tuple %d: %w", loc.FileNum, ref.ID, err)
		}
		if !ok || ke.MinTs > ts {
			rel()
			continue
		}
		if !best.Found || ke.MinTs >= best.MinTs {
			meta, err := r.ReadMeta(ref.ID)
			if err != nil {
				rel()
				return Result{}, err
			}
			best = resolve.Entry{Found: true, MinTs: ke.MinTs, MaxTs: ke.MaxTs, Count: ke.Count, LastKind: ke.LastKind}
			bestNum = meta.ArchiveFileNum
			retired = meta.RetiredUnixNano != 0 || meta.ArchiveRetiredUnixNano != 0
		}
		rel()
	}
	if !best.Found {
		return Result{}, fmt.Errorf("%w: no covering archived segment for %q at %d", base.ErrNotArchived, key, ts)
	}
	// Past the run the archive is not authoritative: the true value at ts
	// comes from the normal resolution (raw versions above the dropped
	// range), so delegate to GetAt. GetArchived is always the exact value
	// at ts (M-030 item 1).
	if ts > best.MaxTs {
		if ts > visible {
			return Result{}, ErrFutureTimestamp
		}
		return db.getAtState(key, ts, rs)
	}
	if retired {
		return Result{}, fmt.Errorf("%w: %q at %d", ErrArchiveRetired, key, ts)
	}
	if bestNum == 0 {
		return Result{}, fmt.Errorf("%w: no archived segment for %q at %d", ErrNotArchived, key, ts)
	}

	r, err := archive.Open(db.opts.FS, archive.Path(db.archiveDir(), bestNum))
	if err != nil {
		// A missing or unreadable segment never harms normal reads (P12.2).
		return Result{}, fmt.Errorf("%w: open archive %d: %v", base.ErrNotArchived, bestNum, err)
	}
	defer r.Close()
	rec, ok, err := archive.Lookup(r, key, ts)
	if err != nil {
		return Result{}, fmt.Errorf("semlsm: getarchived archive %d: %w", bestNum, err)
	}
	if !ok {
		return Result{}, fmt.Errorf("%w: %q at %d not in segment %d", base.ErrNotArchived, key, ts, bestNum)
	}
	if rec.Kind == base.KindDelete {
		// A dropped tombstone: the key is absent at that ts (M-030 item 2).
		return Result{Kind: ResultNotFound, Key: copyBytes(key), VersionTs: rec.Ts}, nil
	}
	return Result{
		Kind:      ResultExact,
		Key:       copyBytes(key),
		Value:     copyBytes(rec.Value),
		VersionTs: rec.Ts,
	}, nil
}
