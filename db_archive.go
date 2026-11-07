package semlsm

import (
	"fmt"
	"path/filepath"

	"semlsm/internal/archive"
	"semlsm/internal/base"
	"semlsm/internal/checkpoint"
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
	rs, _ := db.acquireReadState()
	if rs == nil {
		return Result{}, ErrClosed
	}
	defer releaseReadState(rs)

	group := string(base.GroupOf(key, db.opts.GroupDepth, db.opts.GroupDelimiter))
	var best resolve.Entry
	var bestNum uint64 // the covering tuple's archive segment (0 = none)
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
			best = resolve.Entry{Found: true, MinTs: ke.MinTs, MaxTs: ke.MaxTs, Count: ke.Count, LastKind: ke.LastKind}
			bestNum = tupleArchiveNum(r, ref.ID)
		}
		rel()
	}
	if !best.Found || bestNum == 0 {
		return Result{}, fmt.Errorf("%w: no covering archived segment for %q at %d", base.ErrNotArchived, key, ts)
	}
	// Past the run the archive is not authoritative: the true value at ts
	// comes from the normal resolution (raw versions above the dropped
	// range), so delegate to GetAt. GetArchived is always the exact value
	// at ts (M-030 item 1).
	if ts > best.MaxTs {
		return db.GetAt(key, ts)
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

// tupleArchiveNum reads one tuple's ArchiveFileNum from its meta.
func tupleArchiveNum(r *checkpoint.Reader, id uint64) uint64 {
	meta, err := r.ReadMeta(id)
	if err != nil {
		return 0
	}
	return meta.ArchiveFileNum
}
