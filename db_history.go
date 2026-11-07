package semlsm

import (
	"fmt"
	"sort"

	"github.com/itsramhere/semlsm/internal/base"
	"github.com/itsramhere/semlsm/internal/memtable"
	"github.com/itsramhere/semlsm/internal/resolve"
)

// RawVersion is one raw version in a History listing (B5).
type RawVersion struct {
	Ts    uint64
	Kind  base.Kind
	Value []byte
}

// HistoryItem is one History entry: exactly one of Raw and Checkpoint is set.
type HistoryItem struct {
	Raw        *RawVersion
	Checkpoint *CheckpointInfo
}

// History lists the key's versions with ts in [fromTs, toTs], oldest first
// (P11.2): raw versions from the memtables and all levels, plus one item per
// tuple whose run for this key overlaps the range. Raw versions and summary
// runs never overlap (I7).
func (db *DB) History(key []byte, fromTs, toTs uint64) ([]HistoryItem, error) {
	rs, _ := db.acquireReadState()
	if rs == nil {
		return nil, ErrClosed
	}
	defer releaseReadState(rs)

	type ordered struct {
		ts uint64
		it HistoryItem
	}
	var items []ordered
	rawSeen := map[uint64]bool{}

	// Raw versions: memtable, immutable memtables, then every level. The
	// internal-key order is (key, ts desc), so collect and sort at the end.
	addRaw := func(mt *memtable.Memtable) {
		if mt == nil {
			return
		}
		it := mt.NewIterator()
		defer it.Close()
		it.SeekGE(base.SeekKey(key, ^uint64(0)))
		for it.Valid() {
			u, t, k, ok := base.SplitInternalKey(it.Key())
			if !ok || string(u) != string(key) {
				return
			}
			if t < fromTs {
				return // ts descending: older versions only follow
			}
			if t <= toTs && !rawSeen[t] {
				rawSeen[t] = true
				items = append(items, ordered{t, HistoryItem{Raw: &RawVersion{
					Ts:    t,
					Kind:  k,
					Value: copyBytes(it.Value()),
				}}})
			}
			it.Next()
		}
	}
	addRaw(rs.mem)
	for _, im := range rs.imms {
		addRaw(im.mem)
	}
	for l := range rs.version.Levels {
		for _, f := range rs.version.Levels[l] {
			if !fileMayContain(f, key) {
				continue
			}
			r, rel, err := db.tableCache.Get(f.Num)
			if err != nil {
				return nil, fmt.Errorf("semlsm: history open sst %d: %w", f.Num, err)
			}
			it := r.NewIterator()
			it.SeekGE(base.SeekKey(key, ^uint64(0)))
			for it.Valid() {
				u, t, k, ok := base.SplitInternalKey(it.Key())
				if !ok || string(u) != string(key) {
					break
				}
				if t < fromTs {
					break
				}
				if t <= toTs && !rawSeen[t] {
					items = append(items, ordered{t, HistoryItem{Raw: &RawVersion{
						Ts:    t,
						Kind:  k,
						Value: copyBytes(it.Value()),
					}}})
				}
				it.Next()
			}
			if e := it.Err(); e != nil {
				it.Close()
				rel()
				return nil, fmt.Errorf("semlsm: history sst %d: %w", f.Num, e)
			}
			it.Close()
			rel()
		}
	}

	// Tuples: every tuple of the key's group whose run overlaps the range.
	group := string(base.GroupOf(key, db.opts.GroupDepth, db.opts.GroupDelimiter))
	for _, loc := range rs.version.Ckpts.GroupRefs(group) {
		ref := loc.Ref
		if ref.TsFrom > toTs {
			break // sorted by TsFrom
		}
		r, rel, err := db.ckptCache.Get(loc.FileNum)
		if err != nil {
			return nil, fmt.Errorf("semlsm: history open ckpt %d: %w", loc.FileNum, err)
		}
		ke, ok, err := r.LookupKey(ref.ID, key)
		if err != nil {
			rel()
			return nil, fmt.Errorf("semlsm: history ckpt %d tuple %d: %w", loc.FileNum, ref.ID, err)
		}
		if !ok || ke.MaxTs < fromTs || ke.MinTs > toTs {
			rel()
			continue
		}
		entry := resolve.Entry{Found: true, MinTs: ke.MinTs, MaxTs: ke.MaxTs, Count: ke.Count, LastKind: ke.LastKind}
		info, err := buildCheckpointInfo(r, ref, entry)
		rel()
		if err != nil {
			return nil, err
		}
		items = append(items, ordered{ke.MinTs, HistoryItem{Checkpoint: info}})
	}

	sort.SliceStable(items, func(a, b int) bool { return items[a].ts < items[b].ts })
	out := make([]HistoryItem, len(items))
	for i := range items {
		out[i] = items[i].it
	}
	return out, nil
}

// Checkpoints lists the group's tuples whose [TsFrom, TsTo] overlaps
// [fromTs, toTs], oldest first (B5).
func (db *DB) Checkpoints(group string, fromTs, toTs uint64) ([]CheckpointInfo, error) {
	rs, _ := db.acquireReadState()
	if rs == nil {
		return nil, ErrClosed
	}
	defer releaseReadState(rs)

	var out []CheckpointInfo
	for _, loc := range rs.version.Ckpts.GroupRefs(group) {
		ref := loc.Ref
		if ref.TsFrom > toTs {
			break // sorted by TsFrom
		}
		if ref.TsTo < fromTs {
			continue
		}
		r, rel, err := db.ckptCache.Get(loc.FileNum)
		if err != nil {
			return nil, fmt.Errorf("semlsm: checkpoints open ckpt %d: %w", loc.FileNum, err)
		}
		info, err := buildCheckpointInfo(r, ref, resolve.Entry{})
		rel()
		if err != nil {
			return nil, err
		}
		out = append(out, *info)
	}
	return out, nil
}
