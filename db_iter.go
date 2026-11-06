package semlsm

import (
	"fmt"

	"semlsm/internal/base"
	"semlsm/internal/memtable"
	"semlsm/internal/merge"
	"semlsm/internal/sstable"
	"semlsm/internal/version"
)

// IterOptions controls iteration range and summary collapsing (B5).
type IterOptions struct {
	Start             []byte // inclusive; nil = from the beginning
	End               []byte // exclusive; nil = to the end
	CollapseSummaries bool   // P11: emit each summary tuple once
}

// Iterator walks visible versions in key order, resolving per B6.
// Raw-only until P7.
type Iterator struct {
	merged   merge.Iter
	rs       *readState // owned reference, released on Close
	s        uint64
	start    []byte
	end      []byte
	current  Result
	pending  bool
	valid    bool
	err      error
	snap     *Snapshot // may be nil for db iterators
	releases []func()
}

// NewIterator returns an iterator with an implicit snapshot released on Close.
func (db *DB) NewIterator(o IterOptions) *Iterator {
	rs, visible := db.acquireReadState()
	if rs == nil {
		return &Iterator{err: ErrClosed}
	}
	return db.newIteratorAt(o, visible, rs, nil)
}

// newIteratorAt builds an iterator over a pinned read state.
func (db *DB) newIteratorAt(o IterOptions, s uint64, rs *readState, snap *Snapshot) *Iterator {
	it := &Iterator{rs: rs, s: s, start: o.Start, end: o.End, snap: snap}
	sources := make([]merge.Iter, 0, len(rs.imms)+base.NumLevels)
	sources = append(sources, rs.mem.NewIterator())
	for _, im := range rs.imms {
		sources = append(sources, im.mem.NewIterator())
	}
	for _, level := range rs.version.Levels {
		for _, f := range level {
			reader, rel, err := db.tableCache.Get(f.Num)
			if err != nil {
				// A table that cannot be opened must fail the iteration
				// (M-015 item 2): skipping it would silently omit keys.
				if it.err == nil {
					it.err = fmt.Errorf("semlsm: open sst %d: %w", f.Num, err)
				}
				continue
			}
			it.releases = append(it.releases, rel)
			sources = append(sources, reader.NewIterator())
		}
	}
	it.merged = merge.New(sources...)
	return it
}

// Next advances to the next visible entry; call before the first read.
func (it *Iterator) Next() bool {
	if it.err != nil {
		it.valid = false
		return false
	}
	if it.merged == nil {
		it.valid = false
		return false
	}
	if !it.pending {
		if !it.valid {
			// first positioning
			it.seekToStart()
		} else {
			it.skipRestOfKey()
		}
	} else {
		// the previously emitted entry is consumed
		it.pending = false
		it.skipRestOfKey()
	}
	return it.advanceToNextVisible()
}

// seekToStart positions the merged iterator at Start or the first entry.
func (it *Iterator) seekToStart() {
	if len(it.start) > 0 {
		it.merged.SeekGE(base.SeekKey(it.start, ^uint64(0)))
	} else {
		it.merged.First()
	}
}

// skipRestOfKey moves past every remaining version of the current user key.
func (it *Iterator) skipRestOfKey() {
	if !it.merged.Valid() {
		return
	}
	cur := copyBytes(base.UserKey(it.merged.Key()))
	for it.merged.Valid() && base.CompareUser(base.UserKey(it.merged.Key()), cur) == 0 {
		it.merged.Next()
	}
}

// advanceToNextVisible finds the next key with a visible non-tombstone
// version and stores it in current (P5.6 raw-only).
func (it *Iterator) advanceToNextVisible() bool {
	for it.merged.Valid() {
		user, ts, kind, value, ok := it.visibleVersion()
		if !ok {
			if e := it.merged.Err(); e != nil {
				it.err = e
				it.valid = false
				return false
			}
			// no visible version <= s for this key: skip the whole key
			it.skipRestOfKey()
			continue
		}
		if it.end != nil && base.CompareUser(user, it.end) >= 0 {
			it.valid = false
			return false
		}
		if kind == base.KindDelete {
			// tombstone visible: skip the key
			it.skipRestOfKey()
			continue
		}
		it.current = Result{
			Kind:      ResultExact,
			Key:       copyBytes(user),
			Value:     copyBytes(value),
			VersionTs: ts,
		}
		it.valid = true
		it.pending = true
		return true
	}
	it.valid = false
	if e := it.merged.Err(); e != nil {
		it.err = e
	}
	return false
}

// visibleVersion walks the current key's versions, skipping ts > s, and
// returns the newest visible version (B6 raw part).
func (it *Iterator) visibleVersion() (user []byte, ts uint64, kind base.Kind, value []byte, ok bool) {
	for it.merged.Valid() {
		ikey := it.merged.Key()
		u, t, k, valid := base.SplitInternalKey(ikey)
		if !valid {
			it.merged.Next()
			continue
		}
		if t > it.s {
			// newer than the read timestamp: skip
			it.merged.Next()
			continue
		}
		return u, t, k, it.merged.Value(), true
	}
	return nil, 0, 0, nil, false
}

// Result returns the current entry.
func (it *Iterator) Result() Result { return it.current }

// Err returns any iteration error.
func (it *Iterator) Err() error { return it.err }

// Close releases the iterator's pinned read state and cached readers.
func (it *Iterator) Close() error {
	for _, rel := range it.releases {
		rel()
	}
	it.releases = nil
	if it.merged != nil {
		_ = it.merged.Close()
		it.merged = nil
	}
	if it.rs != nil {
		releaseReadState(it.rs)
		it.rs = nil
	}
	return nil
}

var (
	_ = memtable.New
	_ = sstable.OpenReader
	_ = version.FileMeta{}
)
