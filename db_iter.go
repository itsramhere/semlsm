package semlsm

import (
	"fmt"
	"sort"

	"github.com/itsramhere/semlsm/internal/base"
	"github.com/itsramhere/semlsm/internal/checkpoint"
	"github.com/itsramhere/semlsm/internal/manifest"
	"github.com/itsramhere/semlsm/internal/memtable"
	"github.com/itsramhere/semlsm/internal/merge"
	"github.com/itsramhere/semlsm/internal/resolve"
	"github.com/itsramhere/semlsm/internal/sstable"
	"github.com/itsramhere/semlsm/internal/version"
)

// IterOptions controls iteration range and summary collapsing (B5).
type IterOptions struct {
	Start             []byte // inclusive; nil = from the beginning
	End               []byte // exclusive; nil = to the end
	CollapseSummaries bool   // P11.1: emit each summary tuple once
}

// iterEntry is one tuple key entry relevant to the iterator's range.
type iterEntry struct {
	key  string
	e    resolve.Entry
	file uint64 // ckpt file number
	id   uint64 // tuple id
}

// Iterator walks visible versions in key order, resolving per B6: the newest
// raw version <= S and the covering checkpoint entry go through resolve.Resolve
// (P11.1). A key may exist only in checkpoints. With CollapseSummaries each
// tuple is emitted once, at its first key in order.
type Iterator struct {
	merged     merge.Iter // raw stream
	entries    []iterEntry
	ei         int
	rs         *readState // owned reference, released on Close
	s          uint64
	start      []byte
	end        []byte
	collapse   bool
	groupDepth int
	groupDelim byte
	ckptOpen   func(num uint64) (*checkpoint.Reader, func(), error)
	current    Result
	valid      bool
	err        error
	snap       *Snapshot // may be nil for db iterators
	releases   []func()
	// collapsedTuple is the tuple being skipped after a collapsed summary
	// emission; 0 when not collapsing.
	collapsedTuple uint64
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
	it := &Iterator{
		rs: rs, s: s, start: o.Start, end: o.End,
		collapse:   o.CollapseSummaries,
		groupDepth: db.opts.GroupDepth, groupDelim: db.opts.GroupDelimiter,
		ckptOpen: db.ckptCache.Get,
		snap:     snap,
	}
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
	it.collectEntries()
	if len(it.start) > 0 {
		it.merged.SeekGE(base.SeekKey(it.start, ^uint64(0)))
	} else {
		it.merged.First()
	}
	return it
}

// collectEntries gathers the tuple key entries whose group overlaps
// [Start, End), sorted by (key, MinTs). Tuples are found by group prefix
// comparison (P11.1): the groups of the range endpoints bound every group
// that can contribute keys inside the range.
func (it *Iterator) collectEntries() {
	lo, hi := "", ""
	if len(it.start) > 0 {
		lo = string(base.GroupOf(it.start, it.groupDepth, it.groupDelim))
	}
	if len(it.end) > 0 {
		hi = string(base.GroupOf(it.end, it.groupDepth, it.groupDelim))
	} else {
		hi = "\xff\xff\xff\xff" // above every group
	}
	for num, cf := range it.rs.version.Ckpts.ByNum {
		r, rel, err := it.ckptOpen(num)
		if err != nil {
			if it.err == nil {
				it.err = fmt.Errorf("semlsm: open ckpt %d: %w", num, err)
			}
			continue
		}
		for _, ref := range cf.Tuples {
			if ref.Group < lo || ref.Group > hi {
				continue
			}
			keys, err := r.IterKeys(ref.ID)
			if err != nil {
				if it.err == nil {
					it.err = fmt.Errorf("semlsm: ckpt %d tuple %d index: %w", num, ref.ID, err)
				}
				continue
			}
			meta, err := r.ReadMeta(ref.ID)
			if err != nil {
				if it.err == nil {
					it.err = err
				}
				continue
			}
			for _, ke := range keys {
				if len(it.start) > 0 && base.CompareUser(ke.Key, it.start) < 0 {
					continue
				}
				if len(it.end) > 0 && base.CompareUser(ke.Key, it.end) >= 0 {
					continue
				}
				it.entries = append(it.entries, iterEntry{
					key:  string(ke.Key),
					e:    resolve.Entry{Found: true, MinTs: ke.MinTs, MaxTs: ke.MaxTs, Count: ke.Count, LastKind: ke.LastKind, Retired: meta.RetiredUnixNano != 0},
					file: num,
					id:   ref.ID,
				})
			}
		}
		rel()
	}
	sort.SliceStable(it.entries, func(a, b int) bool {
		if it.entries[a].key != it.entries[b].key {
			return it.entries[a].key < it.entries[b].key
		}
		return it.entries[a].e.MinTs < it.entries[b].e.MinTs
	})
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
	return it.nextKey()
}

// nextKey walks to the next emit-worthy key: the lower of the raw stream's
// current key and the entry stream's current key, resolved per B6.
func (it *Iterator) nextKey() bool {
	for {
		if it.err != nil {
			it.valid = false
			return false
		}
		hasRaw := it.merged.Valid()
		var rawKey string
		if hasRaw {
			rawKey = string(base.UserKey(it.merged.Key()))
		}
		hasEntry := it.ei < len(it.entries)
		var entryKey string
		if hasEntry {
			entryKey = it.entries[it.ei].key
		}
		if !hasRaw && !hasEntry {
			it.valid = false
			if e := it.merged.Err(); e != nil {
				it.err = e
			}
			return false
		}
		key := rawKey
		if !hasRaw || (hasEntry && entryKey < rawKey) {
			key = entryKey
		}
		if len(it.end) > 0 && base.CompareUser([]byte(key), it.end) >= 0 {
			it.valid = false
			return false
		}

		// Collapsed summaries: skip the remaining keys of the tuple just
		// emitted (P11.1). A key whose resolution uses the tuple AND ends in
		// a summary is skipped; one with a newer raw version is emitted raw.
		if it.collapsedTuple != 0 {
			if best, ok := it.bestEntry(key); ok && best.id == it.collapsedTuple {
				r := resolve.Resolve(it.s, it.rawAt(key), best.e)
				if r == resolve.UseSummary || r == resolve.NotFound || r == resolve.UseExpired {
					it.skipKeyRaw(key)
					it.skipKeyEntries(key)
					continue
				}
			}
			it.collapsedTuple = 0
		}

		// Consume both streams for this key.
		raw, rawVal := it.rawAtValue(key)
		it.skipKeyRaw(key)
		best, hasBest := it.bestEntry(key)
		it.skipKeyEntries(key)

		switch resolve.Resolve(it.s, raw, bestOrNotFound(hasBest, best)) {
		case resolve.NotFound:
			continue
		case resolve.UseRaw:
			if raw.Kind == base.KindDelete {
				continue // tombstone visible: the key is absent at s
			}
			it.current = Result{
				Kind:      ResultExact,
				Key:       []byte(key),
				Value:     rawVal,
				VersionTs: raw.Ts,
			}
			it.valid = true
			return true
		case resolve.UseSummary, resolve.UseExpired:
			if !hasBest {
				it.err = fmt.Errorf("semlsm: UseSummary without an entry for key %q", key)
				it.valid = false
				return false
			}
			info, err := it.checkpointInfo(key, best)
			if err != nil {
				it.err = err
				it.valid = false
				return false
			}
			it.current = Result{
				Kind:       ResultSummarized,
				Key:        []byte(key),
				Checkpoint: info,
			}
			if info.RetiredUnixNano != 0 {
				it.current.Kind = ResultExpired
			}
			if it.collapse {
				it.collapsedTuple = best.id
			}
			it.valid = true
			return true
		default: // resolve.Invariant
			it.err = fmt.Errorf("semlsm: I7 violated for key %q at ts %d: %w", key, it.s, base.ErrCorruption)
			it.valid = false
			return false
		}
	}
}

func bestOrNotFound(hasBest bool, best iterEntry) resolve.Entry {
	if !hasBest {
		return resolve.Entry{}
	}
	return best.e
}

// rawAt returns the newest raw version of key at s without consuming the
// stream (the merged head is already at key).
func (it *Iterator) rawAt(key string) resolve.Raw {
	for it.merged.Valid() {
		u, t, k, ok := base.SplitInternalKey(it.merged.Key())
		if !ok || string(u) != key {
			return resolve.Raw{}
		}
		if t <= it.s {
			return resolve.Raw{Found: true, Ts: t, Kind: k}
		}
		it.merged.Next()
	}
	return resolve.Raw{}
}

// rawAtValue is rawAt plus the value copy for the winning version.
func (it *Iterator) rawAtValue(key string) (resolve.Raw, []byte) {
	for it.merged.Valid() {
		u, t, k, ok := base.SplitInternalKey(it.merged.Key())
		if !ok || string(u) != key {
			return resolve.Raw{}, nil
		}
		if t <= it.s {
			return resolve.Raw{Found: true, Ts: t, Kind: k}, copyBytes(it.merged.Value())
		}
		it.merged.Next()
	}
	return resolve.Raw{}, nil
}

// skipKeyRaw consumes every remaining version of key in the raw stream.
func (it *Iterator) skipKeyRaw(key string) {
	for it.merged.Valid() && string(base.UserKey(it.merged.Key())) == key {
		it.merged.Next()
	}
}

// bestEntry returns the key's entry with the largest MinTs <= s (B6's entry
// rule) without consuming the entry stream.
func (it *Iterator) bestEntry(key string) (iterEntry, bool) {
	var best iterEntry
	found := false
	for i := it.ei; i < len(it.entries) && it.entries[i].key == key; i++ {
		if it.entries[i].e.MinTs <= it.s && (!found || it.entries[i].e.MinTs >= best.e.MinTs) {
			best = it.entries[i]
			found = true
		}
	}
	return best, found
}

// skipKeyEntries consumes every entry of key.
func (it *Iterator) skipKeyEntries(key string) {
	for it.ei < len(it.entries) && it.entries[it.ei].key == key {
		it.ei++
	}
}

// checkpointInfo builds the CheckpointInfo for a summarized emission.
func (it *Iterator) checkpointInfo(key string, e iterEntry) (*CheckpointInfo, error) {
	r, rel, err := it.ckptOpen(e.file)
	if err != nil {
		return nil, fmt.Errorf("semlsm: open ckpt %d: %w", e.file, err)
	}
	defer rel()
	return buildCheckpointInfo(r, manifest.TupleRef{ID: e.id}, e.e)
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
