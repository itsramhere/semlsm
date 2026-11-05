package sstable

import (
	"container/list"
	"sync"
)

// TableCache is an LRU of open Readers keyed by file number, ref-counted.
// Eviction closes a reader only when its reference count reaches zero, and
// files stay open while iterators hold references (I6).
type TableCache struct {
	mu       sync.Mutex
	capacity int
	m        map[uint64]*tableEntry
	lru      *list.List // front = most recently used
	open     func(num uint64) (*Reader, error)
}

type tableEntry struct {
	num     uint64
	r       *Reader
	refs    int
	elem    *list.Element
	removed bool // evicted via Evict or capacity; close on last release
}

// NewTableCache builds a cache with the given capacity and opener.
func NewTableCache(capacity int, open func(num uint64) (*Reader, error)) *TableCache {
	if capacity <= 0 {
		capacity = 256
	}
	return &TableCache{
		capacity: capacity,
		m:        map[uint64]*tableEntry{},
		lru:      list.New(),
		open:     open,
	}
}

// Get returns the reader for num with one reference held.
// The caller MUST call release when done (exactly once).
func (tc *TableCache) Get(num uint64) (r *Reader, release func(), err error) {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	if e, ok := tc.m[num]; ok {
		tc.lru.MoveToFront(e.elem)
		e.refs++
		return e.r, tc.releaseFunc(e), nil
	}
	r, err = tc.open(num)
	if err != nil {
		return nil, nil, err
	}
	e := &tableEntry{num: num, r: r, refs: 1}
	e.elem = tc.lru.PushFront(e)
	tc.m[num] = e
	tc.evictLocked()
	return r, tc.releaseFunc(e), nil
}

// releaseFunc captures the entry itself (M-008 item 1): a release after the
// entry was evicted must still reach it, and must not touch a newer entry
// registered under the same file number. Idempotence is per closure
// (M-009 item 1): each Get's release decrements exactly once, so concurrent
// holders release independently.
func (tc *TableCache) releaseFunc(e *tableEntry) func() {
	done := false
	return func() {
		tc.mu.Lock()
		defer tc.mu.Unlock()
		if done {
			return
		}
		done = true
		e.refs--
		if e.refs == 0 && e.removed {
			tc.closeEntry(e)
		}
		tc.evictLocked() // capacity pressure may now be resolvable
	}
}

// Evict removes num from the cache; closes the reader immediately when its
// last reference has been released, otherwise on release.
func (tc *TableCache) Evict(num uint64) {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	e, ok := tc.m[num]
	if !ok {
		return
	}
	tc.lru.Remove(e.elem)
	delete(tc.m, num)
	e.removed = true
	if e.refs == 0 {
		tc.closeEntry(e)
	}
}

// evictLocked drops least-recently-used unreferenced entries over capacity.
func (tc *TableCache) evictLocked() {
	for len(tc.m) > tc.capacity {
		var victim *tableEntry
		for elem := tc.lru.Back(); elem != nil; elem = elem.Prev() {
			cand := elem.Value.(*tableEntry)
			if cand.refs == 0 && !cand.removed {
				victim = cand
				break
			}
		}
		if victim == nil {
			return // all entries are referenced; release retriggers eviction
		}
		tc.lru.Remove(victim.elem)
		delete(tc.m, victim.num)
		victim.removed = true
		tc.closeEntry(victim)
	}
}

func (tc *TableCache) closeEntry(e *tableEntry) {
	// A close failure on an evicted reader must not break callers; the
	// obsolete-file deleter retries removals (I6).
	_ = e.r.Close()
}

// Len returns the number of cached readers.
func (tc *TableCache) Len() int {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	return len(tc.m)
}

// blockKey identifies a cached decoded block.
type blockKey struct {
	file   uint64
	offset uint64
}

// BlockCache is a byte-bounded LRU of decoded block payloads,
// keyed by (file, offset), mutex-protected (B3/P3.5).
type BlockCache struct {
	mu       sync.Mutex
	capacity int64
	size     int64
	m        map[blockKey]*list.Element
	lru      *list.List // front = most recent
}

type bcEntry struct {
	key  blockKey
	data []byte
}

// NewBlockCache returns a cache bounded to capacity bytes (default 64 MiB).
func NewBlockCache(capacity int64) *BlockCache {
	if capacity <= 0 {
		capacity = 64 << 20
	}
	return &BlockCache{
		capacity: capacity,
		m:        map[blockKey]*list.Element{},
		lru:      list.New(),
	}
}

// Get returns the cached payload for (file, offset), if present.
func (bc *BlockCache) Get(file, offset uint64) ([]byte, bool) {
	bc.mu.Lock()
	defer bc.mu.Unlock()
	elem, ok := bc.m[blockKey{file, offset}]
	if !ok {
		return nil, false
	}
	bc.lru.MoveToFront(elem)
	return elem.Value.(*bcEntry).data, true
}

// Put stores a payload, evicting least-recent entries when over capacity.
func (bc *BlockCache) Put(file, offset uint64, data []byte) {
	if int64(len(data)) > bc.capacity {
		return
	}
	bc.mu.Lock()
	defer bc.mu.Unlock()
	key := blockKey{file, offset}
	if elem, ok := bc.m[key]; ok {
		bc.lru.MoveToFront(elem)
		e := elem.Value.(*bcEntry)
		bc.size += int64(len(data) - len(e.data))
		e.data = data
		bc.evictLocked()
		return
	}
	elem := bc.lru.PushFront(&bcEntry{key: key, data: data})
	bc.m[key] = elem
	bc.size += int64(len(data))
	bc.evictLocked()
}

func (bc *BlockCache) evictLocked() {
	for bc.size > bc.capacity && bc.lru.Len() > 0 {
		elem := bc.lru.Back()
		if elem == nil {
			return
		}
		e := elem.Value.(*bcEntry)
		bc.size -= int64(len(e.data))
		bc.lru.Remove(elem)
		delete(bc.m, e.key)
	}
}

// Len returns the number of cached blocks.
func (bc *BlockCache) Len() int {
	bc.mu.Lock()
	defer bc.mu.Unlock()
	return len(bc.m)
}
