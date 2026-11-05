// Package merge implements a min-heap K-way merging iterator.
package merge

import (
	"container/heap"
	"fmt"

	"semlsm/internal/base"
)

// Iter is the source interface consumed by the merge iterator.
type Iter interface {
	First()
	SeekGE(ikey []byte)
	Next()
	Valid() bool
	Key() []byte
	Value() []byte
	Err() error
	Close() error
}

type srcItem struct {
	it  Iter
	idx int // source order for stable tie-breaking
}

type srcHeap []*srcItem

func (h srcHeap) Len() int { return len(h) }
func (h srcHeap) Less(i, j int) bool {
	c := base.Compare(h[i].it.Key(), h[j].it.Key())
	if c != 0 {
		return c < 0
	}
	return h[i].idx < h[j].idx
}
func (h srcHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *srcHeap) Push(x any)   { *h = append(*h, x.(*srcItem)) }
func (h *srcHeap) Pop() any     { old := *h; n := len(old); x := old[n-1]; *h = old[:n-1]; return x }

// mergingIter merges sources in internal-key order. Equal keys from
// different sources are emitted in source order (stable).
type mergingIter struct {
	heap  srcHeap
	valid bool
	err   error
}

// New returns a merged iterator over srcs. Empty sources are skipped.
func New(srcs ...Iter) Iter {
	m := &mergingIter{}
	for i, s := range srcs {
		if s == nil {
			continue
		}
		m.heap = append(m.heap, &srcItem{it: s, idx: i})
	}
	return m
}

// First positions every source at its start and builds the heap.
func (m *mergingIter) First() {
	m.err = nil
	alive := m.heap[:0]
	for _, s := range m.heap {
		s.it.First()
		if e := s.it.Err(); e != nil {
			m.err = fmt.Errorf("merge: source %d: %w", s.idx, e)
			m.valid = false
			m.heap = alive
			return
		}
		if s.it.Valid() {
			alive = append(alive, s)
		}
	}
	m.heap = alive
	heap.Init(&m.heap)
	m.valid = len(m.heap) > 0
}

// SeekGE repositions every source and rebuilds the heap.
func (m *mergingIter) SeekGE(target []byte) {
	m.err = nil
	alive := m.heap[:0]
	for _, s := range m.heap {
		s.it.SeekGE(target)
		if e := s.it.Err(); e != nil {
			m.err = fmt.Errorf("merge: source %d: %w", s.idx, e)
			m.valid = false
			m.heap = alive
			return
		}
		if s.it.Valid() {
			alive = append(alive, s)
		}
	}
	m.heap = alive
	heap.Init(&m.heap)
	m.valid = len(m.heap) > 0
}

// Next advances to the next entry in merge order.
func (m *mergingIter) Next() {
	if len(m.heap) == 0 {
		m.valid = false
		return
	}
	top := heap.Pop(&m.heap).(*srcItem)
	top.it.Next()
	if e := top.it.Err(); e != nil {
		m.err = fmt.Errorf("merge: source %d: %w", top.idx, e)
		m.valid = false
		return
	}
	if top.it.Valid() {
		heap.Push(&m.heap, top)
	}
	m.valid = len(m.heap) > 0
}

// Valid reports whether an entry is available. False once any source errors.
func (m *mergingIter) Valid() bool { return m.valid && m.err == nil }

// Key returns the current internal key (aliased to the winning source).
func (m *mergingIter) Key() []byte { return m.heap[0].it.Key() }

// Value returns the current value (aliased).
func (m *mergingIter) Value() []byte { return m.heap[0].it.Value() }

// Err returns the first source error encountered.
func (m *mergingIter) Err() error { return m.err }

// Close closes all sources and returns the first error.
func (m *mergingIter) Close() error {
	var first error
	for _, s := range m.heap {
		if e := s.it.Close(); e != nil && first == nil {
			first = e
		}
	}
	return first
}
