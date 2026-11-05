// Package memtable implements the in-memory skiplist of internal keys.
//
// Concurrency model (B7): a single writer (the commit goroutine) adds entries;
// any number of readers traverse concurrently. Node next pointers are
// atomic.Pointer, and a node's towers are fully linked before it becomes
// reachable, so readers never observe a partially inserted node. Nodes are
// never removed or mutated.
package memtable

import (
	"math/rand"
	"sync"
	"sync/atomic"

	"semlsm/internal/base"
)

const (
	maxHeight = 12
	// branching factor 1/4: each additional level has probability 1/4.
	branchP = 4
	// nodeOverhead approximates per-node allocator cost for ApproxBytes.
	nodeOverhead = 48
)

// node is one skiplist entry. key is an internal key; val is the value.
// Both are immutable once constructed.
type node struct {
	key  []byte
	val  []byte
	next []atomic.Pointer[node] // len == tower height
}

func (n *node) userKey() []byte { return base.UserKey(n.key) }

// Memtable holds ordered versions of keys.
type Memtable struct {
	head        *node // height == maxHeight
	rngMu       sync.Mutex
	rng         *rand.Rand
	approxBytes atomic.Int64
	count       atomic.Int64
}

// New returns an empty memtable with a deterministic random shape.
func New() *Memtable {
	head := &node{next: make([]atomic.Pointer[node], maxHeight)}
	return &Memtable{
		head: head,
		rng:  rand.New(rand.NewSource(0x5EED5EED)),
	}
}

func (m *Memtable) randomHeight() int {
	m.rngMu.Lock()
	defer m.rngMu.Unlock()
	h := 1
	for h < maxHeight && m.rng.Intn(branchP) == 0 {
		h++
	}
	return h
}

// Add inserts one version. Only the commit goroutine may call Add.
// ikey and value are copied, so callers may reuse their buffers.
func (m *Memtable) Add(ikey []byte, value []byte) {
	h := m.randomHeight()
	n := &node{
		key:  append([]byte(nil), ikey...),
		val:  append([]byte(nil), value...),
		next: make([]atomic.Pointer[node], h),
	}

	// Find predecessors at each level.
	var preds [maxHeight]*node
	cur := m.head
	for level := maxHeight - 1; level >= 0; level-- {
		for {
			next := cur.next[level].Load()
			if next == nil || base.Compare(next.key, ikey) >= 0 {
				break
			}
			cur = next
		}
		preds[level] = cur
	}

	// Link the new node's own towers first, then publish it top-down so a
	// reader that reaches n at any level finds every lower level set.
	for level := 0; level < h; level++ {
		n.next[level].Store(preds[level].next[level].Load())
	}
	for level := h - 1; level >= 0; level-- {
		preds[level].next[level].Store(n)
	}

	m.approxBytes.Add(int64(len(n.key) + len(n.val) + nodeOverhead))
	m.count.Add(1)
}

// Get returns the newest version of user with ts <= S.
// The value aliases the memtable and is valid for the lifetime of the node
// (nodes are never mutated or removed); copy before retaining.
func (m *Memtable) Get(user []byte, s uint64) (ts uint64, kind base.Kind, value []byte, ok bool) {
	target := base.SeekKey(user, s)
	n := m.seekNode(target)
	if n == nil {
		return 0, 0, nil, false
	}
	u, t, k, valid := base.SplitInternalKey(n.key)
	if !valid || string(u) != string(user) {
		return 0, 0, nil, false
	}
	return t, k, n.val, true
}

// seekNode returns the first node with key >= target, or nil.
func (m *Memtable) seekNode(target []byte) *node {
	cur := m.head
	for level := maxHeight - 1; level >= 0; level-- {
		for {
			next := cur.next[level].Load()
			if next == nil || base.Compare(next.key, target) >= 0 {
				break
			}
			cur = next
		}
	}
	return cur.next[0].Load()
}

// ApproxBytes estimates current memory use.
func (m *Memtable) ApproxBytes() int64 { return m.approxBytes.Load() }

// Len returns the number of entries.
func (m *Memtable) Len() int64 { return m.count.Load() }

// Iterator traverses the skiplist in internal-key order.
// A snapshot of iteration position is stable: nodes are immutable, so Key and
// Value stay valid until the next positioning call. The iterator may observe
// entries inserted after its creation.
type Iterator struct {
	m   *Memtable
	cur *node
}

// NewIterator returns an iterator positioned before the first entry.
func (m *Memtable) NewIterator() *Iterator {
	return &Iterator{m: m}
}

// SeekGE positions the iterator at the first entry with internal key >= ikey.
func (it *Iterator) SeekGE(ikey []byte) {
	it.cur = it.m.seekNode(ikey)
}

// First positions the iterator at the smallest entry.
func (it *Iterator) First() {
	it.cur = it.m.head.next[0].Load()
}

// Next advances to the next entry; Valid must be checked afterwards.
func (it *Iterator) Next() {
	if it.cur != nil {
		it.cur = it.cur.next[0].Load()
	}
}

// Valid reports whether the iterator is positioned on an entry.
func (it *Iterator) Valid() bool { return it.cur != nil }

// Key returns the internal key of the current entry (aliased).
func (it *Iterator) Key() []byte { return it.cur.key }

// Value returns the value of the current entry (aliased).
func (it *Iterator) Value() []byte { return it.cur.val }
