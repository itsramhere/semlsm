package merge

import (
	"bytes"
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"semlsm/internal/base"
)

// sliceIter adapts a sorted slice of entries to the Iter interface.
type sliceIter struct {
	entries []modelEntry
	pos     int // -1 = before start
	err     error
}

type modelEntry struct {
	ikey []byte
	val  []byte
	src  int
}

func newSliceIter(entries []modelEntry) *sliceIter {
	return &sliceIter{entries: entries, pos: -1}
}

func (s *sliceIter) First()        { s.pos = 0 }
func (s *sliceIter) Next()         { s.pos++ }
func (s *sliceIter) Valid() bool   { return s.pos >= 0 && s.pos < len(s.entries) }
func (s *sliceIter) Key() []byte   { return s.entries[s.pos].ikey }
func (s *sliceIter) Value() []byte { return s.entries[s.pos].val }
func (s *sliceIter) Err() error    { return s.err }
func (s *sliceIter) Close() error  { return nil }
func (s *sliceIter) SeekGE(target []byte) {
	lo, hi := 0, len(s.entries)
	for lo < hi {
		mid := (lo + hi) / 2
		if base.Compare(s.entries[mid].ikey, target) < 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	s.pos = lo
}

// errorIter fails after maxSteps positioning calls, to test propagation.
type errorIter struct {
	inner     *sliceIter
	steps     int
	maxSteps  int
	sentinel  error
	triggered bool
}

func (e *errorIter) burn() {
	e.steps++
	if e.steps > e.maxSteps && e.sentinel != nil && !e.triggered {
		e.triggered = true
		e.inner.err = e.sentinel
	}
}
func (e *errorIter) First()          { e.burn(); e.inner.First() }
func (e *errorIter) Next()           { e.burn(); e.inner.Next() }
func (e *errorIter) SeekGE(k []byte) { e.burn(); e.inner.SeekGE(k) }
func (e *errorIter) Valid() bool     { return e.inner.Valid() }
func (e *errorIter) Key() []byte     { return e.inner.Key() }
func (e *errorIter) Value() []byte   { return e.inner.Value() }
func (e *errorIter) Err() error      { return e.inner.Err() }
func (e *errorIter) Close() error    { return nil }

func TestMergeVsModel(t *testing.T) {
	seed := int64(991)
	for _, nSources := range []int{1, 2, 3, 8, 17, 64} {
		t.Run(fmt.Sprintf("sources-%d", nSources), func(t *testing.T) {
			rng := rand.New(rand.NewSource(seed + int64(nSources)))
			var sources [][]modelEntry
			var model []modelEntry
			for s := 0; s < nSources; s++ {
				var entries []modelEntry
				count := rng.Intn(200) // includes empty sources
				seen := map[string]bool{}
				for i := 0; i < count; i++ {
					k := fmt.Sprintf("k%03d", rng.Intn(300))
					ts := uint64(rng.Intn(1000))
					uk := fmt.Sprintf("%s/%d", k, ts)
					if seen[uk] {
						continue // identical keys within one source have unspecified heap order
					}
					seen[uk] = true
					e := modelEntry{
						base.MakeInternalKey([]byte(k), ts, base.KindPut),
						[]byte(fmt.Sprintf("src%d-val%d", s, i)),
						s,
					}
					entries = append(entries, e)
					model = append(model, e)
				}
				sort.Slice(entries, func(i, j int) bool {
					return base.Compare(entries[i].ikey, entries[j].ikey) < 0
				})
				sources = append(sources, entries)
			}
			// model: stable sort by (key, source order)
			sort.SliceStable(model, func(i, j int) bool {
				c := base.Compare(model[i].ikey, model[j].ikey)
				if c != 0 {
					return c < 0
				}
				return model[i].src < model[j].src
			})

			iters := make([]Iter, nSources)
			for s := range sources {
				iters[s] = newSliceIter(sources[s])
			}
			m := New(iters...)
			m.First()
			for i, want := range model {
				if !m.Valid() {
					t.Fatalf("ended early at %d of %d", i, len(model))
				}
				if !bytes.Equal(m.Key(), want.ikey) {
					t.Fatalf("at %d: key %s != %s", i, labelM(m.Key()), labelM(want.ikey))
				}
				if !bytes.Equal(m.Value(), want.val) {
					t.Fatalf("at %d: value differs (src order violated?)", i)
				}
				m.Next()
			}
			if m.Valid() {
				t.Fatal("extras after model exhausted")
			}
			if m.Err() != nil {
				t.Fatalf("unexpected error: %v", m.Err())
			}
			if err := m.Close(); err != nil {
				t.Fatalf("close: %v", err)
			}
		})
	}
}

func TestMergeSeekGE(t *testing.T) {
	mk := func(user string, ts uint64) modelEntry {
		return modelEntry{base.MakeInternalKey([]byte(user), ts, base.KindPut), []byte("v"), 0}
	}
	a := []modelEntry{mk("a", 30), mk("a", 10), mk("b", 40), mk("d", 5)}
	b := []modelEntry{mk("c", 25), mk("e", 15)}
	m := New(newSliceIter(a), newSliceIter(b))
	for _, target := range []struct {
		user string
		ts   uint64
		want string
	}{
		{"a", 50, "a@30(2)"},
		{"a", 30, "a@30(2)"},
		{"a", 20, "a@10(2)"}, // (a,20) seek sorts between a@30 and a@10
		{"b", 40, "b@40(2)"},
		{"b", 41, "b@40(2)"}, // (b,41) sorts before b@40
		{"c", 0, "d@5(2)"},   // (c,0) sorts before c@25
		{"d", 5, "d@5(2)"},
		{"e", 15, "e@15(2)"},
		{"f", 1, ""},
	} {
		m.SeekGE(base.SeekKey([]byte(target.user), target.ts))
		if target.want == "" {
			if m.Valid() {
				t.Fatalf("seek %s/%d: got %s want end", target.user, target.ts, labelM(m.Key()))
			}
			continue
		}
		if !m.Valid() || labelM(m.Key()) != target.want {
			t.Fatalf("seek %s/%d: got %v want %s", target.user, target.ts, m.Valid(), target.want)
		}
	}
}

func TestMergeErrorPropagation(t *testing.T) {
	boom := fmt.Errorf("boom")
	srcs := []Iter{
		newSliceIter([]modelEntry{mkEntryM("a", 1), mkEntryM("b", 2)}),
		&errorIter{inner: newSliceIter([]modelEntry{mkEntryM("c", 3)}), maxSteps: 1, sentinel: boom},
	}
	m := New(srcs...)
	m.First()
	if !m.Valid() {
		t.Fatalf("first entry should be available, err=%v", m.Err())
	}
	for i := 0; i < 10 && m.Valid(); i++ {
		m.Next()
	}
	if m.Err() == nil {
		t.Fatal("error must surface once iteration ends")
	}
	if m.Err().Error() == "" {
		t.Fatal("Err must be non-nil")
	}
}

func TestMergeAllEmpty(t *testing.T) {
	m := New(newSliceIter(nil), newSliceIter(nil))
	m.First()
	if m.Valid() {
		t.Fatal("all-empty merge must be invalid")
	}
	if m.Err() != nil {
		t.Fatalf("no error expected: %v", m.Err())
	}
}

func TestMergeEmptyAndNilSources(t *testing.T) {
	m := New(nil, newSliceIter(nil), newSliceIter([]modelEntry{mkEntryM("x", 1)}))
	m.First()
	if !m.Valid() || string(base.UserKey(m.Key())) != "x" {
		t.Fatalf("nil/empty sources must be skipped: valid=%v", m.Valid())
	}
}

func labelM(ikey []byte) string {
	u, ts, k, ok := base.SplitInternalKey(ikey)
	if !ok {
		return "<bad>"
	}
	return fmt.Sprintf("%s@%d(%d)", u, ts, k)
}

func mkEntryM(user string, ts uint64) modelEntry {
	return modelEntry{base.MakeInternalKey([]byte(user), ts, base.KindPut), []byte("v"), 0}
}

// --- M-008 item 2 remediation tests ---

func TestMergeReseekAfterExhaustion(t *testing.T) {
	// a) SeekGE past the end, then SeekGE to a valid key must work.
	a := []modelEntry{mkEntryM("a", 1), mkEntryM("b", 2)}
	b := []modelEntry{mkEntryM("c", 3)}
	m := New(newSliceIter(a), newSliceIter(b))
	m.SeekGE(base.SeekKey([]byte("zz"), 1))
	if m.Valid() {
		t.Fatal("seek past end must be invalid")
	}
	m.SeekGE(base.SeekKey([]byte("a"), 10)) // re-seek to a valid key
	if !m.Valid() || string(base.UserKey(m.Key())) != "a" {
		t.Fatalf("re-seek after exhaustion: valid=%v key=%v", m.Valid(), m.Key())
	}
	m.Next()
	if !m.Valid() || string(base.UserKey(m.Key())) != "b" {
		t.Fatalf("re-seek lost source: valid=%v", m.Valid())
	}
	m.Next()
	if !m.Valid() || string(base.UserKey(m.Key())) != "c" {
		t.Fatalf("re-seek lost second source: valid=%v", m.Valid())
	}
	m.Close()
}

func TestMergeFirstAfterFullScan(t *testing.T) {
	// b) Full scan, then First must see everything again.
	a := []modelEntry{mkEntryM("a", 1), mkEntryM("b", 2)}
	m := New(newSliceIter(a))
	m.First()
	for m.Valid() {
		m.Next()
	}
	m.First()
	n := 0
	for ; m.Valid(); m.Next() {
		n++
	}
	if n != 2 {
		t.Fatalf("First after full scan saw %d entries, want 2", n)
	}
	m.Close()
}

type countingCloseIter struct {
	inner  *sliceIter
	closes int
}

func (c *countingCloseIter) First()          { c.inner.First() }
func (c *countingCloseIter) SeekGE(k []byte) { c.inner.SeekGE(k) }
func (c *countingCloseIter) Next()           { c.inner.Next() }
func (c *countingCloseIter) Valid() bool     { return c.inner.Valid() }
func (c *countingCloseIter) Key() []byte     { return c.inner.Key() }
func (c *countingCloseIter) Value() []byte   { return c.inner.Value() }
func (c *countingCloseIter) Err() error      { return c.inner.Err() }
func (c *countingCloseIter) Close() error {
	c.closes++
	return nil
}

func TestMergeCloseClosesAllSources(t *testing.T) {
	// c) Close after a full scan must reach every source exactly once,
	// and be idempotent.
	a := newSliceIter([]modelEntry{mkEntryM("a", 1)})
	b := newSliceIter([]modelEntry{mkEntryM("b", 2)})
	ca := &countingCloseIter{inner: a}
	cb := &countingCloseIter{inner: b}
	m := New(ca, cb)
	m.First()
	for m.Valid() {
		m.Next()
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	m.Close() // idempotent
	if ca.closes != 1 || cb.closes != 1 {
		t.Fatalf("closes: src0=%d src1=%d, want 1/1", ca.closes, cb.closes)
	}
}

func TestMergeCloseAfterError(t *testing.T) {
	// An errored source must still be closed.
	boom := fmt.Errorf("boom")
	src := &errorIter{inner: newSliceIter([]modelEntry{mkEntryM("a", 1)}), maxSteps: 0, sentinel: boom}
	m := New(src)
	m.First() // error immediately
	if m.Err() == nil {
		t.Fatal("expected error on First")
	}
	if err := m.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}
