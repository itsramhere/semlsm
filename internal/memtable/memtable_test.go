package memtable

import (
	"bytes"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"testing"

	"semlsm/internal/base"
)

type mEntry struct {
	ikey []byte
	val  []byte
}

func mkEntry(user string, ts uint64, k base.Kind, val string) mEntry {
	return mEntry{base.MakeInternalKey([]byte(user), ts, k), []byte(val)}
}

func TestMemtableVsModel(t *testing.T) {
	const ops = 100000
	seed := int64(20261105)
	rng := rand.New(rand.NewSource(seed))
	m := New()

	var model []mEntry
	seen := map[string]bool{}
	for i := 0; i < ops; i++ {
		user := fmt.Sprintf("user%04d", rng.Intn(1000))
		ts := uint64(i + 1)
		kind := base.KindPut
		if rng.Intn(10) == 0 {
			kind = base.KindDelete
		}
		val := fmt.Sprintf("value-%d", i)
		if kind == base.KindDelete {
			val = ""
		}
		uk := fmt.Sprintf("%s/%012d", user, ts) // unique (key, ts)
		if seen[uk] {
			continue
		}
		seen[uk] = true
		e := mkEntry(user, ts, kind, val)
		m.Add(e.ikey, e.val)
		model = append(model, e)
	}
	sort.Slice(model, func(i, j int) bool { return base.Compare(model[i].ikey, model[j].ikey) < 0 })

	// full iteration equals the model
	it := m.NewIterator()
	it.First()
	idx := 0
	for ; it.Valid(); it.Next() {
		if idx >= len(model) {
			t.Fatalf("iterator produced more entries than model")
		}
		if !bytes.Equal(it.Key(), model[idx].ikey) {
			t.Fatalf("entry %d: key differs (seed %d)", idx, seed)
		}
		if !bytes.Equal(it.Value(), model[idx].val) {
			t.Fatalf("entry %d: value differs", idx)
		}
		idx++
	}
	if idx != len(model) {
		t.Fatalf("iterated %d entries, model has %d", idx, len(model))
	}

	// Get(user, S) equals the model for random probes
	for probe := 0; probe < 2000; probe++ {
		user := fmt.Sprintf("user%04d", rng.Intn(1000))
		s := uint64(rng.Intn(ops + 1))
		want := modelGet(model, []byte(user), s)
		ts, kind, val, ok := m.Get([]byte(user), s)
		if !want.ok {
			if ok {
				t.Fatalf("probe %d (%s,%d): unexpected hit ts=%d", probe, user, s, ts)
			}
			continue
		}
		if !ok {
			t.Fatalf("probe %d (%s,%d): miss, want ts=%d kind=%v", probe, user, s, want.ts, want.kind)
		}
		if ts != want.ts || kind != want.kind || string(val) != string(want.val) {
			t.Fatalf("probe %d (%s,%d): got (%d,%v,%q) want (%d,%v,%q)", probe, user, s, ts, kind, val, want.ts, want.kind, want.val)
		}
	}
}

type getWant struct {
	ts   uint64
	kind base.Kind
	val  []byte
	ok   bool
}

func modelGet(model []mEntry, user []byte, s uint64) getWant {
	seek := base.SeekKey(user, s)
	i := sort.Search(len(model), func(i int) bool {
		return base.Compare(model[i].ikey, seek) >= 0
	})
	if i >= len(model) {
		return getWant{}
	}
	u, ts, kind, valid := base.SplitInternalKey(model[i].ikey)
	if !valid || !bytes.Equal(u, user) {
		return getWant{}
	}
	return getWant{ts: ts, kind: kind, val: model[i].val, ok: true}
}

func TestMemtableSeekGE(t *testing.T) {
	m := New()
	adds := []mEntry{
		mkEntry("a", 10, base.KindPut, "a10"),
		mkEntry("a", 5, base.KindPut, "a5"),
		mkEntry("ab", 8, base.KindPut, "ab8"),
		mkEntry("b", 3, base.KindPut, "b3"),
	}
	for _, e := range adds {
		m.Add(e.ikey, e.val)
	}
	tests := []struct {
		user string
		ts   uint64
		want string // expected label of positioned entry, "" for end
	}{
		{"a", 11, "a@10(2)"}, // SeekGE lands on the newest version <= ts
		{"a", 10, "a@10(2)"},
		{"a", 9, "a@5(2)"}, // ts 9 predates version 10
		{"a", 5, "a@5(2)"},
		{"a", 4, "ab@8(2)"},
		{"a", 0, "ab@8(2)"},
		{"ab", 8, "ab@8(2)"},
		{"ab", 0, "b@3(2)"},
		{"b", 3, "b@3(2)"},
		{"b", 0, ""}, // nothing at or after (b,0)
		{"c", 1, ""}, // past the end
	}
	for _, tt := range tests {
		it := m.NewIterator()
		it.SeekGE(base.SeekKey([]byte(tt.user), tt.ts))
		if tt.want == "" {
			if it.Valid() {
				t.Fatalf("seek (%s,%d): got %q, want end", tt.user, tt.ts, label(it.Key()))
			}
			continue
		}
		if !it.Valid() {
			t.Fatalf("seek (%s,%d): invalid, want %s", tt.user, tt.ts, tt.want)
		}
		if label(it.Key()) != tt.want {
			t.Fatalf("seek (%s,%d): got %s, want %s", tt.user, tt.ts, label(it.Key()), tt.want)
		}
	}
}

func label(ikey []byte) string {
	u, ts, k, ok := base.SplitInternalKey(ikey)
	if !ok {
		return "<bad>"
	}
	return fmt.Sprintf("%s@%d(%d)", u, ts, k)
}

// TestMemtableConcurrentReaders is the P2.2 race test: 1 writer, 8 readers.
// Readers assert monotonic internal order and no torn values. Run with
// -race, or -count=20 where the race detector is unavailable.
func TestMemtableConcurrentReaders(t *testing.T) {
	m := New()
	const total = 20000
	expected := func(i int) string {
		return fmt.Sprintf("value-%05d-%05d", i, i*7%1000)
	}

	var writerDone sync.WaitGroup
	writerDone.Add(1)
	go func() {
		defer writerDone.Done()
		for i := 0; i < total; i++ {
			m.Add(base.MakeInternalKey([]byte(fmt.Sprintf("key%05d", i)), uint64(i+1), base.KindPut), []byte(expected(i)))
		}
	}()

	stop := make(chan struct{})
	var readers sync.WaitGroup
	for r := 0; r < 8; r++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				onePass(t, m, expected, -1) // partial passes may see fewer entries
			}
		}()
	}

	writerDone.Wait()
	onePass(t, m, expected, total) // final full pass must see everything
	close(stop)
	readers.Wait()
}

// onePass iterates the whole list once, asserting order and value integrity.
// wantCount < 0 skips the count assertion (for in-progress passes).
func onePass(t *testing.T, m *Memtable, expected func(int) string, wantCount int) {
	t.Helper()
	it := m.NewIterator()
	it.First()
	var prevKey []byte
	seen := 0
	for ; it.Valid(); it.Next() {
		key := it.Key()
		if prevKey != nil && base.Compare(prevKey, key) >= 0 {
			t.Fatalf("order violated: %s not before %s", label(prevKey), label(key))
		}
		u, ts, kind, ok := base.SplitInternalKey(key)
		if !ok || kind != base.KindPut || !bytes.HasPrefix(u, []byte("key")) {
			t.Fatalf("unexpected key %s", label(key))
		}
		idx := int(ts) - 1
		if idx < 0 || idx >= 20000 || string(u) != fmt.Sprintf("key%05d", idx) {
			t.Fatalf("key/value mismatch: %s", label(key))
		}
		val := it.Value()
		if string(val) != expected(idx) {
			t.Fatalf("torn value for %s: %q want %q", label(key), val, expected(idx))
		}
		prevKey = append(prevKey[:0], key...)
		seen++
	}
	if wantCount >= 0 && seen != wantCount {
		t.Fatalf("pass saw %d entries, want %d", seen, wantCount)
	}
}

func TestMemtableApproxBytes(t *testing.T) {
	m := New()
	e := mkEntry("k", 1, base.KindPut, "some value")
	m.Add(e.ikey, e.val)
	if got := m.ApproxBytes(); got < int64(len("k")+9+len("some value")) {
		t.Fatalf("approx bytes too small: %d", got)
	}
	if m.Len() != 1 {
		t.Fatalf("len=%d", m.Len())
	}
}
