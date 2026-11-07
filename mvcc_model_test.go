package semlsm

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"testing"
	"time"

	"semlsm/internal/base"
	"semlsm/internal/llm"
	"semlsm/internal/resolve"
	"semlsm/internal/testmodel"
	"semlsm/internal/vfs"
)

// TestHistoryInterleaving is the P11.2 test: raw versions and tuple runs
// interleave oldest-first and never overlap (I7).
func TestHistoryInterleaving(t *testing.T) {
	db, clk, _ := openSemDB(t, "db", llm.NewMock(llm.MockValid), nil)
	defer db.Close()

	key := []byte("agent/1/h")
	vals := map[uint64]string{}
	for i := 0; i < 14; i++ {
		v := fmt.Sprintf("v%02d %s", i, strings.Repeat("h", 290))
		ts, err := db.Put(key, []byte(v))
		if err != nil {
			t.Fatal(err)
		}
		vals[ts] = v
	}
	driveDown(t, db)
	clk.Set(semStart.Add(2 * time.Hour))
	if err := db.CompactLevel(context.Background(), base.NumLevels-1); err != nil {
		t.Fatal(err)
	}
	// A second generation: its versions form a second run after they age.
	for i := 14; i < 26; i++ {
		v := fmt.Sprintf("v%02d %s", i, strings.Repeat("h", 290))
		ts, err := db.Put(key, []byte(v))
		if err != nil {
			t.Fatal(err)
		}
		vals[ts] = v
	}
	driveDown(t, db)
	clk.Set(semStart.Add(4 * time.Hour))
	if err := db.CompactLevel(context.Background(), base.NumLevels-1); err != nil {
		t.Fatal(err)
	}

	items, err := db.History(key, 0, ^uint64(0))
	if err != nil {
		t.Fatal(err)
	}
	if len(items) < 3 {
		t.Fatalf("history too short: %d items", len(items))
	}
	// Oldest first, exactly one side set per item, and no overlap: raw ts
	// never inside a checkpoint run, runs never overlap each other.
	prevEnd := uint64(0)
	for i, it := range items {
		if it.Raw == nil && it.Checkpoint == nil {
			t.Fatalf("item %d has neither raw nor checkpoint", i)
		}
		if it.Raw != nil && it.Checkpoint != nil {
			t.Fatalf("item %d has both", i)
		}
		if i > 0 {
			var start uint64
			if it.Raw != nil {
				start = it.Raw.Ts
			} else {
				start = it.Checkpoint.KeyMinTs
			}
			if start <= prevEnd {
				t.Fatalf("item %d (start %d) overlaps the previous item (end %d)", i, start, prevEnd)
			}
		}
		if it.Raw != nil {
			prevEnd = it.Raw.Ts
			want, ok := vals[it.Raw.Ts]
			if !ok {
				t.Fatalf("item %d: raw ts %d was never written", i, it.Raw.Ts)
			}
			if it.Raw.Kind != base.KindPut || string(it.Raw.Value) != want {
				t.Fatalf("item %d: raw mismatch: %q", i, it.Raw.Value)
			}
		} else {
			prevEnd = it.Checkpoint.KeyMaxTs
			if it.Checkpoint.Group != "agent/1/" || it.Checkpoint.KeyCount == 0 {
				t.Fatalf("item %d: checkpoint info wrong: %+v", i, it.Checkpoint)
			}
		}
	}
	// The head version is raw and the newest item.
	last := items[len(items)-1]
	if last.Raw == nil || !strings.HasPrefix(string(last.Raw.Value), "v25 ") {
		t.Fatalf("newest history item: %+v", last)
	}
	// A subrange only returns items inside it.
	mid := items[1]
	var lo, hi uint64
	if mid.Raw != nil {
		lo, hi = mid.Raw.Ts, mid.Raw.Ts
	} else {
		lo, hi = mid.Checkpoint.KeyMinTs, mid.Checkpoint.KeyMaxTs
	}
	sub, err := db.History(key, lo, hi)
	if err != nil {
		t.Fatal(err)
	}
	if len(sub) == 0 {
		t.Fatal("subrange history empty")
	}
	for _, it := range sub {
		var ts uint64
		if it.Raw != nil {
			ts = it.Raw.Ts
		} else {
			if it.Checkpoint.KeyMaxTs < lo || it.Checkpoint.KeyMinTs > hi {
				t.Fatalf("subrange returned tuple outside the range: %+v", it.Checkpoint)
			}
			continue
		}
		if ts < lo || ts > hi {
			t.Fatalf("subrange returned raw ts %d outside [%d,%d]", ts, lo, hi)
		}
	}
}

// mvccEntry is one tuple key entry collected for the model test's
// expectations.
type mvccEntry struct {
	min, max, count uint64
	file, tupleID   uint64
}

// collectDroppedRanges reads every tuple of the current version, marks the
// model's dropped ranges, and returns the per-key entries.
func collectDroppedRanges(t *testing.T, db *DB, model *testmodel.Model) map[string][]mvccEntry {
	t.Helper()
	v := db.vs.Acquire()
	defer v.Unref()
	out := map[string][]mvccEntry{}
	for num, cf := range v.Ckpts.ByNum {
		r, rel, err := db.ckptCache.Get(num)
		if err != nil {
			t.Fatal(err)
		}
		for _, ref := range cf.Tuples {
			keys, err := r.IterKeys(ref.ID)
			if err != nil {
				rel()
				t.Fatal(err)
			}
			for _, ke := range keys {
				out[string(ke.Key)] = append(out[string(ke.Key)], mvccEntry{
					min: ke.MinTs, max: ke.MaxTs, count: ke.Count,
					file: num, tupleID: ref.ID,
				})
				model.MarkDropped(ke.Key, ke.MinTs, ke.MaxTs)
			}
		}
		rel()
	}
	return out
}

// expectedAt computes the B6 expectation for (key, S) from the model and the
// collected tuple entries. Returns the outcome plus, for UseSummary, the
// covering entry.
func expectedAt(model *testmodel.Model, versions map[string][]base.Record, entries []mvccEntry, key []byte, s uint64) (resolve.Outcome, base.Record, *mvccEntry) {
	var raw resolve.Raw
	var rawRec base.Record
	keyStr := string(key)
	for i := len(versions[keyStr]) - 1; i >= 0; i-- { // versions are ts-ascending
		r := versions[keyStr][i]
		if r.Ts > s {
			continue
		}
		if !model.IsDropped([]byte(keyStr), r.Ts) {
			raw = resolve.Raw{Found: true, Ts: r.Ts, Kind: r.Kind}
			rawRec = r
			break
		}
	}
	var best *mvccEntry
	for i := range entries {
		if entries[i].min <= s && (best == nil || entries[i].min >= best.min) {
			e := entries[i]
			best = &e
		}
	}
	var entry resolve.Entry
	if best != nil {
		entry = resolve.Entry{Found: true, MinTs: best.min, MaxTs: best.max, Count: best.count}
	}
	outcome := resolve.Resolve(s, raw, entry)
	return outcome, rawRec, best
}

// TestMVCCModel is the master model-based MVCC test (P11.4): over seeded
// runs with concurrent writers, auto compaction, semantic drops, snapshots
// and a reopen, every (key, S) probe must match the B6 expectation derived
// from the model, snapshot results must be byte-identical across probes, and
// iterator results must equal per-key GetAt results.
func TestMVCCModel(t *testing.T) {
	seeds := 20
	if testing.Short() {
		seeds = 5
	}
	for seed := 0; seed < seeds; seed++ {
		t.Run(fmt.Sprintf("seed%02d", seed), func(t *testing.T) {
			runMVCCSeed(t, int64(seed))
		})
	}
}

func runMVCCSeed(t *testing.T, seed int64) {
	rng := rand.New(rand.NewSource(seed))
	fs := vfs.NewMemFS()
	clk := base.NewManualClock(semStart)
	mock := llm.NewMock(llm.MockValid)
	stack := newSemStack(t, fs, "db", mock, 0)
	o := semOptions(fs, clk, stack)
	o.MinCompressionRatio = 0.5 // Gen's event values are small; the gate itself has dedicated tests
	o.DisableAutoCompaction = false
	o.MemtableBytes = 64 << 10
	o.L0CompactTrigger = 3
	o.L0StallTrigger = 24
	o.L1TargetBytes = 48 << 10
	o.LevelMultiplier = 2
	o.TargetFileBytes = 32 << 10
	db, err := Open("db", o)
	if err != nil {
		t.Fatal(err)
	}

	model := testmodel.NewModel()
	gen := testmodel.NewGen(seed, 4, 300)
	var modelMu sync.Mutex
	versions := map[string][]base.Record{} // key -> ts-ascending versions

	// Phase A: concurrent writers while auto compaction runs.
	const writers = 4
	const rounds = 30
	var wg sync.WaitGroup
	var genMu sync.Mutex
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for r := 0; r < rounds; r++ {
				genMu.Lock()
				recs := gen.Next(6)
				genMu.Unlock()
				// Through the batch API: last-write-wins within the batch
				// is the Batch's contract, and the committer relies on it.
				b := NewBatch()
				for i := range recs {
					if recs[i].Kind == base.KindDelete {
						b.Delete(recs[i].Key)
					} else {
						b.Put(recs[i].Key, recs[i].Value)
					}
				}
				ts, err := db.Apply(b)
				if err != nil {
					t.Error(err)
					return
				}
				modelMu.Lock()
				for i := range b.recs {
					b.recs[i].Ts = ts
				}
				model.Commit(b.recs)
				for i := range b.recs {
					versions[string(b.recs[i].Key)] = append(versions[string(b.recs[i].Key)], b.recs[i])
				}
				modelMu.Unlock()
			}
		}()
	}
	wg.Wait()

	// Snapshots pinned before the drops (I9 across compaction installs).
	preSnap := db.NewSnapshot()
	defer preSnap.Release()

	// Phase B: settle, age everything, drive the chain and the sweep.
	if err := db.WaitIdle(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	clk.Set(semStart.Add(3 * time.Hour))
	for lvl := 0; lvl < base.NumLevels; lvl++ {
		if err := db.CompactLevel(context.Background(), lvl); err != nil {
			t.Fatalf("compact %d: %v", lvl, err)
		}
	}
	if err := db.WaitIdle(context.Background()); err != nil {
		t.Fatal(err)
	}
	postSnap := db.NewSnapshot()
	defer postSnap.Release()

	modelMu.Lock()
	entries := collectDroppedRanges(t, db, model)
	keys := model.Keys()
	maxTs := model.MaxTs()
	modelMu.Unlock()
	midTs := maxTs / 2
	midSnap, err := db.OpenSnapshotAt(uint64(midTs))
	if err != nil {
		t.Fatalf("open snapshot at %d: %v", midTs, err)
	}
	defer midSnap.Release()
	if len(entries) == 0 {
		t.Fatal("no tuple entries: the scenario did not produce semantic drops")
	}

	// Probe state: the first result for each pinned snapshot probe is
	// recorded; every later probe must be byte-identical (I9).
	type probeID struct {
		snap string
		key  string
		s    uint64
	}
	first := map[probeID]Result{}
	checkProbe := func(snapName string, snap *Snapshot, key []byte, s uint64) {
		res, err := snap.Get(key)
		if err != nil {
			t.Fatalf("snapshot probe %s %q@%d: %v", snapName, key, s, err)
		}
		id := probeID{snapName, string(key), s}
		if prev, ok := first[id]; ok {
			if prev.Kind != res.Kind || string(prev.Value) != string(res.Value) || prev.VersionTs != res.VersionTs {
				t.Fatalf("snapshot %s %q@%d changed: %+v -> %+v", snapName, key, s, prev, res)
			}
			if (prev.Checkpoint == nil) != (res.Checkpoint == nil) {
				t.Fatalf("snapshot %s %q@%d checkpoint presence changed", snapName, key, s)
			}
			if res.Checkpoint != nil && prev.Checkpoint.ID != res.Checkpoint.ID {
				t.Fatalf("snapshot %s %q@%d checkpoint id changed", snapName, key, s)
			}
			return
		}
		first[id] = res
	}

	probeDB := func(key []byte, s uint64) {
		modelMu.Lock()
		outcome, rawRec, best := expectedAt(model, versions, entries[string(key)], key, s)
		modelMu.Unlock()
		res, err := db.GetAt(key, s)
		if err != nil {
			t.Fatalf("GetAt %q@%d: %v", key, s, err)
		}
		switch outcome {
		case resolve.NotFound:
			if res.Kind != ResultNotFound {
				t.Fatalf("key %q@%d: want NotFound, got %+v", key, s, res)
			}
		case resolve.UseRaw:
			if res.Kind != ResultExact || res.VersionTs != rawRec.Ts || string(res.Value) != string(rawRec.Value) {
				t.Fatalf("key %q@%d: want Exact (%d,%q), got %+v", key, s, rawRec.Ts, rawRec.Value, res)
			}
		case resolve.UseSummary:
			if res.Kind != ResultSummarized || res.Checkpoint == nil {
				t.Fatalf("key %q@%d: want Summarized, got %+v", key, s, res)
			}
			if res.Checkpoint.ID != best.tupleID || res.Checkpoint.KeyMinTs != best.min ||
				res.Checkpoint.KeyMaxTs != best.max || res.Checkpoint.KeyCount != best.count {
				t.Fatalf("key %q@%d: checkpoint mismatch: %+v want entry %+v", key, s, res.Checkpoint, best)
			}
		default:
			t.Fatalf("key %q@%d: B6 says Invariant (I7 broken)", key, s)
		}
	}

	for i := 0; i < 150; i++ {
		key := keys[rng.Intn(len(keys))]
		s := uint64(1 + rng.Int63n(int64(maxTs)))
		probeDB(key, s)
		checkProbe("pre", preSnap, key, s)
		checkProbe("post", postSnap, key, s)
		checkProbe("mid", midSnap, key, s)
	}
	// Re-probe every recorded snapshot probe: still byte-identical.
	for id := range first {
		var snap *Snapshot
		switch id.snap {
		case "pre":
			snap = preSnap
		case "post":
			snap = postSnap
		default:
			snap = midSnap
		}
		res, err := snap.Get([]byte(id.key))
		if err != nil {
			t.Fatalf("re-probe %s: %v", id.snap, err)
		}
		prev := first[id]
		if prev.Kind != res.Kind || string(prev.Value) != string(res.Value) || prev.VersionTs != res.VersionTs {
			t.Fatalf("snapshot %s %q@%d drifted: %+v -> %+v", id.snap, id.key, id.s, prev, res)
		}
	}

	// Iterator consistency: at several S values the iterator's per-key
	// results equal GetAt, in key order; CollapseSummaries emits each tuple
	// once at its first key.
	for _, s := range []uint64{maxTs / 3, (2 * maxTs) / 3, maxTs} {
		snap, err := db.OpenSnapshotAt(s)
		if err != nil {
			t.Fatalf("open snapshot at %d: %v", s, err)
		}
		it := snap.NewIterator(IterOptions{})
		got := map[string]Result{}
		prevKey := ""
		for it.Next() {
			r := it.Result()
			if string(r.Key) <= prevKey {
				t.Fatalf("iterator key order broken at %q after %q", r.Key, prevKey)
			}
			prevKey = string(r.Key)
			got[prevKey] = r
		}
		if err := it.Err(); err != nil {
			t.Fatal(err)
		}
		it.Close()
		for _, mk := range keys {
			want, err := db.GetAt(mk, s)
			if err != nil {
				t.Fatal(err)
			}
			g, ok := got[string(mk)]
			if want.Kind == ResultNotFound {
				if ok && g.Kind != ResultNotFound {
					t.Fatalf("S=%d key %q: iterator has %+v, GetAt NotFound", s, mk, g)
				}
				continue
			}
			if !ok {
				t.Fatalf("S=%d key %q: iterator missed %+v", s, mk, want)
			}
			if g.Kind != want.Kind || string(g.Value) != string(want.Value) || g.VersionTs != want.VersionTs {
				t.Fatalf("S=%d key %q: iterator %+v != GetAt %+v", s, mk, g, want)
			}
			if (g.Checkpoint == nil) != (want.Checkpoint == nil) ||
				(g.Checkpoint != nil && g.Checkpoint.ID != want.Checkpoint.ID) {
				t.Fatalf("S=%d key %q: checkpoint mismatch: %+v vs %+v", s, mk, g.Checkpoint, want.Checkpoint)
			}
		}
		// CollapseSummaries: every tuple appears exactly once, at its first
		// key in order.
		cit := snap.NewIterator(IterOptions{CollapseSummaries: true})
		seen := map[uint64]string{}
		prevKey = ""
		for cit.Next() {
			r := cit.Result()
			if string(r.Key) <= prevKey {
				t.Fatalf("collapsed iterator key order broken at %q", r.Key)
			}
			prevKey = string(r.Key)
			if r.Kind == ResultSummarized {
				id := r.Checkpoint.ID
				if firstKey, dup := seen[id]; dup {
					t.Fatalf("tuple %d emitted twice: at %q and %q", id, firstKey, r.Key)
				}
				seen[id] = string(r.Key)
			}
		}
		if err := cit.Err(); err != nil {
			t.Fatal(err)
		}
		cit.Close()
	}

	// Phase F: reopen on the same FS; the model still predicts every read.
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	stack2 := newSemStack(t, fs, "db", llm.NewMock(llm.MockValid), 0)
	o2 := semOptions(fs, clk, stack2)
	o2.MinCompressionRatio = 0.5
	db2, err := Open("db", o2)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	for i := 0; i < 50; i++ {
		key := keys[rng.Intn(len(keys))]
		s := uint64(1 + rng.Int63n(int64(maxTs)))
		probeDBAfterReopen(t, db2, model, versions, entries, key, s)
	}
	rep, err := db2.VerifyIntegrity(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(rep.Problems) > 0 {
		t.Fatalf("VerifyIntegrity problems: %v", rep.Problems)
	}
	if rep.Tuples == 0 {
		t.Fatal("no tuples after reopen")
	}
}

// probeDBAfterReopen is probeDB against a reopened DB (the closure in
// runMVCCSeed captured the closed DB).
func probeDBAfterReopen(t *testing.T, db *DB, model *testmodel.Model, versions map[string][]base.Record, entries map[string][]mvccEntry, key []byte, s uint64) {
	t.Helper()
	modelMu := &sync.Mutex{}
	modelMu.Lock()
	outcome, rawRec, best := expectedAt(model, versions, entries[string(key)], key, s)
	modelMu.Unlock()
	res, err := db.GetAt(key, s)
	if err != nil {
		t.Fatalf("reopen GetAt %q@%d: %v", key, s, err)
	}
	switch outcome {
	case resolve.NotFound:
		if res.Kind != ResultNotFound {
			t.Fatalf("reopen key %q@%d: want NotFound, got %+v", key, s, res)
		}
	case resolve.UseRaw:
		if res.Kind != ResultExact || res.VersionTs != rawRec.Ts || string(res.Value) != string(rawRec.Value) {
			t.Fatalf("reopen key %q@%d: want Exact (%d,%q), got %+v", key, s, rawRec.Ts, rawRec.Value, res)
		}
	case resolve.UseSummary:
		if res.Kind != ResultSummarized || res.Checkpoint == nil || res.Checkpoint.ID != best.tupleID {
			t.Fatalf("reopen key %q@%d: want Summarized (tuple %d), got %+v", key, s, best.tupleID, res)
		}
	default:
		t.Fatalf("reopen key %q@%d: B6 says Invariant", key, s)
	}
}
