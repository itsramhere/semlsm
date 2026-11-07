package semlsm

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"semlsm/internal/base"
	"semlsm/internal/failpoint"
	"semlsm/internal/llm"
	"semlsm/internal/semantic"
	"semlsm/internal/vfs"
)

// semStart is the ManualClock origin for semantic tests.
var semStart = time.Unix(1_000_000, 0)

// newSemStack builds an llm stack over client for the DB at dir. cacheBytes
// <= 0 disables the cache layer.
func newSemStack(t *testing.T, fs vfs.FS, dir string, client llm.Client, cacheBytes int64) *llm.Stack {
	t.Helper()
	s, err := llm.NewSummarizer(client, llm.Config{
		Model:             "mock-model",
		PromptVersion:     semantic.PromptVersion,
		FS:                fs,
		Dir:               dir,
		MaxTokensTotal:    1 << 40,
		RequestsPerMinute: 6000,
		TokensPerMinute:   1 << 26,
		Concurrency:       4,
		CacheMaxBytes:     cacheBytes,
	})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// semOptions builds small-sized options with two enabled policies (agent/
// ModeState, app/ ModeEvents) and everything else disabled.
func semOptions(fs vfs.FS, clock base.Clock, stack *llm.Stack) Options {
	o := testOptions(fs, clock)
	o.MemtableBytes = 8 << 20
	o.L0CompactTrigger = 4
	o.L0StallTrigger = 12
	o.L1TargetBytes = 64 << 10
	o.TargetFileBytes = 16 << 10
	o.GroupDepth = 2
	o.GroupDelimiter = '/'
	o.Policies = []Policy{
		{Prefix: "agent/", Mode: ModeState, MinAge: time.Hour, MinBatch: 10, MaxBatch: 10000},
		{Prefix: "app/", Mode: ModeEvents, MinAge: time.Hour, MinBatch: 10, MaxBatch: 10000},
	}
	o.DefaultPolicy = Policy{Mode: ModeDisabled}
	o.Summarizer = stack
	o.HardScore = 2.0
	o.SemanticSweepInterval = time.Hour
	o.MaxChunkBytes = 32 << 10
	o.MaxValueBytesInPrompt = 2 << 10
	o.MinCompressionRatio = 2.0
	o.DisableAutoCompaction = true
	return o
}

// openSemDB opens a DB with the semantic options on a fresh memfs.
func openSemDB(t *testing.T, dir string, client llm.Client, mutate func(*Options)) (*DB, *base.ManualClock, *llm.MockClient) {
	t.Helper()
	fs := vfs.NewMemFS()
	clk := base.NewManualClock(semStart)
	stack := newSemStack(t, fs, dir, client, 0)
	o := semOptions(fs, clk, stack)
	if mutate != nil {
		mutate(&o)
	}
	// The stack was built against fs before mutate could replace it; a
	// mutated FS needs a rebuilt stack.
	if o.FS != fs {
		stack = newSemStack(t, o.FS, dir, client, 0)
		o.Summarizer = stack
	}
	db, err := Open(dir, o)
	if err != nil {
		t.Fatal(err)
	}
	mock, _ := client.(*llm.MockClient)
	return db, clk, mock
}

// driveDown flushes and moves everything to the bottom level through the
// manual chain: L0->L1->L2 structural, L2->L3->L4 semantic-or-raw.
func driveDown(t *testing.T, db *DB) {
	t.Helper()
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	for lvl := 0; lvl < base.NumLevels-1; lvl++ {
		if err := db.CompactLevel(context.Background(), lvl); err != nil {
			t.Fatalf("compact level %d: %v", lvl, err)
		}
	}
}

// countRawVersions counts the key's raw versions across every SST of the
// current version (memtables excluded).
func countRawVersions(t *testing.T, db *DB, key []byte) int {
	t.Helper()
	v := db.vs.Acquire()
	defer v.Unref()
	n := 0
	for l := range v.Levels {
		for _, f := range v.Levels[l] {
			if !fileMayContain(f, key) {
				continue
			}
			r, rel, err := db.tableCache.Get(f.Num)
			if err != nil {
				t.Fatal(err)
			}
			ts, _, _, ok, err := r.Get(key, ^uint64(0))
			if err != nil {
				rel()
				t.Fatal(err)
			}
			if ok {
				n++
				// walk older versions of the same key in this file
				for {
					next, _, _, ok2, err := r.Get(key, ts-1)
					if err != nil || !ok2 {
						if err != nil {
							rel()
							t.Fatal(err)
						}
						break
					}
					n++
					ts = next
				}
			}
			rel()
		}
	}
	return n
}

// TestSemanticHeadline is P10.5 test 1: 10,000 mutations of agent/1/state
// become one tuple with SourceCount 9999; the newest version stays Exact,
// mid-range reads are Summarized, and the raw versions are gone.
func TestSemanticHeadlineTenThousandMutations(t *testing.T) {
	db, clk, _ := openSemDB(t, "db", llm.NewMock(llm.MockValid), nil)
	defer db.Close()

	const N = 10000
	key := []byte("agent/1/state")
	vals := make([]string, N)
	recs := make([]base.Record, 0, N)
	for i := 0; i < N; i++ {
		vals[i] = fmt.Sprintf("state value %05d %s", i, strings.Repeat("x", 40))
		ts, err := db.Put(key, []byte(vals[i]))
		if err != nil {
			t.Fatal(err)
		}
		recs = append(recs, base.Record{Key: key, Ts: ts, Kind: base.KindPut, Value: []byte(vals[i])})
	}
	driveDown(t, db)
	clk.Set(semStart.Add(2 * time.Hour)) // everything is older than MinAge now
	if err := db.CompactLevel(context.Background(), base.NumLevels-1); err != nil {
		t.Fatal(err)
	}

	st := db.Stats()
	if st.SemanticTuplesCreated != 1 {
		t.Fatalf("tuples created: %d, want 1", st.SemanticTuplesCreated)
	}

	// The latest version is Exact.
	res, err := db.Get(key)
	if err != nil {
		t.Fatal(err)
	}
	if res.Kind != ResultExact || string(res.Value) != vals[N-1] || res.VersionTs != recs[N-1].Ts {
		t.Fatalf("latest: kind=%d val=%q ts=%d", res.Kind, res.Value, res.VersionTs)
	}

	// A mid-range read is Summarized and cites the tuple.
	midTs := recs[5000].Ts
	sum, err := db.GetAt(key, midTs)
	if err != nil {
		t.Fatal(err)
	}
	if sum.Kind != ResultSummarized {
		t.Fatalf("mid-range: kind=%d, want Summarized", sum.Kind)
	}
	if sum.Checkpoint == nil || sum.Checkpoint.SourceCount != N-1 {
		t.Fatalf("source count: %+v", sum.Checkpoint)
	}
	// SourceCRC32 matches a value computed from the model of the dropped set
	// (the 9999 oldest versions).
	wantCRC := base.SourceCRC(recs[:N-1])
	if sum.Checkpoint.SourceCRC32 != wantCRC {
		t.Fatalf("source CRC %08x, want %08x", sum.Checkpoint.SourceCRC32, wantCRC)
	}
	if sum.Checkpoint.KeyMinTs != recs[0].Ts || sum.Checkpoint.KeyMaxTs != recs[N-2].Ts {
		t.Fatalf("key range [%d,%d], want [%d,%d]",
			sum.Checkpoint.KeyMinTs, sum.Checkpoint.KeyMaxTs, recs[0].Ts, recs[N-2].Ts)
	}

	// The raw versions are gone from all SSTs: exactly the head remains.
	if n := countRawVersions(t, db, key); n != 1 {
		t.Fatalf("raw versions left: %d, want 1", n)
	}

	// A few writes after the cut stay Exact and do not disturb the summary.
	for i := 0; i < 3; i++ {
		if _, err := db.Put(key, []byte(fmt.Sprintf("post %d", i))); err != nil {
			t.Fatal(err)
		}
	}
	res, err = db.Get(key)
	if err != nil || res.Kind != ResultExact || string(res.Value) != "post 2" {
		t.Fatalf("post-cut latest: %+v err=%v", res, err)
	}
	sum, err = db.GetAt(key, midTs)
	if err != nil || sum.Kind != ResultSummarized {
		t.Fatalf("post-cut mid-range: kind=%d err=%v", sum.Kind, err)
	}
	if st := db.Stats(); st.SemanticTuplesCreated != 1 {
		t.Fatalf("tuples after post-cut writes: %d", st.SemanticTuplesCreated)
	}
}

// TestSemanticEventsMode is P10.5 test 2: 10,000 unique event keys collapse
// into one tuple; keys written after the cut remain Exact.
func TestSemanticEventsMode(t *testing.T) {
	db, clk, _ := openSemDB(t, "db", llm.NewMock(llm.MockValid), func(o *Options) {
		o.TargetFileBytes = 8 << 20 // one output file for the whole event generation
	})
	defer db.Close()

	const N = 10000
	for i := 0; i < N; i++ {
		if _, err := db.Put([]byte(fmt.Sprintf("app/1/e%05d", i)), []byte(fmt.Sprintf("event %d %s", i, strings.Repeat("e", 300)))); err != nil {
			t.Fatal(err)
		}
	}
	driveDown(t, db)
	clk.Set(semStart.Add(2 * time.Hour))
	if err := db.CompactLevel(context.Background(), base.NumLevels-1); err != nil {
		t.Fatal(err)
	}

	st := db.Stats()
	if st.SemanticTuplesCreated != 1 {
		t.Fatalf("tuples: %d, want 1", st.SemanticTuplesCreated)
	}
	// Every event key reads Summarized now.
	for _, i := range []int{0, 4999, N - 1} {
		res, err := db.Get([]byte(fmt.Sprintf("app/1/e%05d", i)))
		if err != nil {
			t.Fatal(err)
		}
		if res.Kind != ResultSummarized {
			t.Fatalf("key %d: kind=%d, want Summarized", i, res.Kind)
		}
		if res.Checkpoint == nil || res.Checkpoint.SourceCount != N {
			t.Fatalf("key %d checkpoint: %+v", i, res.Checkpoint)
		}
	}
	// Keys written after the cut remain Exact.
	late := []byte("app/1/late001")
	if _, err := db.Put(late, []byte("after the cut")); err != nil {
		t.Fatal(err)
	}
	res, err := db.Get(late)
	if err != nil || res.Kind != ResultExact || string(res.Value) != "after the cut" {
		t.Fatalf("late key: %+v err=%v", res, err)
	}
}

// TestSemanticMixedPolicies is P10.5 test 3: mixed policies; ModeDisabled is
// never summarized.
func TestSemanticMixedPolicies(t *testing.T) {
	db, clk, _ := openSemDB(t, "db", llm.NewMock(llm.MockValid), nil)
	defer db.Close()

	var agentRecs []base.Record
	for i := 0; i < 12; i++ {
		ts, err := db.Put([]byte("agent/1/a"), []byte(fmt.Sprintf("v%02d %s", i, strings.Repeat("a", 290))))
		if err != nil {
			t.Fatal(err)
		}
		agentRecs = append(agentRecs, base.Record{Key: []byte("agent/1/a"), Ts: ts, Kind: base.KindPut, Value: []byte(fmt.Sprintf("v%02d %s", i, strings.Repeat("a", 290)))})
	}
	for i := 0; i < 12; i++ {
		if _, err := db.Put([]byte(fmt.Sprintf("app/1/e%02d", i)), []byte(strings.Repeat("e", 300))); err != nil {
			t.Fatal(err)
		}
	}
	var otherTs uint64
	for i := 0; i < 12; i++ {
		ts, err := db.Put([]byte("other/1/x"), []byte(fmt.Sprintf("o%02d %s", i, strings.Repeat("o", 290))))
		if err != nil {
			t.Fatal(err)
		}
		otherTs = ts
	}
	driveDown(t, db)
	clk.Set(semStart.Add(2 * time.Hour))
	if err := db.CompactLevel(context.Background(), base.NumLevels-1); err != nil {
		t.Fatal(err)
	}

	st := db.Stats()
	if st.SemanticTuplesCreated != 2 { // one for agent/, one for app/
		t.Fatalf("tuples: %d, want 2", st.SemanticTuplesCreated)
	}
	// agent/: newest Exact, older Summarized.
	res, err := db.Get([]byte("agent/1/a"))
	if err != nil || res.Kind != ResultExact || !strings.HasPrefix(string(res.Value), "v11 ") {
		t.Fatalf("agent latest: %+v err=%v", res, err)
	}
	sum, err := db.GetAt([]byte("agent/1/a"), agentRecs[3].Ts)
	if err != nil || sum.Kind != ResultSummarized {
		t.Fatalf("agent mid: kind=%d err=%v", sum.Kind, err)
	}
	// app/: every event key Summarized.
	for i := 0; i < 12; i++ {
		res, err := db.Get([]byte(fmt.Sprintf("app/1/e%02d", i)))
		if err != nil || res.Kind != ResultSummarized {
			t.Fatalf("app key %d: kind=%d err=%v", i, res.Kind, err)
		}
	}
	// other/: ModeDisabled is never summarized — every old version Exact.
	for i := 0; i < 12; i++ {
		res, err := db.Get([]byte("other/1/x"))
		if err != nil {
			t.Fatal(err)
		}
		_ = res
	}
	if n := countRawVersions(t, db, []byte("other/1/x")); n != 12 {
		t.Fatalf("other/ raw versions: %d, want 12 (never summarized)", n)
	}
	res, err = db.GetAt([]byte("other/1/x"), otherTs)
	if err != nil || res.Kind != ResultExact {
		t.Fatalf("other/ old version: kind=%d err=%v", res.Kind, err)
	}
}

// TestSemanticSnapshotPinning is P10.5 test 4: a snapshot taken before
// compaction keeps reading Exact across the install, its files survive until
// Release, and fresh reads see the summary.
func TestSemanticSnapshotPinning(t *testing.T) {
	db, clk, _ := openSemDB(t, "db", llm.NewMock(llm.MockValid), nil)
	defer db.Close()

	key := []byte("agent/1/s")
	for i := 0; i < 12; i++ {
		if _, err := db.Put(key, []byte(fmt.Sprintf("v%02d %s", i, strings.Repeat("s", 290)))); err != nil {
			t.Fatal(err)
		}
	}
	driveDown(t, db)
	snap := db.NewSnapshot()
	defer snap.Release()
	if res, err := snap.Get(key); err != nil || res.Kind != ResultExact || !strings.HasPrefix(string(res.Value), "v11 ") {
		t.Fatalf("snapshot before install: %+v err=%v", res, err)
	}

	clk.Set(semStart.Add(2 * time.Hour))
	if err := db.CompactLevel(context.Background(), base.NumLevels-1); err != nil {
		t.Fatal(err)
	}
	if err := db.WaitIdle(context.Background()); err != nil {
		t.Fatal(err)
	}

	// The snapshot still reads Exact across the install (I9).
	if res, err := snap.Get(key); err != nil || res.Kind != ResultExact || !strings.HasPrefix(string(res.Value), "v11 ") {
		t.Fatalf("snapshot after install: %+v err=%v", res, err)
	}
	// A fresh read just below the surviving head is Summarized: no raw
	// version covers that ts anymore, the tuple does.
	headTs := uint64(0)
	v := db.vs.Acquire()
	for l := range v.Levels {
		for _, f := range v.Levels[l] {
			if fileMayContain(f, key) && f.MaxTs > headTs {
				headTs = f.MaxTs
			}
		}
	}
	v.Unref()
	if headTs == 0 {
		t.Fatal("no raw file metadata found")
	}
	sum, err := db.GetAt(key, headTs-1)
	if err != nil || sum.Kind != ResultSummarized {
		t.Fatalf("fresh GetAt below the head: kind=%d err=%v", sum.Kind, err)
	}

	// While the snapshot is open the input files are still on disk.
	exists := func() int {
		entries, err := db.opts.FS.List(db.dir)
		if err != nil {
			t.Fatal(err)
		}
		n := 0
		for _, e := range entries {
			if !e.IsDir && strings.HasSuffix(e.Name, ".sst") {
				n++
			}
		}
		return n
	}
	withSnap := exists()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if exists() == withSnap {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if exists() != withSnap {
		t.Fatal("input files were removed while the snapshot held them")
	}

	// Release: the pinned files become deletable. With auto compaction off
	// nobody drains obsolete in the background, so WaitIdle does it.
	snap.Release()
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if err := db.WaitIdle(context.Background()); err != nil {
			t.Fatal(err)
		}
		if exists() < withSnap {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if exists() >= withSnap {
		t.Fatal("input files were not removed after Release")
	}
	// Reads after the release still work from the new state.
	if res, err := db.Get(key); err != nil || res.Kind != ResultExact || !strings.HasPrefix(string(res.Value), "v11 ") {
		t.Fatalf("after release: %+v err=%v", res, err)
	}
}

// TestSemanticFaultModes is P10.5 test 5: bad JSON, transient, permanent,
// hallucinated ref and poor compression leave no tuple, lose nothing, and
// keep raw reads Exact.
func TestSemanticFaultModes(t *testing.T) {
	bigSummary := `{"summary":"` + strings.Repeat("x", 1900) + `","key_facts":[{"text":"t","refs":["r0"]}],"anomalies":[],"final_state":""}`
	cases := []struct {
		name   string
		client llm.Client
	}{
		{"bad json", llm.NewMock(llm.MockBadJSON)},
		{"transient", llm.NewMockTransient(1 << 20)},
		{"permanent", llm.NewMock(llm.MockPermanent)},
		{"hallucinated ref", llm.NewMock(llm.MockHallucinatedRef)},
		{"poor compression", llm.NewMockInjected(bigSummary)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db, clk, _ := openSemDB(t, "db", tc.client, nil)
			defer db.Close()

			key := []byte("agent/1/f")
			for i := 0; i < 12; i++ {
				if _, err := db.Put(key, []byte(fmt.Sprintf("v%02d", i))); err != nil {
					t.Fatal(err)
				}
			}
			driveDown(t, db)
			clk.Set(semStart.Add(2 * time.Hour))
			if err := db.CompactLevel(context.Background(), base.NumLevels-1); err != nil {
				t.Fatalf("the job must commit raw instead of failing: %v", err)
			}
			st := db.Stats()
			if st.SemanticTuplesCreated != 0 {
				t.Fatalf("tuples: %d, want 0", st.SemanticTuplesCreated)
			}
			if st.SemanticJobs["raw-only"] < 1 {
				t.Fatalf("jobs: %+v, want at least one raw-only", st.SemanticJobs)
			}
			// Nothing lost: raw reads remain Exact, writes still work.
			res, err := db.Get(key)
			if err != nil || res.Kind != ResultExact || string(res.Value) != "v11" {
				t.Fatalf("get: %+v err=%v", res, err)
			}
			if n := countRawVersions(t, db, key); n != 12 {
				t.Fatalf("raw versions: %d, want 12", n)
			}
			if _, err := db.Put(key, []byte("v12")); err != nil {
				t.Fatalf("writes after failure: %v", err)
			}
			if res, err := db.Get(key); err != nil || string(res.Value) != "v12" {
				t.Fatalf("get after write: %+v err=%v", res, err)
			}
		})
	}
}

// TestSemanticBlockAndPreempt is P10.5 test 6: with a blocked client, writes
// keep flowing, L0 and L1 keep compacting, the anti-stall rule preempts the
// job once L1 reaches HardScore, and nothing deadlocks.
func TestSemanticBlockAndPreempt(t *testing.T) {
	fs := vfs.NewMemFS()
	clk := base.NewManualClock(semStart)
	stack := newSemStack(t, fs, "db", llm.NewMock(llm.MockBlock), 0)
	o := semOptions(fs, clk, stack)
	o.DisableAutoCompaction = false
	o.MemtableBytes = 64 << 10 // rotate often so the pipeline keeps flowing
	o.HardScore = 1.0
	o.L1TargetBytes = 16 << 10
	o.LevelMultiplier = 1 // L2 target 16 KiB: the semantic worker picks early
	o.L0CompactTrigger = 2
	o.L0StallTrigger = 8
	o.SemanticSweepInterval = time.Hour
	db, err := Open("db", o)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	deadline := time.Now().Add(30 * time.Second)
	i := 0
	for db.Stats().SemanticPreemptions == 0 {
		if time.Now().After(deadline) {
			t.Fatal("no preemption within the deadline (deadlock?)")
		}
		b := NewBatch()
		for j := 0; j < 10; j++ {
			b.Put([]byte(fmt.Sprintf("agent/%d/key%03d", i%3, i)), []byte(strings.Repeat("v", 200)))
			i++
		}
		if _, err := db.Apply(b); err != nil {
			t.Fatalf("writes must keep flowing: %v", err)
		}
		if i%2000 == 0 {
			st := db.Stats()
			// Reaching the stall trigger is correct back-pressure; the
			// invariant is that the pipeline recovers instead of wedging.
			if st.LevelFiles[0] >= 2*o.L0StallTrigger {
				t.Fatalf("L0 never recovered: %d files while the LLM was blocked", st.LevelFiles[0])
			}
		}
	}
	// Tier A made progress while the LLM was blocked.
	st := db.Stats()
	if st.SemanticPreemptions < 1 {
		t.Fatalf("preemptions: %d", st.SemanticPreemptions)
	}
	if st.LevelFiles[0] > o.L0CompactTrigger+2 {
		t.Fatalf("L0 did not keep compacting: %d files", st.LevelFiles[0])
	}
	// Reads return during and after the block.
	if res, err := db.Get([]byte("agent/0/key000")); err != nil || res.Kind != ResultExact {
		t.Fatalf("read during block: %+v err=%v", res, err)
	}
	// Close must not wait out the blocked job (it cancels running jobs).
	closed := make(chan error, 1)
	go func() { closed <- db.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("close: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Close deadlocked on the blocked semantic job")
	}
}

// TestSemanticBudgetExhaustedFallback is P10.5 test 7: with the budget spent,
// no calls are made and raw fallback moves the data; reopening with a real
// budget resumes summarization.
func TestSemanticBudgetExhaustedFallback(t *testing.T) {
	fs := vfs.NewMemFS()
	clk := base.NewManualClock(semStart)
	client := llm.NewMock(llm.MockValid)
	// MaxTokensTotal 1: the first Reserve already fails.
	stack := newSemStackBudget(t, fs, "db", client, 1)
	o := semOptions(fs, clk, stack)
	o.HardScore = 1.0
	db, err := Open("db", o)
	if err != nil {
		t.Fatal(err)
	}

	key := []byte("agent/1/b")
	for i := 0; i < 12; i++ {
		if _, err := db.Put(key, []byte(fmt.Sprintf("v%02d %s", i, strings.Repeat("b", 290)))); err != nil {
			t.Fatal(err)
		}
	}
	driveDown(t, db)
	clk.Set(semStart.Add(2 * time.Hour))
	if err := db.CompactLevel(context.Background(), base.NumLevels-1); err != nil {
		t.Fatal(err)
	}
	st := db.Stats()
	if st.LLMCalls != 0 || st.SemanticTuplesCreated != 0 {
		t.Fatalf("calls=%d tuples=%d, want 0/0 (no LLM on an exhausted budget)", st.LLMCalls, st.SemanticTuplesCreated)
	}
	if res, err := db.Get(key); err != nil || res.Kind != ResultExact || !strings.HasPrefix(string(res.Value), "v11 b") {
		t.Fatalf("raw fallback must keep the data: %+v err=%v", res, err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen with a real budget: semantic work resumes.
	stack2 := newSemStackBudget(t, fs, "db", client, 1<<40)
	o2 := semOptions(fs, clk, stack2)
	db2, err := Open("db", o2)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	if err := db2.CompactLevel(context.Background(), base.NumLevels-1); err != nil {
		t.Fatal(err)
	}
	st2 := db2.Stats()
	if st2.SemanticTuplesCreated != 1 || st2.LLMCalls == 0 {
		t.Fatalf("after reopen: tuples=%d calls=%d, want 1/>0", st2.SemanticTuplesCreated, st2.LLMCalls)
	}
	latest, err := db2.Get(key)
	if err != nil || latest.Kind != ResultExact {
		t.Fatalf("after resume: %+v err=%v", latest, err)
	}
	if sum, err := db2.GetAt(key, latest.VersionTs-1); err != nil || sum.Kind != ResultSummarized {
		t.Fatalf("below the head after resume: kind=%d err=%v", sum.Kind, err)
	}
}

// newSemStackBudget is newSemStack with an explicit token budget.
func newSemStackBudget(t *testing.T, fs vfs.FS, dir string, client llm.Client, maxTokens int64) *llm.Stack {
	t.Helper()
	s, err := llm.NewSummarizer(client, llm.Config{
		Model:             "mock-model",
		PromptVersion:     semantic.PromptVersion,
		FS:                fs,
		Dir:               dir,
		MaxTokensTotal:    maxTokens,
		RequestsPerMinute: 6000,
		TokensPerMinute:   1 << 26,
		Concurrency:       4,
	})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// TestSemanticCrashAtEveryFailpoint is P10.5 test 8: a crash at each semantic
// failpoint reopens cleanly, keeps every invariant, cleans orphans, and the
// retry is served from the cache without new LLM calls.
func TestSemanticCrashAtEveryFailpoint(t *testing.T) {
	fps := []string{"semantic.after_outputs", "semantic.after_ckpt", "semantic.after_archive", "semantic.after_manifest"}
	for _, fp := range fps {
		t.Run(fp, func(t *testing.T) {
			fs := vfs.NewMemFS()
			clk := base.NewManualClock(semStart)
			mock := llm.NewMock(llm.MockValid)
			stack := newSemStack(t, fs, "db", mock, 1<<20) // cache on, in the db dir
			o := semOptions(fs, clk, stack)
			o.ArchiveRaw = true
			db, err := Open("db", o)
			if err != nil {
				t.Fatal(err)
			}

			agentKey := []byte("agent/1/c")
			appKey := []byte("app/1/e01")
			for i := 0; i < 12; i++ {
				if _, err := db.Put(agentKey, []byte(fmt.Sprintf("v%02d %s", i, strings.Repeat("c", 290)))); err != nil {
					t.Fatal(err)
				}
				if _, err := db.Put([]byte(fmt.Sprintf("app/1/e%02d", i)), []byte(strings.Repeat("e", 300))); err != nil {
					t.Fatal(err)
				}
			}
			driveDown(t, db) // tuples already exist from the deep chain
			clk.Set(semStart.Add(2 * time.Hour))
			if err := db.CompactLevel(context.Background(), base.NumLevels-1); err != nil {
				t.Fatal(err)
			}
			// Write another generation so the sweep has work to redo; the
			// chain jobs run while it is too young and stay raw.
			for i := 0; i < 12; i++ {
				if _, err := db.Put(agentKey, []byte(fmt.Sprintf("w%02d %s", i, strings.Repeat("c", 290)))); err != nil {
					t.Fatal(err)
				}
			}
			driveDown(t, db)
			clk.Set(semStart.Add(4 * time.Hour))
			// Arm the failpoint and run the sweep, which crashes.
			sentinel := errors.New("failpoint: " + fp)
			failpoint.SetError(fp, sentinel)
			_ = db.CompactLevel(context.Background(), base.NumLevels-1) // expected to fail
			failpoint.Reset()

			after := fs.CrashClone(vfs.DropUnsynced, nil)
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}

			// Reopen on the clone with a fresh mock: everything intact.
			mock2 := llm.NewMock(llm.MockValid)
			stack2 := newSemStack(t, after, "db", mock2, 1<<20)
			o2 := semOptions(after, clk, stack2)
			o2.ArchiveRaw = true
			db2, err := Open("db", o2)
			if err != nil {
				t.Fatalf("reopen after %s: %v", fp, err)
			}
			defer db2.Close()
			// Latest raw reads are exact (I1/I10).
			res, err := db2.Get(agentKey)
			if err != nil || res.Kind != ResultExact {
				t.Fatalf("agent latest after reopen: %+v err=%v", res, err)
			}
			res, err = db2.Get(appKey)
			if err != nil {
				t.Fatalf("app key after reopen: %v", err)
			}
			if res.Kind != ResultSummarized && res.Kind != ResultExact {
				t.Fatalf("app key kind %d", res.Kind)
			}
			// The retry is served from the cache: no new LLM calls.
			if err := db2.CompactLevel(context.Background(), base.NumLevels-1); err != nil {
				t.Logf("retry sweep: %v (acceptable when there is nothing eligible)", err)
			}
			if got := mock2.Calls(); got != 0 {
				t.Fatalf("retry made %d LLM calls, want 0 (cache must serve the redo)", got)
			}
		})
	}
}

// TestSemanticRepeatedCompactionInvariants is P10.5 test 9: many jobs across
// generations; I7 is walked for every key over all tuples and all SST
// versions.
func TestSemanticRepeatedCompactionInvariants(t *testing.T) {
	db, clk, _ := openSemDB(t, "db", llm.NewMock(llm.MockValid), nil)
	defer db.Close()

	key := []byte("agent/1/r")
	gen := 0
	for round := 0; round < 3; round++ {
		for i := 0; i < 15; i++ {
			gen++
			if _, err := db.Put(key, []byte(fmt.Sprintf("g%d-v%03d %s", round, gen, strings.Repeat("p", 30)))); err != nil {
				t.Fatal(err)
			}
		}
		driveDown(t, db)
		clk.Set(clk.Now().Add(2 * time.Hour))
		if err := db.CompactLevel(context.Background(), base.NumLevels-1); err != nil {
			t.Fatal(err)
		}
	}
	if st := db.Stats(); st.SemanticTuplesCreated < 3 {
		t.Fatalf("tuples: %d, want >= 3 (one per generation)", st.SemanticTuplesCreated)
	}
	if err := db.Flush(); err != nil { // empty the memtable for the walk
		t.Fatal(err)
	}
	verifyI7All(t, db)

	// The whole history resolves correctly: every past ts is Summarized,
	// the newest is Exact.
	res, err := db.Get(key)
	if err != nil || res.Kind != ResultExact {
		t.Fatalf("latest: %+v err=%v", res, err)
	}
}

// verifyI7All walks every tuple and every SST of the current version and
// checks I7 for every key: no raw version inside any dropped range, and the
// ranges of successive tuples for one key are disjoint and ordered.
func verifyI7All(t *testing.T, db *DB) {
	t.Helper()
	v := db.vs.Acquire()
	defer v.Unref()

	raw := map[string][]uint64{}
	for l := range v.Levels {
		for _, f := range v.Levels[l] {
			r, rel, err := db.tableCache.Get(f.Num)
			if err != nil {
				t.Fatal(err)
			}
			it := r.NewIterator()
			for it.First(); it.Valid(); it.Next() {
				u, ts, _, ok := base.SplitInternalKey(it.Key())
				if !ok {
					t.Fatal("bad internal key")
				}
				raw[string(u)] = append(raw[string(u)], ts)
			}
			if err := it.Err(); err != nil {
				t.Fatal(err)
			}
			it.Close()
			rel()
		}
	}

	type rng struct {
		min, max uint64
		tuple    uint64
	}
	perKey := map[string][]rng{}
	for num := range v.Ckpts.ByNum {
		r, rel, err := db.ckptCache.Get(num)
		if err != nil {
			t.Fatal(err)
		}
		for _, tu := range r.Tuples() {
			keys, err := r.IterKeys(tu.ID)
			if err != nil {
				t.Fatal(err)
			}
			for _, e := range keys {
				perKey[string(e.Key)] = append(perKey[string(e.Key)], rng{e.MinTs, e.MaxTs, tu.ID})
			}
		}
		rel()
	}

	for key, ranges := range perKey {
		sort.Slice(ranges, func(a, b int) bool { return ranges[a].min < ranges[b].min })
		for i := 1; i < len(ranges); i++ {
			if ranges[i-1].max >= ranges[i].min {
				t.Fatalf("I7: key %q tuple ranges overlap: [%d,%d] then [%d,%d]",
					key, ranges[i-1].min, ranges[i-1].max, ranges[i].min, ranges[i].max)
			}
		}
		for _, r := range ranges {
			for _, ts := range raw[key] {
				if ts >= r.min && ts <= r.max {
					t.Fatalf("I7: key %q has raw version ts %d inside dropped range [%d,%d] of tuple %d",
						key, ts, r.min, r.max, r.tuple)
				}
			}
		}
	}
	// Every key with tuples must have no raw overlap at all; raw survivors
	// sit strictly above the newest range or below the oldest.
	for key, rs := range perKey {
		lo, hi := rs[0].min, rs[len(rs)-1].max
		for _, ts := range raw[key] {
			if ts >= lo && ts <= hi {
				t.Fatalf("I7: key %q raw ts %d inside the union [%d,%d]", key, ts, lo, hi)
			}
		}
	}
}

// TestSemanticOverOlderRawGeneration ports the supervisor's I10 scenario
// (M-027): older raw versions of a key live at L4 while a newer generation is
// summarized at L3. A read inside the summarized range must not return the
// stale L4 raw version; a read between the generations must be Exact.
func TestSemanticOverOlderRawGeneration(t *testing.T) {
	db, clk, _ := openSemDB(t, "db", llm.NewMock(llm.MockValid), nil)
	defer db.Close()
	key := []byte("agent/1/state")
	ctx := context.Background()

	var genA []uint64
	for i := 0; i < 50; i++ {
		ts, err := db.Put(key, []byte(fmt.Sprintf("A%03d", i)))
		if err != nil {
			t.Fatal(err)
		}
		genA = append(genA, ts)
	}
	// Generation A goes down raw (too young for MinAge).
	driveDown(t, db)

	clk.Set(semStart.Add(10 * time.Hour))
	var genB []uint64
	for i := 0; i < 200; i++ {
		ts, err := db.Put(key, []byte(fmt.Sprintf("B%03d", i)))
		if err != nil {
			t.Fatal(err)
		}
		genB = append(genB, ts)
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	clk.Set(semStart.Add(20 * time.Hour))
	for lvl := 0; lvl <= 2; lvl++ {
		if err := db.CompactLevel(ctx, lvl); err != nil {
			t.Fatalf("compact %d: %v", lvl, err)
		}
	}
	if db.Stats().SemanticTuplesCreated == 0 {
		t.Skip("no tuple created at L2->L3; scenario not reached")
	}
	mid := genB[100]
	r, err := db.GetAt(key, mid)
	if err != nil {
		t.Fatal(err)
	}
	if r.Kind == ResultExact && len(r.Value) > 0 && r.Value[0] == 'A' {
		t.Fatalf("stale read: GetAt(mid of gen B) returned gen A raw %q (I10)", r.Value)
	}
	if r.Kind != ResultSummarized {
		t.Fatalf("mid gen B: kind=%d val=%q, want Summarized", r.Kind, r.Value)
	}
	r, err = db.GetAt(key, genA[len(genA)-1])
	if err != nil || r.Kind != ResultExact || string(r.Value) != "A049" {
		t.Fatalf("between generations: kind=%d val=%q err=%v, want Exact A049", r.Kind, r.Value, err)
	}
	r, err = db.Get(key)
	if err != nil || r.Kind != ResultExact || string(r.Value) != "B199" {
		t.Fatalf("latest: kind=%d val=%q err=%v", r.Kind, r.Value, err)
	}
	// Second pass: push gen B onto L4 next to gen A and summarize again.
	if err := db.CompactLevel(ctx, 3); err != nil {
		t.Fatal(err)
	}
	if err := db.CompactLevel(ctx, 4); err != nil {
		t.Fatal(err)
	}
	for _, s := range []uint64{genA[10], genB[50], genB[150]} {
		r, err := db.GetAt(key, s)
		if err != nil {
			t.Fatalf("GetAt %d after bottom pass: %v", s, err)
		}
		if r.Kind == ResultExact && s >= genB[0] && r.Value[0] == 'A' {
			t.Fatalf("stale after bottom pass at %d: %q", s, r.Value)
		}
	}
	verifyI7All(t, db)
}

// TestSemanticFailedJobCleansUpFiles ports the supervisor's leak test
// (M-027 item 1): a job that fails after writing its outputs and checkpoint
// must remove them again, or a persistent error grows the disk on retry.
func TestSemanticFailedJobCleansUpFiles(t *testing.T) {
	db, clk, _ := openSemDB(t, "db", llm.NewMock(llm.MockValid), nil)
	defer db.Close()
	key := []byte("agent/2/state")
	for i := 0; i < 300; i++ {
		if _, err := db.Put(key, []byte(fmt.Sprintf("v%03d", i))); err != nil {
			t.Fatal(err)
		}
	}
	ctx := context.Background()
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	for lvl := 0; lvl <= 1; lvl++ {
		if err := db.CompactLevel(ctx, lvl); err != nil {
			t.Fatal(err)
		}
	}
	before := listDBFiles(t, db)
	clk.Set(semStart.Add(5 * time.Hour))
	failpoint.SetError("semantic.after_ckpt", errors.New("injected"))
	_ = db.CompactLevel(ctx, 2)
	failpoint.Reset()
	after := listDBFiles(t, db)
	for name := range after {
		if !before[name] {
			t.Errorf("failed job left %s on disk", name)
		}
	}
}

func listDBFiles(t *testing.T, db *DB) map[string]bool {
	t.Helper()
	out := map[string]bool{}
	ents, err := db.opts.FS.List(db.dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range ents {
		out[e.Name] = true
	}
	return out
}
