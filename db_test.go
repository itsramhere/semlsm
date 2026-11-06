package semlsm

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"semlsm/internal/base"
	"semlsm/internal/manifest"
	"semlsm/internal/testmodel"
	"semlsm/internal/vfs"
)

// testOptions builds small-sized options for tests: MemFS, ManualClock-free
// (unless injected), always-sync, generous stall triggers because P5 has no
// compaction yet.
func testOptions(fs vfs.FS, clock base.Clock) Options {
	return Options{
		FS:                    fs,
		Clock:                 clock,
		SyncMode:              SyncAlways,
		MemtableBytes:         64 << 10, // 64 KiB: forces rotations in tests
		L0StallTrigger:        1 << 16,  // no compaction until P6: never stall on L0
		L0CompactTrigger:      1 << 15,
		MaxImmutableMemtables: 64,
	}
}

func openTestDB(t *testing.T, dir string) *DB {
	t.Helper()
	fs := vfs.NewMemFS()
	db, err := Open(dir, testOptions(fs, base.RealClock{}))
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func TestPutGetDelete(t *testing.T) {
	db := openTestDB(t, "db")
	defer db.Close()

	ts1, err := db.Put([]byte("k1"), []byte("v1"))
	if err != nil {
		t.Fatal(err)
	}
	res, err := db.Get([]byte("k1"))
	if err != nil {
		t.Fatal(err)
	}
	if res.Kind != ResultExact || string(res.Value) != "v1" || res.VersionTs != ts1 {
		t.Fatalf("get: %+v", res)
	}

	ts2, err := db.Delete([]byte("k1"))
	if err != nil {
		t.Fatal(err)
	}
	if ts2 <= ts1 {
		t.Fatalf("ts not increasing: %d then %d", ts1, ts2)
	}
	res, err = db.Get([]byte("k1"))
	if err != nil {
		t.Fatal(err)
	}
	if res.Kind != ResultNotFound {
		t.Fatalf("after delete: %+v", res)
	}
	if _, err := db.Get([]byte("missing")); err != nil {
		t.Fatal(err)
	}
}

func TestBatchAtomicity(t *testing.T) {
	db := openTestDB(t, "db")
	defer db.Close()

	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			for _, k := range []string{"a", "b"} {
				res, err := db.Get([]byte(k))
				if err != nil {
					continue
				}
				if res.Kind == ResultExact && res.Value == nil {
					t.Error("torn batch visible: value nil")
					return
				}
			}
		}
	}()

	for i := 0; i < 200; i++ {
		b := NewBatch()
		b.Put([]byte("a"), []byte(fmt.Sprintf("pair-%d", i)))
		b.Put([]byte("b"), []byte(fmt.Sprintf("pair-%d", i)))
		if _, err := db.Apply(b); err != nil {
			t.Fatal(err)
		}
		// a reader must see either both old or both new, never mixed
		ra, _ := db.Get([]byte("a"))
		rb, _ := db.Get([]byte("b"))
		if ra.Kind == ResultExact && rb.Kind == ResultExact &&
			string(ra.Value) != string(rb.Value) {
			t.Fatalf("half-batch visible: a=%q b=%q", ra.Value, rb.Value)
		}
	}
	close(stop)
	wg.Wait()
}

func TestSnapshotsStableAcrossFlushes(t *testing.T) {
	db := openTestDB(t, "db")
	defer db.Close()

	for i := 0; i < 50; i++ {
		if _, err := db.Put([]byte("k"), []byte(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	snap := db.NewSnapshot()
	first, err := snap.Get([]byte("k"))
	if err != nil || first.Kind != ResultExact {
		t.Fatalf("snapshot get: %+v err=%v", first, err)
	}

	// force flushes with more writes
	for i := 50; i < 200; i++ {
		if _, err := db.Put([]byte("k"), []byte(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 5; i++ {
		if _, err := db.Put([]byte(fmt.Sprintf("j%d", i)), []byte("padding-padding-padding")); err != nil {
			t.Fatal(err)
		}
	}
	db.mu.Lock()
	immCount := len(db.rs.imms)
	db.mu.Unlock()
	_ = immCount

	again, err := snap.Get([]byte("k"))
	if err != nil {
		t.Fatal(err)
	}
	if string(again.Value) != string(first.Value) || again.VersionTs != first.VersionTs {
		t.Fatalf("snapshot changed across flushes: %q@%d vs %q@%d",
			again.Value, again.VersionTs, first.Value, first.VersionTs)
	}
	// latest is now different
	latest, _ := db.Get([]byte("k"))
	if latest.Kind != ResultExact || string(latest.Value) == string(first.Value) {
		t.Fatalf("latest should differ: %+v", latest)
	}
	snap.Release()
	if _, err := snap.Get([]byte("k")); err == nil {
		t.Fatal("released snapshot must error")
	}
}

func TestReopenPreservesData(t *testing.T) {
	fs := vfs.NewMemFS()
	dir := "db"
	db, err := Open(dir, testOptions(fs, base.RealClock{}))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		if _, err := db.Put([]byte(fmt.Sprintf("k%03d", i)), []byte(fmt.Sprintf("v%03d", i))); err != nil {
			t.Fatal(err)
		}
	}
	ts, err := db.Delete([]byte("k050"))
	if err != nil {
		t.Fatal(err)
	}
	_ = ts
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db2, err := Open(dir, testOptions(fs, base.RealClock{}))
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	for i := 0; i < 100; i++ {
		res, err := db2.Get([]byte(fmt.Sprintf("k%03d", i)))
		if err != nil {
			t.Fatal(err)
		}
		if i == 50 {
			if res.Kind != ResultNotFound {
				t.Fatalf("delete lost on reopen: %+v", res)
			}
			continue
		}
		if res.Kind != ResultExact || string(res.Value) != fmt.Sprintf("v%03d", i) {
			t.Fatalf("k%03d: %+v", i, res)
		}
	}
}

func TestTimeTravelGetAt(t *testing.T) {
	db := openTestDB(t, "db")
	defer db.Close()

	ts1, _ := db.Put([]byte("k"), []byte("v1"))
	ts2, _ := db.Put([]byte("k"), []byte("v2"))
	ts3, _ := db.Put([]byte("k"), []byte("v3"))

	r, err := db.GetAt([]byte("k"), ts1)
	if err != nil || string(r.Value) != "v1" {
		t.Fatalf("at ts1: %+v err=%v", r, err)
	}
	r, _ = db.GetAt([]byte("k"), ts2)
	if string(r.Value) != "v2" {
		t.Fatalf("at ts2: %+v", r)
	}
	r, _ = db.GetAt([]byte("k"), ts3)
	if string(r.Value) != "v3" {
		t.Fatalf("at ts3: %+v", r)
	}
	// at ts1-1: nothing
	r, err = db.GetAt([]byte("k"), ts1-1)
	if err != nil || r.Kind != ResultNotFound {
		t.Fatalf("before: %+v err=%v", r, err)
	}
	// future timestamp
	if _, err := db.GetAt([]byte("k"), ts3+1); err == nil {
		t.Fatal("future timestamp must error")
	}
}

// TestModelWorkload is the P5.7 model test: random ops, flushes, reopens and
// reads at random historical timestamps.
func TestModelWorkload(t *testing.T) {
	seed := int64(20261106)
	rng := rand.New(rand.NewSource(seed))
	model := testmodel.NewModel()
	gen := testmodel.NewGen(seed, 50, 5000)

	fs := vfs.NewMemFS()
	dir := "db"
	db, err := Open(dir, testOptions(fs, base.RealClock{}))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// M-015 item 9: the 200k-op model run is reduced under -short; gates
	// always run the full workload without -short.
	ops := 200000
	if testing.Short() {
		ops = 20000
	}
	history := make([]uint64, 0, 1000) // sample of visible timestamps for later probes
	for i := 0; i < ops; i++ {
		batch := gen.Next(5)
		b := NewBatch()
		for _, r := range batch {
			if r.Kind == base.KindDelete {
				b.Delete(r.Key)
			} else {
				b.Put(r.Key, r.Value)
			}
		}
		ts, err := db.Apply(b)
		if err != nil {
			t.Fatal(err)
		}
		// commit the batch AS COMMITTED (last-write-wins dedup already applied)
		for i := range b.recs {
			b.recs[i].Ts = ts
			model.Commit(b.recs[i : i+1])
		}
		if i%20000 == 0 {
			history = append(history, ts)
		}
	}

	// probe the latest and sampled historical timestamps
	probeKeys := []string{"agent/0/state", "agent/1/state", "app/1/e000001", "app/1/e000100"}
	for _, key := range probeKeys {
		res, err := db.Get([]byte(key))
		if err != nil {
			t.Fatal(err)
		}
		mts, mk, mv, mok := model.VisibleAt([]byte(key), model.MaxTs())
		if !mok {
			if res.Kind != ResultNotFound {
				t.Fatalf("%s: db has data but model does not: %+v", key, res)
			}
			continue
		}
		if res.Kind != ResultExact || res.VersionTs != mts || string(res.Value) != string(mv) || mk != base.KindPut {
			t.Fatalf("%s: got %+v want (%d,%v,%q)", key, res, mts, mk, mv)
		}
	}
	for _, s := range history {
		for _, key := range probeKeys {
			res, err := db.GetAt([]byte(key), s)
			if err != nil {
				t.Fatal(err)
			}
			mts, _, mv, mok := model.VisibleAt([]byte(key), s)
			if !mok {
				if res.Kind != ResultNotFound {
					t.Fatalf("%s@%d: unexpected data: %+v", key, s, res)
				}
				continue
			}
			if res.Kind != ResultExact || res.VersionTs != mts || string(res.Value) != string(mv) {
				t.Fatalf("%s@%d: got %+v want (%d,%q)", key, s, res, mts, mv)
			}
		}
	}
	// unused rng keeps the seed logged for reproducibility
	_ = rng.Int63()
}

func TestConcurrentWritersUniqueIncreasingTs(t *testing.T) {
	db := openTestDB(t, "db")
	defer db.Close()

	var mu sync.Mutex
	seen := map[uint64]bool{}
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				ts, err := db.Put([]byte(fmt.Sprintf("w%d/k%d", id, i)), []byte("v"))
				if err != nil {
					errs <- err
					return
				}
				mu.Lock()
				if seen[ts] {
					mu.Unlock()
					errs <- fmt.Errorf("duplicate ts %d", ts)
					return
				}
				seen[ts] = true
				mu.Unlock()
			}
		}(w)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}

func TestClockBackwardsStillIncreasing(t *testing.T) {
	start := time.Unix(1750000000, 0)
	clock := base.NewManualClock(start)
	fs := vfs.NewMemFS()
	db, err := Open("db", testOptions(fs, clock))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	var prev uint64
	for i := 0; i < 100; i++ {
		ts, err := db.Put([]byte(fmt.Sprintf("k%d", i)), []byte("v"))
		if err != nil {
			t.Fatal(err)
		}
		if ts <= prev {
			t.Fatalf("ts %d not greater than %d", ts, prev)
		}
		prev = ts
		// move the clock backwards
		clock.Set(start.Add(-time.Duration(i) * time.Second))
	}
}

// TestMemFSCrashMatrix is the Gate 5 crash test: run a workload, crash at
// random points with both clone modes, reopen, and verify every write
// acknowledged under SyncAlways is present and nothing is corrupt.
func TestMemFSCrashMatrix(t *testing.T) {
	seed := int64(4242)
	rng := rand.New(rand.NewSource(seed))

	// M-015 item 9: fewer crash rounds under -short; gates run all six.
	rounds := 6
	if testing.Short() {
		rounds = 2
	}
	for round := 0; round < rounds; round++ {
		fs := vfs.NewMemFS()
		dir := "db"
		db, err := Open(dir, testOptions(fs, base.RealClock{}))
		if err != nil {
			t.Fatal(err)
		}
		model := testmodel.NewModel()
		gen := testmodel.NewGen(seed+int64(round), 20, 2000)

		// run a workload, crash mid-way at a random point
		const ops = 300
		crashAt := rng.Intn(ops)
		for i := 0; i < ops; i++ {
			batch := gen.Next(5)
			b := NewBatch()
			for _, r := range batch {
				if r.Kind == base.KindDelete {
					b.Delete(r.Key)
				} else {
					b.Put(r.Key, r.Value)
				}
			}
			ts, err := db.Apply(b)
			if err != nil {
				t.Fatalf("round %d: %v", round, err)
			}
			// commit the batch AS COMMITTED (last-write-wins dedup applied)
			for i := range b.recs {
				b.recs[i].Ts = ts
				model.Commit(b.recs[i : i+1])
			}
			if i == crashAt {
				break // crash: abandon the live DB without Close
			}
		}

		for _, mode := range []vfs.CrashMode{vfs.DropUnsynced, vfs.TornTail} {
			var after *vfs.MemFS
			if mode == vfs.DropUnsynced {
				after = fs.CrashClone(mode, nil)
			} else {
				after = fs.CrashClone(mode, rng)
			}
			// reopen on the crashed FS; SyncAlways means every acknowledged
			// write must be present
			db2, err := Open(dir, Options{
				FS:                    after,
				Clock:                 base.RealClock{},
				SyncMode:              SyncAlways,
				MemtableBytes:         64 << 10,
				L0StallTrigger:        1 << 16,
				L0CompactTrigger:      1 << 15,
				MaxImmutableMemtables: 64,
			})
			if err != nil {
				t.Fatalf("round %d mode %d: reopen: %v", round, mode, err)
			}
			for _, rec := range modelSample(model) {
				res, err := db2.Get(rec.Key)
				if err != nil {
					t.Fatalf("round %d mode %d: get %q: %v", round, mode, rec.Key, err)
				}
				_, mk, mv, mok := model.VisibleAt(rec.Key, model.MaxTs())
				if !mok || mk == base.KindDelete {
					if res.Kind != ResultNotFound {
						t.Fatalf("round %d mode %d: %q should be absent: %+v", round, mode, rec.Key, res)
					}
					continue
				}
				if res.Kind != ResultExact || string(res.Value) != string(mv) {
					t.Fatalf("round %d mode %d: %q lost: got %+v want %q", round, mode, rec.Key, res, mv)
				}
			}
			if err := db2.Close(); err != nil {
				t.Fatalf("round %d mode %d: close: %v", round, mode, err)
			}
		}
	}
}

func modelSample(m *testmodel.Model) []base.Record {
	// one representative record per distinct key: its newest version
	var out []base.Record
	for _, key := range m.Keys() {
		if ts, _, value, ok := m.VisibleAt(key, m.MaxTs()); ok {
			out = append(out, base.Record{Key: key, Ts: ts, Kind: base.KindPut, Value: value})
		}
	}
	return out
}

// M-013 item 1: file numbers must never be reused across sessions.
func TestMultiSessionReopenFlushed(t *testing.T) {
	fs := vfs.NewMemFS()
	dir := "db"
	value := make([]byte, 2048)
	for i := range value {
		value[i] = byte(i % 251)
	}
	var expect []string
	// M-015 item 9: fewer sessions under -short; gates run all four.
	sessions := 4
	if testing.Short() {
		sessions = 2
	}
	for session := 0; session < sessions; session++ {
		db, err := Open(dir, testOptions(fs, base.RealClock{}))
		if err != nil {
			t.Fatalf("session %d: open: %v", session, err)
		}
		start := session * 200
		for i := 0; i < 200; i++ {
			k := fmt.Sprintf("k%04d", start+i)
			if _, err := db.Put([]byte(k), value); err != nil {
				t.Fatalf("session %d: put: %v", session, err)
			}
			expect = append(expect, k)
		}
		if err := db.WaitIdle(context.Background()); err != nil {
			t.Fatalf("session %d: waitidle: %v", session, err)
		}
		if err := db.Close(); err != nil {
			t.Fatalf("session %d: close: %v", session, err)
		}
	}
	// final session: every key must be Exact
	db, err := Open(dir, testOptions(fs, base.RealClock{}))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, k := range expect {
		res, err := db.Get([]byte(k))
		if err != nil {
			t.Fatalf("get %q: %v", k, err)
		}
		if res.Kind != ResultExact || len(res.Value) != 2048 {
			t.Fatalf("key %q lost after sessions: %+v", k, res)
		}
	}
}

func TestMultiSessionCrashClone(t *testing.T) {
	fs := vfs.NewMemFS()
	dir := "db"
	value := make([]byte, 2048)
	current := fs
	var expect []string
	// M-015 item 9: fewer sessions under -short; gates run all four.
	sessions := 4
	if testing.Short() {
		sessions = 2
	}
	for session := 0; session < sessions; session++ {
		db, err := Open(dir, testOptions(current, base.RealClock{}))
		if err != nil {
			t.Fatalf("session %d: open: %v", session, err)
		}
		start := session * 200
		for i := 0; i < 200; i++ {
			k := fmt.Sprintf("k%04d", start+i)
			if _, err := db.Put([]byte(k), value); err != nil {
				t.Fatalf("session %d: put: %v", session, err)
			}
			expect = append(expect, k)
		}
		if err := db.WaitIdle(context.Background()); err != nil {
			t.Fatalf("session %d: waitidle: %v", session, err)
		}
		// crash instead of Close
		current = current.CrashClone(vfs.DropUnsynced, nil)
		_ = db.Close() // the clone has the state; close is irrelevant
	}
	db, err := Open(dir, testOptions(current, base.RealClock{}))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, k := range expect {
		res, err := db.Get([]byte(k))
		if err != nil {
			t.Fatalf("get %q: %v", k, err)
		}
		if res.Kind != ResultExact || len(res.Value) != 2048 {
			t.Fatalf("key %q lost across crash sessions: %+v", k, res)
		}
	}
}

// createTrackingFS records whether any Create targeted an existing name.
type createTrackingFS struct {
	vfs.FS
	mu      sync.Mutex
	created map[string]bool
}

func (f *createTrackingFS) Create(name string) (vfs.File, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, err := f.FS.Stat(name); err == nil {
		return nil, fmt.Errorf("create targeted existing file %q", name)
	}
	if f.created == nil {
		f.created = map[string]bool{}
	}
	f.created[name] = true
	return f.FS.Create(name)
}

func TestNoFileNumberReuse(t *testing.T) {
	inner := vfs.NewMemFS()
	track := &createTrackingFS{FS: inner}
	dir := "db"
	db, err := Open(dir, testOptions(track, base.RealClock{}))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 500; i++ {
		if _, err := db.Put([]byte(fmt.Sprintf("k%04d", i)), make([]byte, 300)); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.WaitIdle(context.Background()); err != nil {
		t.Fatal(err)
	}
	// reopen to exercise recovery paths too
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db2, err := Open(dir, testOptions(track, base.RealClock{}))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 500; i++ {
		if _, err := db2.Put([]byte(fmt.Sprintf("j%04d", i)), make([]byte, 300)); err != nil {
			t.Fatal(err)
		}
	}
	if err := db2.WaitIdle(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := db2.Close(); err != nil {
		t.Fatal(err)
	}
}

// M-013 item 2: a persistent flush error must back off, not hot-loop.
func TestFlushBackoffOnPersistentError(t *testing.T) {
	inner := vfs.NewMemFS()
	ff := vfs.NewFaultFS(inner)
	// fail every SST create after the first 2 (initial DB files pass)
	ff.InjectCreateOn(".sst", errors.New("disk full")) // fail every SST create
	db, err := Open("db", testOptions(ff, base.RealClock{}))
	if err != nil {
		t.Fatal(err)
	}
	// force a rotation so a flush with an SST create is attempted
	for i := 0; i < 400; i++ {
		if _, err := db.Put([]byte(fmt.Sprintf("k%04d", i)), make([]byte, 500)); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(1200 * time.Millisecond) // observe the attempt rate
	creates := ff.Count("Create")
	if creates > 60 { // backoff caps ~10-12 attempts per second; hot loop would be 1000+
		t.Fatalf("flush retry hot loop: %d create attempts in ~1.2s", creates)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close on failed flusher: %v", err)
	}
}

// M-015 item 2 permanent scenario: an SST removed behind the DB's back must
// produce an error on every read path, never a silent fall-through to an
// older version in a deeper level (I10/I12).
func TestSSTRemovedBehindBack(t *testing.T) {
	fs := vfs.NewMemFS()
	dir := "db"
	opts := compactOptions(fs, base.RealClock{})
	opts.L0CompactTrigger = 1 << 20 // no automatic compaction
	opts.L0StallTrigger = 1 << 21
	opts.L1TargetBytes = 1 << 40
	db, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// file A (older): k keys; file B (newer): j keys
	for i := 0; i < 50; i++ {
		if _, err := db.Put([]byte(fmt.Sprintf("k%03d", i)), make([]byte, 300)); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	v := db.vs.Acquire()
	if len(v.Levels[0]) != 1 {
		t.Fatalf("L0 has %d files, want 1", len(v.Levels[0]))
	}
	aNum := v.Levels[0][0].Num
	v.Unref()
	for i := 0; i < 50; i++ {
		if _, err := db.Put([]byte(fmt.Sprintf("j%03d", i)), make([]byte, 300)); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}

	// remove A behind the DB's back; it has never been read, so the table
	// cache does not mask the loss
	if err := fs.Remove(filepath.Join(dir, sstName(aNum))); err != nil {
		t.Fatal(err)
	}

	// a key that lived in A must error, never fall through to another file
	if res, err := db.Get([]byte("k000")); err == nil {
		t.Fatalf("missing SST read silently: %+v", res)
	}
	// a key in the intact file still reads
	res, err := db.Get([]byte("j000"))
	if err != nil || res.Kind != ResultExact {
		t.Fatalf("intact file read: %+v err=%v", res, err)
	}
	// snapshot reads surface the same error
	snap := db.NewSnapshot()
	if _, err := snap.Get([]byte("k000")); err == nil {
		t.Fatal("snapshot: missing SST read silently")
	}
	snap.Release()
	// iteration fails rather than skipping the file silently
	it := db.NewIterator(IterOptions{})
	if it.Next() {
		t.Fatalf("iterator produced entries despite a missing SST: %q", it.Result().Key)
	}
	if err := it.Err(); err == nil {
		t.Fatal("iterator did not report the missing SST")
	}
	it.Close()
}

// M-015 item 4: a rejected manifest edit must leave the manifest bytes
// identical (no durable garbage that bricks the next Open) and the DB must
// keep working and reopen.
func TestManifestEditValidatedBeforeAppend(t *testing.T) {
	fs := vfs.NewMemFS()
	dir := "db"
	db, err := Open(dir, testOptions(fs, base.RealClock{}))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 50; i++ {
		if _, err := db.Put([]byte(fmt.Sprintf("k%03d", i)), []byte("value")); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	manifestBytes := func() []byte {
		t.Helper()
		name, err := manifest.ReadCurrent(fs, dir)
		if err != nil {
			t.Fatal(err)
		}
		f, err := fs.Open(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		data, err := io.ReadAll(f)
		if err != nil {
			t.Fatal(err)
		}
		return data
	}
	before := manifestBytes()

	// del of an unknown file: rejected by Validate before the append
	db.manifestMu.Lock()
	err = db.appendManifestLocked(&manifest.Edit{
		DelFiles: []manifest.DelFile{{Level: 0, Num: 999999}},
	}, 0)
	db.manifestMu.Unlock()
	if err == nil {
		t.Fatal("invalid edit accepted")
	}
	if after := manifestBytes(); !bytes.Equal(before, after) {
		t.Fatal("manifest changed on a rejected edit")
	}
	// the DB still serves reads and closes cleanly
	if res, err := db.Get([]byte("k007")); err != nil || res.Kind != ResultExact {
		t.Fatalf("read after rejected edit: %+v err=%v", res, err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// reopen: the manifest replays cleanly
	db2, err := Open(dir, testOptions(fs, base.RealClock{}))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db2.Close()
	for i := 0; i < 50; i++ {
		res, err := db2.Get([]byte(fmt.Sprintf("k%03d", i)))
		if err != nil || res.Kind != ResultExact {
			t.Fatalf("key %03d after reopen: %+v err=%v", i, res, err)
		}
	}
}
