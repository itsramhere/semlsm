package semlsm

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"testing"
	"time"

	"semlsm/internal/base"
	"semlsm/internal/failpoint"
	"semlsm/internal/memtable"
	"semlsm/internal/testmodel"
	"semlsm/internal/vfs"
)

// compactOptions enables real compaction with small sizes.
func compactOptions(fs vfs.FS, clock base.Clock) Options {
	lw := slog.New(slog.NewTextHandler(testLogWriter{}, nil))
	o := testOptionsWith(fs, clock, lw)
	o.L0CompactTrigger = 2
	o.L0StallTrigger = 8
	o.L1TargetBytes = 64 << 10
	o.LevelMultiplier = 2
	o.TargetFileBytes = 16 << 10
	return o
}

// testOptionsWith mirrors testOptions with a real logger so compaction
// failures are visible in test output.
func testOptionsWith(fs vfs.FS, clock base.Clock, logger *slog.Logger) Options {
	return Options{
		FS:                    fs,
		Clock:                 clock,
		SyncMode:              SyncAlways,
		MemtableBytes:         64 << 10,
		L0StallTrigger:        1 << 16,
		L0CompactTrigger:      1 << 15,
		MaxImmutableMemtables: 64,
		Logger:                logger,
	}
}

type testLogWriter struct{}

func (testLogWriter) Write(p []byte) (int, error) {
	print(string(p))
	return len(p), nil
}

// checkLSM validates the invariants over the current version against the
// model (P6.4, M-014 item 1): internal-key order (I3), L1+ non-overlap (I4),
// and versions fully retained (structural compaction drops nothing).
// Reusable: P7 and P10 extend it.
func checkLSM(t *testing.T, db *DB, model *testmodel.Model) {
	t.Helper()
	v := db.vs.Acquire()
	defer v.Unref()

	// L1+: sorted by smallest and non-overlapping
	for l := 1; l < base.NumLevels; l++ {
		for i := 1; i < len(v.Levels[l]); i++ {
			prevHi := base.UserKey(v.Levels[l][i-1].Largest)
			curLo := base.UserKey(v.Levels[l][i].Smallest)
			if base.CompareUser(prevHi, curLo) >= 0 {
				t.Fatalf("L%d overlap: %q >= %q", l, prevHi, curLo)
			}
		}
	}

	type ver struct {
		key  string
		ts   uint64
		kind base.Kind
		val  string
	}
	var got []ver
	add := func(ikey, value []byte, where string) {
		u, ts, k, ok := base.SplitInternalKey(ikey)
		if !ok {
			t.Fatalf("%s: bad internal key", where)
		}
		got = append(got, ver{string(u), ts, k, string(value)})
	}
	for l := range v.Levels {
		for _, f := range v.Levels[l] {
			reader, rel, err := db.tableCache.Get(f.Num)
			if err != nil {
				t.Fatalf("open %d: %v", f.Num, err)
			}
			it := reader.NewIterator()
			var prev []byte
			for it.First(); it.Valid(); it.Next() {
				ikey := it.Key()
				if prev != nil && base.Compare(prev, ikey) >= 0 {
					t.Fatalf("file %d: I3 violated at %q", f.Num, ikey)
				}
				prev = append(prev[:0], ikey...)
				add(ikey, it.Value(), fmt.Sprintf("file %d", f.Num))
			}
			if err := it.Err(); err != nil {
				t.Fatalf("file %d: %v", f.Num, err)
			}
			it.Close()
			rel()
		}
	}
	// memtables hold the not-yet-flushed versions
	db.mu.Lock()
	if db.rs != nil {
		collect := func(mt *memtable.Memtable, where string) {
			it := mt.NewIterator()
			var prev []byte
			for it.First(); it.Valid(); it.Next() {
				ikey := it.Key()
				if prev != nil && base.Compare(prev, ikey) >= 0 {
					t.Fatalf("memtable %s: I3 violated", where)
				}
				prev = append(prev[:0], ikey...)
				add(ikey, it.Value(), where)
			}
			it.Close()
		}
		collect(db.rs.mem, "active")
		for i, im := range db.rs.imms {
			collect(im.mem, fmt.Sprintf("imm%d", i))
		}
	}
	db.mu.Unlock()

	// compare the full version multiset with the model (both sorted by
	// key asc, ts asc)
	sort.Slice(got, func(i, j int) bool {
		if got[i].key != got[j].key {
			return got[i].key < got[j].key
		}
		return got[i].ts < got[j].ts
	})
	want := model.AllVersions()
	if len(got) != len(want) {
		t.Fatalf("version count: db has %d, model has %d", len(got), len(want))
	}
	for i := range want {
		g := got[i]
		w := want[i]
		if g.key != string(w.Key) || g.ts != w.Ts || g.kind != w.Kind || g.val != string(w.Value) {
			t.Fatalf("version %d differs: db (%q,%d,%v,%q) model (%q,%d,%v,%q)",
				i, g.key, g.ts, g.kind, g.val, w.Key, w.Ts, w.Kind, w.Value)
		}
	}
}

func TestBulkLoadTriggersCompaction(t *testing.T) {
	fs := vfs.NewMemFS()
	db, err := Open("db", compactOptions(fs, base.RealClock{}))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	model := testmodel.NewModel()
	for i := 0; i < 3000; i++ {
		b := NewBatch()
		b.Put([]byte(fmt.Sprintf("agent/%04d/state", i%200)), []byte(fmt.Sprintf("value-%06d-padding-padding", i)))
		ts, err := db.Apply(b)
		if err != nil {
			t.Fatal(err)
		}
		for j := range b.recs {
			b.recs[j].Ts = ts
			model.Commit(b.recs[j : j+1])
		}
	}
	if err := db.WaitIdle(context.Background()); err != nil {
		t.Fatal(err)
	}
	checkLSM(t, db, model)
	s := db.Stats()
	// data must have moved below L0
	if s.LevelFiles[0] > 2 {
		t.Fatalf("L0 did not drain: %+v", s)
	}
	if s.LevelFiles[1] == 0 && s.LevelFiles[2] == 0 && s.LevelFiles[3] == 0 {
		t.Fatalf("compaction did not move data down: %+v", s)
	}
	// all data still reads back
	for i := 0; i < 200; i++ {
		res, err := db.Get([]byte(fmt.Sprintf("agent/%04d/state", i)))
		if err != nil {
			t.Fatal(err)
		}
		if res.Kind != ResultExact {
			t.Fatalf("key %d: %+v", i, res)
		}
	}
}

func TestCompactionModelConcurrent(t *testing.T) {
	seed := int64(60417)
	model := testmodel.NewModel()
	gen := testmodel.NewGen(seed, 30, 3000)

	fs := vfs.NewMemFS()
	dir := "db"
	db, err := Open(dir, compactOptions(fs, base.RealClock{}))
	if err != nil {
		t.Fatal(err)
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	// snapshot churn
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			s := db.NewSnapshot()
			_, _ = s.Get([]byte("agent/0/state"))
			time.Sleep(2 * time.Millisecond)
			s.Release()
		}
	}()

	const ops = 8000
	history := []uint64{}
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
		for j := range b.recs {
			b.recs[j].Ts = ts
			model.Commit(b.recs[j : j+1])
		}
		if i%500 == 0 {
			history = append(history, ts)
		}
	}
	close(stop)
	wg.Wait()
	if err := db.WaitIdle(context.Background()); err != nil {
		t.Fatal(err)
	}
	checkLSM(t, db, model)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// reopen and verify latest + history against the model
	db2, err := Open(dir, compactOptions(fs, base.RealClock{}))
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	for _, key := range model.Keys() {
		res, err := db2.Get(key)
		if err != nil {
			t.Fatal(err)
		}
		mts, mk, mv, mok := model.VisibleAt(key, model.MaxTs())
		if !mok || mk == base.KindDelete {
			if res.Kind != ResultNotFound {
				t.Fatalf("%q should be absent: %+v", key, res)
			}
			continue
		}
		if res.Kind != ResultExact || res.VersionTs != mts || string(res.Value) != string(mv) {
			t.Fatalf("%q: got %+v want (%d,%q)", key, res, mts, mv)
		}
	}
	for _, s := range history {
		for _, key := range model.Keys() {
			res, err := db2.GetAt(key, s)
			if err != nil {
				t.Fatal(err)
			}
			mts, mk, mv, mok := model.VisibleAt(key, s)
			if !mok || mk == base.KindDelete {
				if res.Kind != ResultNotFound {
					t.Fatalf("%q@%d: unexpected: %+v", key, s, res)
				}
				continue
			}
			if res.Kind != ResultExact || res.VersionTs != mts || string(res.Value) != string(mv) {
				t.Fatalf("%q@%d: got %+v want (%d,%q)", key, s, res, mts, mv)
			}
		}
	}
}

func TestSnapshotPinsFilesUntilRelease(t *testing.T) {
	fs := vfs.NewMemFS()
	dir := "db"
	// no auto compaction: L0 files stay until CompactLevel(0) consumes them,
	// making the consumption of the pinned file deterministic
	opts := compactOptions(fs, base.RealClock{})
	opts.L0CompactTrigger = 1 << 20
	opts.L0StallTrigger = 1 << 21
	opts.L1TargetBytes = 1 << 40
	db, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// flush data to L0
	for i := 0; i < 3000; i++ {
		if _, err := db.Put([]byte(fmt.Sprintf("k%04d", i)), []byte("padding-padding-padding-value")); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.WaitIdle(context.Background()); err != nil {
		t.Fatal(err)
	}
	snap := db.NewSnapshot()
	res, err := snap.Get([]byte("k0000"))
	if err != nil || res.Kind != ResultExact {
		t.Fatalf("snapshot get: %+v err=%v", res, err)
	}
	// capture the L0 file numbers at snapshot time
	v := db.vs.Acquire()
	var nums []uint64
	for _, f := range v.Levels[0] {
		nums = append(nums, f.Num)
	}
	v.Unref()
	if len(nums) == 0 {
		t.Fatal("no L0 files exist at snapshot time")
	}

	// consume all L0 files with one compaction
	if err := db.CompactLevel(context.Background(), 0); err != nil {
		t.Fatal(err)
	}
	if err := db.WaitIdle(context.Background()); err != nil {
		t.Fatal(err)
	}

	// the snapshot must still read its pinned (obsolete) files
	res2, err := snap.Get([]byte("k0000"))
	if err != nil || res2.Kind != ResultExact || string(res2.Value) != string(res.Value) {
		t.Fatalf("pinned read after compaction: %+v err=%v", res2, err)
	}
	// and the tree no longer references them
	v = db.vs.Acquire()
	stillThere := map[uint64]bool{}
	for l := range v.Levels {
		for _, f := range v.Levels[l] {
			stillThere[f.Num] = true
		}
	}
	v.Unref()

	snap.Release()
	// files consumed by the compaction must disappear once refs drop
	deadline := time.Now().Add(3 * time.Second)
	for {
		entries, err := fs.List(dir)
		if err != nil {
			t.Fatal(err)
		}
		present := map[string]bool{}
		for _, e := range entries {
			present[e.Name] = true
		}
		gone := 0
		for _, n := range nums {
			if stillThere[n] {
				gone++ // never consumed: must stay
				continue
			}
			if !present[sstName(n)] {
				gone++
			}
		}
		if gone == len(nums) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("obsolete files still on disk after release")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestCompactFailpointCrash(t *testing.T) {
	for _, fp := range []string{"compact.before_manifest", "compact.after_manifest"} {
		t.Run(fp, func(t *testing.T) {
			fs := vfs.NewMemFS()
			dir := "db"
			db, err := Open(dir, compactOptions(fs, base.RealClock{}))
			if err != nil {
				t.Fatal(err)
			}
			model := testmodel.NewModel()
			for i := 0; i < 300; i++ {
				b := NewBatch()
				b.Put([]byte(fmt.Sprintf("k%04d", i)), []byte("value-padding-value-padding"))
				ts, err := db.Apply(b)
				if err != nil {
					t.Fatal(err)
				}
				for j := range b.recs {
					b.recs[j].Ts = ts
					model.Commit(b.recs[j : j+1])
				}
			}
			if err := db.WaitIdle(context.Background()); err != nil {
				t.Fatal(err)
			}

			// arm the failpoint and force a compaction that hits it
			sentinel := fmt.Errorf("failpoint: %s", fp)
			failpoint.SetError(fp, sentinel)
			defer failpoint.Reset()
			_ = db.CompactLevel(context.Background(), 0) // expected to fail or skip

			// crash at the failpoint moment and reopen
			after := fs.CrashClone(vfs.DropUnsynced, nil)
			_ = db.Close()
			db2, err := Open(dir, compactOptions(after, base.RealClock{}))
			if err != nil {
				t.Fatalf("reopen after %s crash: %v", fp, err)
			}
			for _, key := range model.Keys() {
				res, err := db2.Get(key)
				if err != nil {
					t.Fatalf("get %q: %v", key, err)
				}
				mts, _, mv, mok := model.VisibleAt(key, model.MaxTs())
				if !mok {
					continue
				}
				if res.Kind != ResultExact || res.VersionTs != mts || string(res.Value) != string(mv) {
					t.Fatalf("%q after crash: got %+v want (%d,%q)", key, res, mts, mv)
				}
			}
			if err := db2.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
