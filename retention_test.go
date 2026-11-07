package semlsm

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/itsramhere/semlsm/internal/base"
	"github.com/itsramhere/semlsm/internal/checkpoint"
	"github.com/itsramhere/semlsm/internal/failpoint"
	"github.com/itsramhere/semlsm/internal/llm"
	"github.com/itsramhere/semlsm/internal/testmodel"
	"github.com/itsramhere/semlsm/internal/vfs"
)

func waitRetentionDeletes(t *testing.T, db *DB, names []string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for _, name := range names {
		for {
			_, err := db.opts.FS.Stat(name)
			if errors.Is(err, os.ErrNotExist) {
				break
			}
			if err != nil || time.Now().After(deadline) {
				t.Fatalf("retained obsolete file %s: %v", name, err)
			}
			time.Sleep(time.Millisecond)
		}
	}
}

func checkpointArchiveBytes(t *testing.T, db *DB) int64 {
	t.Helper()
	var total int64
	for _, dir := range []string{db.dir, db.archiveDir()} {
		entries, err := db.opts.FS.List(dir)
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range entries {
			if !e.IsDir && (strings.HasSuffix(e.Name, ".ckpt") || dir == db.archiveDir() && strings.HasSuffix(e.Name, ".sst")) {
				total += e.Size
			}
		}
	}
	return total
}

type retainedRun struct {
	id, lo, hi uint64
	retired    bool
	last       base.Kind
}

func retentionRuns(t *testing.T, db *DB) map[string][]retainedRun {
	t.Helper()
	runs := map[string][]retainedRun{}
	v := db.vs.Acquire()
	defer v.Unref()
	for num, cf := range v.Ckpts.ByNum {
		r, err := checkpoint.OpenReader(db.opts.FS, filepath.Join(db.dir, ckptName(num)))
		if err != nil {
			t.Fatal(err)
		}
		for _, ref := range cf.Tuples {
			meta, err := r.ReadMeta(ref.ID)
			if err != nil {
				r.Close()
				t.Fatal(err)
			}
			keys, err := r.IterKeys(ref.ID)
			if err != nil {
				r.Close()
				t.Fatal(err)
			}
			for _, ke := range keys {
				runs[string(ke.Key)] = append(runs[string(ke.Key)], retainedRun{ref.ID, ke.MinTs, ke.MaxTs, meta.RetiredUnixNano != 0, ke.LastKind})
			}
		}
		r.Close()
	}
	return runs
}

// The oracle starts with the original committed version, then asks whether
// that exact version was summarized and retired. It never accepts a stale
// Exact value as a substitute for the unavailable original version.
func checkRetentionModel(t *testing.T, db *DB, model *testmodel.Model, probes []ckptProbe) {
	t.Helper()
	runs := retentionRuns(t, db)
	for _, p := range probes {
		ts, kind, value, found := model.VisibleAt(p.key, p.ts)
		expected := ResultNotFound
		var tuple uint64
		if found {
			if kind == base.KindPut {
				expected = ResultExact
			}
			for _, run := range runs[string(p.key)] {
				if ts >= run.lo && ts <= run.hi {
					tuple = run.id
					expected = ResultSummarized
					if p.ts >= run.hi && run.last == base.KindDelete {
						expected = ResultNotFound
					} else if run.retired {
						expected = ResultExpired
					}
					break
				}
			}
		}
		res, err := db.GetAt(p.key, p.ts)
		if err != nil {
			t.Fatal(err)
		}
		if res.Kind != expected {
			t.Fatalf("%s@%d: kind %d, want %d", p.key, p.ts, res.Kind, expected)
		}
		if expected == ResultExact && (res.VersionTs != ts || !bytes.Equal(res.Value, value)) {
			t.Fatalf("stale Exact at %s@%d: %+v", p.key, p.ts, res)
		}
		if expected == ResultExpired && (res.VersionTs != 0 || len(res.Value) != 0 || res.Checkpoint == nil || res.Checkpoint.ID != tuple || res.Checkpoint.RetiredUnixNano == 0) {
			t.Fatalf("invalid expired result: %+v", res)
		}
	}
	requireIntegrity(t, db)
}

func TestRetentionPreservesDeletedTail(t *testing.T) {
	db, clk, _ := openSemDB(t, "db", llm.NewMock(llm.MockValid), func(o *Options) {
		o.SummaryRetention = time.Hour
	})
	t.Cleanup(func() { db.Close() })
	// ModeEvents allows the newest version, including the delete, to be summarized.
	key := []byte("app/1/deleted")
	var puts []uint64
	for i := 0; i < 12; i++ {
		ts, err := db.Put(key, []byte(fmt.Sprintf("version %d %s", i, strings.Repeat("old payload ", 40))))
		if err != nil {
			t.Fatal(err)
		}
		puts = append(puts, ts)
	}
	deleted, err := db.Delete(key)
	if err != nil {
		t.Fatal(err)
	}
	clk.Advance(2 * time.Hour)
	driveDown(t, db)
	if countRawVersions(t, db, key) != 0 {
		t.Fatal("fixture must summarize every put and the delete")
	}
	if got, err := db.GetAt(key, puts[0]); err != nil || got.Kind != ResultSummarized {
		t.Fatalf("before retirement: %+v %v", got, err)
	}
	if err := db.ApplyRetention(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got, err := db.Get(key); err != nil || got.Kind != ResultNotFound {
		t.Fatalf("deleted head after retirement: %+v %v", got, err)
	}
	for _, ts := range append(puts, deleted) {
		want := ResultExpired
		if ts == deleted {
			want = ResultNotFound
		}
		point, err := db.GetAt(key, ts)
		if err != nil || point.Kind != want {
			t.Fatalf("GetAt at %d: %+v %v, want kind %d", ts, point, err, want)
		}
		snap, err := db.OpenSnapshotAt(ts)
		if err != nil {
			t.Fatal(err)
		}
		results := scanCheckpointResults(t, snap.NewIterator(IterOptions{}))
		snap.Release()
		if want == ResultNotFound {
			if len(results) != 0 {
				t.Fatalf("iterator emitted deleted key at %d: %+v", ts, results)
			}
		} else if len(results) != 1 || !reflect.DeepEqual(results[0], point) {
			t.Fatalf("iterator/GetAt disagreement at %d: %+v %+v", ts, results, point)
		}
	}
	requireIntegrity(t, db)
}

func TestArchiveRetentionOnly(t *testing.T) {
	db, _, _, probes := checkpointFixture(t, 2, true, func(o *Options) { o.ArchiveRetention = 3 * time.Hour })
	before := recordCheckpointView(t, db, probes)
	if err := db.ApplyRetention(context.Background()); err != nil {
		t.Fatal(err)
	}
	after := recordCheckpointView(t, db, probes)
	var removed []string
	changed := 0
	untouched := 0
	for id, old := range before.Tuples {
		got := after.Tuples[id]
		if retentionDue(old.TsTo, db.opts.Clock.Now(), db.opts.ArchiveRetention) {
			changed++
			if got.ArchiveFileNum != 0 || got.ArchiveRetiredUnixNano == 0 || got.RetiredUnixNano != 0 {
				t.Fatalf("archive not retired: %+v", got)
			}
			removed = append(removed, filepath.Join(db.archiveDir(), sstName(old.ArchiveFileNum)))
		} else {
			untouched++
			if got != old || !bytes.Equal(before.Raw[id], after.Raw[id]) {
				t.Fatal("young tuple rewritten")
			}
		}
	}
	if changed == 0 || untouched == 0 {
		t.Fatal("fixture must include both ages")
	}
	waitRetentionDeletes(t, db, removed)
	for i, p := range probes {
		if !reflect.DeepEqual(before.Historical[i].Checkpoint, after.Historical[i].Checkpoint) && before.Historical[i].Checkpoint != nil {
			if !reflect.DeepEqual(before.Historical[i].Checkpoint.Summary, after.Historical[i].Checkpoint.Summary) {
				t.Fatal("archive retention changed summary")
			}
		}
		if after.Historical[i].Checkpoint != nil && after.Historical[i].Checkpoint.ArchiveRetiredUnixNano != 0 && p.ts <= after.Historical[i].Checkpoint.KeyMaxTs {
			if _, err := db.GetArchived(p.key, p.ts); !errors.Is(err, ErrArchiveRetired) || !errors.Is(err, ErrNotArchived) {
				t.Fatalf("archive error: %v", err)
			}
		}
	}
	files := checkpointNums(db)
	stats := db.Stats().RetentionPasses
	if err := db.ApplyRetention(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(files, checkpointNums(db)) || stats != db.Stats().RetentionPasses {
		t.Fatal("retention rewrote unchanged tuples")
	}
	rep, err := db.VerifyIntegrity(context.Background())
	if err != nil || rep.Stubs != 0 || rep.ArchiveRetiredTuples != changed || len(rep.Problems) != 0 {
		t.Fatalf("verify: %+v %v", rep, err)
	}
}

func TestSummaryRetentionModelSnapshotAndSpace(t *testing.T) {
	db, clk, model, probes := checkpointFixture(t, 4, true, func(o *Options) { o.SummaryRetention = 3 * time.Hour })
	before := recordCheckpointView(t, db, probes)
	size := checkpointArchiveBytes(t, db)
	pin := db.NewSnapshot()
	defer pin.Release()
	summaryPin, err := db.OpenSnapshotAt(probes[0].ts)
	if err != nil {
		t.Fatal(err)
	}
	defer summaryPin.Release()
	oldNums := checkpointNums(db)
	if err := db.ApplyRetention(context.Background()); err != nil {
		t.Fatal(err)
	}
	checkRetentionModel(t, db, model, probes)
	if got, err := summaryPin.Get(probes[0].key); err != nil || got.Kind != ResultSummarized {
		t.Fatalf("old snapshot summary: %+v %v", got, err)
	}
	if got, err := summaryPin.GetArchived(probes[0].key, probes[0].ts); err != nil || got.Kind != ResultExact {
		t.Fatalf("old snapshot archive: %+v %v", got, err)
	}
	after := recordCheckpointView(t, db, probes)
	stubs, young := 0, 0
	var expiredProbe *ckptProbe
	for i, p := range probes {
		if after.Historical[i].Kind == ResultExpired {
			q := p
			expiredProbe = &q
			break
		}
	}
	for id, old := range before.Tuples {
		got := after.Tuples[id]
		if got.RetiredUnixNano != 0 {
			stubs++
			if got.ArchiveFileNum != 0 || got.ArchiveRetiredUnixNano == 0 || got.SourceCount != old.SourceCount || got.SourceCRC32 != old.SourceCRC32 || got.PrevTupleID != old.PrevTupleID {
				t.Fatalf("stub metadata: %+v", got)
			}
		} else {
			young++
			if !bytes.Equal(before.Raw[id], after.Raw[id]) {
				t.Fatal("untouched tuple changed")
			}
		}
	}
	if stubs == 0 || young == 0 || expiredProbe == nil {
		t.Fatal("missing stub or young tuple")
	}
	for i, p := range probes {
		got, err := pin.GetArchived(p.key, p.ts)
		if err != nil && !errors.Is(err, ErrNotArchived) {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, before.Archived[i]) {
			t.Fatalf("pinned archive changed at %d", p.ts)
		}
	}
	oldSnapshot, err := db.OpenSnapshotAt(expiredProbe.ts)
	if err != nil {
		t.Fatal(err)
	}
	defer oldSnapshot.Release()
	got, err := oldSnapshot.Get(expiredProbe.key)
	if err != nil || got.Kind != ResultExpired {
		t.Fatalf("expired snapshot: %+v %v", got, err)
	}
	for _, num := range oldNums {
		if _, err := db.opts.FS.Stat(filepath.Join(db.dir, ckptName(num))); err != nil {
			t.Fatal("snapshot lost input")
		}
	}
	for _, meta := range before.Tuples {
		if _, err := db.opts.FS.Stat(filepath.Join(db.archiveDir(), sstName(meta.ArchiveFileNum))); err != nil {
			t.Fatal("pinned archive deleted")
		}
	}
	pin.Release()
	summaryPin.Release()
	db.drainObsolete()
	var removed []string
	for _, num := range oldNums {
		if !containsNum(checkpointNums(db), num) {
			removed = append(removed, filepath.Join(db.dir, ckptName(num)))
		}
	}
	waitRetentionDeletes(t, db, removed)
	if afterSize := checkpointArchiveBytes(t, db); afterSize >= size {
		t.Fatalf("disk bytes did not shrink: %d >= %d", afterSize, size)
	}
	// Iterator/GetAt agreement at historical timestamps includes Expired.
	for _, p := range probes {
		snap, err := db.OpenSnapshotAt(p.ts)
		if err != nil {
			t.Fatal(err)
		}
		for _, result := range scanCheckpointResults(t, snap.NewIterator(IterOptions{})) {
			point, err := db.GetAt(result.Key, p.ts)
			if err != nil || !reflect.DeepEqual(result, point) {
				t.Fatalf("iterator/GetAt disagreement: %+v %+v %v", result, point, err)
			}
		}
		snap.Release()
	}
	// Retired tuples stay byte-identical while a later pass retires young ones.
	clk.Advance(4 * time.Hour)
	if err := db.ApplyRetention(context.Background()); err != nil {
		t.Fatal(err)
	}
	later := recordCheckpointView(t, db, probes)
	for id, meta := range after.Tuples {
		if meta.RetiredUnixNano != 0 && !bytes.Equal(after.Raw[id], later.Raw[id]) {
			t.Fatal("stub was rewritten or un-retired")
		}
	}
	checkRetentionModel(t, db, model, probes)
	oldSnapshot.Release()
	opts := db.opts
	db.Close()
	reopened, err := Open("db", opts)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	checkRetentionModel(t, reopened, model, probes)
	assertNoOrphanCheckpoints(t, reopened)
}

func containsNum(nums []uint64, want uint64) bool {
	for _, num := range nums {
		if num == want {
			return true
		}
	}
	return false
}

func TestRetentionI10OlderRawGeneration(t *testing.T) {
	db, clk, _ := openSemDB(t, "db", llm.NewMock(llm.MockValid), func(o *Options) { o.SummaryRetention = time.Hour })
	t.Cleanup(func() { db.Close() })
	key := []byte("agent/1/state")
	var a, b []uint64
	for i := 0; i < 50; i++ {
		ts, err := db.Put(key, []byte(fmt.Sprintf("A%03d", i)))
		if err != nil {
			t.Fatal(err)
		}
		a = append(a, ts)
	}
	driveDown(t, db)
	clk.Set(semStart.Add(10 * time.Hour))
	for i := 0; i < 200; i++ {
		ts, err := db.Put(key, []byte(fmt.Sprintf("B%03d", i)))
		if err != nil {
			t.Fatal(err)
		}
		b = append(b, ts)
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	clk.Set(semStart.Add(20 * time.Hour))
	for level := 0; level <= 2; level++ {
		if err := db.CompactLevel(context.Background(), level); err != nil {
			t.Fatal(err)
		}
	}
	if res, err := db.GetAt(key, b[100]); err != nil || res.Kind != ResultSummarized {
		t.Fatalf("I10 fixture: %+v %v", res, err)
	}
	pin, err := db.OpenSnapshotAt(b[100])
	if err != nil {
		t.Fatal(err)
	}
	defer pin.Release()
	if err := db.ApplyRetention(context.Background()); err != nil {
		t.Fatal(err)
	}
	assert := func(db *DB) {
		for _, ts := range b[:len(b)-1] {
			res, err := db.GetAt(key, ts)
			if err != nil || res.Kind != ResultExpired {
				t.Fatalf("stale I10 read at %d: %+v %v", ts, res, err)
			}
		}
		res, err := db.GetAt(key, a[len(a)-1])
		if err != nil || res.Kind != ResultExact || string(res.Value) != "A049" {
			t.Fatalf("old raw damaged: %+v %v", res, err)
		}
		res, err = db.Get(key)
		if err != nil || res.Kind != ResultExact || string(res.Value) != "B199" {
			t.Fatalf("head damaged: %+v %v", res, err)
		}
		requireIntegrity(t, db)
	}
	assert(db)
	if res, err := pin.Get(key); err != nil || res.Kind != ResultSummarized {
		t.Fatalf("pinned summary: %+v %v", res, err)
	}
	pin.Release()
	opts := db.opts
	db.Close()
	reopened, err := Open("db", opts)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	assert(reopened)
}

func TestRetentionCrashAndWriteError(t *testing.T) {
	for _, fp := range []string{fpRetentionAfterWrite, fpRetentionAfterManifest} {
		for _, mode := range []vfs.CrashMode{vfs.DropUnsynced, vfs.TornTail} {
			t.Run(fmt.Sprintf("%s/%d", fp, mode), func(t *testing.T) {
				db, _, model, probes := checkpointFixture(t, 3, true, func(o *Options) { o.SummaryRetention = time.Hour })
				before := checkpointNums(db)
				var image *vfs.MemFS
				sentinel := errors.New("retention crash")
				failpoint.Set(fp, func() error {
					image = db.opts.FS.(*vfs.MemFS).CrashClone(mode, rand.New(rand.NewSource(42)))
					return sentinel
				})
				err := db.ApplyRetention(context.Background())
				failpoint.Reset()
				if !errors.Is(err, sentinel) || image == nil {
					t.Fatalf("missing failpoint: %v", err)
				}
				if fp == fpRetentionAfterWrite {
					if !reflect.DeepEqual(before, checkpointNums(db)) {
						t.Fatal("failed pass installed")
					}
					assertNoOrphanCheckpoints(t, db)
				}
				trackTestMemFS(t, image)
				opts := db.opts
				opts.FS = image
				db.Close()
				reopened, err := Open("db", opts)
				if err != nil {
					t.Fatal(err)
				}
				defer reopened.Close()
				checkRetentionModel(t, reopened, model, probes)
				assertNoOrphanCheckpoints(t, reopened)
				rep, err := reopened.VerifyIntegrity(context.Background())
				if err != nil || len(rep.Problems) != 0 {
					t.Fatal(rep, err)
				}
				if (fp == fpRetentionAfterManifest) != (rep.Stubs > 0) {
					t.Fatalf("crash publication: %+v", rep)
				}
			})
		}
	}
}

func TestRetentionBackgroundInterval(t *testing.T) {
	db, clk, _, _ := checkpointFixture(t, 1, true, func(o *Options) { o.SummaryRetention = 3 * time.Hour; o.RetentionInterval = time.Hour })
	db.compactWG.Add(1)
	go db.fastCompactLoop()
	wait := func(f func() bool) {
		deadline := time.Now().Add(5 * time.Second)
		for !f() {
			if time.Now().After(deadline) {
				t.Fatal("background retention did not progress")
			}
			time.Sleep(time.Millisecond)
		}
	}
	wait(func() bool { db.mu.Lock(); defer db.mu.Unlock(); return !db.lastRetention.IsZero() })
	if db.Stats().RetentionPasses != 0 {
		t.Fatal("retired a young tuple")
	}
	clk.Advance(30 * time.Minute)
	if err := db.CompactLevel(context.Background(), 4); err != nil {
		t.Fatal(err)
	}
	db.mu.Lock()
	last := db.lastRetention
	db.mu.Unlock()
	if !last.Equal(semStart.Add(2 * time.Hour)) {
		t.Fatal("interval advanced prematurely")
	}
	clk.Advance(2 * time.Hour)
	wait(func() bool { return db.Stats().RetentionPasses == 1 })
	files := checkpointNums(db)
	clk.Advance(time.Hour)
	wait(func() bool { db.mu.Lock(); defer db.mu.Unlock(); return db.lastRetention.Equal(clk.Now()) })
	if !reflect.DeepEqual(files, checkpointNums(db)) || db.Stats().RetentionPasses != 1 {
		t.Fatal("background rewrote a stub")
	}
}

func TestRetentionConcurrency(t *testing.T) {
	db, clk, model, probes := checkpointFixture(t, 3, true, func(o *Options) { o.ArchiveRetention = 2 * time.Hour; o.SummaryRetention = 3 * time.Hour })
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	errs := make(chan error, 4)
	t.Cleanup(func() { cancel(); wg.Wait() })
	readProbe := probes[0]
	wg.Go(func() {
		for ctx.Err() == nil {
			res, err := db.GetAt(readProbe.key, readProbe.ts)
			if err != nil {
				errs <- err
				return
			}
			if res.Kind != ResultSummarized && res.Kind != ResultExpired {
				errs <- fmt.Errorf("concurrent stale read: %+v", res)
				return
			}
			time.Sleep(time.Millisecond)
		}
	})
	for _, op := range []func(context.Context) error{db.ApplyRetention, db.CompactCheckpoints} {
		op := op
		wg.Go(func() {
			for ctx.Err() == nil {
				if err := op(ctx); err != nil && !errors.Is(err, context.Canceled) {
					errs <- err
					return
				}
				time.Sleep(time.Millisecond)
			}
		})
	}
	wg.Go(func() {
		for i := 0; ctx.Err() == nil; i++ {
			key, val := []byte("raw/writer"), []byte(fmt.Sprint(i))
			ts, err := db.Put(key, val)
			if err != nil {
				errs <- err
				return
			}
			model.Commit([]base.Record{{Key: key, Value: val, Ts: ts, Kind: base.KindPut}})
			time.Sleep(time.Millisecond)
		}
	})
	for round := 3; round < 15; round++ {
		probes = append(probes, addCheckpointRound(t, db, clk, model, round)...)
	}
	cancel()
	wg.Wait()
	select {
	case err := <-errs:
		t.Fatal(err)
	default:
	}
	if err := db.ApplyRetention(context.Background()); err != nil {
		t.Fatal(err)
	}
	checkRetentionModel(t, db, model, probes)
}

func TestRetentionOldFormatAndStubChecks(t *testing.T) {
	db, _, _, probes := checkpointFixture(t, 1, true, func(o *Options) { o.SummaryRetention = time.Hour })
	before := recordCheckpointView(t, db, probes)
	for _, raw := range before.Raw {
		if bytes.Contains(raw, []byte("retired_unix_nano")) {
			t.Fatal("fixture is not old format")
		}
	}
	if err := db.ApplyRetention(context.Background()); err != nil {
		t.Fatal(err)
	}
	view := recordCheckpointView(t, db, probes)
	for id, meta := range view.Tuples {
		if meta.RetiredUnixNano == 0 {
			t.Fatal("old tuple not retired")
		}
		raw := view.Raw[id]
		pos := 4 + int(binary.LittleEndian.Uint32(raw)) + 4
		sumLen := int(binary.LittleEndian.Uint32(raw[pos:]))
		if sumLen != 5 {
			t.Fatalf("stub still holds payload: %d", sumLen)
		}
	}
	// A stub's unavailable summary CRC is ignored, while its key-index CRC
	// remains mandatory to prevent corruption from exposing older raw data.
	v := db.vs.Acquire()
	var num uint64
	var offset int
	for n, cf := range v.Ckpts.ByNum {
		num = n
		raw := view.Raw[cf.Tuples[0].ID]
		offset = int(cf.Tuples[0].Offset) + 4 + int(binary.LittleEndian.Uint32(raw)) + 4 + 4 + 1
		break
	}
	v.Unref()
	db.ckptCache.CloseAll()
	hardeningFlip(t, db.opts.FS, db.dir, ckptName(num), offset)
	requireIntegrity(t, db)
	v = db.vs.Acquire()
	ref := v.Ckpts.ByNum[num].Tuples[0]
	v.Unref()
	db.ckptCache.CloseAll()
	hardeningFlip(t, db.opts.FS, db.dir, ckptName(num), int(ref.Offset+ref.Length-1))
	rep, err := db.VerifyIntegrity(context.Background())
	if err == nil && len(rep.Problems) == 0 {
		t.Fatal("stub key-index CRC damage was ignored")
	}
}

func TestRetentionValidation(t *testing.T) {
	for _, opts := range []Options{{ArchiveRetention: 2 * time.Hour, SummaryRetention: time.Hour}, {ArchiveRetention: -time.Hour}, {SummaryRetention: -time.Hour}, {RetentionInterval: -time.Hour}} {
		opts.FS = newTestMemFS(t)
		if db, err := Open("db", opts); err == nil {
			db.Close()
			t.Fatal("invalid retention accepted")
		}
	}
	opts, err := (Options{}).withDefaults()
	if err != nil || opts.RetentionInterval != time.Hour || opts.AutoRepair || opts.ArchiveRetention != 0 || opts.SummaryRetention != 0 {
		t.Fatalf("defaults: %+v %v", opts, err)
	}
}
