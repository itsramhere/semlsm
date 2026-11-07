package semlsm

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/itsramhere/semlsm/internal/base"
	"github.com/itsramhere/semlsm/internal/checkpoint"
	"github.com/itsramhere/semlsm/internal/failpoint"
	"github.com/itsramhere/semlsm/internal/llm"
	"github.com/itsramhere/semlsm/internal/manifest"
	"github.com/itsramhere/semlsm/internal/testmodel"
	"github.com/itsramhere/semlsm/internal/vfs"
)

type ckptProbe struct {
	key []byte
	ts  uint64
}

func addCheckpointRound(t *testing.T, db *DB, clk *base.ManualClock, model *testmodel.Model, round int) []ckptProbe {
	t.Helper()
	var probes []ckptProbe
	for i := 0; i < 12; i++ {
		batch := NewBatch()
		for _, key := range []string{"agent/1/s", "agent/1/t", "app/1/e"} {
			value := fmt.Sprintf("round %03d value %03d %s", round, i, strings.Repeat("retained archive payload ", 12))
			if i == 5 {
				batch.Delete([]byte(key))
			} else {
				batch.Put([]byte(key), []byte(value))
			}
		}
		ts, err := db.Apply(batch)
		if err != nil {
			t.Fatal(err)
		}
		for _, rec := range batch.recs {
			rec.Ts = ts
			model.Commit([]base.Record{rec})
			probes = append(probes, ckptProbe{append([]byte(nil), rec.Key...), ts})
		}
	}
	driveDown(t, db)
	clk.Set(clk.Now().Add(2 * time.Hour))
	if err := db.CompactLevel(context.Background(), base.NumLevels-1); err != nil {
		t.Fatal(err)
	}
	return probes
}

func checkpointFixture(t *testing.T, count int, archived bool, mutate ...func(*Options)) (*DB, *base.ManualClock, *testmodel.Model, []ckptProbe) {
	t.Helper()
	db, clk, _ := openSemDB(t, "db", llm.NewMock(llm.MockValid), func(o *Options) {
		o.ArchiveRaw = archived
		for _, f := range mutate {
			f(o)
		}
	})
	t.Cleanup(func() { db.Close() })
	model := testmodel.NewModel()
	var probes []ckptProbe
	for i := 0; i < count; i++ {
		probes = append(probes, addCheckpointRound(t, db, clk, model, i)...)
	}
	if n := checkpointCount(db); n < count {
		t.Fatalf("built %d checkpoint files, want at least %d real semantic jobs", n, count)
	}
	return db, clk, model, probes
}

func checkpointCount(db *DB) int {
	v := db.vs.Acquire()
	defer v.Unref()
	return len(v.Ckpts.ByNum)
}

func checkpointNums(db *DB) []uint64 {
	v := db.vs.Acquire()
	defer v.Unref()
	var nums []uint64
	for num := range v.Ckpts.ByNum {
		nums = append(nums, num)
	}
	sort.Slice(nums, func(i, j int) bool { return nums[i] < nums[j] })
	return nums
}

func scanCheckpointResults(t *testing.T, it *Iterator) []Result {
	t.Helper()
	defer it.Close()
	var out []Result
	for it.Next() {
		out = append(out, it.Result())
	}
	if err := it.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

type checkpointView struct {
	Latest, Historical, Archived []Result
	ArchiveErrors                []string
	Scan                         []Result
	Tuples                       map[uint64]checkpoint.TupleMeta
	Raw                          map[uint64][]byte
	Previous                     map[string]uint64
}

func recordCheckpointView(t *testing.T, db *DB, probes []ckptProbe) checkpointView {
	t.Helper()
	view := checkpointView{Tuples: map[uint64]checkpoint.TupleMeta{}, Raw: map[uint64][]byte{}, Previous: map[string]uint64{}}
	for _, key := range []string{"agent/1/s", "agent/1/t", "app/1/e", "missing"} {
		res, err := db.Get([]byte(key))
		if err != nil {
			t.Fatal(err)
		}
		view.Latest = append(view.Latest, res)
	}
	for _, probe := range probes {
		res, err := db.GetAt(probe.key, probe.ts)
		if err != nil {
			t.Fatal(err)
		}
		view.Historical = append(view.Historical, res)
		res, err = db.GetArchived(probe.key, probe.ts)
		if err != nil && !errors.Is(err, ErrNotArchived) {
			t.Fatal(err)
		}
		view.Archived = append(view.Archived, res)
		if err != nil {
			view.ArchiveErrors = append(view.ArchiveErrors, "not archived")
		} else {
			view.ArchiveErrors = append(view.ArchiveErrors, "")
		}
	}
	view.Scan = scanCheckpointResults(t, db.NewIterator(IterOptions{}))
	v := db.vs.Acquire()
	defer v.Unref()
	for _, cf := range v.Ckpts.ByNum {
		data := hardeningReadFile(t, db.opts.FS, db.dir, ckptName(cf.FileNum))
		r, err := checkpoint.OpenReader(db.opts.FS, filepath.Join(db.dir, ckptName(cf.FileNum)))
		if err != nil {
			t.Fatal(err)
		}
		for _, ref := range cf.Tuples {
			meta, err := r.ReadMeta(ref.ID)
			if err != nil {
				r.Close()
				t.Fatal(err)
			}
			view.Tuples[ref.ID] = meta
			view.Raw[ref.ID] = append([]byte(nil), data[ref.Offset:ref.Offset+ref.Length]...)
			view.Previous[ref.Group] = db.prevTupleID(ref.Group)
		}
		r.Close()
	}
	requireIntegrity(t, db)
	return view
}

func assertNoOrphanCheckpoints(t *testing.T, db *DB) {
	t.Helper()
	live := map[uint64]bool{}
	for _, cf := range db.vs.LiveCheckpoints() {
		live[cf.FileNum] = true
	}
	ents, err := db.opts.FS.List(db.dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range ents {
		if num, ok, suffix := parseFileNum(e.Name); ok && suffix == ".ckpt" && !live[num] {
			t.Fatalf("orphan checkpoint %s", e.Name)
		}
	}
}

func TestCheckpointMergeReadEquivalenceAndSnapshots(t *testing.T) {
	db, _, _, probes := checkpointFixture(t, 42, true)
	before := recordCheckpointView(t, db, probes)
	pin := db.NewSnapshot()
	defer pin.Release()
	scan := scanCheckpointResults(t, pin.NewIterator(IterOptions{}))
	oldNums := checkpointNums(db)
	if err := db.CompactCheckpoints(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := checkpointCount(db); got != len(oldNums)-32+1 {
		t.Fatalf("checkpoint count %d, before %d", got, len(oldNums))
	}
	if after := recordCheckpointView(t, db, probes); !reflect.DeepEqual(before, after) {
		t.Fatal("reads, raw tuple bytes, or chains changed after merge")
	}
	for i, key := range []string{"agent/1/s", "agent/1/t", "app/1/e", "missing"} {
		res, err := pin.Get([]byte(key))
		if err != nil || !reflect.DeepEqual(res, before.Latest[i]) {
			t.Fatalf("snapshot get %s: %+v %v", key, res, err)
		}
	}
	if got := scanCheckpointResults(t, pin.NewIterator(IterOptions{})); !reflect.DeepEqual(scan, got) {
		t.Fatal("snapshot iterator changed")
	}
	for i, probe := range probes {
		res, err := pin.GetArchived(probe.key, probe.ts)
		if err != nil && !errors.Is(err, ErrNotArchived) {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(res, before.Archived[i]) || (err != nil) != (before.ArchiveErrors[i] != "") {
			t.Fatalf("snapshot archive changed at %d", probe.ts)
		}
	}
	for _, num := range oldNums {
		if _, err := db.opts.FS.Stat(filepath.Join(db.dir, ckptName(num))); err != nil {
			t.Fatalf("snapshot lost checkpoint %d: %v", num, err)
		}
	}
	pin.Release()
	db.drainObsolete()
	// A second bounded merge finishes the remaining eleven files.
	if err := db.CompactCheckpoints(context.Background()); err != nil {
		t.Fatal(err)
	}
	if checkpointCount(db) != 1 {
		t.Fatalf("remaining files: %d", checkpointCount(db))
	}
	if stats := db.Stats(); stats.CheckpointMerges != 2 || stats.CheckpointFilesMerged != uint64(len(oldNums)+1) {
		t.Fatalf("merge accounting: %+v", stats)
	}
	opts := db.opts
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	for _, num := range oldNums {
		if _, err := opts.FS.Stat(filepath.Join(db.dir, ckptName(num))); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("old checkpoint remains %d: %v", num, err)
		}
	}
	reopened, err := Open("db", opts)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if after := recordCheckpointView(t, reopened, probes); !reflect.DeepEqual(before, after) {
		t.Fatal("reads changed after reopen")
	}
	assertNoOrphanCheckpoints(t, reopened)
}

func TestPinnedCheckpointArchiveKeepSet(t *testing.T) {
	db, x, ts := archiveFixture(t)
	pin := db.NewSnapshot()
	defer pin.Release()
	// An unrelated checkpoint also refers to X's archives. Its death must
	// consult the snapshot-only X even though current has neither file.
	y := manifest.CkptFile{FileNum: db.allocFile(), ArchiveNums: append([]uint64{}, x.ArchiveNums...)}
	yPath := filepath.Join(db.dir, ckptName(y.FileNum))
	w, err := checkpoint.NewWriter(db.opts.FS, yPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Finish(); err != nil {
		t.Fatal(err)
	}
	installCheckpointEdit(t, db, &manifest.Edit{DelCkpts: []uint64{x.FileNum}, AddCkpts: []manifest.CkptFile{y}})
	installCheckpointEdit(t, db, &manifest.Edit{DelCkpts: []uint64{y.FileNum}})
	db.drainObsolete()
	// The deleter queues Y after its archive candidates. Observing Y's
	// removal makes the snapshot read test deterministic even on the old,
	// incorrect implementation, which would enqueue X's archives first.
	deadline := time.Now().Add(5 * time.Second)
	for {
		_, err := db.opts.FS.Stat(yPath)
		if errors.Is(err, os.ErrNotExist) {
			break
		}
		if err != nil || time.Now().After(deadline) {
			t.Fatalf("unrelated checkpoint was not deleted: %v", err)
		}
		time.Sleep(time.Millisecond)
	}
	res, err := pin.GetArchived([]byte("agent/1/s"), ts)
	if err != nil || res.Kind != ResultExact || string(res.Value) != "value-000 repeated payload repeated payload" {
		t.Fatalf("pinned archive: %+v %v", res, err)
	}
	if len(db.vs.LiveCheckpoints()) != 1 {
		t.Fatal("snapshot checkpoint missing from live accessor")
	}
	pin.Release()
	db.drainObsolete()
	db.Close()
	for _, num := range x.ArchiveNums {
		if _, err := db.opts.FS.Stat(filepath.Join(db.archiveDir(), sstName(num))); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("archive survived final release: %v", err)
		}
	}
}

func TestCheckpointMergeArchiveReadBounds(t *testing.T) {
	db, _, _, probes := checkpointFixture(t, 2, true)
	key := []byte("agent/1/s")
	pin := db.NewSnapshot()
	if _, err := db.GetArchived(key, pin.Ts()+1); !errors.Is(err, ErrFutureTimestamp) {
		t.Fatalf("DB future archive read: %v", err)
	}
	if _, err := pin.GetArchived(key, pin.Ts()+1); !errors.Is(err, ErrFutureTimestamp) {
		t.Fatalf("snapshot future archive read: %v", err)
	}
	if err := db.CompactCheckpoints(context.Background()); err != nil {
		t.Fatal(err)
	}
	pin.Release()
	if _, err := pin.GetArchived(key, probes[0].ts); !errors.Is(err, ErrReleased) {
		t.Fatalf("released snapshot archive read: %v", err)
	}
}

func TestKnownEmptyArchiveNums(t *testing.T) {
	db, _, _, _ := checkpointFixture(t, 1, false)
	cur := db.vs.Acquire()
	for _, cf := range cur.Ckpts.ByNum {
		if cf.ArchiveNums == nil || len(cf.ArchiveNums) != 0 {
			t.Fatal("semantic job lost known-empty archive set")
		}
		data, err := json.Marshal(cf)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(data), `"archive_nums":[]`) {
			t.Fatalf("JSON: %s", data)
		}
		var copy manifest.CkptFile
		if err := json.Unmarshal(data, &copy); err != nil || copy.ArchiveNums == nil {
			t.Fatalf("JSON round trip: %+v %v", copy, err)
		}
	}
	cur.Unref()
	// Exercise the real threshold roll and its full-state snapshot copy.
	db.manifestMu.Lock()
	for db.manifest.Size() < manifest.RollThreshold {
		if err := db.manifest.Append(&manifest.Edit{}); err != nil {
			t.Fatal(err)
		}
	}
	err := db.appendManifestLocked(&manifest.Edit{}, db.vs.CurrentLastTs())
	db.manifestMu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	opts := db.opts
	db.Close()
	for _, cf := range db.vs.LiveCheckpoints() {
		if cf.ArchiveNums == nil {
			t.Fatal("version accessor lost empty set")
		}
		hardeningFlip(t, opts.FS, "db", ckptName(cf.FileNum), 0)
	}
	opts.FS.MkdirAll(filepath.Join("db", "archive"))
	orphan := filepath.Join("db", "archive", "999999.sst")
	w, err := opts.FS.Create(orphan)
	if err != nil {
		t.Fatal(err)
	}
	w.Close()
	reopened, err := Open("db", opts)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	for _, cf := range reopened.vs.LiveCheckpoints() {
		if cf.ArchiveNums == nil {
			t.Fatal("roll/reopen lost known-empty archive set")
		}
	}
	if _, err := opts.FS.Stat(orphan); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("known-empty unreadable checkpoint prevented sweep: %v", err)
	}
}

func TestCheckpointMergeCrash(t *testing.T) {
	for _, fp := range []string{fpCkptMergeAfterWrite, fpCkptMergeAfterManifest} {
		for _, mode := range []vfs.CrashMode{vfs.DropUnsynced, vfs.TornTail} {
			t.Run(fmt.Sprintf("%s/%d", fp, mode), func(t *testing.T) {
				db, _, model, probes := checkpointFixture(t, 4, true)
				before := recordCheckpointView(t, db, probes)
				var after *vfs.MemFS
				sentinel := errors.New("crash checkpoint merge")
				failpoint.Set(fp, func() error {
					after = db.opts.FS.(*vfs.MemFS).CrashClone(mode, rand.New(rand.NewSource(42)))
					return sentinel
				})
				err := db.CompactCheckpoints(context.Background())
				failpoint.Reset()
				if !errors.Is(err, sentinel) || after == nil {
					t.Fatalf("failpoint did not run: %v", err)
				}
				trackTestMemFS(t, after)
				db.Close()
				opts := db.opts
				opts.FS = after
				reopened, err := Open("db", opts)
				if err != nil {
					t.Fatal(err)
				}
				defer reopened.Close()
				if got := recordCheckpointView(t, reopened, probes); !reflect.DeepEqual(before, got) {
					t.Fatal("merge crash changed reads")
				}
				hardeningCheckModel(t, reopened, model, "app/1/e")
				assertNoOrphanCheckpoints(t, reopened)
			})
		}
	}
}

func TestCheckpointMergeErrorAndBusyInputs(t *testing.T) {
	db, _, _, probes := checkpointFixture(t, 4, true)
	before := recordCheckpointView(t, db, probes)
	old := checkpointNums(db)
	sentinel := errors.New("after-write failure")
	failpoint.SetError(fpCkptMergeAfterWrite, sentinel)
	err := db.CompactCheckpoints(context.Background())
	failpoint.Reset()
	if !errors.Is(err, sentinel) {
		t.Fatalf("merge error: %v", err)
	}
	assertNoOrphanCheckpoints(t, db)
	if !reflect.DeepEqual(old, checkpointNums(db)) {
		t.Fatal("failed merge installed state")
	}
	db.mu.Lock()
	db.busy[old[0]] = true
	db.mu.Unlock()
	err = db.CompactCheckpoints(context.Background())
	db.mu.Lock()
	delete(db.busy, old[0])
	db.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	cur := db.vs.Acquire()
	if cur.Ckpts.ByNum[old[0]] == nil {
		t.Fatal("merged a busy checkpoint")
	}
	cur.Unref()
	if got := recordCheckpointView(t, db, probes); !reflect.DeepEqual(before, got) {
		t.Fatal("busy-input merge changed reads")
	}
}

func TestCheckpointMergeStaleInputsAndCancellation(t *testing.T) {
	db, _, _, probes := checkpointFixture(t, 3, true)
	before := recordCheckpointView(t, db, probes)
	old := checkpointNums(db)
	failpoint.Set(fpCkptMergeAfterWrite, func() error {
		v := db.vs.Acquire()
		cf := *v.Ckpts.ByNum[old[0]]
		v.Unref()
		// Replace a selected manifest record while copying is outside locks.
		installCheckpointEdit(t, db, &manifest.Edit{AddCkpts: []manifest.CkptFile{cf}})
		return nil
	})
	err := db.CompactCheckpoints(context.Background())
	failpoint.Reset()
	if err != nil {
		t.Fatal(err)
	}
	if db.Stats().CheckpointMerges != 0 {
		t.Fatal("stale merge installed output")
	}
	assertNoOrphanCheckpoints(t, db)
	ctx, cancel := context.WithCancel(context.Background())
	failpoint.Set(fpCkptMergeAfterWrite, func() error { cancel(); return nil })
	err = db.CompactCheckpoints(ctx)
	failpoint.Reset()
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled merge: %v", err)
	}
	assertNoOrphanCheckpoints(t, db)
	if got := recordCheckpointView(t, db, probes); !reflect.DeepEqual(before, got) {
		t.Fatal("aborted merge changed reads")
	}
}

func TestCheckpointMergeManifestSyncFailure(t *testing.T) {
	var fault *vfs.FaultFS
	db, _, _, probes := checkpointFixture(t, 3, true, func(o *Options) { fault = vfs.NewFaultFS(o.FS); o.FS = fault })
	before := recordCheckpointView(t, db, probes)
	old := checkpointNums(db)
	sentinel := errors.New("manifest sync failed")
	failpoint.Set(fpCkptMergeAfterWrite, func() error {
		fault.Inject("Sync", fault.Count("Sync")+1, sentinel)
		return nil
	})
	err := db.CompactCheckpoints(context.Background())
	failpoint.Reset()
	if !errors.Is(err, sentinel) {
		t.Fatalf("manifest sync failure: %v", err)
	}
	if !reflect.DeepEqual(old, checkpointNums(db)) {
		t.Fatal("failed edit changed current checkpoints")
	}
	assertNoOrphanCheckpoints(t, db)
	if got := recordCheckpointView(t, db, probes); !reflect.DeepEqual(before, got) {
		t.Fatal("failed sync changed reads")
	}
	// A failed Sync may still leave every byte durable. Force the current
	// manifest's full live contents into the image to test that worst case.
	baseFS := fault.FS.(*vfs.MemFS)
	name, err := manifest.ReadCurrent(baseFS, "db")
	if err != nil {
		t.Fatal(err)
	}
	mf, err := baseFS.OpenReadWrite(filepath.Join("db", name))
	if err != nil {
		t.Fatal(err)
	}
	if err := mf.Sync(); err != nil {
		t.Fatal(err)
	}
	mf.Close()
	baseFS.SyncDir("db")
	image := trackTestMemFS(t, baseFS.CrashClone(vfs.TornTail, rand.New(rand.NewSource(42))))
	opts := db.opts
	opts.FS = image
	db.Close()
	reopened, err := Open("db", opts)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if got := recordCheckpointView(t, reopened, probes); !reflect.DeepEqual(before, got) {
		t.Fatal("failed manifest sync broke recovery")
	}
	assertNoOrphanCheckpoints(t, reopened)
}

func TestCheckpointMergeCloseWaitsForManualCopy(t *testing.T) {
	db, _, _, _ := checkpointFixture(t, 3, true)
	written, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(release) }); failpoint.Reset() })
	failpoint.Set(fpCkptMergeAfterWrite, func() error { close(written); <-release; return nil })
	merged, closed := make(chan error, 1), make(chan error, 1)
	go func() { merged <- db.CompactCheckpoints(context.Background()) }()
	<-written
	go func() { closed <- db.Close() }()
	deadline := time.Now().Add(5 * time.Second)
	for !db.closed.Load() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !db.closed.Load() {
		t.Fatal("Close did not start")
	}
	select {
	case err := <-closed:
		t.Fatalf("Close returned with a copy in flight: %v", err)
	default:
	}
	once.Do(func() { close(release) })
	if err := <-merged; !errors.Is(err, ErrClosed) {
		t.Fatalf("closing merge: %v", err)
	}
	if err := <-closed; err != nil {
		t.Fatal(err)
	}
	failpoint.Reset()
	if err := db.CompactCheckpoints(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatalf("merge after Close: %v", err)
	}
}

func TestCheckpointMergeConcurrency(t *testing.T) {
	db, clk, model, _ := checkpointFixture(t, 5, true)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	t.Cleanup(func() { cancel(); wg.Wait() })
	wg.Go(func() {
		for ctx.Err() == nil {
			if err := db.CompactCheckpoints(ctx); err != nil && !errors.Is(err, context.Canceled) {
				errs <- err
				return
			}
			time.Sleep(time.Millisecond)
		}
	})
	writing := make(chan struct{})
	wg.Go(func() {
		for i := 0; ctx.Err() == nil; i++ {
			key, value := []byte("raw/writer"), []byte(fmt.Sprintf("concurrent value %d", i))
			ts, err := db.Put(key, value)
			if err != nil {
				errs <- err
				return
			}
			model.Commit([]base.Record{{Key: key, Value: value, Ts: ts, Kind: base.KindPut}})
			if i == 0 {
				close(writing)
			}
			time.Sleep(time.Millisecond)
		}
	})
	<-writing
	for round := 5; round < 18; round++ {
		addCheckpointRound(t, db, clk, model, round)
	}
	cancel()
	wg.Wait()
	select {
	case err := <-errs:
		t.Fatal(err)
	default:
	}
	hardeningCheckModel(t, db, model, "app/1/e")
	requireIntegrity(t, db)
}

func TestCheckpointMergeBackgroundBound(t *testing.T) {
	db, clk, model, _ := checkpointFixture(t, 3, true)
	// Run the same fast loop used by Open while the test drives semantic
	// jobs synchronously; those jobs overlap the background merge copies.
	db.opts.CkptMergeThreshold = 3
	db.compactWG.Add(1)
	go db.fastCompactLoop()
	maxCount := 0
	for round := 3; round < 32; round++ {
		addCheckpointRound(t, db, clk, model, round)
		maxCount = max(maxCount, checkpointCount(db))
	}
	deadline := time.Now().Add(5 * time.Second)
	for checkpointCount(db) > 3 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if count := checkpointCount(db); count > 3 || maxCount > 8 {
		t.Fatalf("unbounded checkpoint work: final %d max %d", count, maxCount)
	}
	if db.Stats().CheckpointMerges == 0 {
		t.Fatal("background never merged")
	}
	hardeningCheckModel(t, db, model, "app/1/e")
	requireIntegrity(t, db)
}
