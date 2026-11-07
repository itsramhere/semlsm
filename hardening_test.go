package semlsm

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"semlsm/internal/base"
	"semlsm/internal/failpoint"
	"semlsm/internal/llm"
	"semlsm/internal/manifest"
	"semlsm/internal/testmodel"
	"semlsm/internal/vfs"
)

// goroutinesAtStart is the P13.5 baseline for the leak checks.
var goroutinesAtStart int

// TestMain captures the goroutine baseline before any test starts.
func TestMain(m *testing.M) {
	goroutinesAtStart = runtime.NumGoroutine()
	os.Exit(m.Run())
}

// awaitGoroutineSettle polls until the goroutine count is back at the
// baseline (P13.5): every test that opens a DB registers this as a cleanup,
// so a worker or job that survives Close fails the test.
func awaitGoroutineSettle(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if n := runtime.NumGoroutine(); n <= goroutinesAtStart+2 {
			return
		} else if time.Now().After(deadline) {
			buf := make([]byte, 1<<20)
			n := runtime.Stack(buf, true)
			t.Logf("lingering goroutines:\n%s", buf[:n])
			t.Fatalf("goroutines did not settle after Close: %d (baseline %d)", runtime.NumGoroutine(), goroutinesAtStart)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// hardeningCommit writes one record through the DB and the model.
func hardeningCommit(t *testing.T, db *DB, model *testmodel.Model, key string, i int) {
	t.Helper()
	v := fmt.Sprintf("v%03d %s", i, strings.Repeat(strings.ReplaceAll(key, "/", "-"), 4))
	ts, err := db.Put([]byte(key), []byte(v))
	if err != nil {
		t.Fatal(err)
	}
	model.Commit([]base.Record{{Key: []byte(key), Ts: ts, Kind: base.KindPut, Value: []byte(v)}})
}

// TestHardeningCrashMatrix is P13.1: for every failpoint in the codebase and
// both crash modes, a scripted workload with semantic compaction on crashes
// at the point, reopens under MemFS, and must satisfy the model checks and a
// clean VerifyIntegrity. The crash is then repeated during the recovery
// window itself once.
func TestHardeningCrashMatrix(t *testing.T) {
	modes := []struct {
		name string
		mode vfs.CrashMode
		rng  *rand.Rand
	}{
		{"drop-unsynced", vfs.DropUnsynced, nil},
		{"torn-tail", vfs.TornTail, rand.New(rand.NewSource(42))},
	}
	for _, fp := range failpoint.List() {
		for _, m := range modes {
			t.Run(fp+"/"+m.name, func(t *testing.T) {
				hardeningCrashAt(t, fp, m.mode, m.rng)
			})
		}
	}
}

func hardeningCrashAt(t *testing.T, fp string, mode vfs.CrashMode, rng *rand.Rand) {
	t.Helper()
	t.Cleanup(func() { awaitGoroutineSettle(t) })
	ctx := context.Background()
	fs := newTestMemFS(t)
	clk := base.NewManualClock(semStart)
	stack := newSemStack(t, fs, "db", llm.NewMock(llm.MockValid), 0)
	o := semOptions(fs, clk, stack)
	o.ArchiveRaw = true
	db, err := Open("db", o)
	if err != nil {
		t.Fatal(err)
	}
	model := testmodel.NewModel()
	for i := 0; i < 40; i++ {
		hardeningCommit(t, db, model, "agent/1/s", i)
		hardeningCommit(t, db, model, "app/1/e", i)
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	for lvl := 0; lvl < 4; lvl++ {
		if err := db.CompactLevel(ctx, lvl); err != nil {
			t.Fatal(err)
		}
	}
	clk.Set(semStart.Add(3 * time.Hour))

	// Arm the failpoint and trigger the operation that reaches it.
	sentinel := errors.New("crash at " + fp)
	if strings.HasPrefix(fp, "ckptmerge.") {
		// Build two real semantic checkpoints before arming the merge hook.
		for i := 40; i < 60; i++ {
			hardeningCommit(t, db, model, "agent/1/s", i)
			hardeningCommit(t, db, model, "app/1/e", i)
		}
		driveDown(t, db)
		clk.Advance(3 * time.Hour)
		if err := db.CompactLevel(ctx, base.NumLevels-1); err != nil {
			t.Fatal(err)
		}
		if checkpointCount(db) < 2 {
			t.Fatal("merge failpoint needs two checkpoints")
		}
		// Keep the events key's newest read exact for this matrix's model
		// contract; its earlier versions remain summarized in the inputs.
		hardeningCommit(t, db, model, "app/1/e", 60)
	}
	failpoint.SetError(fp, sentinel)
	switch {
	case strings.HasPrefix(fp, "ckptmerge."):
		_ = db.CompactCheckpoints(ctx)
	case strings.HasPrefix(fp, "flush."):
		for i := 40; i < 60; i++ {
			hardeningCommit(t, db, model, "agent/1/s", i)
			hardeningCommit(t, db, model, "app/1/e", i)
		}
		_ = db.Flush()
	case strings.HasPrefix(fp, "compact."):
		_ = db.CompactLevel(ctx, 0)
	default: // semantic.*
		for i := 40; i < 60; i++ {
			hardeningCommit(t, db, model, "agent/1/s", i)
			hardeningCommit(t, db, model, "app/1/e", i)
		}
		if err := db.Flush(); err != nil {
			t.Fatal(err)
		}
		for lvl := 0; lvl < 4; lvl++ {
			_ = db.CompactLevel(ctx, lvl)
		}
		_ = db.CompactLevel(ctx, base.NumLevels-1)
	}
	failpoint.Reset()

	// Crash at the failpoint moment and reopen on the clone.
	after := trackTestMemFS(t, fs.CrashClone(mode, rng))
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	stack2 := newSemStack(t, after, "db", llm.NewMock(llm.MockValid), 0)
	o2 := semOptions(after, clk, stack2)
	o2.ArchiveRaw = true
	db2, err := Open("db", o2)
	if err != nil {
		t.Fatalf("reopen after %s (%v): %v", fp, mode, err)
	}

	// Crash during the recovery window itself: leave unflushed WAL data,
	// crash, arm the flush failpoint, and reopen — recovery replays the WAL
	// and flushes straight into the armed failpoint.
	for i := 60; i < 70; i++ {
		hardeningCommit(t, db2, model, "agent/1/s", i)
	}
	if err := db2.Close(); err != nil {
		t.Fatal(err)
	}
	after2 := trackTestMemFS(t, after.CrashClone(mode, rng))
	failpoint.SetError("flush.after_sst_sync", sentinel)
	if _, err := Open("db", hardeningOptions(t, after2, clk)); err == nil {
		// The recovery flush did not reach the failpoint (nothing to
		// replay): fall back to crashing the first flush after recovery.
		db3, err := Open("db", hardeningOptions(t, after2, clk))
		if err != nil {
			t.Fatalf("reopen after %s: %v", fp, err)
		}
		_ = db3.Flush()
		db3.Close()
		after2 = trackTestMemFS(t, after2.CrashClone(mode, rng))
	}
	failpoint.Reset()
	db3, err := Open("db", hardeningOptions(t, after2, clk))
	if err != nil {
		t.Fatalf("final reopen after %s (%v): %v", fp, mode, err)
	}
	defer db3.Close()
	hardeningCheckModel(t, db3, model)
	rep, err := db3.VerifyIntegrity(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(rep.Problems) > 0 {
		t.Fatalf("VerifyIntegrity after %s (%v) crash: %v", fp, mode, rep.Problems)
	}
}

// hardeningOptions builds semantic options over an existing FS and clock.
func hardeningOptions(t *testing.T, fs vfs.FS, clk *base.ManualClock) Options {
	t.Helper()
	stack := newSemStack(t, fs, "db", llm.NewMock(llm.MockValid), 0)
	o := semOptions(fs, clk, stack)
	o.ArchiveRaw = true
	return o
}

// hardeningCheckModel verifies every key's latest read against the model.
// Keys in allowSummarized (fully-dropped event groups) may legitimately read
// Summarized; an Exact answer must always match the model exactly.
func hardeningCheckModel(t *testing.T, db *DB, model *testmodel.Model, allowSummarized ...string) {
	t.Helper()
	ok := map[string]bool{}
	for _, k := range allowSummarized {
		ok[k] = true
	}
	for _, mk := range model.Keys() {
		mts, _, mv, mok := model.VisibleAt(mk, model.MaxTs())
		if !mok {
			continue
		}
		res, err := db.Get(mk)
		if err != nil {
			// ErrCorruption is an acceptable outcome under corruption; the
			// crash and IO matrices would surface it in VerifyIntegrity.
			if errors.Is(err, ErrCorruption) {
				continue
			}
			t.Fatalf("get %q: %v", mk, err)
		}
		if res.Kind == ResultSummarized && ok[string(mk)] {
			continue
		}
		if res.Kind != ResultExact || res.VersionTs != mts || string(res.Value) != string(mv) {
			t.Fatalf("key %q: got %+v, want (%d,%q)", mk, res, mts, mv)
		}
	}
}

// TestHardeningCorruptionMatrix is P13.2: flip a byte in every file type at
// sampled offsets. The result must be ErrCorruption on read or verify, or an
// exact correct result — never a wrong answer, never a panic.
func TestHardeningCorruptionMatrix(t *testing.T) {
	t.Cleanup(func() { awaitGoroutineSettle(t) })
	ctx := context.Background()
	fs := newTestMemFS(t)
	clk := base.NewManualClock(semStart)
	stack := newSemStack(t, fs, "db", llm.NewMock(llm.MockValid), 0)
	o := semOptions(fs, clk, stack)
	o.ArchiveRaw = true
	db, err := Open("db", o)
	if err != nil {
		t.Fatal(err)
	}
	model := testmodel.NewModel()
	for i := 0; i < 40; i++ {
		hardeningCommit(t, db, model, "agent/1/s", i)
		hardeningCommit(t, db, model, "app/1/e", i)
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	for lvl := 0; lvl < 4; lvl++ {
		if err := db.CompactLevel(ctx, lvl); err != nil {
			t.Fatal(err)
		}
	}
	clk.Set(semStart.Add(3 * time.Hour))
	if err := db.CompactLevel(ctx, base.NumLevels-1); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	type fileRef struct{ dir, name string }
	var files []fileRef
	for _, dir := range []string{"db", "db/archive"} {
		ents, err := fs.List(dir)
		if err != nil {
			continue
		}
		for _, e := range ents {
			if !e.IsDir && e.Size > 0 {
				files = append(files, fileRef{dir, e.Name})
			}
		}
	}
	if len(files) == 0 {
		t.Fatal("no files to corrupt")
	}
	currentName, err := manifest.ReadCurrent(fs, "db")
	if err != nil {
		t.Fatal(err)
	}

	for _, fr := range files {
		data := hardeningReadFile(t, fs, fr.dir, fr.name)
		offsets := []int{0, 1, len(data) / 2, len(data) - 2, len(data) - 1}
		for _, off := range offsets {
			if off < 0 || off >= len(data) {
				continue
			}
			t.Run(fr.name+"@"+fmt.Sprint(off), func(t *testing.T) {
				bad := trackTestMemFS(t, fs.CrashClone(vfs.DropUnsynced, nil)) // a fresh copy
				hardeningFlip(t, bad, fr.dir, fr.name, off)
				stack2 := newSemStack(t, bad, "db", llm.NewMock(llm.MockValid), 0)
				o2 := semOptions(bad, clk, stack2)
				o2.ArchiveRaw = true
				db2, err := Open("db", o2)
				if err != nil {
					// Failing open is fine when it is a corruption or a
					// documented mismatch error; anything else is unclean.
					if errors.Is(err, ErrCorruption) || errors.Is(err, ErrOptionsMismatch) {
						return
					}
					t.Fatalf("unclean open error: %v", err)
				}
				defer db2.Close()
				if fr.name == currentName && off >= len(data)-2 {
					t.Fatal("complete final manifest frame with a bad CRC must fail Open")
				}
				// Every read either returns the exact correct value or an
				// ErrCorruption — never a wrong answer. app/1/e is ModeEvents
				// and fully dropped, so a Summarized answer is correct there.
				hardeningCheckModel(t, db2, model, "app/1/e")
				rep, err := db2.VerifyIntegrity(ctx)
				if err != nil && !errors.Is(err, ErrCorruption) {
					t.Fatalf("verify: unclean error %v", err)
				}
				_ = rep
			})
		}
	}
}

func hardeningReadFile(t *testing.T, fs vfs.FS, dir, name string) []byte {
	t.Helper()
	f, err := fs.Open(fs2path(dir, name))
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

func hardeningFlip(t *testing.T, fs vfs.FS, dir, name string, off int) {
	t.Helper()
	data := hardeningReadFile(t, fs, dir, name)
	data[off] ^= 0xFF
	w, err := fs.Create(fs2path(dir, name))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write(data); err != nil {
		w.Close()
		t.Fatal(err)
	}
	if err := w.Sync(); err != nil {
		w.Close()
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

// fs2path joins dir and name (filepath.Join for tests).
func fs2path(dir, name string) string {
	if dir == "." || dir == "" {
		return name
	}
	return dir + string(os.PathSeparator) + name
}

// TestHardeningIOErrors is P13.3: fail each kind of write and sync during
// flush, structural compaction, semantic compaction, checkpoint write and
// manifest append. The DB surfaces the error or retries; every write
// acknowledged before the fault survives the reopen.
func TestHardeningIOErrors(t *testing.T) {
	phases := []struct {
		name   string
		setup  func(t *testing.T, db *DB, model *testmodel.Model, clk *base.ManualClock)
		target func(db *DB) error
	}{
		{"flush", func(t *testing.T, db *DB, model *testmodel.Model, clk *base.ManualClock) {
			hardeningCommit(t, db, model, "agent/1/s", 1000)
		}, func(db *DB) error { return db.Flush() }},
		{"compact-structural", func(t *testing.T, db *DB, model *testmodel.Model, clk *base.ManualClock) {
			if err := db.Flush(); err != nil {
				t.Fatal(err)
			}
		}, func(db *DB) error { return db.CompactLevel(context.Background(), 0) }},
		{"compact-semantic", func(t *testing.T, db *DB, model *testmodel.Model, clk *base.ManualClock) {
			if err := db.Flush(); err != nil {
				t.Fatal(err)
			}
			for lvl := 0; lvl < 2; lvl++ {
				if err := db.CompactLevel(context.Background(), lvl); err != nil {
					t.Fatal(err)
				}
			}
			clk.Set(semStart.Add(3 * time.Hour))
		}, func(db *DB) error { return db.CompactLevel(context.Background(), 2) }},
		{"ckpt-archive", func(t *testing.T, db *DB, model *testmodel.Model, clk *base.ManualClock) {
			if err := db.Flush(); err != nil {
				t.Fatal(err)
			}
			for lvl := 0; lvl < 4; lvl++ {
				if err := db.CompactLevel(context.Background(), lvl); err != nil {
					t.Fatal(err)
				}
			}
			clk.Set(semStart.Add(3 * time.Hour))
		}, func(db *DB) error { return db.CompactLevel(context.Background(), base.NumLevels-1) }},
	}
	injected := []string{"Create", "Write", "Sync", "SyncDir"}
	for _, ph := range phases {
		for _, op := range injected {
			t.Run(ph.name+"/"+op, func(t *testing.T) {
				t.Cleanup(func() { awaitGoroutineSettle(t) })
				errIO := errors.New("injected io error")
				mem := newTestMemFS(t)
				fs := vfs.NewFaultFS(mem)
				clk := base.NewManualClock(semStart)
				stack := newSemStack(t, fs, "db", llm.NewMock(llm.MockValid), 0)
				o := semOptions(fs, clk, stack)
				o.ArchiveRaw = true
				db, err := Open("db", o)
				if err != nil {
					t.Fatal(err)
				}
				model := testmodel.NewModel()
				for i := 0; i < 20; i++ {
					hardeningCommit(t, db, model, "agent/1/s", i)
					hardeningCommit(t, db, model, "app/1/e", i)
				}
				if err := db.WaitIdle(context.Background()); err != nil {
					t.Fatal(err)
				}

				before := fs.Count(op)
				fs.Inject(op, 1, errIO)
				func() {
					defer func() {
						if r := recover(); r != nil {
							t.Fatalf("panic on %s/%s: %v", ph.name, op, r)
						}
					}()
					ph.setup(t, db, model, clk)
					_ = ph.target(db) // the injected error may or may not abort it
				}()
				hit := fs.Count(op) > before
				fs.Clear(op)
				_ = db.Close() // the DB may be in a failed state; reopen decides

				// Reopen on the same FS: every acknowledged write survives.
				stack2 := newSemStack(t, fs, "db", llm.NewMock(llm.MockValid), 0)
				o2 := semOptions(fs, clk, stack2)
				o2.ArchiveRaw = true
				db2, err := Open("db", o2)
				if err != nil {
					t.Fatalf("reopen after %s/%s: %v", ph.name, op, err)
				}
				defer db2.Close()
				hardeningCheckModel(t, db2, model, "app/1/e")
				rep, err := db2.VerifyIntegrity(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				if len(rep.Problems) > 0 {
					t.Fatalf("problems after %s/%s: %v", ph.name, op, rep.Problems)
				}
				if !hit {
					t.Logf("op %s not exercised by phase %s", op, ph.name)
				}
			})
		}
	}
}

// TestCkptHandlesReleasedAfterClose is the P13.5 Windows file-handle check:
// after Close, every .ckpt and archive file must be removable — a removal
// failure means a leaked handle (this caught Close never calling
// ckptCache.CloseAll, which the soak's reopen churn exposed).
func TestCkptHandlesReleasedAfterClose(t *testing.T) {
	t.Cleanup(func() { awaitGoroutineSettle(t) })
	ctx := context.Background()
	dir := t.TempDir()
	fs := vfs.OSFS{}
	clk := base.NewManualClock(semStart)
	stack := newSemStack(t, fs, dir, llm.NewMock(llm.MockValid), 0)
	o := semOptions(fs, clk, stack)
	o.ArchiveRaw = true
	db, err := Open(dir, o)
	if err != nil {
		t.Fatal(err)
	}
	key := []byte("agent/1/x")
	for i := 0; i < 14; i++ {
		if _, err := db.Put(key, []byte(fmt.Sprintf("v%02d %s", i, strings.Repeat("x", 290)))); err != nil {
			t.Fatal(err)
		}
	}
	driveDown(t, db)
	clk.Set(semStart.Add(2 * time.Hour))
	if err := db.CompactLevel(ctx, base.NumLevels-1); err != nil {
		t.Fatal(err)
	}
	if db.Stats().SemanticTuplesCreated == 0 {
		t.Skip("no tuple")
	}
	// Exercise every checkpoint-opening path before the close.
	if _, err := db.Get(key); err != nil {
		t.Fatal(err)
	}
	if _, err := db.History(key, 0, ^uint64(0)); err != nil {
		t.Fatal(err)
	}
	it := db.NewIterator(IterOptions{})
	for it.Next() {
	}
	if err := it.Err(); err != nil {
		t.Fatal(err)
	}
	it.Close()
	if _, err := db.GetArchived(key, 1); err != nil && !errors.Is(err, ErrNotArchived) {
		t.Fatal(err)
	}
	rep, err := db.VerifyIntegrity(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(rep.Problems) > 0 {
		t.Fatalf("verify problems: %v", rep.Problems)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// Every ckpt and archive file must now be removable.
	for _, dir2 := range []string{dir, filepath.Join(dir, "archive")} {
		ents, err := fs.List(dir2)
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range ents {
			if e.IsDir {
				continue
			}
			if filepath.Ext(e.Name) != ".ckpt" && filepath.Ext(e.Name) != ".sst" {
				continue
			}
			if err := os.Remove(filepath.Join(dir2, e.Name)); err != nil {
				t.Errorf("leaked handle on %s: %v", filepath.Join(dir2, e.Name), err)
			}
		}
	}
}
