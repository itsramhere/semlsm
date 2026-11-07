package semlsm

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"semlsm/internal/base"
	"semlsm/internal/llm"
	"semlsm/internal/manifest"
	"semlsm/internal/vfs"
	"semlsm/internal/wal"
)

func lastFrameOffset(t *testing.T, data []byte) int {
	t.Helper()
	last := 0
	for off := 0; off < len(data); {
		if off+8 > len(data) {
			t.Fatal("short test frame")
		}
		last = off
		off += 8 + int(binary.LittleEndian.Uint32(data[off:]))
	}
	return last
}

func requireIntegrity(t *testing.T, db *DB) {
	t.Helper()
	rep, err := db.VerifyIntegrity(context.Background())
	if err != nil || len(rep.Problems) != 0 {
		t.Fatalf("integrity: %+v, %v", rep, err)
	}
}

func TestRepairManifestQuarantinesRolledBackEdit(t *testing.T) {
	db, clk, _ := openSemDB(t, "db", llm.NewMock(llm.MockValid), func(o *Options) { o.ArchiveRaw = true })
	t.Cleanup(func() { db.Close() })
	var timestamps []uint64
	for i := 0; i < 40; i++ {
		ts, err := db.Put([]byte("agent/1/s"), []byte(fmt.Sprintf("value-%03d repeated payload repeated payload", i)))
		if err != nil {
			t.Fatal(err)
		}
		timestamps = append(timestamps, ts)
	}
	driveDown(t, db)
	// Keep the pre-edit SSTs until the simulated crash, as a snapshot would.
	snap := db.NewSnapshot()
	defer snap.Release()
	clk.Set(semStart.Add(3 * time.Hour))
	if err := db.CompactLevel(context.Background(), base.NumLevels-1); err != nil {
		t.Fatal(err)
	}
	fs := trackTestMemFS(t, db.opts.FS.(*vfs.MemFS).CrashClone(vfs.DropUnsynced, nil))
	opts := db.opts
	opts.FS = fs
	name, err := manifest.ReadCurrent(fs, "db")
	if err != nil {
		t.Fatal(err)
	}
	data := hardeningReadFile(t, fs, "db", name)
	off := lastFrameOffset(t, data)
	edits, err := manifest.ReadAll(fs, filepath.Join("db", name))
	if err != nil {
		t.Fatal(err)
	}
	edit := edits[len(edits)-1]
	if len(edit.AddCkpts) == 0 || len(edit.AddCkpts[0].ArchiveNums) == 0 {
		t.Fatal("expected semantic edit with archives")
	}
	hardeningFlip(t, fs, "db", name, off+8)
	if opened, err := Open("db", opts); !errors.Is(err, ErrCorruption) {
		if opened != nil {
			opened.Close()
		}
		t.Fatalf("damaged manifest: %v", err)
	}
	report, err := Repair("db", opts)
	if err != nil {
		t.Fatal(err)
	}
	wantTruncated := []TruncatedFile{{name, int64(len(data) - off)}}
	if !reflect.DeepEqual(report.TruncatedFiles, wantTruncated) {
		t.Fatalf("truncations: %+v", report)
	}
	wantMoves := len(edit.AddFiles) + len(edit.AddCkpts) + len(edit.AddCkpts[0].ArchiveNums)
	if len(report.QuarantinedFiles) < wantMoves {
		t.Fatalf("quarantine: %+v, want %d", report, wantMoves)
	}
	quarantined := map[string]bool{}
	for _, name := range report.QuarantinedFiles {
		quarantined[name] = true
	}
	for _, f := range edit.AddFiles {
		if !quarantined[filepath.Join("lost+found", sstName(f.Num))] {
			t.Fatalf("missing output %d: %+v", f.Num, report)
		}
	}
	for _, cf := range edit.AddCkpts {
		if !quarantined[filepath.Join("lost+found", ckptName(cf.FileNum))] {
			t.Fatalf("missing checkpoint: %+v", report)
		}
		for _, num := range cf.ArchiveNums {
			if !quarantined[filepath.Join("lost+found", "archive", sstName(num))] {
				t.Fatalf("missing archive: %+v", report)
			}
		}
	}
	for _, name := range report.QuarantinedFiles {
		if _, err := fs.Stat(filepath.Join("db", name)); err != nil {
			t.Fatal(err)
		}
	}
	reopened, err := Open("db", opts)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	requireIntegrity(t, reopened)
	for i, ts := range timestamps {
		res, err := reopened.GetAt([]byte("agent/1/s"), ts)
		if err != nil || res.Kind != ResultExact || string(res.Value) != fmt.Sprintf("value-%03d repeated payload repeated payload", i) {
			t.Fatalf("pre-edit read %d: %+v %v", i, res, err)
		}
	}
	// Open's sweep must leave every quarantine entry intact.
	for _, name := range report.QuarantinedFiles {
		if _, err := fs.Stat(filepath.Join("db", name)); err != nil {
			t.Fatal(err)
		}
	}
}

func TestRepairLastWAL(t *testing.T) {
	fs := newTestMemFS(t)
	opts := testOptions(fs, base.RealClock{})
	db, err := Open("db", opts)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		if _, err := db.Put([]byte(fmt.Sprint(i)), []byte(fmt.Sprintf("value-%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	name := walName(db.walNum)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	data := hardeningReadFile(t, fs, "db", name)
	off := lastFrameOffset(t, data)
	hardeningFlip(t, fs, "db", name, len(data)-1)
	for i := 0; i < 2; i++ {
		if opened, err := Open("db", opts); !errors.Is(err, ErrCorruption) {
			if opened != nil {
				opened.Close()
			}
			t.Fatalf("damaged WAL: %v", err)
		}
		if fs.OpenFiles() != 0 {
			t.Fatalf("failed Open leaked: %v", fs.OpenFileNames())
		}
	}
	report, err := Repair("db", opts)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(report.TruncatedFiles, []TruncatedFile{{name, int64(len(data) - off)}}) {
		t.Fatalf("report: %+v", report)
	}
	db, err = Open("db", opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for i := 0; i < 10; i++ {
		res, err := db.Get([]byte(fmt.Sprint(i)))
		if err != nil {
			t.Fatal(err)
		}
		if i == 9 {
			if res.Kind != ResultNotFound {
				t.Fatalf("damaged write survived: %+v", res)
			}
		} else if res.Kind != ResultExact || string(res.Value) != fmt.Sprintf("value-%d", i) {
			t.Fatalf("earlier write %d: %+v", i, res)
		}
	}
	requireIntegrity(t, db)
}

func TestRepairRejectsOtherCorruption(t *testing.T) {
	for _, log := range []string{"manifest", "wal"} {
		for _, problem := range []string{"middle-crc", "good-crc-bad-payload", "hidden-frame", "hidden-length", "older-wal"} {
			if log == "manifest" && problem == "older-wal" {
				continue
			}
			t.Run(log+"/"+problem, func(t *testing.T) {
				fs := newTestMemFS(t)
				opts := testOptions(fs, base.RealClock{})
				db, err := Open("db", opts)
				if err != nil {
					t.Fatal(err)
				}
				name := walName(db.walNum)
				if log == "manifest" {
					name = manifest.ManifestName(db.manifestNum)
				}
				if err := db.Close(); err != nil {
					t.Fatal(err)
				}
				payload := []byte("{}")
				if log == "wal" {
					payload, err = wal.EncodeBatch(1, []base.Record{{Key: []byte("k"), Value: []byte("v"), Kind: base.KindPut}})
					if err != nil {
						t.Fatal(err)
					}
				}
				good, err := wal.Frame(payload)
				if err != nil {
					t.Fatal(err)
				}
				bad := append([]byte(nil), good...)
				bad[len(bad)-1] ^= 1
				var tail []byte
				switch problem {
				case "middle-crc":
					tail = append(bad, good...)
				case "hidden-length":
					tail = append(bad, good...)
					binary.LittleEndian.PutUint32(tail, uint32(len(tail)+1))
				case "good-crc-bad-payload":
					tail, err = wal.Frame([]byte("invalid"))
				case "hidden-frame":
					tail, err = wal.Frame(append([]byte("damage"), good...))
					tail[8] ^= 1
				case "older-wal":
					tail = bad
					w, e := fs.Create(filepath.Join("db", walName(db.walNum+100)))
					if e != nil {
						t.Fatal(e)
					}
					w.Close()
				}
				if err != nil {
					t.Fatal(err)
				}
				before := hardeningReadFile(t, fs, "db", name)
				w, err := fs.Create(filepath.Join("db", name))
				if err != nil {
					t.Fatal(err)
				}
				w.Write(append(before, tail...))
				w.Close()
				before = hardeningReadFile(t, fs, "db", name)
				if _, err := Repair("db", opts); !errors.Is(err, ErrCorruption) {
					t.Fatalf("repair: %v", err)
				}
				if after := hardeningReadFile(t, fs, "db", name); !reflect.DeepEqual(before, after) {
					t.Fatal("failed repair changed log")
				}
			})
		}
	}
}

func TestRepairExclusiveLock(t *testing.T) {
	for _, fs := range []vfs.FS{newTestMemFS(t), vfs.OSFS{}} {
		dir := t.TempDir()
		opts := testOptions(fs, base.RealClock{})
		db, err := Open(dir, opts)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := Repair(dir, opts); err == nil {
			t.Error("repair acquired an open DB's lock")
		}
		db.Close()
		if _, err := Repair(dir, opts); err != nil {
			t.Fatal(err)
		}
	}
}

func TestRepairQuarantinePreservesExistingFiles(t *testing.T) {
	fs := newTestMemFS(t)
	opts := testOptions(fs, base.RealClock{})
	db, err := Open("db", opts)
	if err != nil {
		t.Fatal(err)
	}
	db.Close()
	fs.MkdirAll(filepath.Join("db", "lost+found"))
	fs.MkdirAll(filepath.Join("db", "archive"))
	for name, data := range map[string]string{
		filepath.Join("lost+found", "999999.sst"): "existing quarantine",
		"999999.sst":                           "orphan table",
		"orphan.ckpt":                          "orphan checkpoint",
		filepath.Join("archive", "999999.sst"): "orphan archive",
	} {
		w, err := fs.Create(filepath.Join("db", name))
		if err != nil {
			t.Fatal(err)
		}
		w.Write([]byte(data))
		w.Sync()
		w.Close()
	}
	report, err := Repair("db", opts)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.TruncatedFiles) != 0 || len(report.QuarantinedFiles) != 3 {
		t.Fatalf("report: %+v", report)
	}
	for name, want := range map[string]string{
		filepath.Join("lost+found", "999999.sst"):            "existing quarantine",
		filepath.Join("lost+found", "999999.sst.1"):          "orphan table",
		filepath.Join("lost+found", "orphan.ckpt"):           "orphan checkpoint",
		filepath.Join("lost+found", "archive", "999999.sst"): "orphan archive",
	} {
		if got := string(hardeningReadFile(t, fs, "db", name)); got != want {
			t.Fatalf("%s: %q, want %q", name, got, want)
		}
	}
	// Quarantine names and contents must survive a crash, as well as Open.
	after := trackTestMemFS(t, fs.CrashClone(vfs.DropUnsynced, nil))
	if got := string(hardeningReadFile(t, after, "db", filepath.Join("lost+found", "archive", "999999.sst"))); got != "orphan archive" {
		t.Fatalf("quarantine lost across crash: %q", got)
	}
	report, err = Repair("db", opts)
	if err != nil || len(report.QuarantinedFiles) != 0 {
		t.Fatalf("repeat repair: %+v %v", report, err)
	}
}

func TestRotateFailureClosesNewWAL(t *testing.T) {
	for _, operation := range []string{"SyncDir", "Sync"} {
		t.Run(operation, func(t *testing.T) {
			fs := newTestMemFS(t)
			fault := vfs.NewFaultFS(fs)
			db, err := Open("db", testOptions(fault, base.RealClock{}))
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			before := fs.OpenFiles()
			sentinel := errors.New("rotation failure")
			fault.Inject(operation, fault.Count(operation)+1, sentinel)
			if err := db.rotate(); !errors.Is(err, sentinel) {
				t.Fatalf("rotate: %v", err)
			}
			if fs.OpenFiles() != before {
				t.Fatalf("rotation leaked: %v", fs.OpenFileNames())
			}
		})
	}
}
