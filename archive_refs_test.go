package semlsm

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/itsramhere/semlsm/internal/base"
	"github.com/itsramhere/semlsm/internal/llm"
	"github.com/itsramhere/semlsm/internal/manifest"
	"github.com/itsramhere/semlsm/internal/vfs"
	"github.com/itsramhere/semlsm/internal/wal"
)

func archiveFixture(t *testing.T) (*DB, manifest.CkptFile, uint64) {
	t.Helper()
	db, clk, _ := openSemDB(t, "db", llm.NewMock(llm.MockValid), func(o *Options) { o.ArchiveRaw = true })
	t.Cleanup(func() { db.Close() })
	var ts uint64
	for i := 0; i < 40; i++ {
		written, err := db.Put([]byte("agent/1/s"), []byte(fmt.Sprintf("value-%03d repeated payload repeated payload", i)))
		if err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			ts = written
		}
	}
	driveDown(t, db)
	clk.Set(semStart.Add(3 * time.Hour))
	if err := db.CompactLevel(context.Background(), base.NumLevels-1); err != nil {
		t.Fatal(err)
	}
	cur := db.vs.Acquire()
	defer cur.Unref()
	for _, cf := range cur.Ckpts.ByNum {
		if len(cf.ArchiveNums) == 0 {
			t.Fatal("semantic job did not record archives")
		}
		return db, *cf, ts
	}
	t.Fatal("no checkpoint")
	return nil, manifest.CkptFile{}, 0
}

func installCheckpointEdit(t *testing.T, db *DB, e *manifest.Edit) {
	t.Helper()
	db.manifestMu.Lock()
	err := db.appendManifestLocked(e, db.vs.CurrentLastTs())
	if err == nil {
		db.mu.Lock()
		old := db.rs
		db.rs = &readState{mem: old.mem, imms: old.imms, version: db.vs.Acquire()}
		old.release()
		db.mu.Unlock()
	}
	db.manifestMu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
}

func TestDyingCheckpointArchivesWithoutFile(t *testing.T) {
	for _, damage := range []string{"removed", "corrupted"} {
		t.Run(damage, func(t *testing.T) {
			db, cf, _ := archiveFixture(t)
			pin := db.NewSnapshot()
			defer pin.Release()
			if damage == "removed" {
				if err := db.opts.FS.Remove(filepath.Join("db", ckptName(cf.FileNum))); err != nil {
					t.Fatal(err)
				}
			} else {
				hardeningFlip(t, db.opts.FS, "db", ckptName(cf.FileNum), 0)
			}
			installCheckpointEdit(t, db, &manifest.Edit{DelCkpts: []uint64{cf.FileNum}})
			db.drainObsolete()
			for _, num := range cf.ArchiveNums {
				if _, err := db.opts.FS.Stat(filepath.Join(db.archiveDir(), sstName(num))); err != nil {
					t.Fatalf("snapshot lost archive: %v", err)
				}
			}
			pin.Release()
			db.drainObsolete()
			// Close drains the deletion queue and joins its worker.
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			for _, num := range cf.ArchiveNums {
				if _, err := db.opts.FS.Stat(filepath.Join(db.archiveDir(), sstName(num))); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("dying archive %d remains: %v", num, err)
				}
			}
		})
	}
}

func TestDyingCheckpointPreservesCurrentArchives(t *testing.T) {
	db, cf, _ := archiveFixture(t)
	// Model an edit transferring archives to another checkpoint. Checkpoint
	// merge itself is deliberately left for the follow-up.
	newCF := cf
	newCF.FileNum = db.allocFile()
	data := hardeningReadFile(t, db.opts.FS, "db", ckptName(cf.FileNum))
	w, err := db.opts.FS.Create(filepath.Join("db", ckptName(newCF.FileNum)))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write(data); err != nil {
		t.Fatal(err)
	}
	w.Close()

	installCheckpointEdit(t, db, &manifest.Edit{DelCkpts: []uint64{cf.FileNum}, AddCkpts: []manifest.CkptFile{newCF}})
	db.drainObsolete()
	db.Close()
	for _, num := range cf.ArchiveNums {
		if _, err := db.opts.FS.Stat(filepath.Join(db.archiveDir(), sstName(num))); err != nil {
			t.Fatalf("current archive deleted: %v", err)
		}
	}
}

func TestLegacyCheckpointArchives(t *testing.T) {
	for _, damage := range []string{"none", "header", "tuple"} {
		t.Run(damage, func(t *testing.T) {
			db, cf, ts := archiveFixture(t)
			cf.ArchiveNums = nil // a pre-archive_nums manifest
			installCheckpointEdit(t, db, &manifest.Edit{AddCkpts: []manifest.CkptFile{cf}})
			if err := db.rewriteManifest(); err != nil {
				t.Fatal(err)
			}
			fs := db.opts.FS.(*vfs.MemFS)
			opts := db.opts
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			// Make the legacy fixture literally omit archive_nums, rather
			// than relying on the equivalent nil/null representation.
			name, err := manifest.ReadCurrent(fs, "db")
			if err != nil {
				t.Fatal(err)
			}
			edits, err := manifest.ReadAll(fs, filepath.Join("db", name))
			if err != nil {
				t.Fatal(err)
			}
			mw, err := fs.Create(filepath.Join("db", name))
			if err != nil {
				t.Fatal(err)
			}
			for _, edit := range edits {
				payload, err := json.Marshal(edit)
				if err != nil {
					t.Fatal(err)
				}
				payload = bytes.ReplaceAll(payload, []byte(`,"archive_nums":null`), nil)
				frame, err := wal.Frame(payload)
				if err != nil {
					t.Fatal(err)
				}
				mw.Write(frame)
			}
			mw.Sync()
			mw.Close()
			var log bytes.Buffer
			opts.Logger = slog.New(slog.NewTextHandler(&log, nil))
			// Include an unrelated archive: unknown legacy references must
			// disable the entire archive sweep, not just preserve known ones.
			orphan := filepath.Join(db.archiveDir(), "999999.sst")
			w, err := fs.Create(orphan)
			if err != nil {
				t.Fatal(err)
			}
			w.Close()
			if damage != "none" {
				off := 0
				if damage == "tuple" {
					off = int(cf.Tuples[0].Offset) + 8
				}
				hardeningFlip(t, fs, "db", ckptName(cf.FileNum), off)
			}
			reopened, err := Open("db", opts)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			if damage == "none" {
				res, err := reopened.GetArchived([]byte("agent/1/s"), ts)
				if err != nil || res.Kind != ResultExact || string(res.Value) != "value-000 repeated payload repeated payload" {
					t.Fatalf("legacy archive read: %+v %v", res, err)
				}
				requireIntegrity(t, reopened)
				if _, err := fs.Stat(orphan); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("known orphan remains: %v", err)
				}
			} else {
				if _, err := fs.Stat(orphan); err != nil {
					t.Fatalf("unknown archive deleted: %v", err)
				}
				if !bytes.Contains(log.Bytes(), []byte("skipping archive orphan sweep")) {
					t.Fatalf("missing diagnostic: %s", log.String())
				}
				entries, _ := fs.List(db.archiveDir())
				if len(entries) < 2 {
					t.Fatalf("live legacy archive deleted: %v", entries)
				}
			}
		})
	}
}

func TestArchiveNumsSurviveManifestRoll(t *testing.T) {
	db, cf, ts := archiveFixture(t)
	oldNum := db.manifestNum
	// Fill the writer through its regular Append API without changing state;
	// appendManifestLocked then exercises the actual threshold roll path.
	db.manifestMu.Lock()
	for db.manifest.Size() < manifest.RollThreshold {
		if err := db.manifest.Append(&manifest.Edit{NextFileNum: db.nextFileNum}); err != nil {
			t.Fatal(err)
		}
	}
	err := db.appendManifestLocked(&manifest.Edit{}, db.vs.CurrentLastTs())
	db.manifestMu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if oldNum == db.manifestNum {
		t.Fatal("manifest did not roll")
	}
	opts := db.opts
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open("db", opts)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	cur := reopened.vs.Acquire()
	defer cur.Unref()
	if got := cur.Ckpts.ByNum[cf.FileNum]; got == nil || !reflect.DeepEqual(got.ArchiveNums, cf.ArchiveNums) {
		t.Fatalf("archive numbers lost: %+v", got)
	}
	if res, err := reopened.GetArchived([]byte("agent/1/s"), ts); err != nil || res.Kind != ResultExact {
		t.Fatalf("archive read after roll: %+v %v", res, err)
	}
	requireIntegrity(t, reopened)
}
