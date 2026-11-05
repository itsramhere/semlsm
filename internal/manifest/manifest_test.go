package manifest

import (
	"errors"
	"os"
	"testing"

	"path/filepath"

	"semlsm/internal/base"
	"semlsm/internal/vfs"
	"semlsm/internal/wal"
)

func sampleEdit(snapshot bool) *Edit {
	return &Edit{
		NextFileNum: 42,
		LastTs:      1000,
		LogNum:      7,
		AddFiles: []FileMeta{
			{
				Level:    0,
				Num:      1,
				Size:     1234,
				Smallest: base.MakeInternalKey([]byte("a"), 500, base.KindPut),
				Largest:  base.MakeInternalKey([]byte("z"), 100, base.KindDelete),
				MinTs:    100,
				MaxTs:    500,
				Count:    9,
			},
			{
				Level:    2,
				Num:      2,
				Size:     999,
				Smallest: base.MakeInternalKey([]byte("m"), 10, base.KindPut),
				Largest:  base.MakeInternalKey([]byte("q"), 5, base.KindPut),
				MinTs:    5,
				MaxTs:    10,
				Count:    4,
			},
		},
		DelFiles: []DelFile{{Level: 0, Num: 99}},
		AddCkpts: []CkptFile{
			{
				FileNum: 3,
				Size:    555,
				Tuples: []TupleRef{
					{ID: 1, Group: "agent/1/", TsFrom: 10, TsTo: 20, Offset: 64, Length: 128},
					{ID: 2, Group: "agent/1/", TsFrom: 30, TsTo: 40, Offset: 200, Length: 300},
				},
			},
		},
		DelCkpts: []uint64{88},
	}
}

func TestManifestRoundTrip(t *testing.T) {
	fs := vfs.NewMemFS()
	e1 := sampleEdit(true)
	w, err := NewWriter(fs, "MANIFEST-000001")
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Append(e1); err != nil {
		t.Fatal(err)
	}
	e2 := &Edit{NextFileNum: 43, LastTs: 1001, LogNum: 8, AddFiles: []FileMeta{{
		Level: 1, Num: 5, Size: 10,
		Smallest: base.MakeInternalKey([]byte("b"), 1, base.KindPut),
		Largest:  base.MakeInternalKey([]byte("d"), 1, base.KindPut),
	}}}
	if err := w.Append(e2); err != nil {
		t.Fatal(err)
	}
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	edits, err := ReadAll(fs, "MANIFEST-000001")
	if err != nil {
		t.Fatal(err)
	}
	if len(edits) != 2 {
		t.Fatalf("got %d edits, want 2", len(edits))
	}
	if edits[0].NextFileNum != 42 || edits[0].LastTs != 1000 || edits[0].LogNum != 7 {
		t.Fatalf("edit 0 header wrong: %+v", edits[0])
	}
	if len(edits[0].AddFiles) != 2 || edits[0].AddFiles[0].Num != 1 {
		t.Fatalf("edit 0 files wrong: %+v", edits[0].AddFiles)
	}
	f := edits[0].AddFiles[0]
	if string(f.Smallest) != string(e1.AddFiles[0].Smallest) || f.MaxTs != 500 || f.Count != 9 {
		t.Fatalf("file meta wrong: %+v", f)
	}
	if len(edits[0].DelFiles) != 1 || edits[0].DelFiles[0].Num != 99 {
		t.Fatalf("del files wrong: %+v", edits[0].DelFiles)
	}
	if len(edits[0].AddCkpts) != 1 || len(edits[0].AddCkpts[0].Tuples) != 2 {
		t.Fatalf("ckpts wrong: %+v", edits[0].AddCkpts)
	}
	tu := edits[0].AddCkpts[0].Tuples[1]
	if tu.ID != 2 || tu.Group != "agent/1/" || tu.TsFrom != 30 || tu.TsTo != 40 || tu.Offset != 200 || tu.Length != 300 {
		t.Fatalf("tuple ref wrong: %+v", tu)
	}
	if len(edits[0].DelCkpts) != 1 || edits[0].DelCkpts[0] != 88 {
		t.Fatalf("del ckpts wrong: %+v", edits[0].DelCkpts)
	}
	if len(edits[1].AddFiles) != 1 || edits[1].AddFiles[0].Num != 5 {
		t.Fatalf("edit 1 wrong: %+v", edits[1])
	}
}

func TestManifestTornTailTolerance(t *testing.T) {
	fs := vfs.NewMemFS()
	w, _ := NewWriter(fs, "MANIFEST-000001")
	if err := w.Append(sampleEdit(true)); err != nil {
		t.Fatal(err)
	}
	if err := w.Append(&Edit{NextFileNum: 5}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	full := readAllManifest(t, fs, "MANIFEST-000001")

	// truncate into the last frame: earlier edits survive, no error
	for cut := len(full) - 20; cut < len(full); cut++ {
		tfs := vfs.NewMemFS()
		f, _ := tfs.Create("MANIFEST-000001")
		f.Write(full[:cut])
		f.Close()
		edits, err := ReadAll(tfs, "MANIFEST-000001")
		if err != nil {
			t.Fatalf("cut %d: %v", cut, err)
		}
		if len(edits) != 1 || edits[0].NextFileNum != 42 {
			t.Fatalf("cut %d: edits=%d", cut, len(edits))
		}
	}
}

func TestManifestCorruptFrame(t *testing.T) {
	// A complete frame with a valid CRC but invalid JSON is real corruption.
	// (A flipped byte inside the final frame is a torn tail by spec and is
	// ignored, so we craft the bad payload instead.)
	fs := vfs.NewMemFS()
	w, _ := NewWriter(fs, "MANIFEST-000001")
	if err := w.Append(sampleEdit(true)); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	full := readAllManifest(t, fs, "MANIFEST-000001")
	bad, ferr := wal.Frame([]byte("{not json"))
	if ferr != nil {
		t.Fatal(ferr)
	}
	cfs := vfs.NewMemFS()
	f, _ := cfs.Create("MANIFEST-000001")
	f.Write(full)
	f.Write(bad)
	f.Close()
	if _, err := ReadAll(cfs, "MANIFEST-000001"); !errors.Is(err, base.ErrCorruption) {
		t.Fatalf("err=%v, want ErrCorruption", err)
	}

	// a flipped byte inside the FINAL frame's payload is a torn tail and
	// is ignored (earlier edits survive): each frame is fsynced, so only
	// the last can be torn
	w2, _ := NewWriter(fs, "MANIFEST-000002")
	if err := w2.Append(sampleEdit(true)); err != nil {
		t.Fatal(err)
	}
	if err := w2.Append(&Edit{NextFileNum: 9}); err != nil {
		t.Fatal(err)
	}
	if err := w2.Close(); err != nil {
		t.Fatal(err)
	}
	two := readAllManifest(t, fs, "MANIFEST-000002")
	torn := append([]byte(nil), two...)
	torn[len(full)+4] ^= 0x01 // flip inside frame 2's payload
	tfs := vfs.NewMemFS()
	f3, _ := tfs.Create("MANIFEST-000001")
	f3.Write(torn)
	f3.Close()
	edits, err := ReadAll(tfs, "MANIFEST-000001")
	if err != nil {
		t.Fatalf("torn tail must be ignored: %v", err)
	}
	if len(edits) != 1 {
		t.Fatalf("torn tail: edits=%d, want 1", len(edits))
	}
}

func TestCurrentAtomicity(t *testing.T) {
	fs := vfs.NewMemFS()
	fs.MkdirAll("db")

	if err := SetCurrent(fs, "db", "MANIFEST-000001"); err != nil {
		t.Fatal(err)
	}
	name, err := ReadCurrent(fs, "db")
	if err != nil || name != "MANIFEST-000001" {
		t.Fatalf("got %q err=%v", name, err)
	}

	// crash after rename but before SyncDir: old value survives.
	// (SetCurrent completes with SyncDir, so reproduce the window manually.)
	tmp := filepath.Join("db", "CURRENT.tmp")
	f, err := fs.Create(tmp)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte("MANIFEST-000002")); err != nil {
		t.Fatal(err)
	}
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if err := fs.Rename(tmp, filepath.Join("db", "CURRENT")); err != nil {
		t.Fatal(err)
	}
	after := fs.CrashClone(vfs.DropUnsynced, nil)
	cn, err := ReadCurrent(after, "db")
	if err != nil || cn != "MANIFEST-000001" {
		t.Fatalf("after crash: got %q err=%v (rename must not be durable before SyncDir)", cn, err)
	}
	// once SyncDir runs, the new value is durable
	if err := fs.SyncDir("db"); err != nil {
		t.Fatal(err)
	}
	after2 := fs.CrashClone(vfs.DropUnsynced, nil)
	cn2, err := ReadCurrent(after2, "db")
	if err != nil || cn2 != "MANIFEST-000002" {
		t.Fatalf("after syncdir+crash: got %q err=%v", cn2, err)
	}
	// live view sees the new value immediately
	name, err = ReadCurrent(fs, "db")
	if err != nil || name != "MANIFEST-000002" {
		t.Fatalf("live: got %q err=%v", name, err)
	}

	// empty CURRENT is corruption
	efs := vfs.NewMemFS()
	efs.MkdirAll("db2")
	f2, _ := efs.Create(filepath.Join("db2", "CURRENT"))
	f2.Close()
	if _, err := ReadCurrent(efs, "db2"); !errors.Is(err, base.ErrCorruption) {
		t.Fatalf("empty current: err=%v", err)
	}
	// garbage CURRENT is corruption
	gfs := vfs.NewMemFS()
	gfs.MkdirAll("db3")
	g, _ := gfs.Create(filepath.Join("db3", "CURRENT"))
	g.Write([]byte("garbage"))
	g.Close()
	if _, err := ReadCurrent(gfs, "db3"); !errors.Is(err, base.ErrCorruption) {
		t.Fatalf("garbage current: err=%v", err)
	}
}

func TestManifestNameRoundTrip(t *testing.T) {
	for _, n := range []uint64{0, 1, 42, 123456} {
		name := ManifestName(n)
		got, ok := ParseManifestNum(name)
		if !ok || got != n {
			t.Fatalf("name %s: got %d ok=%v", name, got, ok)
		}
	}
	if _, ok := ParseManifestNum("CURRENT"); ok {
		t.Fatal("CURRENT must not parse")
	}
}

func readAllManifest(t *testing.T, fs vfs.FS, name string) []byte {
	t.Helper()
	f, err := fs.Open(name)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, st.Size)
	if _, err := f.ReadAt(buf, 0); err != nil && !errors.Is(err, os.ErrNotExist) && err.Error() != "EOF" {
		// ReadAt of exactly Size bytes should not error on MemFS or OSFS
		t.Fatal(err)
	}
	return buf
}
