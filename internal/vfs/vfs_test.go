package vfs

import (
	"errors"
	"io"
	"math/rand"
	"os"
	"strings"
	"testing"
)

func writeAll(t *testing.T, f File, data string) {
	t.Helper()
	if _, err := f.Write([]byte(data)); err != nil {
		t.Fatalf("write: %v", err)
	}
}

func TestMemFSUnsyncedDataLost(t *testing.T) {
	fs := testMemFS(t)
	f, err := fs.Create("db/000001.sst")
	if err != nil {
		t.Fatal(err)
	}
	writeAll(t, f, "0123456789")
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
	writeAll(t, f, "abcdef") // unsynced tail
	f.Close()
	fs.SyncDir("db")

	after := trackMemFS(t, fs.CrashClone(DropUnsynced, nil))
	rf, err := after.Open("db/000001.sst")
	if err != nil {
		t.Fatalf("file should exist after crash: %v", err)
	}
	defer rf.Close()
	st, _ := rf.Stat()
	if st.Size != 10 {
		t.Fatalf("size after crash = %d, want 10 (unsynced bytes lost)", st.Size)
	}
	buf := make([]byte, 10)
	if _, err := io.ReadFull(rf, buf); err != nil {
		t.Fatal(err)
	}
	if string(buf) != "0123456789" {
		t.Fatalf("content = %q", buf)
	}
}

func TestMemFSTornTail(t *testing.T) {
	fs := testMemFS(t)
	f, _ := fs.Create("db/000001.sst")
	writeAll(t, f, "0123456789")
	f.Sync()
	writeAll(t, f, "abcdef")
	f.Close()
	fs.SyncDir("db")

	rng := rand.New(rand.NewSource(7))
	for i := 0; i < 50; i++ {
		after := trackMemFS(t, fs.CrashClone(TornTail, rng))
		rf, err := after.Open("db/000001.sst")
		if err != nil {
			t.Fatal(err)
		}
		st, _ := rf.Stat()
		if st.Size < 10 || st.Size > 16 {
			t.Fatalf("torn size %d out of [10,16]", st.Size)
		}
		buf := make([]byte, st.Size)
		if _, err := io.ReadFull(rf, buf); err != nil {
			t.Fatal(err)
		}
		want := ("0123456789" + "abcdef")[:st.Size]
		if string(buf) != want {
			t.Fatalf("torn content %q, want prefix %q", buf, want)
		}
		rf.Close()
	}
}

func TestMemFSRenameRevertsOnCrash(t *testing.T) {
	fs := testMemFS(t)
	f, _ := fs.Create("db/CURRENT.tmp")
	writeAll(t, f, "MANIFEST-000001")
	f.Sync()
	f.Close()
	fs.SyncDir("db")

	// rename without SyncDir: reverts on crash
	if err := fs.Rename("db/CURRENT.tmp", "db/CURRENT"); err != nil {
		t.Fatal(err)
	}
	after := trackMemFS(t, fs.CrashClone(DropUnsynced, nil))
	if _, err := after.Open("db/CURRENT"); err == nil {
		t.Fatal("CURRENT should not exist after crash before SyncDir")
	}
	rf, err := after.Open("db/CURRENT.tmp")
	if err != nil {
		t.Fatalf("CURRENT.tmp should still exist after crash: %v", err)
	}
	rf.Close()

	// now with SyncDir: rename is durable
	fs.SyncDir("db")
	after2 := trackMemFS(t, fs.CrashClone(DropUnsynced, nil))
	rf2, err := after2.Open("db/CURRENT")
	if err != nil {
		t.Fatalf("CURRENT should exist after SyncDir: %v", err)
	}
	rf2.Close()
	if _, err := after2.Open("db/CURRENT.tmp"); err == nil {
		t.Fatal("CURRENT.tmp should be gone")
	}
}

func TestMemFSRemoveNotDurableSurvives(t *testing.T) {
	fs := testMemFS(t)
	f, _ := fs.Create("db/000001.sst")
	writeAll(t, f, "data")
	f.Sync()
	f.Close()
	fs.SyncDir("db")

	fs.Remove("db/000001.sst")
	after := trackMemFS(t, fs.CrashClone(DropUnsynced, nil))
	if f, err := after.Open("db/000001.sst"); err != nil {
		t.Fatal("removal without SyncDir must not be durable")
	} else {
		f.Close()
	}
	fs.SyncDir("db")
	after2 := trackMemFS(t, fs.CrashClone(DropUnsynced, nil))
	if _, err := after2.Open("db/000001.sst"); err == nil {
		t.Fatal("removal should be durable after SyncDir")
	}
}

func TestMemFSCreatedNotInDurableDirLost(t *testing.T) {
	fs := testMemFS(t)
	f, _ := fs.Create("db/000001.sst")
	writeAll(t, f, "data")
	f.Sync()
	f.Close()
	// no SyncDir("db") at all
	after := trackMemFS(t, fs.CrashClone(DropUnsynced, nil))
	if _, err := after.Open("db/000001.sst"); err == nil {
		t.Fatal("file in never-synced dir must be absent after crash")
	}
}

func TestMemFSReadSemantics(t *testing.T) {
	fs := testMemFS(t)
	f, _ := fs.Create("x")
	writeAll(t, f, "hello world")
	f.Close()
	r, _ := fs.Open("x")
	defer r.Close()
	buf := make([]byte, 5)
	if n, err := r.Read(buf); err != nil || n != 5 || string(buf) != "hello" {
		t.Fatalf("read: n=%d err=%v buf=%q", n, err, buf)
	}
	if n, err := r.ReadAt(buf, 6); err != nil || n != 5 || string(buf) != "world" {
		t.Fatalf("readat: n=%d err=%v buf=%q", n, err, buf)
	}
	// EOF only after all data has been read: first call returns the rest.
	big := make([]byte, 32)
	if n, err := r.Read(big); err != nil || n != 6 {
		t.Fatalf("expected (6, nil), got (%d, %v)", n, err)
	}
	if _, err := r.Read(big); err != io.EOF {
		t.Fatalf("expected io.EOF, got %v", err)
	}
}

func TestMemFSListAndStat(t *testing.T) {
	fs := testMemFS(t)
	f, _ := fs.Create("db/a.sst")
	writeAll(t, f, "12345")
	f.Close()
	g, _ := fs.Create("db/b.ckpt")
	writeAll(t, g, "1")
	g.Close()
	fs.MkdirAll("db/archive")

	entries, err := fs.List("db")
	if err != nil {
		t.Fatal(err)
	}
	byName := map[string]FileInfo{}
	for _, e := range entries {
		byName[e.Name] = e
	}
	a, okA := byName["a.sst"]
	if !okA || a.IsDir || a.Size != 5 {
		t.Fatalf("a.sst wrong: %+v present=%v", a, okA)
	}
	b, okB := byName["b.ckpt"]
	if !okB || b.IsDir {
		t.Fatalf("b.ckpt wrong: %+v present=%v", b, okB)
	}
	arch, okArch := byName["archive"]
	if !okArch || !arch.IsDir {
		t.Fatalf("archive dir wrong: %+v present=%v", arch, okArch)
	}
	st, err := fs.Stat("db/a.sst")
	if err != nil || st.Size != 5 {
		t.Fatalf("stat: %+v err=%v", st, err)
	}
	if _, err := fs.Stat("db/missing"); err == nil || !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stat missing: err=%v", err)
	}
}

func TestFaultFSInjectedErrorsSurface(t *testing.T) {
	base := testMemFS(t)
	ff := NewFaultFS(base)
	sentinel := errors.New("injected sync failure")
	ff.Inject("Sync", 1, sentinel)

	f, err := ff.Create("db/wal")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	writeAll(t, f, "payload")
	if err := f.Sync(); err != sentinel {
		t.Fatalf("first sync: %v", err)
	}
	if err := f.Sync(); err != nil {
		t.Fatalf("second sync should pass: %v", err)
	}
	if ff.Count("Sync") != 2 {
		t.Fatalf("sync count = %d", ff.Count("Sync"))
	}

	// "payload" was already the 1st Write through the wrapper, so the next
	// write is the 2nd overall.
	ff.Inject("Write", 2, sentinel)
	if _, err := f.Write([]byte("x")); err != sentinel {
		t.Fatalf("second write: %v", err)
	}
	if _, err := f.Write([]byte("y")); err != nil {
		t.Fatalf("third write should pass: %v", err)
	}

	ff.Inject("Rename", 1, sentinel)
	if err := ff.Rename("db/wal", "db/wal2"); err != sentinel {
		t.Fatalf("rename: %v", err)
	}
	ff.Clear("")
	if err := ff.Rename("db/wal", "db/wal2"); err != nil {
		t.Fatalf("rename after clear: %v", err)
	}
}

func TestFaultFSWrapsRealFS(t *testing.T) {
	base := OSFS{}
	ff := NewFaultFS(base)
	dir := t.TempDir()
	ff.Inject("Create", 1, errors.New("nope"))
	if _, err := ff.Create(dir + "/f"); err == nil {
		t.Fatal("expected injected error")
	}
	ff.Clear("")
	f, err := ff.Create(dir + "/f")
	if err != nil {
		t.Fatal(err)
	}
	writeAll(t, f, "abc")
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
	f.Close()
	st, err := base.Stat(dir + "/f")
	if err != nil || st.Size != 3 {
		t.Fatalf("underlying FS got wrong content: %+v %v", st, err)
	}
	if !strings.HasSuffix(st.Name, "f") {
		t.Fatalf("name = %q", st.Name)
	}
}

func TestOSFSBasic(t *testing.T) {
	dir := t.TempDir()
	fs := OSFS{}
	f, err := fs.Create(dir + "/x.txt")
	if err != nil {
		t.Fatal(err)
	}
	writeAll(t, f, "hello")
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
	f.Close()
	if err := fs.SyncDir(dir); err != nil {
		t.Fatal(err)
	}
	r, err := fs.Open(dir + "/x.txt")
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 5)
	if _, err := io.ReadFull(r, buf); err != nil || string(buf) != "hello" {
		t.Fatalf("read back: %q %v", buf, err)
	}
	r.Close()
	if err := fs.Rename(dir+"/x.txt", dir+"/y.txt"); err != nil {
		t.Fatal(err)
	}
	if err := fs.Remove(dir + "/y.txt"); err != nil {
		t.Fatal(err)
	}
}

func TestOSFSLockExclusive(t *testing.T) {
	dir := t.TempDir()
	fs := OSFS{}
	l1, err := fs.Lock(dir + "/LOCK")
	if err != nil {
		t.Fatalf("first lock: %v", err)
	}
	l2, err := fs.Lock(dir + "/LOCK")
	if err == nil {
		t.Fatal("second lock must fail while held")
		_ = l2
	}
	if err := l1.Unlock(); err != nil {
		t.Fatalf("unlock: %v", err)
	}
	l3, err := fs.Lock(dir + "/LOCK")
	if err != nil {
		t.Fatalf("relock after unlock: %v", err)
	}
	l3.Unlock()
}
