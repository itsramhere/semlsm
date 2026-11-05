package vfs

import (
	"errors"
	"io"
	"os"
	"strings"
	"testing"
)

// contractScenario exercises behavior that must be identical between OSFS and
// MemFS (M-004 item 8). Differences are bugs in MemFS.
func contractScenario(t *testing.T, fs FS, root string) {
	t.Helper()

	// create / write / sync / close / read to EOF / ReadAt
	f, err := fs.Create(root + "/a.txt")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte("hello world")); err != nil {
		t.Fatal(err)
	}
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	r, err := fs.Open(root + "/a.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close() // also guards abrupt t.Fatalf paths on Windows
	buf := make([]byte, 32)
	var all []byte
	for {
		n, err := r.Read(buf)
		all = append(all, buf[:n]...)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("read: %v", err)
		}
	}
	if string(all) != "hello world" {
		t.Fatalf("read to EOF = %q", all)
	}
	// ReadAt at EOF and past EOF: zero bytes read, io.EOF
	n, err := r.ReadAt(buf, 11)
	if n != 0 || err != io.EOF {
		t.Fatalf("ReadAt at EOF: n=%d err=%v", n, err)
	}
	n, err = r.ReadAt(buf, 50)
	if n != 0 || err != io.EOF {
		t.Fatalf("ReadAt past EOF: n=%d err=%v", n, err)
	}
	n, err = r.ReadAt(buf[:5], 8)
	if err != io.EOF || n != 3 || string(buf[:3]) != "rld" {
		t.Fatalf("ReadAt short: n=%d err=%v buf=%q", n, err, buf[:3])
	}
	// use after close
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	if err := r.Close(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("double close: %v", err)
	}
	if _, err := r.Read(buf); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("read after close: %v", err)
	}
	if _, err := r.ReadAt(buf, 0); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("readat after close: %v", err)
	}
	if err := r.Sync(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("sync after close: %v", err)
	}
	// Stat after close: os.File behavior is platform-specific here (Windows
	// returns an invalid-handle error, not os.ErrClosed), so the contract is
	// only that it must fail.
	if _, err := r.Stat(); err == nil {
		t.Fatal("stat after close must fail")
	}

	// write offsets on reopen: OpenReadWrite starts at offset 0
	rw, err := fs.OpenReadWrite(root + "/a.txt")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rw.Write([]byte("HE")); err != nil {
		t.Fatal(err)
	}
	rw.Close()
	r2, _ := fs.Open(root + "/a.txt")
	check := make([]byte, 11)
	if _, err := io.ReadFull(r2, check); err != nil || string(check) != "HEllo world" {
		t.Fatalf("reopen overwrite: %q err=%v", check, err)
	}
	r2.Close()

	// Truncate then Write: the handle offset does NOT reset (os.File
	// semantics), so the write lands at the old offset, zero-padding the gap.
	tf, _ := fs.Create(root + "/b.txt")
	if _, err := tf.Write([]byte("abcdef")); err != nil {
		t.Fatal(err)
	}
	if err := tf.Truncate(2); err != nil {
		t.Fatal(err)
	}
	if _, err := tf.Write([]byte("XYZ")); err != nil {
		t.Fatal(err)
	}
	tf.Sync()
	tf.Close()
	v := readFileContract(t, fs, root+"/b.txt")
	if v != "ab\x00\x00\x00\x00XYZ" {
		t.Fatalf("truncate+write = %q, want the zero-padded os.File result", v)
	}

	// remove then open / stat / list
	if err := fs.Remove(root + "/b.txt"); err != nil {
		t.Fatal(err)
	}
	if _, err := fs.Open(root + "/b.txt"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("open after remove: %v", err)
	}
	if _, err := fs.Stat(root + "/b.txt"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stat after remove: %v", err)
	}
	entries, err := fs.List(root)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if e.Name == "b.txt" {
			t.Fatalf("list still contains removed file: %+v", entries)
		}
	}

	// rename over an existing file
	cf, _ := fs.Create(root + "/c.txt")
	if _, err := cf.Write([]byte("ccc")); err != nil {
		t.Fatal(err)
	}
	cf.Close()
	df, _ := fs.Create(root + "/d.txt")
	if _, err := df.Write([]byte("ddd")); err != nil {
		t.Fatal(err)
	}
	df.Close()
	if err := fs.Rename(root+"/c.txt", root+"/d.txt"); err != nil {
		t.Fatal(err)
	}
	if got := readFileContract(t, fs, root+"/d.txt"); got != "ccc" {
		t.Fatalf("rename over existing: d.txt = %q, want ccc", got)
	}
	if _, err := fs.Open(root + "/c.txt"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("renamed source must be gone: %v", err)
	}

	// Stat name is the base name
	st, err := fs.Stat(root + "/a.txt")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(st.Name, "a.txt") || st.Name == root+"/a.txt" {
		t.Fatalf("stat name = %q, want base name a.txt", st.Name)
	}
}

func readFileContract(t *testing.T, fs FS, name string) string {
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
	if _, err := io.ReadFull(f, buf); err != nil {
		t.Fatal(err)
	}
	return string(buf)
}

func TestFSContract(t *testing.T) {
	t.Run("osfs", func(t *testing.T) {
		contractScenario(t, OSFS{}, t.TempDir())
	})
	t.Run("memfs", func(t *testing.T) {
		fs := NewMemFS()
		fs.MkdirAll("root")
		contractScenario(t, fs, "root")
		if n := fs.OpenHandles(); n != 0 {
			t.Fatalf("handle leak in contract scenario: %d open", n)
		}
	})
}

// M-004 item 3: a durable entry must never be lost by later non-durable
// operations on the same name.

func TestMemFSRemoveCreateRemoveKeepsDurableOnCrash(t *testing.T) {
	fs := NewMemFS()
	f, _ := fs.Create("db/X")
	f.Write([]byte("v1"))
	f.Sync()
	f.Close()
	fs.SyncDir("db")

	// durable X, remove, recreate, remove again, no SyncDir
	fs.Remove("db/X")
	g, _ := fs.Create("db/X")
	g.Write([]byte("v2-unsynced"))
	g.Close()
	fs.Remove("db/X")

	after := fs.CrashClone(DropUnsynced, nil)
	data := readAllVFS(t, after, "db/X")
	if string(data) != "v1" {
		t.Fatalf("X after crash = %q, want v1 (durable entry lost)", data)
	}
	// live view: X is gone
	if _, err := fs.Open("db/X"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("live open after remove: %v", err)
	}
}

func TestMemFSRenameOverDurableTargetCrash(t *testing.T) {
	fs := NewMemFS()
	for name, data := range map[string]string{"db/A": "va", "db/T": "vt"} {
		f, _ := fs.Create(name)
		f.Write([]byte(data))
		f.Sync()
		f.Close()
	}
	fs.SyncDir("db")

	// rename over an existing durable target without SyncDir
	fs.Rename("db/A", "db/T")
	after := fs.CrashClone(DropUnsynced, nil)
	if got := readAllVFS(t, after, "db/T"); string(got) != "vt" {
		t.Fatalf("T after crash = %q, want vt (revert)", got)
	}
	if got := readAllVFS(t, after, "db/A"); string(got) != "va" {
		t.Fatalf("A after crash = %q, want va", got)
	}
	// after SyncDir the rename is durable
	fs.SyncDir("db")
	after2 := fs.CrashClone(DropUnsynced, nil)
	if got := readAllVFS(t, after2, "db/T"); string(got) != "va" {
		t.Fatalf("T after sync+crash = %q, want va", got)
	}
	if _, err := after2.Open("db/A"); err == nil {
		t.Fatal("A must be gone after durable rename")
	}
}

func TestMemFSTwoRenamesOntoTargetCrash(t *testing.T) {
	fs := NewMemFS()
	for name, data := range map[string]string{"db/A": "va", "db/B": "vb", "db/T": "vt"} {
		f, _ := fs.Create(name)
		f.Write([]byte(data))
		f.Sync()
		f.Close()
	}
	fs.SyncDir("db")

	fs.Rename("db/A", "db/T") // displaced T (durable) and, below, displaced A (durable)
	fs.Rename("db/B", "db/T") // displaced A state must not erase the durable T state

	after := fs.CrashClone(DropUnsynced, nil)
	if got := readAllVFS(t, after, "db/A"); string(got) != "va" {
		t.Fatalf("A = %q, want va", got)
	}
	if got := readAllVFS(t, after, "db/B"); string(got) != "vb" {
		t.Fatalf("B = %q, want vb", got)
	}
	if got := readAllVFS(t, after, "db/T"); string(got) != "vt" {
		t.Fatalf("T = %q, want vt", got)
	}
}

// M-004 item 4: closed-handle behavior and the leak counter.

func TestMemFSUseAfterCloseAndOpenHandles(t *testing.T) {
	fs := NewMemFS()
	f, _ := fs.Create("db/x")
	if n := fs.OpenHandles(); n != 1 {
		t.Fatalf("handles after create = %d", n)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("double close: %v", err)
	}
	if _, err := f.Write([]byte("x")); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("write after close: %v", err)
	}
	if _, err := f.Read(make([]byte, 1)); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("read after close: %v", err)
	}
	if err := f.Sync(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("sync after close: %v", err)
	}
	if err := f.Truncate(0); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("truncate after close: %v", err)
	}
	if _, err := f.Stat(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("stat after close: %v", err)
	}
	if n := fs.OpenHandles(); n != 0 {
		t.Fatalf("handles after close = %d, want 0", n)
	}
}

// M-004 item 5: Stat reports the current name, not a stale one.

func TestMemFSStatTracksCurrentName(t *testing.T) {
	fs := NewMemFS()
	f, _ := fs.Create("db/new.tmp")
	st, err := f.Stat()
	if err != nil || st.Name != "new.tmp" {
		t.Fatalf("stat on new file: %+v err=%v", st, err)
	}
	f.Sync()
	f.Close()
	fs.SyncDir("db")
	fs.Rename("db/new.tmp", "db/final")
	r, err := fs.Open("db/final")
	if err != nil {
		t.Fatal(err)
	}
	st, err = r.Stat()
	if err != nil || st.Name != "final" {
		t.Fatalf("stat after rename: %+v err=%v", st, err)
	}
	r.Close()
}

// Write must honor the handle offset: overwriting in place, then extending.
func TestMemFSWriteHonorsOffset(t *testing.T) {
	fs := NewMemFS()
	f, _ := fs.Create("db/x")
	f.Write([]byte("0123456789"))
	f.Sync()
	f.Close()

	rw, _ := fs.OpenReadWrite("db/x") // offset 0, like os.File O_RDWR
	rw.Write([]byte("AB"))
	rw.Close()
	if got := readAllVFS(t, fs, "db/x"); string(got) != "AB23456789" {
		t.Fatalf("overwrite at 0: %q", got)
	}

	// Seek past EOF via a fresh handle at offset 0 cannot happen; emulate by
	// writing from a handle opened after growth: offset tracking is per handle.
	h, _ := fs.OpenReadWrite("db/x")
	h.Write([]byte("0123456789")) // rewrite first 10 bytes, offset now 10
	h.Write([]byte("ZZ"))         // extends
	h.Close()
	if got := readAllVFS(t, fs, "db/x"); string(got) != "0123456789ZZ" {
		t.Fatalf("extend: %q", got)
	}
}

func readAllVFS(t *testing.T, fs FS, name string) []byte {
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
	if _, err := io.ReadFull(f, buf); err != nil {
		t.Fatal(err)
	}
	return buf
}
