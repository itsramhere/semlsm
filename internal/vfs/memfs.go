package vfs

import (
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
)

// CrashMode selects what happens to unsynced data in CrashClone.
type CrashMode int

const (
	// DropUnsynced keeps only bytes that were Sync()ed before the crash.
	DropUnsynced CrashMode = iota
	// TornTail additionally keeps a random prefix of the unsynced bytes,
	// simulating partial page-cache flush.
	TornTail
)

// memState is the simulated on-disk state of one file. The state object is
// shared by all handles to the same file, like an inode.
type memState struct {
	name      string // current name in the live view
	data      []byte
	syncedLen int    // bytes durable after a crash (Sync updates this)
	crashName string // name the file has after a crash; "" = entry not durable
}

// MemFS is an in-memory FS for crash simulation. It models:
//   - Sync()ed bytes survive a crash; unsynced bytes are lost (or torn).
//   - Directory entries (creates, renames, removals) become durable on
//     SyncDir. A rename without SyncDir reverts on crash; a removal without
//     SyncDir leaves the file present with its synced content.
//   - The live view is always what a running process sees: a removed file is
//     immediately invisible to Open/Stat/List, exactly like a real OS.
//     Pending directory changes affect only CrashClone.
type MemFS struct {
	mu          sync.Mutex
	files       map[string]*memState
	graveyard   []*memState // displaced/removed states whose dir-entry change is not durable yet
	durableDirs map[string]bool
	handles     int // open handle count, for leak checks
}

// NewMemFS returns an empty MemFS.
func NewMemFS() *MemFS {
	return &MemFS{
		files:       map[string]*memState{},
		graveyard:   nil,
		durableDirs: map[string]bool{},
	}
}

func parentOf(name string) string { return filepath.Dir(name) }

func baseOf(name string) string { return filepath.Base(name) }

// tombstone records that st left the live view (removed, or displaced by a
// rename). It keeps appearing at st.crashName after a crash until the
// containing directory is synced. States are never overwritten here: each
// displaced state is independent, so a crash cannot lose a durable entry.
func (fs *MemFS) tombstone(st *memState) {
	fs.graveyard = append(fs.graveyard, st)
}

// errNotExist builds a not-found error for name.
func errNotExist(op, name string) error {
	return fmt.Errorf("vfs(memfs): %s %s: %w", op, name, os.ErrNotExist)
}

// errClosed builds an already-closed error matching os.File behavior.
func errClosed(op string) error {
	return fmt.Errorf("vfs(memfs): %s: %w", op, os.ErrClosed)
}

// Create implements FS.
func (fs *MemFS) Create(name string) (File, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	st := &memState{name: name}
	fs.files[name] = st
	fs.handles++
	return &memFile{fs: fs, st: st, writable: true}, nil
}

// Open implements FS. A removed file is not openable, even before SyncDir.
func (fs *MemFS) Open(name string) (File, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	st, ok := fs.files[name]
	if !ok {
		return nil, errNotExist("open", name)
	}
	fs.handles++
	return &memFile{fs: fs, st: st, writable: false}, nil
}

// OpenReadWrite implements FS.
func (fs *MemFS) OpenReadWrite(name string) (File, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	st, ok := fs.files[name]
	if !ok {
		st = &memState{name: name}
		fs.files[name] = st
	}
	fs.handles++
	return &memFile{fs: fs, st: st, writable: true}, nil
}

// Rename implements FS.
func (fs *MemFS) Rename(oldname, newname string) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	st, ok := fs.files[oldname]
	if !ok {
		return errNotExist("rename", oldname)
	}
	delete(fs.files, oldname)
	if target, exists := fs.files[newname]; exists {
		// POSIX rename replaces the target; the old target keeps its synced
		// content until the directory is synced.
		fs.tombstone(target)
	}
	st.name = newname
	fs.files[newname] = st
	return nil
}

// Remove implements FS. The file disappears from the live view immediately.
func (fs *MemFS) Remove(name string) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	st, ok := fs.files[name]
	if !ok {
		return errNotExist("remove", name)
	}
	delete(fs.files, name)
	fs.tombstone(st)
	return nil
}

// MkdirAll implements FS.
func (fs *MemFS) MkdirAll(path string) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	// Register the directory so List and Stat see it.
	fs.durableDirs[path] = true
	return nil
}

// List implements FS (live view only).
func (fs *MemFS) List(dir string) ([]FileInfo, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	var out []FileInfo
	for name, st := range fs.files {
		if parentOf(name) == dir {
			out = append(out, FileInfo{Name: baseOf(name), Size: int64(len(st.data))})
		}
	}
	for d := range fs.durableDirs {
		if parentOf(d) == dir {
			out = append(out, FileInfo{Name: baseOf(d), IsDir: true})
		}
	}
	return out, nil
}

// Stat implements FS (live view only).
func (fs *MemFS) Stat(name string) (FileInfo, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if st, ok := fs.files[name]; ok {
		return FileInfo{Name: baseOf(st.name), Size: int64(len(st.data))}, nil
	}
	if fs.durableDirs[name] {
		return FileInfo{Name: baseOf(name), IsDir: true}, nil
	}
	return FileInfo{}, errNotExist("stat", name)
}

// SyncDir implements FS: makes creates, renames and removals in dir durable.
func (fs *MemFS) SyncDir(dir string) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.durableDirs[dir] = true
	for name, st := range fs.files {
		if parentOf(name) == dir {
			st.crashName = name
		}
	}
	kept := fs.graveyard[:0]
	for _, st := range fs.graveyard {
		if parentOf(st.crashName) != dir {
			kept = append(kept, st)
		}
	}
	fs.graveyard = kept
	return nil
}

// Lock implements FS (a no-op in-memory lock).
func (fs *MemFS) Lock(name string) (LockHandle, error) {
	return nopLock{}, nil
}

type nopLock struct{}

func (nopLock) Unlock() error { return nil }

// OpenHandles returns the number of currently open file handles, for leak checks.
func (fs *MemFS) OpenHandles() int {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return fs.handles
}

// CrashClone returns the filesystem as it would look after a process crash
// right now. rng is used only for TornTail mode and may be nil for DropUnsynced.
func (fs *MemFS) CrashClone(mode CrashMode, rng *rand.Rand) *MemFS {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	nfs := NewMemFS()
	clone := func(st *memState) {
		if st.crashName == "" {
			return // creation/rename never made durable: file is absent
		}
		if !fs.durableDirs[parentOf(st.crashName)] {
			return // the directory entry itself is not durable
		}
		n := st.syncedLen
		if mode == TornTail && len(st.data) > st.syncedLen {
			n += rng.Intn(len(st.data) - st.syncedLen + 1)
		}
		data := make([]byte, n)
		copy(data, st.data[:n])
		nfs.files[st.crashName] = &memState{name: st.crashName, data: data, syncedLen: n, crashName: st.crashName}
	}
	for _, st := range fs.files {
		clone(st)
	}
	// Removals and displaced entries not yet durable: they survive with their
	// synced content, each at its own durable location.
	for _, st := range fs.graveyard {
		clone(st)
	}
	for dir := range fs.durableDirs {
		nfs.durableDirs[dir] = true
	}
	return nfs
}

// --- memFile handle methods ---

type memFile struct {
	fs       *MemFS
	st       *memState
	writable bool
	off      int64
	closed   bool
}

func (m *memFile) Read(p []byte) (int, error) {
	m.fs.mu.Lock()
	defer m.fs.mu.Unlock()
	if m.closed {
		return 0, errClosed("read")
	}
	if m.off >= int64(len(m.st.data)) {
		return 0, io.EOF
	}
	n := copy(p, m.st.data[m.off:])
	m.off += int64(n)
	return n, nil
}

func (m *memFile) ReadAt(p []byte, off int64) (int, error) {
	m.fs.mu.Lock()
	defer m.fs.mu.Unlock()
	if m.closed {
		return 0, errClosed("readat")
	}
	if off < 0 || off > int64(len(m.st.data)) {
		if off == int64(len(m.st.data)) && len(p) == 0 {
			return 0, nil
		}
		return 0, io.EOF
	}
	n := copy(p, m.st.data[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func (m *memFile) Write(p []byte) (int, error) {
	m.fs.mu.Lock()
	defer m.fs.mu.Unlock()
	if m.closed {
		return 0, errClosed("write")
	}
	if !m.writable {
		return 0, fmt.Errorf("vfs(memfs): write: not opened for writing: %w", os.ErrInvalid)
	}
	// Honor the handle offset: overwrite in place, then extend. A seek past
	// EOF pads with zeros like os.File.
	end := m.off + int64(len(p))
	if m.off > int64(len(m.st.data)) {
		m.st.data = append(m.st.data, make([]byte, m.off-int64(len(m.st.data)))...)
	}
	if end > int64(len(m.st.data)) {
		m.st.data = append(m.st.data, make([]byte, end-int64(len(m.st.data)))...)
	}
	copy(m.st.data[m.off:], p)
	m.off = end
	return len(p), nil
}

func (m *memFile) Sync() error {
	m.fs.mu.Lock()
	defer m.fs.mu.Unlock()
	if m.closed {
		return errClosed("sync")
	}
	m.st.syncedLen = len(m.st.data)
	return nil
}

func (m *memFile) Truncate(size int64) error {
	m.fs.mu.Lock()
	defer m.fs.mu.Unlock()
	if m.closed {
		return errClosed("truncate")
	}
	if size < int64(len(m.st.data)) {
		m.st.data = m.st.data[:size]
	}
	if size > int64(len(m.st.data)) {
		m.st.data = append(m.st.data, make([]byte, size-int64(len(m.st.data)))...)
	}
	if m.st.syncedLen > len(m.st.data) {
		m.st.syncedLen = len(m.st.data)
	}
	return nil
}

func (m *memFile) Stat() (FileInfo, error) {
	m.fs.mu.Lock()
	defer m.fs.mu.Unlock()
	if m.closed {
		return FileInfo{}, errClosed("stat")
	}
	return FileInfo{Name: baseOf(m.st.name), Size: int64(len(m.st.data))}, nil
}

func (m *memFile) Close() error {
	m.fs.mu.Lock()
	defer m.fs.mu.Unlock()
	if m.closed {
		return errClosed("close")
	}
	m.closed = true
	m.fs.handles--
	return nil
}
