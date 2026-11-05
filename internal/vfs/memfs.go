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

// memState is the simulated on-disk state of one file.
type memState struct {
	data      []byte
	syncedLen int    // bytes durable after a crash (Sync updates this)
	crashName string // name the file has after a crash; "" = entry not durable
}

// MemFS is an in-memory FS for crash simulation. It models:
//   - Sync()ed bytes survive a crash; unsynced bytes are lost (or torn).
//   - Directory entries (creates, renames, removals) become durable on SyncDir.
//     A rename without SyncDir reverts on crash; a removal without SyncDir
//     leaves the file present with its synced content.
type MemFS struct {
	mu          sync.Mutex
	files       map[string]*memState
	graveyard   map[string]*memState // removed files whose deletion is not durable yet
	durableDirs map[string]bool
}

// NewMemFS returns an empty MemFS.
func NewMemFS() *MemFS {
	return &MemFS{
		files:       map[string]*memState{},
		graveyard:   map[string]*memState{},
		durableDirs: map[string]bool{},
	}
}

// memFile is a handle into a MemFS file.
type memFile struct {
	fs       *MemFS
	st       *memState
	writable bool
	off      int64
}

func parentOf(name string) string { return filepath.Dir(name) }

func baseOf(name string) string { return filepath.Base(name) }

// Create implements FS.
func (fs *MemFS) Create(name string) (File, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	st := &memState{}
	fs.files[name] = st
	return &memFile{fs: fs, st: st, writable: true}, nil
}

// Open implements FS.
func (fs *MemFS) Open(name string) (File, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	st, ok := fs.files[name]
	if !ok {
		if st, ok = fs.graveyard[name]; ok {
			// Openable until the deletion is durable, like a real FS.
			return &memFile{fs: fs, st: st, writable: false}, nil
		}
		return nil, fmt.Errorf("vfs(memfs): open %s: %w", name, os.ErrNotExist)
	}
	return &memFile{fs: fs, st: st, writable: false}, nil
}

// OpenReadWrite implements FS.
func (fs *MemFS) OpenReadWrite(name string) (File, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if st, ok := fs.files[name]; ok {
		return &memFile{fs: fs, st: st, writable: true}, nil
	}
	st := &memState{}
	fs.files[name] = st
	return &memFile{fs: fs, st: st, writable: true}, nil
}

// Rename implements FS.
func (fs *MemFS) Rename(oldname, newname string) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	st, ok := fs.files[oldname]
	if !ok {
		return fmt.Errorf("vfs(memfs): rename %s: %w", oldname, os.ErrNotExist)
	}
	delete(fs.files, oldname)
	if target, exists := fs.files[newname]; exists {
		// POSIX rename replaces the target; model the replaced file as a
		// pending deletion so a crash before SyncDir can keep the old file.
		fs.graveyard[newname] = target
	}
	fs.files[newname] = st
	return nil
}

// Remove implements FS.
func (fs *MemFS) Remove(name string) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	st, ok := fs.files[name]
	if !ok {
		return fmt.Errorf("vfs(memfs): remove %s: %w", name, os.ErrNotExist)
	}
	delete(fs.files, name)
	fs.graveyard[name] = st
	return nil
}

// MkdirAll implements FS.
func (fs *MemFS) MkdirAll(path string) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	// Register the directory so List and Stat see it. In the current-view
	// model a directory exists immediately once created.
	fs.durableDirs[path] = true
	return nil
}

// List implements FS.
func (fs *MemFS) List(dir string) ([]FileInfo, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	seen := map[string]bool{}
	var out []FileInfo
	for name := range fs.files {
		if parentOf(name) == dir {
			seen[baseOf(name)] = true
			out = append(out, FileInfo{Name: baseOf(name), Size: int64(len(fs.files[name].data))})
		}
	}
	for name := range fs.graveyard {
		if parentOf(name) == dir {
			seen[baseOf(name)] = true
		}
	}
	for d := range fs.durableDirs {
		if parentOf(d) == dir {
			seen[baseOf(d)] = true
		}
	}
	for name := range seen {
		found := false
		for i := range out {
			if out[i].Name == name {
				found = true
				break
			}
		}
		if !found {
			out = append(out, FileInfo{Name: name, IsDir: true})
		}
	}
	return out, nil
}

// Stat implements FS.
func (fs *MemFS) Stat(name string) (FileInfo, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if st, ok := fs.files[name]; ok {
		return FileInfo{Name: baseOf(name), Size: int64(len(st.data))}, nil
	}
	if st, ok := fs.graveyard[name]; ok {
		return FileInfo{Name: baseOf(name), Size: int64(len(st.data))}, nil
	}
	for d := range fs.durableDirs {
		if d == name {
			return FileInfo{Name: baseOf(name), IsDir: true}, nil
		}
	}
	return FileInfo{}, fmt.Errorf("vfs(memfs): stat %s: %w", name, os.ErrNotExist)
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
	for name := range fs.graveyard {
		if parentOf(name) == dir {
			delete(fs.graveyard, name)
		}
	}
	return nil
}

// Lock implements FS (a no-op in-memory lock).
func (fs *MemFS) Lock(name string) (LockHandle, error) {
	return nopLock{}, nil
}

type nopLock struct{}

func (nopLock) Unlock() error { return nil }

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
		nfs.files[st.crashName] = &memState{data: data, syncedLen: n, crashName: st.crashName}
	}
	for _, st := range fs.files {
		clone(st)
	}
	// Removals not yet durable: the file survives with its synced content.
	for _, st := range fs.graveyard {
		clone(st)
	}
	for dir := range fs.durableDirs {
		nfs.durableDirs[dir] = true
	}
	return nfs
}

// --- memFile handle methods ---

func (m *memFile) Read(p []byte) (int, error) {
	m.fs.mu.Lock()
	defer m.fs.mu.Unlock()
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
	if !m.writable {
		return 0, fmt.Errorf("vfs(memfs): write: not writable")
	}
	m.st.data = append(m.st.data, p...)
	return len(p), nil
}

func (m *memFile) Sync() error {
	m.fs.mu.Lock()
	defer m.fs.mu.Unlock()
	m.st.syncedLen = len(m.st.data)
	return nil
}

func (m *memFile) Close() error { return nil }

func (m *memFile) Truncate(size int64) error {
	m.fs.mu.Lock()
	defer m.fs.mu.Unlock()
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
	return FileInfo{Name: baseOf(m.st.crashName), Size: int64(len(m.st.data))}, nil
}
