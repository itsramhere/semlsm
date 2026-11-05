package vfs

import (
	"sync"
)

// FaultFS wraps an FS and injects an error on the Nth occurrence of a named
// operation. Op names are: Create, Open, OpenReadWrite, Rename, Remove,
// MkdirAll, List, Stat, SyncDir, Lock, and on files: Write, Sync, Truncate.
type FaultFS struct {
	FS
	mu     sync.Mutex
	counts map[string]int
	faults map[string][]faultSpec
}

type faultSpec struct {
	from, to int // occurrence range, inclusive
	err      error
}

// NewFaultFS wraps fs.
func NewFaultFS(fs FS) *FaultFS {
	return &FaultFS{FS: fs, counts: map[string]int{}, faults: map[string][]faultSpec{}}
}

// Inject causes the nth occurrence of op (1-based) to return err.
// Replaces any prior faults for the same op.
func (f *FaultFS) Inject(op string, nth int, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.faults[op] = []faultSpec{{from: nth, to: nth, err: err}}
}

// InjectRange adds faults for every occurrence of op in [from, to] (1-based),
// keeping existing specs for the op. Used for retry tests.
func (f *FaultFS) InjectRange(op string, from, to int, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.faults[op] = append(f.faults[op], faultSpec{from: from, to: to, err: err})
}

// Clear removes the faults for op (all ops if op is empty).
func (f *FaultFS) Clear(op string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if op == "" {
		f.faults = map[string][]faultSpec{}
		return
	}
	delete(f.faults, op)
}

// Count returns how many times op has been invoked through this wrapper.
func (f *FaultFS) Count(op string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.counts[op]
}

func (f *FaultFS) hit(op string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.counts[op]++
	for _, spec := range f.faults[op] {
		if f.counts[op] >= spec.from && f.counts[op] <= spec.to {
			return spec.err
		}
	}
	return nil
}

// Create implements FS.
func (f *FaultFS) Create(name string) (File, error) {
	if err := f.hit("Create"); err != nil {
		return nil, err
	}
	file, err := f.FS.Create(name)
	if err != nil {
		return nil, err
	}
	return &faultFile{File: file, fs: f}, nil
}

// Open implements FS.
func (f *FaultFS) Open(name string) (File, error) {
	if err := f.hit("Open"); err != nil {
		return nil, err
	}
	file, err := f.FS.Open(name)
	if err != nil {
		return nil, err
	}
	return &faultFile{File: file, fs: f}, nil
}

// OpenReadWrite implements FS.
func (f *FaultFS) OpenReadWrite(name string) (File, error) {
	if err := f.hit("OpenReadWrite"); err != nil {
		return nil, err
	}
	file, err := f.FS.OpenReadWrite(name)
	if err != nil {
		return nil, err
	}
	return &faultFile{File: file, fs: f}, nil
}

// Rename implements FS.
func (f *FaultFS) Rename(oldname, newname string) error {
	if err := f.hit("Rename"); err != nil {
		return err
	}
	return f.FS.Rename(oldname, newname)
}

// Remove implements FS.
func (f *FaultFS) Remove(name string) error {
	if err := f.hit("Remove"); err != nil {
		return err
	}
	return f.FS.Remove(name)
}

// MkdirAll implements FS.
func (f *FaultFS) MkdirAll(path string) error {
	if err := f.hit("MkdirAll"); err != nil {
		return err
	}
	return f.FS.MkdirAll(path)
}

// List implements FS.
func (f *FaultFS) List(dir string) ([]FileInfo, error) {
	if err := f.hit("List"); err != nil {
		return nil, err
	}
	return f.FS.List(dir)
}

// Stat implements FS.
func (f *FaultFS) Stat(name string) (FileInfo, error) {
	if err := f.hit("Stat"); err != nil {
		return FileInfo{}, err
	}
	return f.FS.Stat(name)
}

// SyncDir implements FS.
func (f *FaultFS) SyncDir(dir string) error {
	if err := f.hit("SyncDir"); err != nil {
		return err
	}
	return f.FS.SyncDir(dir)
}

// Lock implements FS.
func (f *FaultFS) Lock(name string) (LockHandle, error) {
	if err := f.hit("Lock"); err != nil {
		return nil, err
	}
	return f.FS.Lock(name)
}

// faultFile injects faults into per-file operations.
type faultFile struct {
	File
	fs *FaultFS
}

func (ff *faultFile) Write(p []byte) (int, error) {
	if err := ff.fs.hit("Write"); err != nil {
		return 0, err
	}
	return ff.File.Write(p)
}

func (ff *faultFile) Sync() error {
	if err := ff.fs.hit("Sync"); err != nil {
		return err
	}
	return ff.File.Sync()
}

func (ff *faultFile) Truncate(size int64) error {
	if err := ff.fs.hit("Truncate"); err != nil {
		return err
	}
	return ff.File.Truncate(size)
}

var _ FS = (*FaultFS)(nil)
