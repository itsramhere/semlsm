package vfs

import (
	"fmt"
	"os"
)

// OSFS is the real operating-system filesystem.
type OSFS struct{}

// osFile wraps *os.File to implement File.
type osFile struct {
	f *os.File
}

func (o *osFile) Read(p []byte) (int, error)              { return o.f.Read(p) }
func (o *osFile) ReadAt(p []byte, off int64) (int, error) { return o.f.ReadAt(p, off) }
func (o *osFile) Write(p []byte) (int, error)             { return o.f.Write(p) }
func (o *osFile) Sync() error                             { return o.f.Sync() }
func (o *osFile) Close() error                            { return o.f.Close() }
func (o *osFile) Truncate(size int64) error               { return o.f.Truncate(size) }

func (o *osFile) Stat() (FileInfo, error) {
	st, err := o.f.Stat()
	if err != nil {
		return FileInfo{}, err
	}
	return fromOSInfo(st), nil
}

func fromOSInfo(st os.FileInfo) FileInfo {
	return FileInfo{
		Name:    st.Name(),
		Size:    st.Size(),
		IsDir:   st.IsDir(),
		ModTime: st.ModTime(),
	}
}

// Create implements FS.
func (OSFS) Create(name string) (File, error) {
	f, err := os.Create(name)
	if err != nil {
		return nil, fmt.Errorf("vfs: create %s: %w", name, err)
	}
	return &osFile{f: f}, nil
}

// Open implements FS.
func (OSFS) Open(name string) (File, error) {
	f, err := os.Open(name)
	if err != nil {
		return nil, fmt.Errorf("vfs: open %s: %w", name, err)
	}
	return &osFile{f: f}, nil
}

// OpenReadWrite implements FS.
func (OSFS) OpenReadWrite(name string) (File, error) {
	f, err := os.OpenFile(name, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, fmt.Errorf("vfs: openrw %s: %w", name, err)
	}
	return &osFile{f: f}, nil
}

// Rename implements FS.
func (OSFS) Rename(oldname, newname string) error {
	return os.Rename(oldname, newname)
}

// Remove implements FS.
func (OSFS) Remove(name string) error {
	return os.Remove(name)
}

// MkdirAll implements FS.
func (OSFS) MkdirAll(path string) error {
	return os.MkdirAll(path, 0o755)
}

// List implements FS.
func (OSFS) List(dir string) ([]FileInfo, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("vfs: list %s: %w", dir, err)
	}
	out := make([]FileInfo, 0, len(entries))
	for _, e := range entries {
		info := FileInfo{Name: e.Name(), IsDir: e.IsDir()}
		if st, err := e.Info(); err == nil {
			info.Size = st.Size()
			info.ModTime = st.ModTime()
		}
		out = append(out, info)
	}
	return out, nil
}

// Stat implements FS.
func (OSFS) Stat(name string) (FileInfo, error) {
	st, err := os.Stat(name)
	if err != nil {
		return FileInfo{}, fmt.Errorf("vfs: stat %s: %w", name, err)
	}
	return fromOSInfo(st), nil
}
