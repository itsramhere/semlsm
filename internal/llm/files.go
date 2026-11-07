package llm

import (
	"fmt"
	"io"
	"path/filepath"

	"github.com/itsramhere/semlsm/internal/vfs"
)

// writeFileAtomic writes data to name through a temp file in the same
// directory: the file is synced, then renamed over the target, then the
// directory entry is synced. A crash therefore leaves either the old or the
// new content, never a partial file.
func writeFileAtomic(fs vfs.FS, name string, data []byte) error {
	tmp := name + ".tmp"
	f, err := fs.Create(tmp)
	if err != nil {
		return fmt.Errorf("llm: create %s: %w", tmp, err)
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		fs.Remove(tmp)
		return fmt.Errorf("llm: write %s: %w", tmp, err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		fs.Remove(tmp)
		return fmt.Errorf("llm: sync %s: %w", tmp, err)
	}
	if err := f.Close(); err != nil {
		fs.Remove(tmp)
		return fmt.Errorf("llm: close %s: %w", tmp, err)
	}
	if err := fs.Rename(tmp, name); err != nil {
		fs.Remove(tmp)
		return fmt.Errorf("llm: rename %s: %w", name, err)
	}
	if err := fs.SyncDir(filepath.Dir(name)); err != nil {
		return fmt.Errorf("llm: sync dir of %s: %w", name, err)
	}
	return nil
}

// readFileAll reads a whole file through the FS interface.
func readFileAll(fs vfs.FS, name string) ([]byte, error) {
	f, err := fs.Open(name)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return io.ReadAll(f)
}
