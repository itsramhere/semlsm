// Package testutil contains shared test fixtures.
package testutil

import (
	"testing"

	"github.com/itsramhere/semlsm/internal/vfs"
)

// TrackMemFS registers a check after the caller's later cleanup callbacks.
// Close DBs and stacks before this callback runs; it never hides a leak.
func TrackMemFS(t testing.TB, fs *vfs.MemFS) *vfs.MemFS {
	t.Helper()
	t.Cleanup(func() {
		if n := fs.OpenFiles(); n != 0 {
			t.Errorf("leaked %d file handles: %v", n, fs.OpenFileNames())
		}
	})
	return fs
}

func NewMemFS(t testing.TB) *vfs.MemFS {
	t.Helper()
	return TrackMemFS(t, vfs.NewMemFS())
}
