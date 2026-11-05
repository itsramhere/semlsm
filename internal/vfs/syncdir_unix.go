//go:build !windows

package vfs

import (
	"fmt"
	"os"
)

// SyncDir for OSFS on Unix: open the directory and fsync it so the dentry
// changes (creates, renames, removals) are durable.
func (OSFS) SyncDir(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("vfs: syncdir %s: %w", dir, err)
	}
	defer f.Close()
	if err := f.Sync(); err != nil {
		return fmt.Errorf("vfs: syncdir %s: %w", dir, err)
	}
	return nil
}
