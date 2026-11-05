//go:build !windows

package vfs

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

// unixLock is an exclusive lock on Unix: flock with LOCK_EX|LOCK_NB.
type unixLock struct {
	f *os.File
}

// Lock implements FS for Unix.
func (fs OSFS) Lock(name string) (LockHandle, error) {
	if err := fs.MkdirAll(filepath.Dir(name)); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(name, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, fmt.Errorf("vfs: lock %s: %w", name, err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		return nil, fmt.Errorf("vfs: lock %s: held elsewhere: %w", name, err)
	}
	return &unixLock{f: f}, nil
}

// Unlock releases the lock.
func (l *unixLock) Unlock() error {
	if err := syscall.Flock(int(l.f.Fd()), syscall.LOCK_UN); err != nil {
		l.f.Close()
		return fmt.Errorf("vfs: unlock: %w", err)
	}
	name := l.f.Name()
	if err := l.f.Close(); err != nil {
		return fmt.Errorf("vfs: unlock: %w", err)
	}
	_ = os.Remove(name)
	return nil
}
