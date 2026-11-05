//go:build windows

package vfs

import (
	"fmt"
	"os"
	"syscall"
)

// winLock is an exclusive lock on Windows: CreateFile with share mode 0.
// The handle is kept open for the lifetime of the lock; the OS releases it
// on process death.
type winLock struct {
	h    syscall.Handle
	path string
}

// Lock implements FS for Windows.
func (fs OSFS) Lock(name string) (LockHandle, error) {
	if err := fs.MkdirAll(dirOf(name)); err != nil {
		return nil, err
	}
	pathPtr, err := syscall.UTF16PtrFromString(name)
	if err != nil {
		return nil, fmt.Errorf("vfs: lock %s: %w", name, err)
	}
	h, err := syscall.CreateFile(pathPtr,
		syscall.GENERIC_WRITE,
		0, // share mode 0: no other handle may open this file
		nil,
		syscall.OPEN_ALWAYS,
		syscall.FILE_ATTRIBUTE_NORMAL,
		0)
	if err != nil {
		return nil, fmt.Errorf("vfs: lock %s: %w", name, err)
	}
	return &winLock{h: h, path: name}, nil
}

// Unlock releases the lock.
func (l *winLock) Unlock() error {
	if err := syscall.CloseHandle(l.h); err != nil {
		return fmt.Errorf("vfs: unlock %s: %w", l.path, err)
	}
	// Best-effort removal; leftover LOCK files are harmless.
	_ = os.Remove(l.path)
	return nil
}

func dirOf(path string) string {
	for i := len(path) - 1; i >= 0; i-- {
		if path[i] == '\\' || path[i] == '/' {
			return path[:i]
		}
	}
	return "."
}
