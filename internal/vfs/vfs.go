// Package vfs abstracts the filesystem so tests can swap the OS filesystem
// for an in-memory crash-simulating one. All DB file access goes through it.
package vfs

import (
	"os"
	"time"
)

// FileInfo describes a file or directory entry.
type FileInfo struct {
	Name    string // base name
	Size    int64
	IsDir   bool
	ModTime time.Time
}

// File is a regular file handle. Read returns io.EOF at end of file,
// matching os.File semantics.
type File interface {
	Read(p []byte) (int, error)
	ReadAt(p []byte, off int64) (int, error)
	Write(p []byte) (int, error)
	Sync() error
	Close() error
	Truncate(size int64) error
	Stat() (FileInfo, error)
}

// LockHandle is an exclusive lock on a path. The OS releases it on process
// death; Release is for clean shutdown.
type LockHandle interface {
	Unlock() error
}

// FS is the filesystem interface used by the engine.
// Paths are absolute or relative strings built by the caller with filepath.Join.
type FS interface {
	// Create creates or truncates a file for writing.
	Create(name string) (File, error)
	// Open opens an existing file for reading.
	Open(name string) (File, error)
	// OpenReadWrite opens an existing file for reading and writing,
	// creating it if missing.
	OpenReadWrite(name string) (File, error)
	Rename(oldname, newname string) error
	Remove(name string) error
	MkdirAll(path string) error
	// List returns the entries of a directory (base names, no order guarantee).
	List(dir string) ([]FileInfo, error)
	Stat(name string) (FileInfo, error)
	// SyncDir flushes directory metadata (dentry) changes. A no-op is
	// acceptable on platforms without fsync-on-directories (Windows).
	SyncDir(dir string) error
	// Lock acquires an exclusive lock on name, failing immediately if held.
	Lock(name string) (LockHandle, error)
}

// ErrNotExist converts a wrapped not-found error; helper for MemFS and callers.
var ErrNotExist = os.ErrNotExist
