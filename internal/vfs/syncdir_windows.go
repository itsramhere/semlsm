//go:build windows

package vfs

// SyncDir for OSFS on Windows: Windows does not expose directory fsync,
// so this is a documented no-op (P1.2). Durability of renames on NTFS is
// metadata-ordered; crash tests therefore run on MemFS only.
func (OSFS) SyncDir(dir string) error { return nil }
