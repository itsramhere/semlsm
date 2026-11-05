// Package manifest implements the version-edit log (MANIFEST-N) and CURRENT
// pointer. Frames follow the WAL format; payloads are JSON edits per B8.
package manifest

import (
	"encoding/json"
	"fmt"
	"io"
	"path/filepath"
	"strings"

	"semlsm/internal/base"
	"semlsm/internal/vfs"
	"semlsm/internal/wal"
)

// RollThreshold is the manifest size at which the DB rolls to a new manifest.
const RollThreshold = 4 << 20

// FileMeta describes one SST file in the tree.
type FileMeta struct {
	Level    int    `json:"level"`
	Num      uint64 `json:"num"`
	Size     int64  `json:"size"`
	Smallest []byte `json:"smallest"`
	Largest  []byte `json:"largest"`
	MinTs    uint64 `json:"min_ts"`
	MaxTs    uint64 `json:"max_ts"`
	Count    uint64 `json:"count"`
}

// DelFile identifies a removed file.
type DelFile struct {
	Level int    `json:"level"`
	Num   uint64 `json:"num"`
}

// TupleRef is one tuple's location in a .ckpt file (opaque until P7).
type TupleRef struct {
	ID     uint64 `json:"id"`
	Group  string `json:"group"`
	TsFrom uint64 `json:"ts_from"`
	TsTo   uint64 `json:"ts_to"`
	Offset uint64 `json:"offset"`
	Length uint64 `json:"length"`
}

// CkptFile describes one .ckpt file.
type CkptFile struct {
	FileNum uint64     `json:"file_num"`
	Size    int64      `json:"size"`
	Tuples  []TupleRef `json:"tuples"`
}

// Edit is one manifest record. Snapshot edits carry the full state.
type Edit struct {
	NextFileNum uint64     `json:"next_file_num"`
	LastTs      uint64     `json:"last_ts"`
	LogNum      uint64     `json:"log_num"`
	AddFiles    []FileMeta `json:"add_files,omitempty"`
	DelFiles    []DelFile  `json:"del_files,omitempty"`
	AddCkpts    []CkptFile `json:"add_ckpts,omitempty"`
	DelCkpts    []uint64   `json:"del_ckpts,omitempty"`
}

// Writer appends framed edits to one manifest file.
type Writer struct {
	f    vfs.File
	path string
	size int64
}

// NewWriter creates (or truncates) the manifest at path.
func NewWriter(fs vfs.FS, path string) (*Writer, error) {
	f, err := fs.Create(path)
	if err != nil {
		return nil, fmt.Errorf("manifest: create %s: %w", path, err)
	}
	return &Writer{f: f, path: path}, nil
}

// Append frames one edit as JSON. Empty edits are rejected by the shared
// frame builder (a zero-length frame looks like a zero-filled crash tail).
func (w *Writer) Append(e *Edit) error {
	payload, err := json.Marshal(e)
	if err != nil {
		return fmt.Errorf("manifest: encode %s: %w", w.path, err)
	}
	frame, err := wal.Frame(payload)
	if err != nil {
		return fmt.Errorf("manifest: append %s: %w", w.path, err)
	}
	if _, err := w.f.Write(frame); err != nil {
		return fmt.Errorf("manifest: append %s: %w", w.path, err)
	}
	w.size += int64(len(frame))
	return nil
}

// Sync flushes the manifest.
func (w *Writer) Sync() error {
	if err := w.f.Sync(); err != nil {
		return fmt.Errorf("manifest: sync %s: %w", w.path, err)
	}
	return nil
}

// Close closes the file.
func (w *Writer) Close() error {
	if err := w.f.Close(); err != nil {
		return fmt.Errorf("manifest: close %s: %w", w.path, err)
	}
	return nil
}

// Size returns bytes written so far.
func (w *Writer) Size() int64 { return w.size }

// ReadAll replays all edits from a manifest. A torn tail (the final frame) is
// ignored, like in the WAL. JSON corruption of a complete frame is an error.
func ReadAll(fs vfs.FS, path string) ([]Edit, error) {
	var edits []Edit
	_, err := wal.Replay(fs, path, true, func(payload []byte) error {
		var e Edit
		if err := json.Unmarshal(payload, &e); err != nil {
			return fmt.Errorf("manifest: decode %s: %w", path, base.ErrCorruption)
		}
		edits = append(edits, e)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return edits, nil
}

// manifestName builds MANIFEST-%06d.
func ManifestName(num uint64) string {
	return fmt.Sprintf("MANIFEST-%06d", num)
}

// ParseManifestNum extracts the number from a MANIFEST-%06d name.
func ParseManifestNum(name string) (uint64, bool) {
	if !strings.HasPrefix(name, "MANIFEST-") {
		return 0, false
	}
	var n uint64
	if _, err := fmt.Sscanf(name[len("MANIFEST-"):], "%d", &n); err != nil {
		return 0, false
	}
	return n, true
}

// SetCurrent atomically points CURRENT at manifestName:
// write CURRENT.tmp, sync, close, rename, sync dir (B8).
func SetCurrent(fs vfs.FS, dir, manifestName string) error {
	tmp := filepath.Join(dir, "CURRENT.tmp")
	f, err := fs.Create(tmp)
	if err != nil {
		return fmt.Errorf("manifest: set current: %w", err)
	}
	if _, err := f.Write([]byte(manifestName)); err != nil {
		f.Close()
		return fmt.Errorf("manifest: set current: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("manifest: set current: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("manifest: set current: %w", err)
	}
	if err := fs.Rename(tmp, filepath.Join(dir, "CURRENT")); err != nil {
		return fmt.Errorf("manifest: set current: %w", err)
	}
	if err := fs.SyncDir(dir); err != nil {
		return fmt.Errorf("manifest: set current: %w", err)
	}
	return nil
}

// ReadCurrent returns the manifest filename recorded in CURRENT.
func ReadCurrent(fs vfs.FS, dir string) (string, error) {
	f, err := fs.Open(filepath.Join(dir, "CURRENT"))
	if err != nil {
		return "", fmt.Errorf("manifest: read current: %w", err)
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return "", fmt.Errorf("manifest: read current: %w", err)
	}
	buf := make([]byte, st.Size)
	if _, err := f.Read(buf); err != nil && err != io.EOF {
		return "", fmt.Errorf("manifest: read current: %w", err)
	}
	name := strings.TrimSpace(string(buf))
	if name == "" {
		return "", fmt.Errorf("manifest: current empty: %w", base.ErrCorruption)
	}
	if _, ok := ParseManifestNum(name); !ok {
		return "", fmt.Errorf("manifest: current %q invalid: %w", name, base.ErrCorruption)
	}
	return name, nil
}
