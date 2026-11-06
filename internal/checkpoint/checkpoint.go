// Package checkpoint implements the .ckpt file format (B8): framed tuples
// with CRC-protected metadata, summary and key-index sections, plus a tuple
// directory and footer.
package checkpoint

import (
	"bytes"
	"compress/flate"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"

	"semlsm/internal/base"
	"semlsm/internal/bloom"
	"semlsm/internal/vfs"
)

const (
	magicLen     = 4
	footerLen    = 24 // u64 dirOffset | u32 tupleCount | u32 crc | "SLCKEND\0"
	restartEvery = 32
)

var (
	magic     = []byte("SLCK")
	magicEnd  = []byte("SLCKEND\x00")
	formatVer = uint32(1)
)

// TupleMeta is the per-tuple metadata (B8).
type TupleMeta struct {
	ID              uint64 `json:"id"`
	Group           string `json:"group"`
	TsFrom          uint64 `json:"ts_from"`
	TsTo            uint64 `json:"ts_to"`
	SourceCount     uint64 `json:"source_count"`
	SourceBytes     uint64 `json:"source_bytes"`
	SourceCRC32     uint32 `json:"source_crc32"`
	PutCount        uint64 `json:"put_count"`
	DeleteCount     uint64 `json:"delete_count"`
	KeyCount        uint64 `json:"key_count"`
	FromLevel       int    `json:"from_level"`
	ToLevel         int    `json:"to_level"`
	Model           string `json:"model"`
	PromptVersion   int    `json:"prompt_version"`
	CreatedUnixNano int64  `json:"created_unix_nano"`
	PrevTupleID     uint64 `json:"prev_tuple_id"`
	ArchiveFileNum  uint64 `json:"archive_file_num"`
	InputTokens     uint64 `json:"input_tokens"`
	OutputTokens    uint64 `json:"output_tokens"`
	Degraded        bool   `json:"degraded"`
}

// KeyEntry is one key's run within a tuple's key index.
type KeyEntry struct {
	Key      []byte
	MinTs    uint64
	MaxTs    uint64
	Count    uint64
	LastKind base.Kind
}

// Writer builds one .ckpt file.
type Writer struct {
	f        vfs.File
	path     string
	fs       vfs.FS
	offset   int64
	dir      []DirEntry
	tuples   int
	finished bool
}

// NewWriter creates the .ckpt at path.
func NewWriter(fs vfs.FS, path string) (*Writer, error) {
	f, err := fs.Create(path)
	if err != nil {
		return nil, fmt.Errorf("ckpt: create %s: %w", path, err)
	}
	if _, err := f.Write(magic); err != nil {
		f.Close()
		return nil, fmt.Errorf("ckpt: header %s: %w", path, err)
	}
	var v [4]byte
	binary.LittleEndian.PutUint32(v[:], formatVer)
	if _, err := f.Write(v[:]); err != nil {
		f.Close()
		return nil, fmt.Errorf("ckpt: header %s: %w", path, err)
	}
	return &Writer{f: f, path: path, fs: fs, offset: int64(magicLen + 4)}, nil
}

// AddTuple appends one tuple: meta JSON, summary JSON, and its key index.
func (w *Writer) AddTuple(meta TupleMeta, summaryJSON []byte, entries []KeyEntry) error {
	if w.finished {
		return fmt.Errorf("ckpt: add after finish %s", w.path)
	}
	start := w.offset

	metaJSON, err := json.Marshal(meta)
	if err != nil {
		return fmt.Errorf("ckpt: meta %d: %w", meta.ID, err)
	}

	// key index payload
	idxPayload, err := buildIndexPayload(entries)
	if err != nil {
		return fmt.Errorf("ckpt: index %d: %w", meta.ID, err)
	}
	idxSection, err := encodeSection(idxPayload)
	if err != nil {
		return fmt.Errorf("ckpt: index %d: %w", meta.ID, err)
	}
	sumSection, err := encodeSection(summaryJSON)
	if err != nil {
		return fmt.Errorf("ckpt: summary %d: %w", meta.ID, err)
	}

	var buf []byte
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(metaJSON)))
	buf = append(buf, metaJSON...)
	buf = binary.LittleEndian.AppendUint32(buf, base.CRC(metaJSON))
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(sumSection)))
	buf = append(buf, sumSection...)
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(idxSection)))
	buf = append(buf, idxSection...)

	if _, err := w.f.Write(buf); err != nil {
		return fmt.Errorf("ckpt: write tuple %d: %w", meta.ID, err)
	}
	w.offset += int64(len(buf))
	w.dir = append(w.dir, DirEntry{ID: meta.ID, Offset: uint64(start), Length: uint64(w.offset - start)})
	w.tuples++
	return nil
}

// Finish writes the directory and footer, syncs, and closes.
func (w *Writer) Finish() error {
	if w.finished {
		return fmt.Errorf("ckpt: finish twice %s", w.path)
	}
	w.finished = true
	dirJSON, err := json.Marshal(w.dir)
	if err != nil {
		return fmt.Errorf("ckpt: dir %s: %w", w.path, err)
	}
	var framed []byte
	framed = binary.LittleEndian.AppendUint32(framed, uint32(len(dirJSON)))
	framed = binary.LittleEndian.AppendUint32(framed, base.CRC(dirJSON))
	framed = append(framed, dirJSON...)
	dirOffset := w.offset
	if _, err := w.f.Write(framed); err != nil {
		return fmt.Errorf("ckpt: write dir %s: %w", w.path, err)
	}
	w.offset += int64(len(framed))

	var footer [footerLen]byte
	binary.LittleEndian.PutUint64(footer[0:8], uint64(dirOffset))
	binary.LittleEndian.PutUint32(footer[8:12], uint32(w.tuples))
	binary.LittleEndian.PutUint32(footer[12:16], base.CRC(footer[:12]))
	copy(footer[16:], magicEnd)
	if _, err := w.f.Write(footer[:]); err != nil {
		return fmt.Errorf("ckpt: footer %s: %w", w.path, err)
	}
	w.offset += footerLen
	if err := w.f.Sync(); err != nil {
		return fmt.Errorf("ckpt: sync %s: %w", w.path, err)
	}
	return w.f.Close()
}

// Abort closes and removes the file.
func (w *Writer) Abort() {
	_ = w.f.Close()
	_ = w.fs.Remove(w.path)
}

// encodeSection wraps a payload with a codec byte (flate when smaller) and
// a CRC over codec||data (B8).
func encodeSection(payload []byte) ([]byte, error) {
	compressed := new(bytes.Buffer)
	cw, _ := flate.NewWriter(compressed, 1)
	if _, err := cw.Write(payload); err != nil {
		return nil, err
	}
	if err := cw.Close(); err != nil {
		return nil, err
	}
	body := payload
	codec := byte(0)
	if compressed.Len() < len(payload) {
		body = compressed.Bytes()
		codec = 1
	}
	out := make([]byte, 0, len(body)+5)
	out = append(out, codec)
	out = append(out, body...)
	out = binary.LittleEndian.AppendUint32(out, base.CRC(append([]byte{codec}, body...)))
	return out, nil
}

// decodeSection verifies the CRC and decompresses.
func decodeSection(sec []byte) ([]byte, error) {
	if len(sec) < 5 {
		return nil, fmt.Errorf("ckpt: section short: %w", base.ErrCorruption)
	}
	codec := sec[0]
	body := sec[1 : len(sec)-4]
	wantCRC := binary.LittleEndian.Uint32(sec[len(sec)-4:])
	if base.CRC(append([]byte{codec}, body...)) != wantCRC {
		return nil, fmt.Errorf("ckpt: section crc: %w", base.ErrCorruption)
	}
	switch codec {
	case 0:
		return body, nil
	case 1:
		r := flate.NewReader(bytes.NewReader(body))
		defer r.Close()
		out, err := io.ReadAll(r)
		if err != nil {
			return nil, fmt.Errorf("ckpt: decompress: %w", base.ErrCorruption)
		}
		return out, nil
	default:
		return nil, fmt.Errorf("ckpt: codec %d: %w", codec, base.ErrCorruption)
	}
}

// buildIndexPayload builds the key index payload per B8: prefix-compressed
// entries, restarts every 32, bloom filter, fixed trailer.
func buildIndexPayload(entries []KeyEntry) ([]byte, error) {
	var b bytes.Buffer
	var restarts []uint32
	var lastKey []byte
	for i, e := range entries {
		if i%restartEvery == 0 {
			restarts = append(restarts, uint32(b.Len()))
			lastKey = lastKey[:0]
		}
		shared := 0
		limit := len(lastKey)
		if len(e.Key) < limit {
			limit = len(e.Key)
		}
		for shared < limit && lastKey[shared] == e.Key[shared] {
			shared++
		}
		unshared := e.Key[shared:]
		var tmp [binary.MaxVarintLen64]byte
		put := func(v uint64) {
			n := binary.PutUvarint(tmp[:], v)
			b.Write(tmp[:n])
		}
		put(uint64(shared))
		put(uint64(len(unshared)))
		b.Write(unshared)
		put(e.MinTs)
		put(e.MaxTs - e.MinTs)
		put(e.Count)
		b.WriteByte(byte(e.LastKind))
		lastKey = append(lastKey[:0], e.Key...)
	}

	var out bytes.Buffer
	out.Write(b.Bytes())
	for _, r := range restarts {
		var t [4]byte
		binary.LittleEndian.PutUint32(t[:], r)
		out.Write(t[:])
	}
	var n [4]byte
	binary.LittleEndian.PutUint32(n[:], uint32(len(restarts)))
	out.Write(n[:])

	// bloom over distinct user keys
	bl := bloom.New(len(entries))
	for i := range entries {
		bl.Add(entries[i].Key)
	}
	bloomBytes := bl.Encode()
	out.Write(bloomBytes)

	// fixed trailer: bloom length
	var blen [4]byte
	binary.LittleEndian.PutUint32(blen[:], uint32(len(bloomBytes)))
	out.Write(blen[:])
	return out.Bytes(), nil
}

// DirEntry is one directory row: where a tuple lives in the file.
type DirEntry struct {
	ID     uint64
	Offset uint64
	Length uint64
}

// DirEntry is the public form of a tuple's location in the file.
func (w *Writer) DirEntries() []DirEntry {
	out := make([]DirEntry, len(w.dir))
	copy(out, w.dir)
	return out
}
