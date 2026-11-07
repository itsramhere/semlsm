package checkpoint

import (
	"encoding/binary"
	"encoding/json"
	"fmt"

	"semlsm/internal/base"
	"semlsm/internal/bloom"
	"semlsm/internal/vfs"
)

// Reader reads one open .ckpt file. Parsed tuples are cached lazily.
type Reader struct {
	f    vfs.File
	path string
	dir  []DirEntry
	tup  map[uint64]*parsedTuple
}

type parsedTuple struct {
	meta    TupleMeta
	summary []byte // raw JSON
	index   *parsedIndex
}

// parsedIndex is a decoded key index ready for lookups.
type parsedIndex struct {
	data     []byte // entries region (without restarts/bloom/trailer)
	restarts []uint32
	bloom    *bloom.Bloom
}

// OpenReader opens and validates a .ckpt file.
func OpenReader(fs vfs.FS, path string) (*Reader, error) {
	f, err := fs.Open(path)
	if err != nil {
		return nil, fmt.Errorf("ckpt: open %s: %w", path, err)
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("ckpt: stat %s: %w", path, err)
	}
	if st.Size < int64(magicLen+4+footerLen) {
		f.Close()
		return nil, fmt.Errorf("ckpt: %s too short: %w", path, base.ErrCorruption)
	}
	var header [magicLen + 4]byte
	if _, err := f.ReadAt(header[:], 0); err != nil {
		f.Close()
		return nil, fmt.Errorf("ckpt: header %s: %w", path, err)
	}
	if !bytesEqual(header[:magicLen], magic) {
		f.Close()
		return nil, fmt.Errorf("ckpt: bad magic %s: %w", path, base.ErrCorruption)
	}
	if binary.LittleEndian.Uint32(header[magicLen:magicLen+4]) != formatVer {
		f.Close()
		return nil, fmt.Errorf("ckpt: bad version %s: %w", path, base.ErrCorruption)
	}

	var footer [footerLen]byte
	if _, err := f.ReadAt(footer[:], st.Size-footerLen); err != nil {
		f.Close()
		return nil, fmt.Errorf("ckpt: footer %s: %w", path, err)
	}
	if !bytesEqual(footer[16:], magicEnd) {
		f.Close()
		return nil, fmt.Errorf("ckpt: bad end magic %s: %w", path, base.ErrCorruption)
	}
	if base.CRC(footer[:12]) != binary.LittleEndian.Uint32(footer[12:16]) {
		f.Close()
		return nil, fmt.Errorf("ckpt: footer crc %s: %w", path, base.ErrCorruption)
	}
	dirOffset := binary.LittleEndian.Uint64(footer[0:8])
	tupleCount := binary.LittleEndian.Uint32(footer[8:12])

	var hdr [8]byte
	if _, err := f.ReadAt(hdr[:], int64(dirOffset)); err != nil {
		f.Close()
		return nil, fmt.Errorf("ckpt: dir header %s: %w", path, err)
	}
	dirLen := binary.LittleEndian.Uint32(hdr[0:4])
	if false {
		// replaced below: the CRC covers dirJSON, not the length prefix
	}
	dirJSON := make([]byte, dirLen)
	if _, err := f.ReadAt(dirJSON, int64(dirOffset)+8); err != nil {
		f.Close()
		return nil, fmt.Errorf("ckpt: dir read %s: %w", path, err)
	}
	if base.CRC(dirJSON) != binary.LittleEndian.Uint32(hdr[4:8]) {
		f.Close()
		return nil, fmt.Errorf("ckpt: dir crc %s: %w", path, base.ErrCorruption)
	}
	var dir []DirEntry
	if err := json.Unmarshal(dirJSON, &dir); err != nil {
		f.Close()
		return nil, fmt.Errorf("ckpt: dir parse %s: %w", path, base.ErrCorruption)
	}
	if uint32(len(dir)) != tupleCount {
		f.Close()
		return nil, fmt.Errorf("ckpt: dir count %d != footer %d: %w", len(dir), tupleCount, base.ErrCorruption)
	}
	return &Reader{f: f, path: path, dir: dir, tup: map[uint64]*parsedTuple{}}, nil
}

func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// Close closes the file.
func (r *Reader) Close() error {
	if err := r.f.Close(); err != nil {
		return fmt.Errorf("ckpt: close %s: %w", r.path, err)
	}
	return nil
}

// tupleBytes reads and returns the raw bytes of one tuple.
func (r *Reader) tupleBytes(id uint64) ([]byte, error) {
	for i := range r.dir {
		if r.dir[i].ID == id {
			buf := make([]byte, r.dir[i].Length)
			if _, err := r.f.ReadAt(buf, int64(r.dir[i].Offset)); err != nil {
				return nil, fmt.Errorf("ckpt: read tuple %d: %w", id, err)
			}
			return buf, nil
		}
	}
	return nil, fmt.Errorf("ckpt: tuple %d not found: %w", id, base.ErrCorruption)
}

// loadTuple parses and caches one tuple.
func (r *Reader) loadTuple(id uint64) (*parsedTuple, error) {
	if t, ok := r.tup[id]; ok {
		return t, nil
	}
	buf, err := r.tupleBytes(id)
	if err != nil {
		return nil, err
	}
	t, err := parseTuple(id, buf)
	if err == nil {
		r.tup[id] = t
	}
	return t, err
}

// parseTuple verifies the CRC and structure of every section.
func parseTuple(id uint64, buf []byte) (*parsedTuple, error) {
	t := &parsedTuple{}
	pos := 0
	readU32 := func() (uint32, error) {
		if pos+4 > len(buf) {
			return 0, fmt.Errorf("ckpt: tuple %d truncated: %w", id, base.ErrCorruption)
		}
		v := binary.LittleEndian.Uint32(buf[pos : pos+4])
		pos += 4
		return v, nil
	}
	metaLen, err := readU32()
	if err != nil {
		return nil, err
	}
	if pos+int(metaLen)+4 > len(buf) {
		return nil, fmt.Errorf("ckpt: tuple %d meta truncated: %w", id, base.ErrCorruption)
	}
	metaJSON := buf[pos : pos+int(metaLen)]
	pos += int(metaLen)
	if base.CRC(metaJSON) != binary.LittleEndian.Uint32(buf[pos:pos+4]) {
		return nil, fmt.Errorf("ckpt: tuple %d meta crc: %w", id, base.ErrCorruption)
	}
	pos += 4
	if err := json.Unmarshal(metaJSON, &t.meta); err != nil {
		return nil, fmt.Errorf("ckpt: tuple %d meta json: %w", id, base.ErrCorruption)
	}

	sumLen, err := readU32()
	if err != nil {
		return nil, err
	}
	if pos+int(sumLen) > len(buf) {
		return nil, fmt.Errorf("ckpt: tuple %d summary truncated: %w", id, base.ErrCorruption)
	}
	t.summary, err = decodeSection(buf[pos : pos+int(sumLen)])
	if err != nil {
		return nil, err
	}
	pos += int(sumLen)

	idxLen, err := readU32()
	if err != nil {
		return nil, err
	}
	if pos+int(idxLen) > len(buf) {
		return nil, fmt.Errorf("ckpt: tuple %d index truncated: %w", id, base.ErrCorruption)
	}
	idxSection := buf[pos : pos+int(idxLen)]
	idxPayload, err := decodeSection(idxSection)
	if err != nil {
		return nil, err
	}
	t.index, err = parseIndex(idxPayload)
	if err != nil {
		return nil, fmt.Errorf("ckpt: tuple %d index: %w", id, err)
	}
	pos += int(idxLen)
	if pos != len(buf) || t.meta.ID != id {
		return nil, fmt.Errorf("ckpt: tuple %d identity or length mismatch: %w", id, base.ErrCorruption)
	}
	return t, nil
}

// parseIndex splits entries, restarts and bloom from an index payload.
func parseIndex(payload []byte) (*parsedIndex, error) {
	if len(payload) < 8 {
		return nil, fmt.Errorf("ckpt: index short: %w", base.ErrCorruption)
	}
	bloomLen := binary.LittleEndian.Uint32(payload[len(payload)-4:])
	if int(bloomLen)+4 > len(payload) {
		return nil, fmt.Errorf("ckpt: index bloom length: %w", base.ErrCorruption)
	}
	bloomBytes := payload[len(payload)-4-int(bloomLen) : len(payload)-4]
	head := payload[:len(payload)-4-int(bloomLen)]
	if len(head) < 4 {
		return nil, fmt.Errorf("ckpt: index restarts: %w", base.ErrCorruption)
	}
	n := binary.LittleEndian.Uint32(head[len(head)-4:])
	if int(n)*4+4 > len(head) || n == 0 {
		return nil, fmt.Errorf("ckpt: index restart count: %w", base.ErrCorruption)
	}
	restarts := make([]uint32, n)
	for i := range restarts {
		restarts[i] = binary.LittleEndian.Uint32(head[len(head)-4-int(n)*4+4*i:])
	}
	if int(restarts[0]) != 0 {
		return nil, fmt.Errorf("ckpt: index first restart: %w", base.ErrCorruption)
	}
	for _, r := range restarts {
		if int(r) >= len(head)-4-int(n)*4 {
			return nil, fmt.Errorf("ckpt: index restart out of range: %w", base.ErrCorruption)
		}
	}
	bl, err := bloom.Decode(bloomBytes)
	if err != nil {
		return nil, err
	}
	return &parsedIndex{
		data:     head[:len(head)-4-int(n)*4],
		restarts: restarts,
		bloom:    bl,
	}, nil
}

// Tuples returns all tuple metas in directory order.
func (r *Reader) Tuples() []TupleMeta {
	out := make([]TupleMeta, 0, len(r.dir))
	for _, d := range r.dir {
		if t, err := r.loadTuple(d.ID); err == nil {
			out = append(out, t.meta)
		}
	}
	return out
}

// ReadMeta returns one tuple's metadata.
func (r *Reader) ReadMeta(id uint64) (TupleMeta, error) {
	t, err := r.loadTuple(id)
	if err != nil {
		return TupleMeta{}, err
	}
	return t.meta, nil
}

// ReadSummary returns the tuple's raw summary JSON.
func (r *Reader) ReadSummary(id uint64) ([]byte, error) {
	t, err := r.loadTuple(id)
	if err != nil {
		return nil, err
	}
	return t.summary, nil
}

// LookupKey returns the key entry for key within tuple id.
func (r *Reader) LookupKey(id uint64, key []byte) (KeyEntry, bool, error) {
	t, err := r.loadTuple(id)
	if err != nil {
		return KeyEntry{}, false, err
	}
	return lookupIndex(t.index, key)
}

// IterKeys returns all entries of tuple id in key order.
func (r *Reader) IterKeys(id uint64) ([]KeyEntry, error) {
	t, err := r.loadTuple(id)
	if err != nil {
		return nil, err
	}
	return iterIndex(t.index)
}

// lookupIndex finds key in a parsed index. prevKey threading keeps the
// prefix compression decode non-recursive.
func lookupIndex(pi *parsedIndex, key []byte) (KeyEntry, bool, error) {
	if !pi.bloom.MayContain(key) {
		return KeyEntry{}, false, nil
	}
	// binary search restart points for the last restart whose key < target
	lo, hi := 0, len(pi.restarts)-1
	start := 0
	for lo <= hi {
		mid := (lo + hi) / 2
		e, ok := decodeAt(pi, int(pi.restarts[mid]), nil)
		if !ok {
			return KeyEntry{}, false, fmt.Errorf("ckpt: index restart decode: %w", base.ErrCorruption)
		}
		if base.CompareUser(e.Key, key) < 0 {
			start = int(pi.restarts[mid])
			lo = mid + 1
		} else {
			hi = mid - 1
		}
	}
	// scan forward, threading the previous full key
	off := start
	var prev []byte
	for {
		e, ok := decodeAt(pi, off, prev)
		if !ok {
			return KeyEntry{}, false, nil // end of index: absent
		}
		c := base.CompareUser(e.Key, key)
		if c == 0 {
			return e.KeyEntry, true, nil
		}
		if c > 0 {
			return KeyEntry{}, false, nil
		}
		prev = e.Key
		off = e.end
	}
}

// keyEntryEnd is the decode result with the next offset.
type keyEntryEnd struct {
	KeyEntry
	end int
}

// decodeAt decodes the entry at off. prevKey is the full key of the entry
// immediately before this one (nil at restart boundaries, which always store
// shared=0).
func decodeAt(pi *parsedIndex, off int, prevKey []byte) (e keyEntryEnd, ok bool) {
	data := pi.data
	if off < 0 || off >= len(data) {
		return keyEntryEnd{}, false
	}
	shared, n1 := binary.Uvarint(data[off:])
	if n1 <= 0 {
		return keyEntryEnd{}, false
	}
	unshared, n2 := binary.Uvarint(data[off+n1:])
	if n2 <= 0 {
		return keyEntryEnd{}, false
	}
	dataLen := uint64(len(data))
	if shared > dataLen || unshared > dataLen || shared > uint64(len(prevKey)) {
		return keyEntryEnd{}, false
	}
	keyStart := uint64(off) + uint64(n1+n2)
	if keyStart+unshared > dataLen {
		return keyEntryEnd{}, false
	}
	keyEnd := keyStart + unshared

	readU := func(pos uint64) (uint64, uint64, bool) {
		if pos >= dataLen {
			return 0, 0, false
		}
		v, n := binary.Uvarint(data[pos:])
		if n <= 0 {
			return 0, 0, false
		}
		return v, uint64(n), true
	}
	minTs, n3, ok1 := readU(keyEnd)
	if !ok1 {
		return keyEntryEnd{}, false
	}
	span, n4, ok2 := readU(keyEnd + n3)
	if !ok2 {
		return keyEntryEnd{}, false
	}
	count, n5, ok3 := readU(keyEnd + n3 + n4)
	if !ok3 {
		return keyEntryEnd{}, false
	}
	kindPos := keyEnd + n3 + n4 + n5
	if kindPos >= dataLen {
		return keyEntryEnd{}, false
	}
	key := make([]byte, 0, int(shared)+int(unshared))
	key = append(key, prevKey[:shared]...)
	key = append(key, data[keyStart:keyEnd]...)
	e.Key = key
	e.MinTs = minTs
	e.MaxTs = minTs + span
	e.Count = count
	e.LastKind = base.Kind(data[kindPos])
	e.end = int(kindPos + 1)
	return e, true
}

// iterIndex returns all entries in key order.
func iterIndex(pi *parsedIndex) ([]KeyEntry, error) {
	var out []KeyEntry
	off := 0
	var prev []byte
	for {
		e, ok := decodeAt(pi, off, prev)
		if !ok {
			break
		}
		out = append(out, e.KeyEntry)
		prev = e.Key
		off = e.end
	}
	return out, nil
}
