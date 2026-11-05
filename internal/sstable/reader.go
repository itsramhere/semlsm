package sstable

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"semlsm/internal/base"
	"semlsm/internal/bloom"
	"semlsm/internal/vfs"
)

// footerLen is fixed at 64 bytes per B8.
const footerLen = 64

// sstMagic is the footer magic 0x53454D4C534D5354 ("SEMLSMST" as ASCII LE).
const sstMagic = uint64(0x53454D4C534D5354)

// targetDataBlockSize is the soft data block target (B8: 16 KiB).
const targetDataBlockSize = 16 << 10

// Props is the JSON props block of an SST.
type Props struct {
	Count         uint64 `json:"count"`
	Deletes       uint64 `json:"deletes"`
	RawKeyBytes   uint64 `json:"raw_key_bytes"`
	RawValueBytes uint64 `json:"raw_value_bytes"`
	MinTs         uint64 `json:"min_ts"`
	MaxTs         uint64 `json:"max_ts"`
	Smallest      []byte `json:"smallest"`
	Largest       []byte `json:"largest"`
}

// Writer builds one .sst file.
type Writer struct {
	fs       vfs.FS
	f        vfs.File
	path     string
	bb       *blockBuilder
	index    *blockBuilder
	distinct map[string]struct{}
	props    Props
	lastIkey []byte
	offset   int64
	finished bool
}

// NewWriter creates the file at path for writing.
func NewWriter(fs vfs.FS, path string) (*Writer, error) {
	f, err := fs.Create(path)
	if err != nil {
		return nil, fmt.Errorf("sstable: create %s: %w", path, err)
	}
	return &Writer{
		fs:       fs,
		f:        f,
		path:     path,
		bb:       newBlockBuilder(),
		index:    newBlockBuilder(),
		distinct: map[string]struct{}{},
	}, nil
}

// Add appends one entry; internal keys must be strictly ascending (I3).
func (w *Writer) Add(ikey, value []byte) error {
	if w.finished {
		return fmt.Errorf("sstable: add %s after finish", w.path)
	}
	if w.lastIkey != nil && base.Compare(w.lastIkey, ikey) >= 0 {
		return fmt.Errorf("sstable: add %s: key %q not greater than previous", w.path, base.UserKey(ikey))
	}
	user, ts, kind, ok := base.SplitInternalKey(ikey)
	if !ok {
		return fmt.Errorf("sstable: add %s: bad internal key", w.path)
	}
	if err := w.bb.Add(ikey, value); err != nil {
		return fmt.Errorf("sstable: add %s: %w", w.path, err)
	}
	w.props.Count++
	if kind == base.KindDelete {
		w.props.Deletes++
	}
	w.props.RawKeyBytes += uint64(len(user))
	w.props.RawValueBytes += uint64(len(value))
	if w.props.Count == 1 || ts < w.props.MinTs {
		w.props.MinTs = ts
	}
	if ts > w.props.MaxTs {
		w.props.MaxTs = ts
	}
	if w.props.Smallest == nil {
		w.props.Smallest = append([]byte(nil), ikey...)
	}
	w.props.Largest = append([]byte(nil), ikey...)
	w.distinct[string(user)] = struct{}{}
	w.lastIkey = append(w.lastIkey[:0], ikey...)

	if w.bb.size() >= targetDataBlockSize {
		if err := w.flushBlock(); err != nil {
			return err
		}
	}
	return nil
}

// flushBlock writes the current data block and an index entry for it.
func (w *Writer) flushBlock() error {
	if w.bb.empty() {
		return nil
	}
	payload := w.bb.finish()
	onDisk := onDiskBlock(payload)
	lastKey := append([]byte(nil), w.bb.lastKey...)
	var pos [16]byte
	binary.LittleEndian.PutUint64(pos[0:8], uint64(w.offset))
	binary.LittleEndian.PutUint64(pos[8:16], uint64(len(onDisk)))
	if err := w.index.Add(lastKey, pos[:]); err != nil {
		return fmt.Errorf("sstable: index %s: %w", w.path, err)
	}
	if _, err := w.f.Write(onDisk); err != nil {
		return fmt.Errorf("sstable: write block %s: %w", w.path, err)
	}
	w.offset += int64(len(onDisk))
	w.bb.reset()
	return nil
}

// ErrEmptyTable is returned by Writer.Finish when no entry was added; callers
// must Abort instead of installing an unreadable file (M-008 item 8).
var ErrEmptyTable = errors.New("sstable: empty table")

// Finish writes all trailing sections, syncs and closes the file.
func (w *Writer) Finish() error {
	if w.finished {
		return fmt.Errorf("sstable: finish %s twice", w.path)
	}
	if w.props.Count == 0 {
		w.finished = true
		return fmt.Errorf("sstable: finish %s: %w", w.path, ErrEmptyTable)
	}
	w.finished = true
	if err := w.flushBlock(); err != nil {
		return err
	}

	// Bloom over distinct user keys: 10 bits per key (B8).
	bl := bloom.New(len(w.distinct))
	for k := range w.distinct {
		bl.Add([]byte(k))
	}

	// Props block.
	propsJSON, err := json.Marshal(w.props)
	if err != nil {
		return fmt.Errorf("sstable: props %s: %w", w.path, err)
	}

	// Index block.
	indexPayload := w.index.finish()

	bloomOnDisk := onDiskBlock(bl.Encode())
	propsOnDisk := onDiskBlock(propsJSON)
	indexOnDisk := onDiskBlock(indexPayload)

	bloomOff := w.offset
	if _, err := w.f.Write(bloomOnDisk); err != nil {
		return fmt.Errorf("sstable: write bloom %s: %w", w.path, err)
	}
	w.offset += int64(len(bloomOnDisk))
	propsOff := w.offset
	if _, err := w.f.Write(propsOnDisk); err != nil {
		return fmt.Errorf("sstable: write props %s: %w", w.path, err)
	}
	w.offset += int64(len(propsOnDisk))
	indexOff := w.offset
	if _, err := w.f.Write(indexOnDisk); err != nil {
		return fmt.Errorf("sstable: write index %s: %w", w.path, err)
	}
	w.offset += int64(len(indexOnDisk))

	// Footer: exactly 64 bytes.
	var footer [footerLen]byte
	binary.LittleEndian.PutUint64(footer[0:8], uint64(indexOff))
	binary.LittleEndian.PutUint64(footer[8:16], uint64(len(indexOnDisk)))
	binary.LittleEndian.PutUint64(footer[16:24], uint64(bloomOff))
	binary.LittleEndian.PutUint64(footer[24:32], uint64(len(bloomOnDisk)))
	binary.LittleEndian.PutUint64(footer[32:40], uint64(propsOff))
	binary.LittleEndian.PutUint64(footer[40:48], uint64(len(propsOnDisk)))
	binary.LittleEndian.PutUint32(footer[48:52], 1) // version
	binary.LittleEndian.PutUint32(footer[52:56], base.CRC(footer[:52]))
	binary.LittleEndian.PutUint64(footer[56:64], sstMagic)
	if _, err := w.f.Write(footer[:]); err != nil {
		return fmt.Errorf("sstable: write footer %s: %w", w.path, err)
	}
	w.offset += footerLen

	if err := w.f.Sync(); err != nil {
		return fmt.Errorf("sstable: sync %s: %w", w.path, err)
	}
	return w.f.Close()
}

// Abort closes and removes the file (used when a build fails mid-way).
func (w *Writer) Abort() {
	_ = w.f.Close()
	_ = w.fs.Remove(w.path)
}

// Size returns the bytes written so far (after Finish: the file size).
func (w *Writer) Size() int64 { return w.offset }

// readSection reads and decodes one on-disk block section.
func readSection(f vfs.File, off, length uint64) ([]byte, error) {
	if length == 0 || off+length > uint64(int64(^uint64(0)>>1)) {
		return nil, fmt.Errorf("sstable: bad section extent: %w", base.ErrCorruption)
	}
	buf := make([]byte, length)
	if _, err := f.ReadAt(buf, int64(off)); err != nil && err != io.EOF {
		return nil, fmt.Errorf("sstable: read section: %w", err)
	}
	return decodeOnDiskBlock(buf)
}

// Reader reads one open .sst file.
type Reader struct {
	f         vfs.File
	path      string
	size      int64
	props     Props
	bloom     *bloom.Bloom
	indexKeys [][]byte // last internal key of each block, ascending
	indexOffs []uint64
	indexLens []uint64
	fileNum   uint64      // identity for BlockCache keys (0 = uncached)
	cache     *BlockCache // optional decoded-block cache (M-008 item 4)
}

// UseCache wires an optional decoded-block cache. fileNum identifies this
// table in cache keys. Call before any reads.
func (r *Reader) UseCache(fileNum uint64, bc *BlockCache) {
	r.fileNum = fileNum
	r.cache = bc
}

// OpenReader opens and fully validates an SST.
func OpenReader(fs vfs.FS, path string) (*Reader, error) {
	f, err := fs.Open(path)
	if err != nil {
		return nil, fmt.Errorf("sstable: open %s: %w", path, err)
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("sstable: stat %s: %w", path, err)
	}
	if st.Size < footerLen {
		f.Close()
		return nil, fmt.Errorf("sstable: %s shorter than footer: %w", path, base.ErrCorruption)
	}
	var footer [footerLen]byte
	if _, err := f.ReadAt(footer[:], st.Size-footerLen); err != nil {
		f.Close()
		return nil, fmt.Errorf("sstable: footer %s: %w", path, err)
	}
	if binary.LittleEndian.Uint64(footer[56:64]) != sstMagic {
		f.Close()
		return nil, fmt.Errorf("sstable: bad magic %s: %w", path, base.ErrCorruption)
	}
	if binary.LittleEndian.Uint32(footer[48:52]) != 1 {
		f.Close()
		return nil, fmt.Errorf("sstable: bad version %s: %w", path, base.ErrCorruption)
	}
	if base.CRC(footer[:52]) != binary.LittleEndian.Uint32(footer[52:56]) {
		f.Close()
		return nil, fmt.Errorf("sstable: footer crc %s: %w", path, base.ErrCorruption)
	}

	indexOff := binary.LittleEndian.Uint64(footer[0:8])
	indexLen := binary.LittleEndian.Uint64(footer[8:16])
	bloomOff := binary.LittleEndian.Uint64(footer[16:24])
	bloomLen := binary.LittleEndian.Uint64(footer[24:32])
	propsOff := binary.LittleEndian.Uint64(footer[32:40])
	propsLen := binary.LittleEndian.Uint64(footer[40:48])

	indexPayload, err := readSection(f, indexOff, indexLen)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("sstable: index %s: %w", path, err)
	}
	bloomPayload, err := readSection(f, bloomOff, bloomLen)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("sstable: bloom %s: %w", path, err)
	}
	propsPayload, err := readSection(f, propsOff, propsLen)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("sstable: props %s: %w", path, err)
	}

	blk, err := parseBlock(indexPayload)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("sstable: index parse %s: %w", path, err)
	}
	r := &Reader{f: f, path: path, size: st.Size}
	it := blk.newIterator()
	it.First()
	for it.Valid() {
		r.indexKeys = append(r.indexKeys, append([]byte(nil), it.Key()...))
		var pos [16]byte
		copy(pos[:], it.Value())
		r.indexOffs = append(r.indexOffs, binary.LittleEndian.Uint64(pos[0:8]))
		r.indexLens = append(r.indexLens, binary.LittleEndian.Uint64(pos[8:16]))
		it.Next()
	}
	if it.Err() != nil {
		f.Close()
		return nil, fmt.Errorf("sstable: index entries %s: %w", path, it.Err())
	}
	if len(r.indexKeys) == 0 {
		f.Close()
		return nil, fmt.Errorf("sstable: empty index %s: %w", path, base.ErrCorruption)
	}

	bl, err := bloom.Decode(bloomPayload)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("sstable: bloom %s: %w", path, err)
	}
	r.bloom = bl

	if err := json.Unmarshal(propsPayload, &r.props); err != nil {
		f.Close()
		return nil, fmt.Errorf("sstable: props %s: %w", path, base.ErrCorruption)
	}
	return r, nil
}

// Props returns the table's props.
func (r *Reader) Props() Props { return r.props }

// Size returns the file size.
func (r *Reader) Size() int64 { return r.size }

// Smallest returns the smallest internal key (a copy).
func (r *Reader) Smallest() []byte { return append([]byte(nil), r.props.Smallest...) }

// Largest returns the largest internal key (a copy).
func (r *Reader) Largest() []byte { return append([]byte(nil), r.props.Largest...) }

// MayContain checks the bloom filter and key range.
func (r *Reader) MayContain(user []byte) bool {
	if !r.bloom.MayContain(user) {
		return false
	}
	// range check on user keys via smallest/largest internal keys
	if r.props.Smallest != nil {
		su := base.UserKey(r.props.Smallest)
		if base.CompareUser(su, user) > 0 {
			return false
		}
	}
	if r.props.Largest != nil {
		lu := base.UserKey(r.props.Largest)
		if base.CompareUser(lu, user) < 0 {
			return false
		}
	}
	return true
}

// readBlock loads and parses block i.
func (r *Reader) readBlock(i int) (*block, error) {
	off := r.indexOffs[i]
	if r.cache != nil {
		if data, ok := r.cache.Get(r.fileNum, off); ok {
			return parseBlock(data)
		}
	}
	payload, err := readSection(r.f, off, r.indexLens[i])
	if err != nil {
		return nil, fmt.Errorf("sstable: block %d %s: %w", i, r.path, err)
	}
	if r.cache != nil {
		r.cache.Put(r.fileNum, off, payload)
	}
	blk, err := parseBlock(payload)
	if err != nil {
		return nil, fmt.Errorf("sstable: block %d parse %s: %w", i, r.path, err)
	}
	return blk, nil
}

// Get returns the newest version of user with ts <= s in this table.
// The value aliases an internal buffer; copy before retaining.
func (r *Reader) Get(user []byte, s uint64) (ts uint64, kind base.Kind, value []byte, ok bool, err error) {
	if !r.MayContain(user) {
		return 0, 0, nil, false, nil
	}
	target := base.SeekKey(user, s)
	// first index entry whose last key >= target
	i := sortSearchIndex(r.indexKeys, target)
	if i >= len(r.indexKeys) {
		return 0, 0, nil, false, nil
	}
	blk, err := r.readBlock(i)
	if err != nil {
		return 0, 0, nil, false, err
	}
	it := blk.newIterator()
	it.SeekGE(target)
	if !it.Valid() || it.Err() != nil {
		return 0, 0, nil, false, it.Err()
	}
	u, t, k, valid := base.SplitInternalKey(it.Key())
	if !valid || string(u) != string(user) {
		return 0, 0, nil, false, nil
	}
	return t, k, it.Value(), true, nil
}

func sortSearchIndex(indexKeys [][]byte, target []byte) int {
	lo, hi := 0, len(indexKeys)
	for lo < hi {
		mid := (lo + hi) / 2
		if base.Compare(indexKeys[mid], target) < 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

// Close closes the underlying file.
func (r *Reader) Close() error {
	if err := r.f.Close(); err != nil {
		return fmt.Errorf("sstable: close %s: %w", r.path, err)
	}
	return nil
}

// Name returns the file name for a table number.
func Name(num uint64) string {
	return fmt.Sprintf("%06d.sst", num)
}

// Path returns the file path this writer writes to.
func (w *Writer) Path() string { return w.path }

// Props returns the writer's accumulated props.
func (w *Writer) Props() Props { return w.props }
