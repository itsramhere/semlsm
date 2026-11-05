// Package sstable implements the sorted-string-table format of B8:
// [data block]* [bloom] [props] [index] [footer], with prefix-compressed
// entries, restart points, and CRC-checked on-disk blocks.
package sstable

import (
	"bytes"
	"compress/flate"
	"encoding/binary"
	"fmt"
	"io"

	"semlsm/internal/base"
)

// restartInterval is the number of entries between restart points (B8: 16
// for data and index blocks).
const restartInterval = 16

// onDiskBlock builds the on-disk form of a block: payload | codec(1) | crc32(4),
// CRC over payload||codec, where payload may be flate-compressed at level 1
// when that shrinks the block (codec 1); otherwise codec 0.
func onDiskBlock(payload []byte) []byte {
	compressed := new(bytes.Buffer)
	cw, _ := flate.NewWriter(compressed, 1)
	cw.Write(payload)
	cw.Close()
	useCompressed := compressed.Len() < len(payload)

	body := payload
	codec := byte(0)
	if useCompressed {
		body = compressed.Bytes()
		codec = 1
	}
	out := make([]byte, 0, len(body)+1+4)
	out = append(out, body...)
	out = append(out, codec)
	crc := base.CRC(append(append([]byte(nil), body...), codec))
	out = binary.LittleEndian.AppendUint32(out, crc)
	return out
}

// decodeOnDiskBlock verifies the CRC and returns the uncompressed payload.
func decodeOnDiskBlock(buf []byte) ([]byte, error) {
	if len(buf) < 5 {
		return nil, fmt.Errorf("sstable: block shorter than trailer: %w", base.ErrCorruption)
	}
	body := buf[:len(buf)-5]
	codec := buf[len(buf)-5]
	wantCRC := binary.LittleEndian.Uint32(buf[len(buf)-4:])
	crc := base.CRC(append(append([]byte(nil), body...), codec))
	if crc != wantCRC {
		return nil, fmt.Errorf("sstable: block crc mismatch: %w", base.ErrCorruption)
	}
	switch codec {
	case 0:
		return body, nil
	case 1:
		r := flate.NewReader(bytes.NewReader(body))
		defer r.Close()
		out, err := io.ReadAll(r)
		if err != nil {
			return nil, fmt.Errorf("sstable: block decompress: %w", base.ErrCorruption)
		}
		return out, nil
	default:
		return nil, fmt.Errorf("sstable: unknown codec %d: %w", codec, base.ErrCorruption)
	}
}

// blockBuilder builds one data or index block payload.
type blockBuilder struct {
	buf          []byte
	restarts     []uint32
	counter      int // entries since last restart
	lastKey      []byte
	entriesAdded int
}

func newBlockBuilder() *blockBuilder {
	return &blockBuilder{restarts: []uint32{0}}
}

// Add appends one entry. key must be strictly greater than the previous key.
func (b *blockBuilder) Add(key, value []byte) error {
	if b.entriesAdded > 0 && base.Compare(b.lastKey, key) >= 0 {
		return fmt.Errorf("sstable: block entry out of order: %w", base.ErrCorruption)
	}
	shared := 0
	if b.counter < restartInterval && b.entriesAdded > 0 {
		// prefix compression against the previous key
		limit := len(b.lastKey)
		if len(key) < limit {
			limit = len(key)
		}
		for shared < limit && b.lastKey[shared] == key[shared] {
			shared++
		}
	} else if b.counter >= restartInterval {
		b.restarts = append(b.restarts, uint32(len(b.buf)))
		b.counter = 0
		shared = 0
	}

	unshared := key[shared:]
	b.buf = binary.AppendUvarint(b.buf, uint64(shared))
	b.buf = binary.AppendUvarint(b.buf, uint64(len(unshared)))
	b.buf = binary.AppendUvarint(b.buf, uint64(len(value)))
	b.buf = append(b.buf, unshared...)
	b.buf = append(b.buf, value...)

	b.lastKey = append(b.lastKey[:0], key...)
	b.counter++
	b.entriesAdded++
	return nil
}

// finish appends the restart trailer and returns the block payload.
func (b *blockBuilder) finish() []byte {
	out := make([]byte, 0, len(b.buf)+4*len(b.restarts)+4)
	out = append(out, b.buf...)
	for _, r := range b.restarts {
		out = binary.LittleEndian.AppendUint32(out, r)
	}
	out = binary.LittleEndian.AppendUint32(out, uint32(len(b.restarts)))
	return out
}

func (b *blockBuilder) empty() bool { return b.entriesAdded == 0 }

func (b *blockBuilder) size() int {
	return len(b.buf) + 4*len(b.restarts) + 4
}

func (b *blockBuilder) reset() {
	b.buf = b.buf[:0]
	b.restarts = b.restarts[:1] // keep the initial 0
	b.restarts[0] = 0
	b.counter = 0
	b.lastKey = b.lastKey[:0]
	b.entriesAdded = 0
}

// block is a parsed block payload, ready for iteration.
type block struct {
	data     []byte   // entries region (without restart trailer)
	restarts []uint32 // restart offsets
}

// parseBlock validates structure and splits the restart trailer.
func parseBlock(payload []byte) (*block, error) {
	if len(payload) < 4 {
		return nil, fmt.Errorf("sstable: block too short: %w", base.ErrCorruption)
	}
	n := binary.LittleEndian.Uint32(payload[len(payload)-4:])
	if int(n) < 1 || int(n)*4+4 > len(payload) {
		return nil, fmt.Errorf("sstable: bad restart count %d: %w", n, base.ErrCorruption)
	}
	restarts := make([]uint32, n)
	for i := range restarts {
		restarts[i] = binary.LittleEndian.Uint32(payload[len(payload)-4-int(n)*4+4*i:])
	}
	if int(restarts[0]) != 0 {
		return nil, fmt.Errorf("sstable: first restart not 0: %w", base.ErrCorruption)
	}
	for _, r := range restarts {
		if int(r) >= len(payload)-4-int(n)*4 {
			return nil, fmt.Errorf("sstable: restart out of range: %w", base.ErrCorruption)
		}
	}
	data := payload[:len(payload)-4-int(n)*4]
	return &block{data: data, restarts: restarts}, nil
}

// blockIterator iterates entries of one parsed block.
// Keys and values alias the block data and stay valid while the block lives.
type blockIterator struct {
	blk *block
	off int // current offset into blk.data
	key []byte
	val []byte
	err error
}

func (b *block) newIterator() *blockIterator {
	return &blockIterator{blk: b}
}

// firstEntryAt decodes the entry starting at off.
func (b *blockIterator) decodeAt(off int) (keyEnd int, ok bool) {
	shared, n1 := binary.Uvarint(b.blk.data[off:])
	if n1 <= 0 {
		return 0, false
	}
	unshared, n2 := binary.Uvarint(b.blk.data[off+n1:])
	if n2 <= 0 {
		return 0, false
	}
	vlen, n3 := binary.Uvarint(b.blk.data[off+n1+n2:])
	if n3 <= 0 {
		return 0, false
	}
	keyStart := off + n1 + n2 + n3
	keyEnd = keyStart + int(unshared)
	if keyEnd+int(vlen) > len(b.blk.data) || int(shared) > len(b.key) {
		return 0, false
	}
	b.key = append(b.key[:0], b.key[:shared]...)
	b.key = append(b.key, b.blk.data[keyStart:keyEnd]...)
	b.val = b.blk.data[keyEnd : keyEnd+int(vlen)]
	return keyEnd + int(vlen), true
}

// First positions at the first entry.
func (it *blockIterator) First() {
	it.err = nil
	it.key = it.key[:0]
	it.off = 0
	if _, ok := it.decodeAt(0); !ok {
		it.off = -1
		if len(it.blk.data) > 0 {
			it.err = fmt.Errorf("sstable: block: bad first entry: %w", base.ErrCorruption)
		}
	}
}

// SeekGE positions at the first entry with key >= target.
func (it *blockIterator) SeekGE(target []byte) {
	it.err = nil
	// Binary search restart points: find the last restart whose full key is
	// < target, then scan forward.
	lo, hi := 0, len(it.blk.restarts)-1
	start := 0
	for lo <= hi {
		mid := (lo + hi) / 2
		off := int(it.blk.restarts[mid])
		saved := it.off
		if _, ok := it.decodeAt(off); !ok {
			it.off = saved
			it.err = fmt.Errorf("sstable: block: bad restart entry: %w", base.ErrCorruption)
			it.off = -1
			return
		}
		if base.Compare(it.key, target) < 0 {
			start = off
			lo = mid + 1
		} else {
			hi = mid - 1
		}
	}
	// Scan forward from `start`.
	it.key = it.key[:0]
	it.off = start
	for {
		end, ok := it.decodeAt(it.off)
		if !ok {
			it.off = -1
			it.err = fmt.Errorf("sstable: block: seek scan failed: %w", base.ErrCorruption)
			return
		}
		if base.Compare(it.key, target) >= 0 {
			return
		}
		it.off = end
	}
}

// Next advances; Valid must be checked afterwards.
// it.off always points at the current entry; Next re-derives its end offset,
// then decodes the following entry in place.
func (it *blockIterator) Next() {
	if it.off < 0 {
		return
	}
	end, ok := it.decodeAt(it.off)
	if !ok {
		it.off = -1
		it.err = fmt.Errorf("sstable: block: next scan failed: %w", base.ErrCorruption)
		return
	}
	if end >= len(it.blk.data) {
		it.off = -1
		return
	}
	if _, ok := it.decodeAt(end); !ok {
		it.off = -1
		it.err = fmt.Errorf("sstable: block: bad next entry: %w", base.ErrCorruption)
		return
	}
	it.off = end
}

// Valid reports whether the iterator is positioned.
func (it *blockIterator) Valid() bool { return it.off >= 0 && it.err == nil }

// Key returns the current internal key (aliased).
func (it *blockIterator) Key() []byte { return it.key }

// Value returns the current value (aliased).
func (it *blockIterator) Value() []byte { return it.val }

// Err returns any structural corruption encountered.
func (it *blockIterator) Err() error { return it.err }
