package wal

import (
	"encoding/binary"
	"fmt"

	"semlsm/internal/base"
)

// EncodeBatch encodes a batch payload per B8:
// u64 ts | uvarint count | repeat(u8 kind, uvarint klen, key, uvarint vlen, value).
// All records share the batch ts (invariant I2).
func EncodeBatch(ts uint64, recs []base.Record) ([]byte, error) {
	buf := make([]byte, 0, 32)
	buf = binary.LittleEndian.AppendUint64(buf, ts)
	buf = binary.AppendUvarint(buf, uint64(len(recs)))
	for i := range recs {
		r := &recs[i]
		buf = append(buf, byte(r.Kind))
		buf = binary.AppendUvarint(buf, uint64(len(r.Key)))
		buf = append(buf, r.Key...)
		buf = binary.AppendUvarint(buf, uint64(len(r.Value)))
		buf = append(buf, r.Value...)
	}
	if len(buf) > MaxPayloadSize {
		return nil, fmt.Errorf("wal: encode batch: %w (%d bytes)", ErrFrameTooLarge, len(buf))
	}
	return buf, nil
}

// DecodeBatch decodes a payload written by EncodeBatch.
// The returned records own their memory; the payload is not retained.
// Malformed input returns an error wrapping base.ErrCorruption.
func DecodeBatch(payload []byte) (ts uint64, recs []base.Record, err error) {
	fail := func(reason string) (uint64, []base.Record, error) {
		return 0, nil, fmt.Errorf("wal: decode batch: %s: %w", reason, base.ErrCorruption)
	}
	if len(payload) < 8 {
		return fail("payload shorter than ts")
	}
	ts = binary.LittleEndian.Uint64(payload[:8])
	rest := payload[8:]
	pos := 0

	readUvarint := func() (uint64, bool) {
		v, n := binary.Uvarint(rest[pos:])
		if n <= 0 {
			return 0, false
		}
		pos += n
		return v, true
	}
	readBytes := func(n uint64) ([]byte, bool) {
		if uint64(len(rest)-pos) < n {
			return nil, false
		}
		b := rest[pos : pos+int(n)]
		pos += int(n)
		return b, true
	}

	count, ok := readUvarint()
	if !ok {
		return fail("missing count")
	}
	if count > uint64(len(rest)) {
		return fail("count exceeds payload")
	}
	recs = make([]base.Record, 0, count)
	for i := uint64(0); i < count; i++ {
		var r base.Record
		kb, ok := readBytes(1)
		if !ok {
			return fail("missing kind")
		}
		r.Kind = base.Kind(kb[0])
		klen, ok := readUvarint()
		if !ok {
			return fail("missing key length")
		}
		key, ok := readBytes(klen)
		if !ok {
			return fail("key truncated")
		}
		r.Key = append([]byte(nil), key...)
		vlen, ok := readUvarint()
		if !ok {
			return fail("missing value length")
		}
		val, ok := readBytes(vlen)
		if !ok {
			return fail("value truncated")
		}
		r.Value = append([]byte(nil), val...)
		r.Ts = ts
		recs = append(recs, r)
	}
	return ts, recs, nil
}
