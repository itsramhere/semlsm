package base

import (
	"encoding/binary"
	"hash/crc32"
	"sort"
)

// crcTable is the single Castagnoli table (Pitfall 16: only this one is used).
var crcTable = crc32.MakeTable(crc32.Castagnoli)

// CRC computes CRC-32C over data.
func CRC(data []byte) uint32 {
	return crc32.Checksum(data, crcTable)
}

// SourceCRC computes the integrity hash of a set of raw records per B8:
// records are hashed sorted by (ts asc, key asc); each record contributes
// uvarint klen | key | u64 BE ts | u8 kind | uvarint vlen | value.
// The input order does not matter; the hash is deterministic.
func SourceCRC(records []Record) uint32 {
	sorted := make([]Record, len(records))
	copy(sorted, records)
	sort.SliceStable(sorted, func(i, j int) bool {
		if sorted[i].Ts != sorted[j].Ts {
			return sorted[i].Ts < sorted[j].Ts
		}
		return string(sorted[i].Key) < string(sorted[j].Key)
	})

	var scratch [binary.MaxVarintLen64]byte
	var tsBE [8]byte
	crc := uint32(0)
	update := func(b []byte) {
		crc = crc32.Update(crc, crcTable, b)
	}
	for _, r := range sorted {
		n := binary.PutUvarint(scratch[:], uint64(len(r.Key)))
		update(scratch[:n])
		update(r.Key)
		binary.BigEndian.PutUint64(tsBE[:], r.Ts)
		update(tsBE[:])
		update([]byte{byte(r.Kind)})
		n = binary.PutUvarint(scratch[:], uint64(len(r.Value)))
		update(scratch[:n])
		update(r.Value)
	}
	return crc
}

// Note: crc32.Update chains across calls, so incremental feeding is equivalent
// to hashing one contiguous buffer.
