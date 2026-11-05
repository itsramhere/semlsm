// Package base holds shared types used across all SemLSM internal packages:
// internal keys, ordering, grouping, records, clocks, CRC helpers and errors.
//
// Dependency rule: base imports only the Go standard library.
package base

import (
	"bytes"
	"encoding/binary"
)

// Kind is the type of a version record.
type Kind uint8

const (
	// KindDelete marks a tombstone.
	KindDelete Kind = 1
	// KindPut marks a value write.
	KindPut Kind = 2
)

// String returns the kind name used in logs and prompts.
func (k Kind) String() string {
	switch k {
	case KindDelete:
		return "DELETE"
	case KindPut:
		return "PUT"
	default:
		return "UNKNOWN"
	}
}

// Record is one raw version of a key, used by grouping, CRC and the semantic pipeline.
type Record struct {
	Key   []byte
	Ts    uint64
	Kind  Kind
	Value []byte
}

// internal key trailer: 8-byte big-endian ts + 1-byte kind.
const internalTrailerLen = 9

// MaxInternalKeyLen guards against absurd allocations.
const MaxInternalKeyLen = 64<<10 + internalTrailerLen

// MakeInternalKey builds userKey || ts(8, big-endian) || kind(1).
func MakeInternalKey(user []byte, ts uint64, k Kind) []byte {
	out := make([]byte, 0, len(user)+internalTrailerLen)
	return AppendInternalKey(out, user, ts, k)
}

// AppendInternalKey appends an internal key to dst and returns the extended slice.
func AppendInternalKey(dst []byte, user []byte, ts uint64, k Kind) []byte {
	dst = append(dst, user...)
	var trailer [internalTrailerLen]byte
	binary.BigEndian.PutUint64(trailer[:8], ts)
	trailer[8] = byte(k)
	dst = append(dst, trailer[:]...)
	return dst
}

// SplitInternalKey splits an internal key into its user key, timestamp and kind.
// ok is false for malformed (too short) keys.
func SplitInternalKey(ikey []byte) (user []byte, ts uint64, k Kind, ok bool) {
	if len(ikey) < internalTrailerLen {
		return nil, 0, 0, false
	}
	user = ikey[:len(ikey)-internalTrailerLen]
	ts = binary.BigEndian.Uint64(ikey[len(ikey)-internalTrailerLen : len(ikey)-1])
	k = Kind(ikey[len(ikey)-1])
	return user, ts, k, true
}

// UserKey returns the user key portion of an internal key.
// The result aliases ikey; copy it before retaining.
func UserKey(ikey []byte) []byte {
	if len(ikey) < internalTrailerLen {
		return ikey
	}
	return ikey[:len(ikey)-internalTrailerLen]
}

// Compare orders internal keys per invariant I3: user key ascending,
// then ts descending (newer first), then kind descending.
func Compare(a, b []byte) int {
	ua, ta, ka, oka := SplitInternalKey(a)
	ub, tb, kb, okb := SplitInternalKey(b)
	if !oka || !okb {
		// Malformed keys: fall back to raw byte order so callers never panic.
		return bytes.Compare(a, b)
	}
	if c := bytes.Compare(ua, ub); c != 0 {
		return c
	}
	if ta != tb {
		if ta > tb {
			return -1
		}
		return 1
	}
	if ka != kb {
		if ka > kb {
			return -1
		}
		return 1
	}
	return 0
}

// SeekKey builds the internal seek key for "newest version with ts <= S":
// the smallest internal key that sorts before any version of user with ts <= S.
func SeekKey(user []byte, ts uint64) []byte {
	return MakeInternalKey(user, ts, 0xFF)
}

// CompareUser orders two user keys by plain byte comparison.
func CompareUser(a, b []byte) int {
	return bytes.Compare(a, b)
}
