// Package bloom implements the fixed bloom filter used by SSTables and
// checkpoint key indexes: 10 bits per distinct user key, 7 probes, double
// hashing from FNV-1a 64.
package bloom

import (
	"fmt"

	"semlsm/internal/base"
)

const (
	// BitsPerKey is the bloom sizing rule (B8).
	BitsPerKey = 10
	// NumProbes is the number of hash probes (k).
	NumProbes = 7
)

// Bloom is a fixed-size filter over user keys.
type Bloom struct {
	bits []byte // bit array
	k    uint8  // probes
}

// New allocates a filter for approximately n distinct user keys.
func New(n int) *Bloom {
	bits := n * BitsPerKey
	if bits < 64 {
		bits = 64
	}
	nbytes := (bits + 7) / 8
	return &Bloom{bits: make([]byte, nbytes), k: NumProbes}
}

// hash computes the FNV-1a 64 hash of key.
func hash(key []byte) uint64 {
	const (
		offset64 = 14695981039346656037
		prime64  = 1099511628211
	)
	h := uint64(offset64)
	for _, c := range key {
		h ^= uint64(c)
		h *= prime64
	}
	return h
}

// Add inserts one user key.
func (b *Bloom) Add(key []byte) {
	h := hash(key)
	h1 := uint32(h)       // low 32 bits
	h2 := uint32(h >> 32) // high 32 bits
	m := uint32(len(b.bits)) * 8
	if m == 0 {
		return
	}
	for i := uint32(0); i < uint32(b.k); i++ {
		// Double hashing: probe = h1 + i*h2, spread with a golden-ratio twist
		// so consecutive h1 values do not collide modulo small m.
		pos := (h1 + i*h2 + i*0x9E3779B9) % m
		b.bits[pos>>3] |= 1 << (pos & 7)
	}
}

// MayContain reports whether key might be present. False positives are
// possible; false negatives never are.
func (b *Bloom) MayContain(key []byte) bool {
	h := hash(key)
	h1 := uint32(h)
	h2 := uint32(h >> 32)
	m := uint32(len(b.bits)) * 8
	if m == 0 {
		return false
	}
	for i := uint32(0); i < uint32(b.k); i++ {
		pos := (h1 + i*h2 + i*0x9E3779B9) % m
		if b.bits[pos>>3]&(1<<(pos&7)) == 0 {
			return false
		}
	}
	return true
}

// Encode serializes: bits | u8 k (B8 payload format).
func (b *Bloom) Encode() []byte {
	out := make([]byte, 0, len(b.bits)+1)
	out = append(out, b.bits...)
	out = append(out, b.k)
	return out
}

// Decode parses an encoded filter. Corruption returns an error wrapping
// base.ErrCorruption.
func Decode(payload []byte) (*Bloom, error) {
	if len(payload) < 2 {
		return nil, fmt.Errorf("bloom: decode: payload too short: %w", base.ErrCorruption)
	}
	k := payload[len(payload)-1]
	if k == 0 {
		return nil, fmt.Errorf("bloom: decode: zero probes: %w", base.ErrCorruption)
	}
	return &Bloom{bits: append([]byte(nil), payload[:len(payload)-1]...), k: k}, nil
}
