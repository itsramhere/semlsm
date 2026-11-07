// Package resolve implements the pure MVCC resolution function of B6.
// Both Get and the iterator MUST call it; it decides between raw versions
// and checkpoint summaries and detects broken invariants.
package resolve

import (
	"fmt"

	"github.com/itsramhere/semlsm/internal/base"
)

// Raw is the newest raw version with ts <= S, as found by the raw lookup.
type Raw struct {
	Found bool
	Ts    uint64
	Kind  base.Kind
}

// Entry is a checkpoint key entry: among checkpoint entries for this key
// with MinTs <= S, the one with the largest MinTs.
type Entry struct {
	Found    bool
	MinTs    uint64
	MaxTs    uint64
	Count    uint64
	LastKind base.Kind
	Retired  bool
}

// Outcome is the resolution result.
type Outcome uint8

const (
	// NotFound: no data covers the read.
	NotFound Outcome = iota
	// UseRaw: return the raw version.
	UseRaw
	// UseSummary: return the checkpoint summary.
	UseSummary
	// Invariant: I7 is broken; the caller must return ErrCorruption.
	Invariant
	// UseExpired: a retired tuple covers the read; never expose older raw data.
	UseExpired
)

// Resolve applies the B6 truth table.
func Resolve(s uint64, r Raw, e Entry) Outcome {
	switch {
	case !r.Found && !e.Found:
		// Row 1
		return NotFound

	case r.Found && !e.Found:
		// Rows 2-3
		if r.Kind == base.KindPut {
			return UseRaw
		}
		return NotFound

	case r.Found && e.Found:
		switch {
		case r.Ts > e.MaxTs:
			// Rows 4-5: raw is newer than everything the checkpoint covered.
			if r.Kind == base.KindPut {
				return UseRaw
			}
			return NotFound
		case r.Ts >= e.MinTs && r.Ts <= e.MaxTs:
			// Row 6: a raw version inside the dropped range means I7 is broken.
			return Invariant
		default:
			// r.Ts < e.MinTs: raw is stale, entry covers.
			return resolveEntryOnly(s, r, e)
		}

	default:
		// entry only (no raw found)
		return resolveEntryOnly(s, r, e)
	}
}

// resolveEntryOnly handles rows 7-8 (entry covers the read; raw is absent or
// older than the checkpoint).
func resolveEntryOnly(s uint64, r Raw, e Entry) Outcome {
	// Row 7: the last version <= s is known to be a delete (the checkpoint
	// covered up to MaxTs and ended with a delete, and nothing newer exists).
	if s >= e.MaxTs && e.LastKind == base.KindDelete && (!r.Found || r.Ts < e.MinTs) {
		return NotFound
	}
	// Row 8
	if e.Retired {
		return UseExpired
	}
	return UseSummary
}

// ErrInvariant is wrapped by callers when Resolve returns Invariant.
func ErrInvariant(key []byte, s uint64, e Entry) error {
	return fmt.Errorf("semlsm: raw version of %q at ts %d lies inside dropped range [%d,%d]: %w",
		key, s, e.MinTs, e.MaxTs, base.ErrCorruption)
}
