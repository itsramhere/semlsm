package base

import (
	"errors"
	"fmt"
)

// Sentinel errors. Re-exported from the root semlsm package.
var (
	// ErrCorruption is returned when any CRC check or structural validation fails.
	ErrCorruption = errors.New("semlsm: corruption detected")
	// ErrClosed is returned on operations against a closed DB or file.
	ErrClosed = errors.New("semlsm: closed")
	// ErrFutureTimestamp is returned when a read requests a timestamp newer
	// than the last visible commit timestamp.
	ErrFutureTimestamp = errors.New("semlsm: requested timestamp is in the future")
	// ErrOptionsMismatch is returned when OPTIONS.json group settings differ on reopen.
	ErrOptionsMismatch = errors.New("semlsm: options mismatch with existing database")
	// ErrNotArchived is returned by GetArchived when no archive copy exists.
	ErrNotArchived    = errors.New("semlsm: raw version not archived")
	ErrArchiveRetired = fmt.Errorf("semlsm: archive retired: %w", ErrNotArchived)
	// ErrKeyEmpty is returned when a Put/Delete/Get receives an empty key.
	ErrKeyEmpty = errors.New("semlsm: key is empty")
	// ErrKeyTooLarge is returned when a key exceeds MaxKeyBytes.
	ErrKeyTooLarge = errors.New("semlsm: key too large")
	// ErrValueTooLarge is returned when a value exceeds MaxValueBytes.
	ErrValueTooLarge = errors.New("semlsm: value too large")
	// ErrReleased is returned when using a released Snapshot or Iterator.
	ErrReleased = errors.New("semlsm: snapshot or iterator released")
)

// Size limits enforced by the commit pipeline.
const (
	// MaxKeyBytes is the largest user key accepted (64 KiB).
	MaxKeyBytes = 64 << 10
	// MaxValueBytes is the largest value accepted (8 MiB).
	MaxValueBytes = 8 << 20
)

// NumLevels is the fixed level count: L0 through L4 (bottom).
const NumLevels = 5

// ValidateKV applies the key and value size rules; returns a sentinel error or nil.
func ValidateKV(key, value []byte) error {
	if len(key) == 0 {
		return ErrKeyEmpty
	}
	if len(key) > MaxKeyBytes {
		return ErrKeyTooLarge
	}
	if len(value) > MaxValueBytes {
		return ErrValueTooLarge
	}
	return nil
}
