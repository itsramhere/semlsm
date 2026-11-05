// Package wal implements the write-ahead log: length-prefixed, CRC-checked
// frames of batch payloads, with torn-tail-tolerant replay for the last WAL.
package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"semlsm/internal/base"
	"semlsm/internal/vfs"
)

// MaxPayloadSize is the largest batch payload a frame may carry (64 MiB).
const MaxPayloadSize = 64 << 20

// frameHeaderLen is u32 payloadLen | u32 crc.
const frameHeaderLen = 8

// ErrFrameTooLarge is returned by Append for oversized payloads.
var ErrFrameTooLarge = errors.New("wal: frame payload too large")

// Writer appends frames to one WAL file.
type Writer struct {
	f    vfs.File
	path string
	size int64
}

// NewWriter creates (or truncates) the WAL at path.
func NewWriter(fs vfs.FS, path string) (*Writer, error) {
	f, err := fs.Create(path)
	if err != nil {
		return nil, fmt.Errorf("wal: create %s: %w", path, err)
	}
	return &Writer{f: f, path: path}, nil
}

// Append writes one frame: header + payload, in a single Write call.
// An empty payload is rejected: a zero-length frame is indistinguishable from
// a zero-filled crash tail and would break replay (M-006 item 2).
func (w *Writer) Append(payload []byte) error {
	frame, err := Frame(payload)
	if err != nil {
		return fmt.Errorf("wal: append %s: %w", w.path, err)
	}
	if _, err := w.f.Write(frame); err != nil {
		return fmt.Errorf("wal: append %s: %w", w.path, err)
	}
	w.size += int64(len(frame))
	return nil
}

// Sync flushes the WAL to durable storage.
func (w *Writer) Sync() error {
	if err := w.f.Sync(); err != nil {
		return fmt.Errorf("wal: sync %s: %w", w.path, err)
	}
	return nil
}

// Close closes the underlying file.
func (w *Writer) Close() error {
	if err := w.f.Close(); err != nil {
		return fmt.Errorf("wal: close %s: %w", w.path, err)
	}
	return nil
}

// Size returns the current file size including frame headers.
func (w *Writer) Size() int64 { return w.size }

// Replay reads frames from path, calling fn for each payload.
// fn must not retain the payload slice.
//
// last selects the recovery rule for the file's tail:
//   - last=true:  a short header, short payload, or CRC mismatch is a torn
//     tail; replay stops there and returns the bytes before it (the caller
//     truncates the file to validBytes). No error.
//   - last=false: the same conditions return base.ErrCorruption.
//
// validBytes is the offset after the last complete valid frame.
func Replay(fs vfs.FS, path string, last bool, fn func(payload []byte) error) (validBytes int64, err error) {
	f, err := fs.Open(path)
	if err != nil {
		return 0, fmt.Errorf("wal: replay %s: %w", path, err)
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return 0, fmt.Errorf("wal: replay %s: %w", path, err)
	}
	fileSize := st.Size

	torn := func(reason string) (int64, error) {
		if last {
			return validBytes, nil
		}
		return validBytes, fmt.Errorf("wal: replay %s at offset %d: %s: %w", path, validBytes, reason, base.ErrCorruption)
	}

	var header [frameHeaderLen]byte
	payload := make([]byte, 0, 4096)
	for {
		n, err := f.ReadAt(header[:], validBytes)
		if err != nil && err != io.EOF {
			return validBytes, fmt.Errorf("wal: replay %s: read header: %w", path, err)
		}
		if n == 0 {
			// Exactly at end of file: a clean end in both modes (M-006 item 1).
			return validBytes, nil
		}
		if n < frameHeaderLen {
			// Partial header left in the file: torn tail.
			return torn("short header")
		}
		payloadLen := binary.LittleEndian.Uint32(header[0:4])
		wantCRC := binary.LittleEndian.Uint32(header[4:8])

		// A zero-filled tail parses as a length-0 frame whose CRC of nothing
		// is 0; reject it explicitly (M-006 item 2).
		if payloadLen == 0 {
			return torn("zero-length frame")
		}

		// Check against remaining file size BEFORE allocating.
		remaining := fileSize - validBytes - frameHeaderLen
		if payloadLen > MaxPayloadSize || int64(payloadLen) > remaining {
			return torn(fmt.Sprintf("payload length %d exceeds %d remaining bytes", payloadLen, remaining))
		}
		if int64(cap(payload)) < int64(payloadLen) {
			payload = make([]byte, payloadLen)
		} else {
			payload = payload[:payloadLen]
		}
		if _, err := f.ReadAt(payload, validBytes+frameHeaderLen); err != nil {
			return validBytes, fmt.Errorf("wal: replay %s: read payload: %w", path, err)
		}
		if got := base.CRC(payload); got != wantCRC {
			return torn("crc mismatch")
		}
		if err := fn(payload); err != nil {
			return validBytes, fmt.Errorf("wal: replay %s: callback: %w", path, err)
		}
		validBytes += frameHeaderLen + int64(payloadLen)
	}
}

// Frame builds one on-disk frame (header + payload) without writing it.
// Used by the manifest, whose frames share the WAL format. The payload must
// be non-empty and within MaxPayloadSize (M-009 item 3): a zero-length frame
// is indistinguishable from a zero-filled crash tail and breaks replay.
func Frame(payload []byte) ([]byte, error) {
	if len(payload) == 0 {
		return nil, fmt.Errorf("wal: frame: empty payload")
	}
	if len(payload) > MaxPayloadSize {
		return nil, fmt.Errorf("wal: frame: %w (%d bytes)", ErrFrameTooLarge, len(payload))
	}
	frame := make([]byte, frameHeaderLen+len(payload))
	binary.LittleEndian.PutUint32(frame[0:4], uint32(len(payload)))
	binary.LittleEndian.PutUint32(frame[4:8], base.CRC(payload))
	copy(frame[frameHeaderLen:], payload)
	return frame, nil
}

// ReplayStrict replays frames with the manifest recovery rules (M-011 item 1).
// Each manifest edit is fsynced on its own, so only the final frame can
// legitimately be torn:
//   - a bad CRC is a torn tail only if that frame ends exactly at EOF;
//     a bad CRC with bytes after the frame is ErrCorruption;
//   - a declared length past EOF or a short header at the tail is torn;
//   - a zero-length frame is torn only if all remaining bytes are zero,
//     otherwise ErrCorruption;
//   - a clean end of file (n == 0) is done.
//
// validBytes is the offset after the last complete valid frame.
func ReplayStrict(fs vfs.FS, path string, fn func(payload []byte) error) (validBytes int64, err error) {
	f, err := fs.Open(path)
	if err != nil {
		return 0, fmt.Errorf("wal: replay strict %s: %w", path, err)
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return 0, fmt.Errorf("wal: replay strict %s: %w", path, err)
	}
	fileSize := st.Size

	var header [frameHeaderLen]byte
	payload := make([]byte, 0, 4096)
	for {
		n, err := f.ReadAt(header[:], validBytes)
		if err != nil && err != io.EOF {
			return validBytes, fmt.Errorf("wal: replay strict %s: read header: %w", path, err)
		}
		if n == 0 {
			return validBytes, nil // clean end
		}
		if n < frameHeaderLen {
			return validBytes, nil // short header at the tail: torn
		}
		payloadLen := binary.LittleEndian.Uint32(header[0:4])
		wantCRC := binary.LittleEndian.Uint32(header[4:8])

		remaining := fileSize - validBytes - frameHeaderLen
		if payloadLen == 0 {
			// Torn only if everything after the header is zero.
			rest := make([]byte, remaining)
			if remaining > 0 {
				if _, err := f.ReadAt(rest, validBytes+frameHeaderLen); err != nil && err != io.EOF {
					return validBytes, fmt.Errorf("wal: replay strict %s: read tail: %w", path, err)
				}
			}
			allZero := true
			for _, b := range rest {
				if b != 0 {
					allZero = false
					break
				}
			}
			if allZero {
				return validBytes, nil
			}
			return validBytes, fmt.Errorf("wal: replay strict %s at offset %d: zero-length frame with non-zero tail: %w",
				path, validBytes, base.ErrCorruption)
		}
		if payloadLen > MaxPayloadSize || int64(payloadLen) > remaining {
			// The declared length is past EOF. The frame header is not CRC
			// covered, so a flipped length byte in a non-final frame also
			// lands here: scan forward for any offset that parses as a
			// complete frame with a valid CRC. Found => corruption, else a
			// real torn tail (M-012 item 2).
			if hasValidFrameAfter(f, validBytes+1, fileSize) {
				return validBytes, fmt.Errorf("wal: replay strict %s at offset %d: declared length past EOF but valid frame follows: %w",
					path, validBytes, base.ErrCorruption)
			}
			return validBytes, nil // torn tail
		}
		if int64(cap(payload)) < int64(payloadLen) {
			payload = make([]byte, payloadLen)
		} else {
			payload = payload[:payloadLen]
		}
		if _, err := f.ReadAt(payload, validBytes+frameHeaderLen); err != nil {
			return validBytes, fmt.Errorf("wal: replay strict %s: read payload: %w", path, err)
		}
		if got := base.CRC(payload); got != wantCRC {
			frameEnd := validBytes + frameHeaderLen + int64(payloadLen)
			if frameEnd == fileSize {
				return validBytes, nil // final frame torn
			}
			return validBytes, fmt.Errorf("wal: replay strict %s at offset %d: crc mismatch with data after the frame: %w",
				path, validBytes, base.ErrCorruption)
		}
		if err := fn(payload); err != nil {
			return validBytes, fmt.Errorf("wal: replay strict %s: callback: %w", path, err)
		}
		validBytes += frameHeaderLen + int64(payloadLen)
	}
}

// hasValidFrameAfter scans the tail from `from` for any offset that parses
// as a complete frame with a non-zero in-bounds length and a valid CRC.
// Used by ReplayStrict to distinguish a real torn tail from a corrupted
// length field hiding live frames.
func hasValidFrameAfter(f vfs.File, from int64, fileSize int64) bool {
	if from >= fileSize {
		return false
	}
	tail := make([]byte, fileSize-from)
	if _, err := f.ReadAt(tail, from); err != nil && err != io.EOF {
		return false
	}
	for off := 0; off+frameHeaderLen <= len(tail); off++ {
		pl := binary.LittleEndian.Uint32(tail[off : off+4])
		wantCRC := binary.LittleEndian.Uint32(tail[off+4 : off+8])
		if pl == 0 || pl > MaxPayloadSize {
			continue
		}
		if off+frameHeaderLen+int(pl) > len(tail) {
			continue
		}
		payload := tail[off+frameHeaderLen : off+frameHeaderLen+int(pl)]
		if base.CRC(payload) == wantCRC {
			return true
		}
	}
	return false
}

// AppendFrame writes a pre-built frame (from Frame) and accounts its size.
func (w *Writer) AppendFrame(frame []byte) error {
	if _, err := w.f.Write(frame); err != nil {
		return fmt.Errorf("wal: append %s: %w", w.path, err)
	}
	w.size += int64(len(frame))
	return nil
}
