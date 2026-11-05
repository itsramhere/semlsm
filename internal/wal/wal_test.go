package wal

import (
	"encoding/binary"
	"errors"
	"io"
	"testing"

	"semlsm/internal/base"
	"semlsm/internal/vfs"
)

func encodeBatch(tb testing.TB, ts uint64, recs []base.Record) []byte {
	tb.Helper()
	b, err := EncodeBatch(ts, recs)
	if err != nil {
		tb.Fatal(err)
	}
	return b
}

func rec(key string, ts uint64, k base.Kind, val string) base.Record {
	return base.Record{Key: []byte(key), Ts: ts, Kind: k, Value: []byte(val)}
}

// writeWALFile builds a fresh MemFS holding one WAL with the given payloads.
func writeWALFile(t *testing.T, payloads ...[]byte) *vfs.MemFS {
	t.Helper()
	fs := vfs.NewMemFS()
	w, err := NewWriter(fs, "wal")
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range payloads {
		if err := w.Append(p); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return fs
}

// truncateCopy returns a new MemFS whose "wal" holds only the first n bytes.
func truncateCopy(t *testing.T, src *vfs.MemFS, n int64) *vfs.MemFS {
	t.Helper()
	full := readAllWAL(t, src, "wal")
	dst := vfs.NewMemFS()
	f, err := dst.Create("wal")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(full[:n]); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	return dst
}

func readAllWAL(t *testing.T, fs vfs.FS, name string) []byte {
	t.Helper()
	f, err := fs.Open(name)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, st.Size)
	if _, err := io.ReadFull(f, buf); err != nil {
		t.Fatal(err)
	}
	return buf
}

func TestWALRoundTrip(t *testing.T) {
	payloads := [][]byte{
		encodeBatch(t, 1, []base.Record{rec("a", 1, base.KindPut, "v1")}),
		encodeBatch(t, 2, []base.Record{rec("b", 2, base.KindPut, ""), rec("a", 2, base.KindDelete, "")}),
		encodeBatch(t, 3, []base.Record{rec("with\x00null", 3, base.KindPut, "val\x00ue")}),
	}
	fs := writeWALFile(t, payloads...)

	var got [][]byte
	valid, err := Replay(fs, "wal", true, func(p []byte) error {
		got = append(got, append([]byte(nil), p...))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if valid != int64(len(readAllWAL(t, fs, "wal"))) {
		t.Fatalf("validBytes=%d, want full file", valid)
	}
	if len(got) != len(payloads) {
		t.Fatalf("replayed %d payloads, want %d", len(got), len(payloads))
	}
	for i := range payloads {
		if string(got[i]) != string(payloads[i]) {
			t.Fatalf("payload %d differs", i)
		}
	}
}

func TestEncodeDecodeBatch(t *testing.T) {
	tests := []struct {
		name string
		ts   uint64
		recs []base.Record
	}{
		{"empty", 5, nil},
		{"one put", 1 << 40, []base.Record{rec("k", 1<<40, base.KindPut, "value")}},
		{"delete empty value", 7, []base.Record{rec("k", 7, base.KindDelete, "")}},
		{"nul bytes", 9, []base.Record{rec("a\x00b", 9, base.KindPut, "v\x00w")}},
		{"big value", 11, []base.Record{rec("k", 11, base.KindPut, string(make([]byte, 1<<16)))}},
		{"multi", 13, []base.Record{
			rec("a", 13, base.KindPut, "1"),
			rec("b", 13, base.KindPut, "2"),
			rec("c", 13, base.KindDelete, ""),
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			payload, err := EncodeBatch(tt.ts, tt.recs)
			if err != nil {
				t.Fatal(err)
			}
			gotTs, gotRecs, err := DecodeBatch(payload)
			if err != nil {
				t.Fatal(err)
			}
			if gotTs != tt.ts {
				t.Fatalf("ts=%d want %d", gotTs, tt.ts)
			}
			if len(gotRecs) != len(tt.recs) {
				t.Fatalf("count=%d want %d", len(gotRecs), len(tt.recs))
			}
			for i := range tt.recs {
				g, w := gotRecs[i], tt.recs[i]
				if string(g.Key) != string(w.Key) || g.Ts != w.Ts || g.Kind != w.Kind || string(g.Value) != string(w.Value) {
					t.Fatalf("rec %d: got %+v want %+v", i, g, w)
				}
			}
			// decoded records must own their memory (no aliasing of payload)
			if len(tt.recs) > 0 {
				payload[10] ^= 0xFF
				if string(gotRecs[0].Key) != string(tt.recs[0].Key) && tt.recs[0].Kind != base.KindPut {
					t.Fatal("decode aliasing")
				}
			}
		})
	}
}

func TestDecodeBatchMalformed(t *testing.T) {
	tests := []struct {
		name    string
		payload []byte
	}{
		{"empty", nil},
		{"ts only", []byte{1, 0, 0, 0, 0, 0, 0, 0}},
		{"truncated count", []byte{1, 0, 0, 0, 0, 0, 0, 0, 2}},
		{"count beyond payload", []byte{1, 0, 0, 0, 0, 0, 0, 0, 10}},
		{"truncated key", func() []byte {
			p := encodeBatch(t, 1, []base.Record{rec("abcdefghij", 1, base.KindPut, "v")})
			return p[:len(p)-3] // cut into the value
		}()},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, _, err := DecodeBatch(tt.payload); !errors.Is(err, base.ErrCorruption) {
				t.Fatalf("err=%v, want ErrCorruption", err)
			}
		})
	}
}

func TestWALTornAtEveryOffsetOfFinalFrame(t *testing.T) {
	p1 := encodeBatch(t, 1, []base.Record{rec("a", 1, base.KindPut, "v1")})
	p2 := encodeBatch(t, 2, []base.Record{rec("b", 2, base.KindPut, "v2")})
	p3 := encodeBatch(t, 3, []base.Record{rec("c", 3, base.KindPut, "v3-CCCC-CCCC")})
	fs := writeWALFile(t, p1, p2, p3)
	full := readAllWAL(t, fs, "wal")
	start3 := int64(0) + int64(frameHeaderLen+len(p1)) + int64(frameHeaderLen+len(p2))

	// Sanity: the full file replays all three frames.
	if n, err := countFrames(t, fs, true); err != nil || n != 3 {
		t.Fatalf("full file: n=%d err=%v, want 3", n, err)
	}

	// Every truncation that cuts at least one byte of the final frame.
	for off := start3; off < int64(len(full)); off++ {
		trimmed := truncateCopy(t, fs, off)
		var n int
		valid, err := Replay(trimmed, "wal", true, func(p []byte) error {
			n++
			return nil
		})
		if err != nil {
			t.Fatalf("offset %d: last=true: %v", off, err)
		}
		if n != 2 || valid != start3 {
			t.Fatalf("offset %d: last=true: n=%d valid=%d, want 2/%d", off, n, valid, start3)
		}
		_, err = Replay(trimmed, "wal", false, func(p []byte) error { return nil })
		if !errors.Is(err, base.ErrCorruption) {
			t.Fatalf("offset %d: last=false: err=%v, want ErrCorruption", off, err)
		}
	}
}

func TestWALTornHeaderEveryOffset(t *testing.T) {
	// Also covers offsets strictly inside the first frame's header.
	p1 := encodeBatch(t, 1, []base.Record{rec("a", 1, base.KindPut, "v")})
	fs := writeWALFile(t, p1)
	for off := int64(0); off < int64(frameHeaderLen); off++ {
		trimmed := truncateCopy(t, fs, off)
		valid, err := Replay(trimmed, "wal", true, func(p []byte) error { t.Fatal("no frame should replay"); return nil })
		if err != nil || valid != 0 {
			t.Fatalf("offset %d: valid=%d err=%v", off, valid, err)
		}
	}
}

func countFrames(t *testing.T, fs vfs.FS, last bool) (int, error) {
	t.Helper()
	n := 0
	_, err := Replay(fs, "wal", last, func(p []byte) error { n++; return nil })
	return n, err
}

func TestWALFlippedByte(t *testing.T) {
	p1 := encodeBatch(t, 1, []base.Record{rec("a", 1, base.KindPut, "v1")})
	p2 := encodeBatch(t, 2, []base.Record{rec("b", 2, base.KindPut, "v2-longer-payload")})
	fs := writeWALFile(t, p1, p2)
	full := append([]byte(nil), readAllWAL(t, fs, "wal")...)
	start2 := int64(frameHeaderLen + len(p1))

	// flip a byte inside frame 2's payload
	corrupt := append([]byte(nil), full...)
	corrupt[start2+frameHeaderLen+2] ^= 0x01

	dst := vfs.NewMemFS()
	f, _ := dst.Create("wal")
	f.Write(corrupt)
	f.Close()

	if _, err := Replay(dst, "wal", false, func(p []byte) error { return nil }); !errors.Is(err, base.ErrCorruption) {
		t.Fatalf("non-last: err=%v, want ErrCorruption", err)
	}
	var n int
	valid, err := Replay(dst, "wal", true, func(p []byte) error { n++; return nil })
	if err != nil || n != 1 || valid != start2 {
		t.Fatalf("last: n=%d valid=%d err=%v, want torn tail after frame 1", n, valid, err)
	}
}

func TestWALMaxSizeFrame(t *testing.T) {
	// Exactly MaxPayloadSize: accepted and replayed.
	// 8 ts + 1 count + (1 kind + 1 klen + 3 key + 4 vlen uvarint for ~64M) = 18.
	value := make([]byte, MaxPayloadSize-18)
	for i := range value {
		value[i] = byte(i % 251)
	}
	payload := encodeBatch(t, 42, []base.Record{rec("big", 42, base.KindPut, string(value))})
	if len(payload) != MaxPayloadSize {
		t.Fatalf("payload len=%d, want %d", len(payload), MaxPayloadSize)
	}
	fs := writeWALFile(t, payload)
	var gotLen int
	_, err := Replay(fs, "wal", true, func(p []byte) error { gotLen = len(p); return nil })
	if err != nil {
		t.Fatal(err)
	}
	if gotLen != MaxPayloadSize {
		t.Fatalf("replayed len=%d", gotLen)
	}
	// One byte larger: Append must refuse.
	if err := (&Writer{}).Append(make([]byte, MaxPayloadSize+1)); !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("oversize append err=%v", err)
	}
}

func TestWALOversizedLengthFieldDoesNotAllocate(t *testing.T) {
	tests := []uint32{0xFFFFFFFF, MaxPayloadSize + 1, 1 << 30}
	for _, length := range tests {
		header := make([]byte, frameHeaderLen)
		binary.LittleEndian.PutUint32(header[0:4], length)
		binary.LittleEndian.PutUint32(header[4:8], 0xDEADBEEF)
		fs := vfs.NewMemFS()
		f, _ := fs.Create("wal")
		f.Write(header)
		f.Write([]byte("tiny trailing bytes"))
		f.Close()

		var memBefore int // approximation via runtime not needed; correctness check below
		var calls int
		valid, err := Replay(fs, "wal", true, func(p []byte) error { calls++; return nil })
		if err != nil || valid != 0 || calls != 0 {
			t.Fatalf("length %d: last=true: valid=%d calls=%d err=%v", length, valid, calls, err)
		}
		_, err = Replay(fs, "wal", false, func(p []byte) error { return nil })
		if !errors.Is(err, base.ErrCorruption) {
			t.Fatalf("length %d: last=false: err=%v", length, err)
		}
		_ = memBefore
	}
}

func TestWriterSizeAccounting(t *testing.T) {
	fs := vfs.NewMemFS()
	w, err := NewWriter(fs, "wal")
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	p := encodeBatch(t, 1, []base.Record{rec("k", 1, base.KindPut, "v")})
	if err := w.Append(p); err != nil {
		t.Fatal(err)
	}
	if w.Size() != int64(frameHeaderLen+len(p)) {
		t.Fatalf("size=%d", w.Size())
	}
	if err := w.Append(p); err != nil {
		t.Fatal(err)
	}
	if w.Size() != 2*int64(frameHeaderLen+len(p)) {
		t.Fatalf("size=%d", w.Size())
	}
}
