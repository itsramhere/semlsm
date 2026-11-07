package archive

import (
	"strings"
	"testing"

	"semlsm/internal/base"
	"semlsm/internal/vfs"
)

func TestWriteVerifyAndLookup(t *testing.T) {
	fs := vfs.NewMemFS()
	var recs []base.Record
	for i := 0; i < 5; i++ {
		recs = append(recs, base.Record{
			Key:   []byte("k"),
			Ts:    uint64(100 + i),
			Kind:  base.KindPut,
			Value: []byte(strings.Repeat("v", 40)),
		})
	}
	crc := base.SourceCRC(recs)

	// A wrong expected CRC fails the read-back verification.
	if err := Write(fs, "arch", 7, recs, uint64(len(recs)), crc^0xFFFF); err == nil {
		t.Fatal("mismatched SourceCRC must fail verification")
	}

	// The correct one writes, verifies, and serves lookups.
	if err := Write(fs, "arch", 7, recs, uint64(len(recs)), crc); err != nil {
		t.Fatal(err)
	}
	r, err := Open(fs, Path("arch", 7))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	rec, ok, err := Lookup(r, []byte("k"), 103)
	if err != nil || !ok {
		t.Fatalf("lookup 103: ok=%v err=%v", ok, err)
	}
	if rec.Ts != 103 {
		t.Fatalf("lookup 103 got ts %d", rec.Ts)
	}
	rec, ok, err = Lookup(r, []byte("k"), 99)
	if err != nil || ok {
		t.Fatalf("lookup below the run must miss: ok=%v err=%v", ok, err)
	}
}
