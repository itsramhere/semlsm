package checkpoint

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/itsramhere/semlsm/internal/base"
	"github.com/itsramhere/semlsm/internal/testutil"
)

func TestRewriteTupleRetentionPreservesSections(t *testing.T) {
	fs := testutil.NewMemFS(t)
	w, err := NewWriter(fs, "old.ckpt")
	if err != nil {
		t.Fatal(err)
	}
	meta := TupleMeta{ID: 1, Group: "g/", TsFrom: 1000, TsTo: 1200, SourceCount: 200, SourceCRC32: 42, PrevTupleID: 7, ArchiveFileNum: 9}
	entries := mkEntries(20)
	if err := w.AddTuple(meta, []byte(`{ "summary": "keep these exact bytes" }`), entries); err != nil {
		t.Fatal(err)
	}
	if err := w.Finish(); err != nil {
		t.Fatal(err)
	}
	r, err := OpenReader(fs, "old.ckpt")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	original, err := r.tupleBytes(1)
	if err != nil {
		t.Fatal(err)
	}
	sections := func(raw []byte) ([]byte, []byte) {
		pos := 4 + int(binary.LittleEndian.Uint32(raw)) + 4
		end := pos + 4 + int(binary.LittleEndian.Uint32(raw[pos:]))
		return raw[pos:end], raw[end:]
	}
	origSum, origIndex := sections(original)
	for _, stub := range []bool{false, true} {
		path := fmt.Sprintf("new-%v.ckpt", stub)
		writer, err := NewWriter(fs, path)
		if err != nil {
			t.Fatal(err)
		}
		changed := meta
		changed.ArchiveFileNum = 0
		changed.ArchiveRetiredUnixNano = 10000
		if stub {
			changed.RetiredUnixNano = 20000
		}
		if _, err := writer.RewriteTuple(r, 1, changed, stub); err != nil {
			t.Fatal(err)
		}
		if err := writer.Finish(); err != nil {
			t.Fatal(err)
		}
		out, err := OpenReader(fs, path)
		if err != nil {
			t.Fatal(err)
		}
		raw, err := out.tupleBytes(1)
		if err != nil {
			t.Fatal(err)
		}
		sum, index := sections(raw)
		if !bytes.Equal(origIndex, index) || !stub && !bytes.Equal(origSum, sum) {
			t.Fatal("encoded section changed")
		}
		got, err := out.ReadMeta(1)
		if err != nil || got != changed {
			t.Fatalf("metadata: %+v %v", got, err)
		}
		keys, err := out.IterKeys(1)
		if err != nil || !reflect.DeepEqual(keys, entries) {
			t.Fatalf("stub lost key ranges: %v", err)
		}
		payload, err := out.ReadSummary(1)
		if err != nil || stub && len(payload) != 0 {
			t.Fatalf("stub payload remains: %q %v", payload, err)
		}
		out.Close()
	}
}

func TestCopyTuplePreservesBytesAndChecksCRCs(t *testing.T) {
	fs := testutil.NewMemFS(t)
	w, err := NewWriter(fs, "input.ckpt")
	if err != nil {
		t.Fatal(err)
	}
	meta := TupleMeta{ID: 7, Group: "g/", TsFrom: 1000, TsTo: 2000, PrevTupleID: 3, ArchiveFileNum: 50}
	if err := w.AddTuple(meta, []byte(`{ "summary" : "original whitespace" }`), mkEntries(20)); err != nil {
		t.Fatal(err)
	}
	if err := w.Finish(); err != nil {
		t.Fatal(err)
	}
	r, err := OpenReader(fs, "input.ckpt")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	raw, err := r.tupleBytes(7)
	if err != nil {
		t.Fatal(err)
	}
	out, err := NewWriter(fs, "output.ckpt")
	if err != nil {
		t.Fatal(err)
	}
	// Put a tuple before the copy so its directory offset really changes.
	if err := out.AddTuple(TupleMeta{ID: 6}, []byte(`{}`), mkEntries(1)); err != nil {
		t.Fatal(err)
	}
	got, loc, err := out.CopyTuple(r, 7)
	if err != nil || got != meta || loc.Offset == w.DirEntries()[0].Offset {
		t.Fatalf("copy: %+v %+v %v", got, loc, err)
	}
	if err := out.Finish(); err != nil {
		t.Fatal(err)
	}
	r2, err := OpenReader(fs, "output.ckpt")
	if err != nil {
		t.Fatal(err)
	}
	copyRaw, err := r2.tupleBytes(7)
	r2.Close()
	if err != nil || !bytes.Equal(raw, copyRaw) {
		t.Fatal("copy re-encoded the tuple")
	}
	metaLen := int(binary.LittleEndian.Uint32(raw))
	summaryStart := 4 + metaLen + 4 + 4
	summaryLen := int(binary.LittleEndian.Uint32(raw[summaryStart-4:]))
	indexStart := summaryStart + summaryLen + 4
	for _, offset := range []int{4, summaryStart + 1, indexStart + 1} {
		// ReadMeta fills the lazy cache first; CopyTuple must still freshly
		// read and verify the file after it is damaged.
		if _, err := r.ReadMeta(7); err != nil {
			t.Fatal(err)
		}
		bad := append([]byte(nil), raw...)
		bad[offset] ^= 1
		f, err := fs.OpenReadWrite("input.ckpt")
		if err != nil {
			t.Fatal(err)
		}
		// memFile has no Seek: overwrite a prefix containing the raw tuple.
		prefix := append(append([]byte(nil), magic...), 1, 0, 0, 0)
		f.Write(append(prefix, bad...))
		f.Close()
		writer, err := NewWriter(fs, "bad-copy.ckpt")
		if err != nil {
			t.Fatal(err)
		}
		_, _, err = writer.CopyTuple(r, 7)
		writer.Abort()
		if !errors.Is(err, base.ErrCorruption) {
			t.Fatalf("damage at %d copied: %v", offset, err)
		}
		f, _ = fs.OpenReadWrite("input.ckpt")
		f.Write(append(prefix, raw...))
		f.Close()
	}
}

func mkEntries(n int) []KeyEntry {
	entries := make([]KeyEntry, 0, n)
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("agent/42/key%06d", i)
		minTs := uint64(1000 + i*10)
		entries = append(entries, KeyEntry{
			Key:      []byte(k),
			MinTs:    minTs,
			MaxTs:    minTs + 9,
			Count:    10,
			LastKind: base.KindPut,
		})
	}
	return entries
}

func TestCkptRoundTrip(t *testing.T) {
	fs := testutil.NewMemFS(t)
	w, err := NewWriter(fs, "db/000001.ckpt")
	if err != nil {
		t.Fatal(err)
	}
	meta := TupleMeta{
		ID: 7, Group: "agent/42/", TsFrom: 1000, TsTo: 1000 + 50*10 - 1,
		SourceCount: 500, SourceBytes: 12345, SourceCRC32: 0xDEADBEEF,
		PutCount: 480, DeleteCount: 20, KeyCount: 50,
		FromLevel: 2, ToLevel: 3, Model: "mock", PromptVersion: 1,
		PrevTupleID: 3, ArchiveFileNum: 0,
	}
	summary := []byte(`{"summary":"all state","key_facts":[]}`)
	if err := w.AddTuple(meta, summary, mkEntries(50)); err != nil {
		t.Fatal(err)
	}
	meta2 := meta
	meta2.ID = 8
	meta2.PrevTupleID = 7
	if err := w.AddTuple(meta2, []byte(`{"summary":"second"}`), mkEntries(10)); err != nil {
		t.Fatal(err)
	}
	if err := w.Finish(); err != nil {
		t.Fatal(err)
	}

	r, err := OpenReader(fs, "db/000001.ckpt")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	tups := r.Tuples()
	if len(tups) != 2 || tups[0].ID != 7 || tups[1].ID != 8 {
		t.Fatalf("tuples: %+v", tups)
	}
	got, err := r.ReadMeta(7)
	if err != nil {
		t.Fatal(err)
	}
	if got.Group != "agent/42/" || got.SourceCount != 500 || got.SourceCRC32 != 0xDEADBEEF || got.PrevTupleID != 3 {
		t.Fatalf("meta: %+v", got)
	}
	sum, err := r.ReadSummary(7)
	if err != nil || string(sum) != string(summary) {
		t.Fatalf("summary: %q err=%v", sum, err)
	}
	// lookups
	e, ok, err := r.LookupKey(7, []byte("agent/42/key000025"))
	if err != nil || !ok {
		t.Fatalf("lookup: ok=%v err=%v", ok, err)
	}
	if e.MinTs != 1000+250 || e.MaxTs != 1000+259 || e.Count != 10 {
		t.Fatalf("entry: %+v", e)
	}
	if _, ok, _ := r.LookupKey(7, []byte("agent/42/key000999")); ok {
		t.Fatal("absent key found")
	}
	if _, ok, _ := r.LookupKey(7, []byte("other/key")); ok {
		t.Fatal("wrong key found")
	}
	// iteration
	keys, err := r.IterKeys(7)
	if err != nil || len(keys) != 50 {
		t.Fatalf("iter: %d err=%v", len(keys), err)
	}
	for i, e := range keys {
		want := fmt.Sprintf("agent/42/key%06d", i)
		if string(e.Key) != want {
			t.Fatalf("iter %d: %q != %q", i, e.Key, want)
		}
	}
}

func TestCkptFiftyThousandKeys(t *testing.T) {
	fs := testutil.NewMemFS(t)
	w, _ := NewWriter(fs, "big.ckpt")
	entries := mkEntries(50000)
	meta := TupleMeta{ID: 1, Group: "g/", TsFrom: 1000, TsTo: 600000, SourceCount: 500000}
	if err := w.AddTuple(meta, []byte(`{}`), entries); err != nil {
		t.Fatal(err)
	}
	if err := w.Finish(); err != nil {
		t.Fatal(err)
	}
	r, err := OpenReader(fs, "big.ckpt")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	// spot-check lookups across the whole space
	for _, i := range []int{0, 1, 31, 32, 33, 1000, 25000, 49999} {
		k := fmt.Sprintf("agent/42/key%06d", i)
		e, ok, err := r.LookupKey(1, []byte(k))
		if err != nil || !ok {
			t.Fatalf("lookup %s: ok=%v err=%v", k, ok, err)
		}
		if e.MinTs != uint64(1000+i*10) {
			t.Fatalf("%s: minTs=%d", k, e.MinTs)
		}
	}
	if _, ok, _ := r.LookupKey(1, []byte("agent/42/key030000")); !ok {
		t.Fatal("middle key must be found")
	}
}

func TestCkptSingleKeyAndEmptySummary(t *testing.T) {
	fs := testutil.NewMemFS(t)
	w, _ := NewWriter(fs, "one.ckpt")
	meta := TupleMeta{ID: 1, Group: "x"}
	if err := w.AddTuple(meta, nil, []KeyEntry{{Key: []byte("only"), MinTs: 1, MaxTs: 2, Count: 2, LastKind: base.KindDelete}}); err != nil {
		t.Fatal(err)
	}
	if err := w.Finish(); err != nil {
		t.Fatal(err)
	}
	r, err := OpenReader(fs, "one.ckpt")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	e, ok, err := r.LookupKey(1, []byte("only"))
	if err != nil || !ok || e.LastKind != base.KindDelete || e.MaxTs != 2 {
		t.Fatalf("entry: %+v ok=%v err=%v", e, ok, err)
	}
	sum, err := r.ReadSummary(1)
	if err != nil || len(sum) != 0 {
		t.Fatalf("empty summary: %q err=%v", sum, err)
	}
}

func TestCkptCorruption(t *testing.T) {
	fs := testutil.NewMemFS(t)
	w, _ := NewWriter(fs, "c.ckpt")
	meta := TupleMeta{ID: 1, Group: "g/"}
	if err := w.AddTuple(meta, []byte(`{}`), mkEntries(100)); err != nil {
		t.Fatal(err)
	}
	if err := w.Finish(); err != nil {
		t.Fatal(err)
	}
	f, _ := fs.Open("c.ckpt")
	st, _ := f.Stat()
	size := st.Size
	f.Close()
	buf := make([]byte, size)
	{
		r2, _ := fs.Open("c.ckpt")
		r2.ReadAt(buf, 0)
		r2.Close()
	}

	// flip bytes at sampled offsets: either correct data or ErrCorruption
	samples := []int64{0, 5, 30, 100, size / 3, size / 2, size - 30, size - 20, size - 5}
	for _, off := range samples {
		corrupt := append([]byte(nil), buf...)
		corrupt[off] ^= 0x01
		cfs := testutil.NewMemFS(t)
		fc, _ := cfs.Create("c.ckpt")
		fc.Write(corrupt)
		fc.Close()
		r, err := OpenReader(cfs, "c.ckpt")
		if err != nil {
			continue
		}
		// reads must be correct or corrupted, never wrong
		e, ok, err := r.LookupKey(1, []byte("agent/42/key000050"))
		r.Close()
		if err != nil {
			continue
		}
		if ok && (e.MinTs != 1500) {
			t.Fatalf("flip at %d: wrong data: %+v", off, e)
		}
		_ = e
	}
}

func TestCkptPrefixCompression(t *testing.T) {
	// shared-prefix keys must survive compression and round trip
	fs := testutil.NewMemFS(t)
	w, _ := NewWriter(fs, "p.ckpt")
	entries := []KeyEntry{
		{Key: []byte("a/1/x"), MinTs: 1, MaxTs: 1, Count: 1, LastKind: base.KindPut},
		{Key: []byte("a/1/y"), MinTs: 2, MaxTs: 2, Count: 1, LastKind: base.KindDelete},
		{Key: []byte("a/1/z"), MinTs: 3, MaxTs: 3, Count: 1, LastKind: base.KindPut},
		{Key: []byte("a/10"), MinTs: 4, MaxTs: 4, Count: 1, LastKind: base.KindPut},
		{Key: []byte("b"), MinTs: 5, MaxTs: 5, Count: 1, LastKind: base.KindPut},
	}
	if err := w.AddTuple(TupleMeta{ID: 1}, []byte(`{}`), entries); err != nil {
		t.Fatal(err)
	}
	if err := w.Finish(); err != nil {
		t.Fatal(err)
	}
	r, err := OpenReader(fs, "p.ckpt")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	keys, err := r.IterKeys(1)
	if err != nil || len(keys) != 5 {
		t.Fatalf("iter: %d err=%v", len(keys), err)
	}
	for i := range entries {
		if string(keys[i].Key) != string(entries[i].Key) {
			t.Fatalf("iter %d: %q != %q", i, keys[i].Key, entries[i].Key)
		}
		if keys[i].MinTs != entries[i].MinTs || keys[i].LastKind != entries[i].LastKind {
			t.Fatalf("iter %d: %+v != %+v", i, keys[i], entries[i])
		}
	}
	for i := range entries {
		if _, ok, _ := r.LookupKey(1, entries[i].Key); !ok {
			t.Fatalf("lookup %q failed", entries[i].Key)
		}
	}
}
