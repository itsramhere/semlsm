package base

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"
	"time"
)

func ikey(user string, ts uint64, k Kind) []byte {
	return MakeInternalKey([]byte(user), ts, k)
}

func TestCompareOrdering(t *testing.T) {
	tests := []struct {
		a, b []byte
		want int // expected sign of Compare(a, b)
		desc string
	}{
		// same key, different ts: larger ts first
		{ikey("k", 100, KindPut), ikey("k", 90, KindPut), -1, "newer ts first"},
		{ikey("k", 90, KindPut), ikey("k", 100, KindPut), 1, "older ts second"},
		// same key same ts: larger kind first
		{ikey("k", 100, KindPut), ikey("k", 100, KindDelete), -1, "put before delete at same ts"},
		// same ts, prefix keys: byte order on user key
		{ikey("a", 5, KindPut), ikey("ab", 5, KindPut), -1, "prefix key a before ab"},
		{ikey("ab", 5, KindPut), ikey("a", 500, KindPut), 1, "user key order dominates ts"},
		// keys containing 0x00: plain byte compare
		{ikey("a\x00", 5, KindPut), ikey("a", 5, KindPut), 1, "0x00 extends key"},
		{ikey("a\x00b", 5, KindPut), ikey("a\x00c", 5, KindPut), -1, "byte compare after 0x00"},
		// identical keys equal
		{ikey("k", 7, KindDelete), ikey("k", 7, KindDelete), 0, "equal keys"},
		// seek key position: (K,S,0xFF) sorts before every (K,S,kind) since
		// 0xFF is larger than any real kind, and after every ts > S entry.
		{SeekKey([]byte("k"), 100), ikey("k", 101, KindPut), 1, "seek key after newer ts"},
		{SeekKey([]byte("k"), 100), ikey("k", 100, KindPut), -1, "seek key before same-ts put"},
		{SeekKey([]byte("k"), 100), ikey("k", 99, KindPut), -1, "seek key before older ts"},
	}
	for _, tt := range tests {
		got := Compare(tt.a, tt.b)
		if sign(got) != tt.want {
			t.Errorf("%s: Compare(a,b)=%d want %d", tt.desc, got, tt.want)
		}
		if sign(Compare(tt.b, tt.a)) != -tt.want {
			t.Errorf("%s: antisymmetry violated: %d", tt.desc, Compare(tt.b, tt.a))
		}
	}
}

func sign(x int) int {
	switch {
	case x < 0:
		return -1
	case x > 0:
		return 1
	default:
		return 0
	}
}

func TestCompareTransitivity(t *testing.T) {
	keys := []string{"", "a", "a\x00", "ab", "b"}
	kinds := []Kind{KindDelete, KindPut}
	var all [][]byte
	for _, k := range keys {
		for _, ts := range []uint64{1, 5, 5, 9} {
			for _, kd := range kinds {
				all = append(all, ikey(k, ts, kd))
			}
		}
	}
	for i := range all {
		for j := range all {
			for m := range all {
				ab, bc, ac := Compare(all[i], all[j]), Compare(all[j], all[m]), Compare(all[i], all[m])
				if ab <= 0 && bc <= 0 && ac > 0 {
					t.Fatalf("transitivity violated: cmp(i,j)=%d cmp(j,m)=%d cmp(i,m)=%d (i=%d j=%d m=%d)", ab, bc, ac, i, j, m)
				}
			}
		}
	}
}

func TestInternalKeyRoundTrip(t *testing.T) {
	tests := []struct {
		user string
		ts   uint64
		k    Kind
	}{
		{"", 0, KindPut},                       // empty user key is representable internally
		{"simple", 1, KindPut},                 //
		{"with\x00nulls", 1 << 63, KindDelete}, //
		{"max", ^uint64(0), KindPut},           //
	}
	for _, tt := range tests {
		ik := MakeInternalKey([]byte(tt.user), tt.ts, tt.k)
		u, ts, k, ok := SplitInternalKey(ik)
		if !ok {
			t.Fatalf("%q: split failed", tt.user)
		}
		if !bytes.Equal(u, []byte(tt.user)) || ts != tt.ts || k != tt.k {
			t.Errorf("%q: got (%q,%d,%v)", tt.user, u, ts, k)
		}
	}
	// too-short keys are not ok
	for n := 0; n < internalTrailerLen; n++ {
		if _, _, _, ok := SplitInternalKey(make([]byte, n)); ok {
			t.Errorf("len %d: expected !ok", n)
		}
	}
}

func TestSeekKeyShape(t *testing.T) {
	sk := SeekKey([]byte("k"), 42)
	if len(sk) != len("k")+internalTrailerLen {
		t.Fatalf("len=%d", len(sk))
	}
	if sk[len(sk)-1] != 0xFF {
		t.Fatalf("seek kind byte = %#x", sk[len(sk)-1])
	}
	// The newest version <= S is the first entry at or after SeekKey(user, S).
	entries := []struct {
		ts   uint64
		kind Kind
	}{
		{50, KindPut}, {40, KindPut}, {30, KindDelete},
	}
	target := SeekKey([]byte("k"), 40)
	found := false
	for _, e := range entries {
		if Compare(ikey("k", e.ts, e.kind), target) >= 0 && !found {
			if e.ts != 40 {
				t.Fatalf("seek landed on ts=%d, want 40", e.ts)
			}
			found = true
		}
	}
	if !found {
		t.Fatal("seek found no entry")
	}
}

func TestGroupOf(t *testing.T) {
	tests := []struct {
		key   string
		depth int
		delim byte
		want  string
	}{
		{"agent/42/events/9", 2, '/', "agent/42/"}, // B4 example
		{"agent/42", 2, '/', "agent/42"},           // B4 example: fewer delimiters -> whole key
		{"x", 2, '/', "x"},                         // B4 example
		{"a/b/c/d", 1, '/', "a/"},
		{"a/b/c/d/", 4, '/', "a/b/c/d/"}, // depth == occurrences
		{"a/b/c/d", 4, '/', "a/b/c/d"},   // depth > occurrences -> whole key
		{"", 2, '/', ""},
		{"a//b", 2, '/', "a//"},
		{"a:b:c", 2, ':', "a:b:"},
		{"abc", 2, '/', "abc"}, // no delimiter at all
		{"abc", 0, '/', "abc"}, // depth 0 disables grouping
	}
	for _, tt := range tests {
		got := GroupOf([]byte(tt.key), tt.depth, tt.delim)
		if string(got) != tt.want {
			t.Errorf("GroupOf(%q,%d,%q)=%q want %q", tt.key, tt.depth, tt.delim, got, tt.want)
		}
	}
}

func TestGroupContiguity(t *testing.T) {
	// Keys sharing a group prefix must be contiguous in Compare order.
	// Verify on a sample: every key with group "g/" lies between the smallest
	// and largest such key when only prefixed keys exist in the set.
	keys := []string{"a/1", "a/2/x", "b/1", "g/1", "g/1/x", "g/10", "g/2", "h/1"}
	for _, k := range keys {
		if got := string(GroupOf([]byte(k), 1, '/')); len(k) < len(got) || !hasPrefix(k, got) {
			t.Errorf("group %q not a prefix of key %q", got, k)
		}
	}
}

func hasPrefix(s, p string) bool { return len(s) >= len(p) && s[:len(p)] == p }

func TestKindString(t *testing.T) {
	if KindPut.String() != "PUT" || KindDelete.String() != "DELETE" {
		t.Fatal("kind strings wrong")
	}
	if Kind(9).String() != "UNKNOWN" {
		t.Fatal("unknown kind string wrong")
	}
}

func TestValidateKV(t *testing.T) {
	if err := ValidateKV(nil, nil); !errors.Is(err, ErrKeyEmpty) {
		t.Errorf("empty key: %v", err)
	}
	big := make([]byte, MaxKeyBytes+1)
	if err := ValidateKV(big, nil); !errors.Is(err, ErrKeyTooLarge) {
		t.Errorf("big key: %v", err)
	}
	if err := ValidateKV([]byte("k"), make([]byte, MaxValueBytes+1)); !errors.Is(err, ErrValueTooLarge) {
		t.Errorf("big value: %v", err)
	}
	if err := ValidateKV([]byte("k"), []byte("v")); err != nil {
		t.Errorf("valid: %v", err)
	}
}

func TestSourceCRC(t *testing.T) {
	recs := []Record{
		{Key: []byte("k1"), Ts: 10, Kind: KindPut, Value: []byte("v1")},
		{Key: []byte("k2"), Ts: 5, Kind: KindDelete, Value: nil},
		{Key: []byte("k0"), Ts: 10, Kind: KindPut, Value: []byte("v0")},
	}
	c1 := SourceCRC(recs)
	// order independence
	shuffled := []Record{recs[2], recs[0], recs[1]}
	if SourceCRC(shuffled) != c1 {
		t.Fatal("SourceCRC not order-independent")
	}
	// sensitivity to every field
	mut := func(f func(r *Record)) uint32 {
		cp := append([]Record(nil), recs...)
		f(&cp[0])
		return SourceCRC(cp)
	}
	if mut(func(r *Record) { r.Ts = 11 }) == c1 {
		t.Fatal("ts not covered")
	}
	if mut(func(r *Record) { r.Value = []byte("v2") }) == c1 {
		t.Fatal("value not covered")
	}
	if mut(func(r *Record) { r.Kind = KindDelete }) == c1 {
		t.Fatal("kind not covered")
	}
	if mut(func(r *Record) { r.Key = []byte("k9") }) == c1 {
		t.Fatal("key not covered")
	}
	// (ts asc, key asc) tie ordering is covered: swapping equal-ts keys changes the CRC
	if mut(func(r *Record) { r.Key = []byte("k2") }) == c1 {
		t.Fatal("key order at equal ts not covered")
	}
	// deterministic
	if SourceCRC(recs) != c1 {
		t.Fatal("nondeterministic")
	}
	// known-vector style check: matches single-shot concatenation
	var concat []byte
	sorted := append([]Record(nil), recs...)
	sortRecords(sorted)
	for _, r := range sorted {
		concat = append(concat, uvarintBytes(len(r.Key))...)
		concat = append(concat, r.Key...)
		var tsBE [8]byte
		concat = append(concat, beBytes(tsBE[:], r.Ts)...)
		concat = append(concat, byte(r.Kind))
		concat = append(concat, uvarintBytes(len(r.Value))...)
		concat = append(concat, r.Value...)
	}
	if CRC(concat) != c1 {
		t.Fatal("incremental != one-shot")
	}
}

func TestManualClock(t *testing.T) {
	base := time.Unix(1700000000, 0)
	c := NewManualClock(base)
	if !c.Now().Equal(base) {
		t.Fatal("initial time wrong")
	}
	c.Advance(3 * time.Second)
	if c.Now().Sub(base) != 3*time.Second {
		t.Fatal("advance wrong")
	}
	c.Set(base)
	if !c.Now().Equal(base) {
		t.Fatal("set wrong")
	}
}

func beBytes(dst []byte, v uint64) []byte {
	dst[0] = byte(v >> 56)
	dst[1] = byte(v >> 48)
	dst[2] = byte(v >> 40)
	dst[3] = byte(v >> 32)
	dst[4] = byte(v >> 24)
	dst[5] = byte(v >> 16)
	dst[6] = byte(v >> 8)
	dst[7] = byte(v)
	return dst
}

func uvarintBytes(v int) []byte {
	var scratch [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(scratch[:], uint64(v))
	return append([]byte(nil), scratch[:n]...)
}

func sortRecords(rs []Record) {
	for i := 1; i < len(rs); i++ {
		for j := i; j > 0 && recLess(rs[j], rs[j-1]); j-- {
			rs[j], rs[j-1] = rs[j-1], rs[j]
		}
	}
}

func recLess(a, b Record) bool {
	if a.Ts != b.Ts {
		return a.Ts < b.Ts
	}
	return string(a.Key) < string(b.Key)
}

func TestCRCknown(t *testing.T) {
	// CRC-32C of "123456789" is 0xE3069283 (standard check value).
	if got := CRC([]byte("123456789")); got != 0xE3069283 {
		t.Fatalf("CRC = %08x, want e3069283", got)
	}
}
