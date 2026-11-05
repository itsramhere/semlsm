package bloom

import (
	"fmt"
	"testing"
)

func TestBloomNoFalseNegatives(t *testing.T) {
	const n = 100000
	b := New(n)
	for i := 0; i < n; i++ {
		b.Add([]byte(fmt.Sprintf("key-%06d", i)))
	}
	for i := 0; i < n; i++ {
		if !b.MayContain([]byte(fmt.Sprintf("key-%06d", i))) {
			t.Fatalf("false negative for key-%06d", i)
		}
	}
}

func TestBloomFalsePositiveRate(t *testing.T) {
	const n = 100000
	b := New(n)
	for i := 0; i < n; i++ {
		b.Add([]byte(fmt.Sprintf("present-%06d", i)))
	}
	fp := 0
	const probes = 100000
	for i := 0; i < probes; i++ {
		if b.MayContain([]byte(fmt.Sprintf("absent-%06d", i))) {
			fp++
		}
	}
	rate := float64(fp) / probes
	if rate >= 0.02 {
		t.Fatalf("false positive rate %.4f, want < 0.02 (fp=%d/%d)", rate, fp, probes)
	}
	t.Logf("false positive rate: %.4f", rate)
}

func TestBloomRoundTrip(t *testing.T) {
	b := New(1000)
	for i := 0; i < 1000; i++ {
		b.Add([]byte(fmt.Sprintf("k%d", i)))
	}
	enc := b.Encode()
	d, err := Decode(enc)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 1000; i++ {
		if !d.MayContain([]byte(fmt.Sprintf("k%d", i))) {
			t.Fatalf("decoded filter lost key k%d", i)
		}
	}
	// same bits, so same FPR behavior
	if len(d.bits) != len(b.bits) || d.k != b.k {
		t.Fatal("decoded filter shape differs")
	}
}

func TestBloomDecodeCorruption(t *testing.T) {
	if _, err := Decode(nil); err == nil {
		t.Fatal("empty payload must fail")
	}
	b := New(100)
	enc := b.Encode()
	// zeroed k byte must fail
	bad := append([]byte(nil), enc...)
	bad[len(bad)-1] = 0
	if _, err := Decode(bad); err == nil {
		t.Fatal("k=0 must fail")
	}
}

func TestBloomEmptyFilter(t *testing.T) {
	b := New(0)
	if b.MayContain([]byte("anything")) {
		t.Fatal("empty filter must not contain anything")
	}
}
