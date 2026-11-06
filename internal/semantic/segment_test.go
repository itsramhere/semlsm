package semantic

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"
	"time"

	"semlsm/internal/base"
)

var segClock = base.NewManualClock(time.Unix(1_000_000, 0))

func segPolicy(mode base.Mode, minBatch, maxBatch int) base.Policy {
	return base.Policy{
		Mode:     mode,
		MinAge:   time.Hour,
		MinBatch: minBatch,
		MaxBatch: maxBatch,
	}
}

// oldTs returns a ts comfortably below now-MinAge; youngTs above it.
func oldTs(offset time.Duration) uint64 {
	return uint64(segClock.Now().Add(-time.Hour - offset).UnixNano())
}
func youngTs(offset time.Duration) uint64 {
	return uint64(segClock.Now().Add(-offset).UnixNano())
}

// recID is a comparable identity for one record (base.Record has slices).
type recID struct {
	key string
	ts  uint64
	val string
}

func id(r base.Record) recID { return recID{string(r.Key), r.Ts, string(r.Value)} }

// I7 property: per key, the versions inside the segments form a contiguous
// oldest-first prefix of that key's ascending version list, and the segment
// order never puts a newer version of a key before an older one. Every input
// record appears exactly once across segments+raw.
func checkSegmentProperty(t *testing.T, records []base.Record, segs []Segment, raw []base.Record) {
	t.Helper()
	seen := map[recID]bool{}
	for _, r := range records {
		seen[id(r)] = false
	}
	perKey := map[string][]uint64{} // key -> ts order across segments
	for si, s := range segs {
		if len(s.Records) == 0 {
			t.Fatalf("segment %d empty", si)
		}
		for i := 1; i < len(s.Records); i++ {
			a, b := s.Records[i-1], s.Records[i]
			if a.Ts > b.Ts || (a.Ts == b.Ts && string(a.Key) > string(b.Key)) {
				t.Fatalf("segment %d not sorted by (ts,key)", si)
			}
		}
		for _, r := range s.Records {
			rid := id(r)
			if _, ok := seen[rid]; !ok {
				t.Fatalf("segment %d has unknown record %v", si, rid)
			}
			if seen[rid] {
				t.Fatalf("record %v appears twice", rid)
			}
			seen[rid] = true
			perKey[string(r.Key)] = append(perKey[string(r.Key)], r.Ts)
		}
	}
	for _, r := range raw {
		rid := id(r)
		if _, ok := seen[rid]; !ok {
			t.Fatalf("raw has unknown record %v", rid)
		}
		if seen[rid] {
			t.Fatalf("raw record %v also in a segment", rid)
		}
		seen[rid] = true
	}
	for _, r := range records {
		if !seen[id(r)] {
			t.Fatalf("record %v lost entirely", id(r))
		}
	}

	// per key: the segment ts list is a contiguous prefix of the ascending
	// version list
	byKey := map[string][]uint64{}
	for _, r := range records {
		byKey[string(r.Key)] = append(byKey[string(r.Key)], r.Ts)
	}
	for k, ts := range byKey {
		sort.Slice(ts, func(i, j int) bool { return ts[i] < ts[j] })
		got := perKey[k]
		for i, g := range got {
			if g != ts[i] {
				t.Fatalf("key %s: segment version %d is %d, want ascending prefix value %d", k, i, g, ts[i])
			}
		}
	}
}

func TestSegmentPropertyI7(t *testing.T) {
	seed := int64(88123)
	rng := rand.New(rand.NewSource(seed))
	var records []base.Record
	for i := 0; i < 6; i++ {
		key := []byte(fmt.Sprintf("agent/%d/state", i))
		n := 3 + rng.Intn(6)
		for j := 0; j < n; j++ {
			records = append(records, base.Record{
				Key:   append([]byte(nil), key...),
				Ts:    oldTs(time.Duration(rng.Intn(3600)) * time.Second),
				Kind:  base.KindPut,
				Value: []byte(fmt.Sprintf("v%d-%d", i, j)),
			})
		}
		// a couple of young versions that must stay raw
		records = append(records, base.Record{
			Key: append([]byte(nil), key...), Ts: youngTs(time.Duration(rng.Intn(600)) * time.Second),
			Kind: base.KindPut, Value: []byte("young"),
		})
	}
	for _, mode := range []base.Mode{base.ModeState, base.ModeEvents} {
		segs, raw := Segmenter(records, segPolicy(mode, 5, 20), segClock.Now())
		if len(segs) == 0 {
			t.Fatalf("%s: no segments", mode)
		}
		checkSegmentProperty(t, records, segs, raw)
	}
}

func TestSegmentModeStateHeadRetained(t *testing.T) {
	var records []base.Record
	for j := 0; j < 5; j++ {
		records = append(records, base.Record{
			Key: []byte("k"), Ts: oldTs(time.Duration(100-j) * time.Second),
			Kind: base.KindPut, Value: []byte{byte(j)},
		})
	}
	segs, raw := Segmenter(records, segPolicy(base.ModeState, 4, 10), segClock.Now())
	got := 0
	for _, s := range segs {
		got += len(s.Records)
	}
	if got != 4 {
		t.Fatalf("ModeState summarized %d versions, want 4 (head retained)", got)
	}
	if len(raw) != 1 || raw[0].Ts != records[4].Ts {
		t.Fatalf("raw must hold the newest version: %+v", raw)
	}
	checkSegmentProperty(t, records, segs, raw)
}

func TestSegmentModeEventsHeadDroppable(t *testing.T) {
	var records []base.Record
	for j := 0; j < 5; j++ {
		records = append(records, base.Record{
			Key: []byte(fmt.Sprintf("e%d", j)), Ts: oldTs(time.Duration(50-j) * time.Second),
			Kind: base.KindPut, Value: []byte{byte(j)},
		})
	}
	segs, raw := Segmenter(records, segPolicy(base.ModeEvents, 5, 5), segClock.Now())
	if len(segs) != 1 || len(segs[0].Records) != 5 {
		t.Fatalf("ModeEvents must drop the head: %+v", segs)
	}
	if len(raw) != 0 {
		t.Fatalf("nothing should stay raw: %+v", raw)
	}
}

func TestSegmentSameTsNeverSplit(t *testing.T) {
	ts := oldTs(0)
	var records []base.Record
	for j := 0; j < 12; j++ { // one batch ts: 12 keys share the same ts
		records = append(records, base.Record{
			Key: []byte(fmt.Sprintf("k%02d", j)), Ts: ts,
			Kind: base.KindPut, Value: []byte("v"),
		})
	}
	segs, raw := Segmenter(records, segPolicy(base.ModeEvents, 10, 5), segClock.Now())
	if len(segs) != 1 || len(segs[0].Records) != 12 {
		t.Fatalf("same-ts run split or incomplete: segs=%v raw=%v", segs, raw)
	}
	if len(raw) != 0 {
		t.Fatalf("unexpected raw: %+v", raw)
	}
}

func TestSegmentMinBatchMaxBatch(t *testing.T) {
	var records []base.Record
	for j := 0; j < 25; j++ { // distinct ascending ts: no overflow possible
		records = append(records, base.Record{
			Key: []byte(fmt.Sprintf("k%02d", j)), Ts: oldTs(time.Duration(1000-j) * time.Second),
			Kind: base.KindPut, Value: []byte("v"),
		})
	}
	segs, raw := Segmenter(records, segPolicy(base.ModeEvents, 10, 10), segClock.Now())
	if len(segs) != 2 || len(segs[0].Records) != 10 || len(segs[1].Records) != 10 {
		t.Fatalf("want two segments of 10: %+v", segs)
	}
	if len(raw) != 5 {
		t.Fatalf("remainder below MinBatch must stay raw: %+v", raw)
	}
	// below MinBatch nothing is summarized at all
	segs, raw = Segmenter(records[:5], segPolicy(base.ModeEvents, 10, 10), segClock.Now())
	if len(segs) != 0 || len(raw) != 5 {
		t.Fatalf("below MinBatch: segs=%v raw=%d", segs, len(raw))
	}
}

func TestSegmentMinAgeHorizon(t *testing.T) {
	var records []base.Record
	records = append(records,
		base.Record{Key: []byte("k"), Ts: oldTs(time.Hour), Kind: base.KindPut, Value: []byte("old")},
		base.Record{Key: []byte("k"), Ts: youngTs(time.Minute), Kind: base.KindPut, Value: []byte("young")},
	)
	segs, raw := Segmenter(records, segPolicy(base.ModeEvents, 1, 10), segClock.Now())
	if len(segs) != 1 || len(segs[0].Records) != 1 || string(segs[0].Records[0].Value) != "old" {
		t.Fatalf("horizon not honored: %+v", segs)
	}
	if len(raw) != 1 || string(raw[0].Value) != "young" {
		t.Fatalf("young record must stay raw: %+v", raw)
	}
	// a clock older than MinAge makes nothing eligible (saturating horizon)
	early := base.NewManualClock(time.Unix(10, 0))
	segs, raw = Segmenter(records, segPolicy(base.ModeEvents, 1, 10), early.Now())
	if len(segs) != 0 || len(raw) != 2 {
		t.Fatalf("saturating horizon: segs=%v raw=%d", segs, len(raw))
	}
}

func TestSegmentDisabledAndEmpty(t *testing.T) {
	records := []base.Record{{Key: []byte("k"), Ts: oldTs(0), Kind: base.KindPut, Value: []byte("v")}}
	segs, raw := Segmenter(records, segPolicy(base.ModeDisabled, 1, 10), segClock.Now())
	if len(segs) != 0 || len(raw) != 1 {
		t.Fatalf("ModeDisabled must stay raw: segs=%v raw=%d", segs, len(raw))
	}
	segs, raw = Segmenter(nil, segPolicy(base.ModeEvents, 1, 10), segClock.Now())
	if len(segs) != 0 || len(raw) != 0 {
		t.Fatalf("empty input: segs=%v raw=%d", segs, len(raw))
	}
}
