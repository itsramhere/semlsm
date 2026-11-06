package semantic

import (
	"fmt"
	"testing"

	"semlsm/internal/base"
)

func TestComputeStatsBasics(t *testing.T) {
	records := []base.Record{
		{Key: []byte("a"), Ts: 100, Kind: base.KindPut, Value: []byte("one")},
		{Key: []byte("b"), Ts: 200, Kind: base.KindPut, Value: []byte("two")},
		{Key: []byte("a"), Ts: 300, Kind: base.KindDelete, Value: nil},
		{Key: []byte("a"), Ts: 250, Kind: base.KindPut, Value: []byte("three")},
	}
	s := ComputeStats(records)
	if s.RecordCount != 4 || s.PutCount != 3 || s.DeleteCount != 1 {
		t.Fatalf("counts: %+v", s)
	}
	if s.DistinctKeys != 2 {
		t.Fatalf("distinct: %d", s.DistinctKeys)
	}
	if s.FirstTs != 100 || s.LastTs != 300 {
		t.Fatalf("ts range: %d..%d", s.FirstTs, s.LastTs)
	}
	if s.ValueBytes != 3+3+0+5 {
		t.Fatalf("value bytes: %d", s.ValueBytes)
	}
	// TopKeys: a has 3 versions, b has 1
	if len(s.TopKeys) != 2 || s.TopKeys[0].Key != "a" || s.TopKeys[0].Count != 3 ||
		s.TopKeys[1].Key != "b" || s.TopKeys[1].Count != 1 {
		t.Fatalf("top keys: %+v", s.TopKeys)
	}
}

func TestComputeStatsTopKeysTiesAndCap(t *testing.T) {
	var records []base.Record
	// 22 keys, 3 versions each: ties broken by key, capped at 20
	for i := 0; i < 22; i++ {
		for j := 0; j < 3; j++ {
			records = append(records, base.Record{
				Key: []byte(fmt.Sprintf("k%02d", i)), Ts: uint64(i*10 + j),
				Kind: base.KindPut, Value: []byte("v"),
			})
		}
	}
	s := ComputeStats(records)
	if len(s.TopKeys) != 20 {
		t.Fatalf("cap: %d top keys", len(s.TopKeys))
	}
	for i := 0; i < 20; i++ {
		if s.TopKeys[i].Key != fmt.Sprintf("k%02d", i) {
			t.Fatalf("tie order at %d: %+v", i, s.TopKeys[i])
		}
	}
}

func TestComputeStatsNumericFields(t *testing.T) {
	records := []base.Record{
		{Key: []byte("k1"), Ts: 1, Kind: base.KindPut, Value: []byte(`{"cpu":0.5,"mem":10,"ok":true}`)},
		{Key: []byte("k2"), Ts: 2, Kind: base.KindPut, Value: []byte(`{"cpu":1.5,"mem":-10}`)},
		{Key: []byte("k3"), Ts: 3, Kind: base.KindPut, Value: []byte(`{"cpu":1}`)},
		// non-JSON values are ignored
		{Key: []byte("k4"), Ts: 4, Kind: base.KindPut, Value: []byte("plain text")},
		{Key: []byte("k5"), Ts: 5, Kind: base.KindPut, Value: []byte(`[1,2,3]`)},            // not an object
		{Key: []byte("k6"), Ts: 6, Kind: base.KindPut, Value: []byte(`{"cpu":"str"}`)},      // not numeric
		{Key: []byte("k7"), Ts: 7, Kind: base.KindPut, Value: []byte(`{"cpu":2}`)},          // counted
		{Key: []byte("k8"), Ts: 8, Kind: base.KindPut, Value: []byte(`{"nested":{"x":1}}`)}, // top-level only
		{Key: []byte("k9"), Ts: 9, Kind: base.KindPut, Value: []byte(`{"bad": 1e999}`)},     // huge: ignored
	}
	s := ComputeStats(records)
	var cpu, mem *NumericField
	for i := range s.NumericFields {
		switch s.NumericFields[i].Name {
		case "cpu":
			cpu = &s.NumericFields[i]
		case "mem":
			mem = &s.NumericFields[i]
		}
	}
	if cpu == nil {
		t.Fatalf("cpu missing: %+v", s.NumericFields)
	}
	if cpu.Count != 4 || cpu.Min != 0.5 || cpu.Max != 2 || cpu.Sum != 5 {
		t.Fatalf("cpu: %+v", cpu)
	}
	if mem == nil || mem.Count != 2 || mem.Min != -10 || mem.Max != 10 || mem.Sum != 0 {
		t.Fatalf("mem: %+v", mem)
	}
	// ok/nested/bad must not appear
	for _, f := range s.NumericFields {
		if f.Name == "ok" || f.Name == "nested" || f.Name == "bad" || f.Name == "x" {
			t.Fatalf("unexpected field %q", f.Name)
		}
	}
}

func TestComputeStatsEmptyAndDeterministic(t *testing.T) {
	s := ComputeStats(nil)
	if s.RecordCount != 0 || s.FirstTs != 0 || s.LastTs != 0 {
		t.Fatalf("empty: %+v", s)
	}
	// deterministic ordering across runs
	records := []base.Record{
		{Key: []byte("z"), Ts: 1, Kind: base.KindPut, Value: []byte(`{"b":2,"a":1}`)},
		{Key: []byte("a"), Ts: 2, Kind: base.KindPut, Value: []byte(`{"a":3,"c":9}`)},
	}
	s1 := ComputeStats(records)
	s2 := ComputeStats(records)
	if fmt.Sprint(s1) != fmt.Sprint(s2) {
		t.Fatal("not deterministic")
	}
	if len(s1.NumericFields) != 3 || s1.NumericFields[0].Name != "a" ||
		s1.NumericFields[1].Name != "b" || s1.NumericFields[2].Name != "c" {
		t.Fatalf("field order: %+v", s1.NumericFields)
	}
}
