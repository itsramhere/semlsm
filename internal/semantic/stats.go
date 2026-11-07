package semantic

import (
	"bytes"
	"encoding/json"
	"sort"

	"github.com/itsramhere/semlsm/internal/base"
)

// maxTopEntries caps the TopKeys and NumericFields lists (B9).
const maxTopEntries = 20

// numAcc accumulates one numeric field across values.
type numAcc struct {
	n     uint64
	min   float64
	max   float64
	sum   float64
	float bool // saw at least one convertible value
}

// ComputeStats summarizes a segment's records into the engine-side Stats
// (B9, P8.3). NumericFields are taken from values that look like JSON
// objects (first byte '{'): only top-level fields whose value is a JSON
// number, ignoring every parse error. Both top lists have deterministic
// ordering: count descending, ties by name.
func ComputeStats(records []base.Record) Stats {
	var s Stats
	fields := map[string]*numAcc{}
	keys := map[string]uint64{}
	for i := range records {
		r := records[i]
		s.RecordCount++
		switch r.Kind {
		case base.KindPut:
			s.PutCount++
		case base.KindDelete:
			s.DeleteCount++
		}
		keys[string(r.Key)]++
		if s.RecordCount == 1 || r.Ts < s.FirstTs {
			s.FirstTs = r.Ts
		}
		if r.Ts > s.LastTs {
			s.LastTs = r.Ts
		}
		s.ValueBytes += uint64(len(r.Value))
		collectNumeric(r.Value, fields)
	}
	s.DistinctKeys = uint64(len(keys))
	for k, n := range keys {
		s.TopKeys = append(s.TopKeys, KeyCount{Key: k, Count: n})
	}
	sort.Slice(s.TopKeys, func(i, j int) bool {
		if s.TopKeys[i].Count != s.TopKeys[j].Count {
			return s.TopKeys[i].Count > s.TopKeys[j].Count
		}
		return s.TopKeys[i].Key < s.TopKeys[j].Key
	})
	if len(s.TopKeys) > maxTopEntries {
		s.TopKeys = s.TopKeys[:maxTopEntries]
	}
	for name, a := range fields {
		if !a.float {
			continue
		}
		s.NumericFields = append(s.NumericFields, NumericField{
			Name: name, Count: a.n, Min: a.min, Max: a.max, Sum: a.sum,
		})
	}
	sort.Slice(s.NumericFields, func(i, j int) bool {
		if s.NumericFields[i].Count != s.NumericFields[j].Count {
			return s.NumericFields[i].Count > s.NumericFields[j].Count
		}
		return s.NumericFields[i].Name < s.NumericFields[j].Name
	})
	if len(s.NumericFields) > maxTopEntries {
		s.NumericFields = s.NumericFields[:maxTopEntries]
	}
	return s
}

// collectNumeric folds one value's top-level numeric JSON fields into acc.
// Only values starting with '{' are parsed; every error is ignored (P8.3).
func collectNumeric(value []byte, fields map[string]*numAcc) {
	if len(value) == 0 || value[0] != '{' {
		return
	}
	dec := json.NewDecoder(bytes.NewReader(value))
	dec.UseNumber()
	var obj map[string]interface{}
	if err := dec.Decode(&obj); err != nil {
		return
	}
	for name, v := range obj {
		num, ok := v.(json.Number)
		if !ok {
			continue // not a number (string, bool, null, nested object, ...)
		}
		f, err := num.Float64()
		if err != nil {
			continue // huge or malformed: ignore this field
		}
		a := fields[name]
		if a == nil {
			a = &numAcc{}
			fields[name] = a
		}
		if !a.float || f < a.min {
			a.min = f
		}
		if !a.float || f > a.max {
			a.max = f
		}
		a.sum += f
		a.n++
		a.float = true
	}
}
