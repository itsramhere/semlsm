// Package testmodel provides the oracle model and workload generator used by
// model-based tests: it records every committed record and answers
// "what is visible at timestamp S" queries.
package testmodel

import (
	"fmt"
	"math/rand"
	"sort"
	"sync"

	"semlsm/internal/base"
)

// Model records every committed (key, ts, kind, value) and dropped ranges.
type Model struct {
	mu   sync.Mutex
	recs []base.Record
	// dropped records per key: ts ranges that compaction replaced with
	// summaries (filled by MarkDropped)
	dropped map[string][]tsRange
}

type tsRange struct{ min, max uint64 }

// NewModel returns an empty model.
func NewModel() *Model {
	return &Model{dropped: map[string][]tsRange{}}
}

// Commit records a batch's records (ts already assigned by the DB).
func (m *Model) Commit(recs []base.Record) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, r := range recs {
		cp := base.Record{
			Key:   append([]byte(nil), r.Key...),
			Ts:    r.Ts,
			Kind:  r.Kind,
			Value: append([]byte(nil), r.Value...),
		}
		m.recs = append(m.recs, cp)
	}
}

// MarkDropped records that all versions of key in [minTs, maxTs] were
// replaced by a checkpoint.
func (m *Model) MarkDropped(key []byte, minTs, maxTs uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.dropped[string(key)] = append(m.dropped[string(key)], tsRange{minTs, maxTs})
}

// VisibleAt returns the newest version of key with ts <= S from the raw
// truth (ignoring drops). The caller applies the B6 resolution rules using
// IsDropped to verify reader behavior.
func (m *Model) VisibleAt(key []byte, s uint64) (ts uint64, kind base.Kind, value []byte, ok bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	best := -1
	for i := range m.recs {
		r := &m.recs[i]
		if string(r.Key) == string(key) && r.Ts <= s {
			if best == -1 || r.Ts > m.recs[best].Ts {
				best = i
			}
		}
	}
	if best == -1 {
		return 0, 0, nil, false
	}
	r := m.recs[best]
	return r.Ts, r.Kind, append([]byte(nil), r.Value...), true
}

// IsDropped reports whether (key, ts) lies in a dropped range.
func (m *Model) IsDropped(key []byte, ts uint64) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, rg := range m.dropped[string(key)] {
		if ts >= rg.min && ts <= rg.max {
			return true
		}
	}
	return false
}

// ScanAt returns the visible version of every key in [start, end) at s,
// oldest-first by key (P5.7 oracle for iteration).
func (m *Model) ScanAt(start, end []byte, s uint64) []base.Record {
	m.mu.Lock()
	defer m.mu.Unlock()
	keys := map[string]base.Record{}
	for i := range m.recs {
		r := m.recs[i]
		if r.Ts > s {
			continue
		}
		if len(start) > 0 && base.CompareUser(r.Key, start) < 0 {
			continue
		}
		if len(end) > 0 && base.CompareUser(r.Key, end) >= 0 {
			continue
		}
		prev, exists := keys[string(r.Key)]
		if !exists || r.Ts > prev.Ts {
			keys[string(r.Key)] = r
		}
	}
	out := make([]base.Record, 0, len(keys))
	for _, r := range keys {
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool { return base.CompareUser(out[i].Key, out[j].Key) < 0 })
	return out
}

// MaxTs returns the highest recorded timestamp.
func (m *Model) MaxTs() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	var max uint64
	for i := range m.recs {
		if m.recs[i].Ts > max {
			max = m.recs[i].Ts
		}
	}
	return max
}

// Count returns the number of recorded records.
func (m *Model) Count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.recs)
}

// Gen is a seeded workload generator mixing state-update keys and
// event-stream keys (P5.7).
type Gen struct {
	rng       *rand.Rand
	stateKeys []string
	eventN    int // next event key number
	eventCap  int
}

// NewGen returns a generator: stateKeys distinct state keys updated in place,
// and event keys appended under "app/1/".
func NewGen(seed int64, stateKeys, eventCap int) *Gen {
	g := &Gen{
		rng:      rand.New(rand.NewSource(seed)),
		eventCap: eventCap,
	}
	for i := 0; i < stateKeys; i++ {
		g.stateKeys = append(g.stateKeys, fmt.Sprintf("agent/%d/state", i))
	}
	return g
}

// Next returns one batch: a random mix of state updates (Puts on state keys)
// and event appends (unique keys). The caller commits it through the DB and
// feeds the assigned ts to Model.Commit.
func (g *Gen) Next(maxRecords int) []base.Record {
	n := 1 + g.rng.Intn(maxRecords)
	recs := make([]base.Record, 0, n)
	for len(recs) < n {
		if len(g.stateKeys) > 0 && g.rng.Intn(3) != 0 {
			key := g.stateKeys[g.rng.Intn(len(g.stateKeys))]
			recs = append(recs, base.Record{
				Key:   []byte(key),
				Kind:  base.KindPut,
				Value: []byte(fmt.Sprintf("state-%d-%d", g.rng.Intn(1<<30), len(recs))),
			})
			continue
		}
		if g.eventN >= g.eventCap {
			// fall back to state updates when events are exhausted
			if len(g.stateKeys) > 0 {
				key := g.stateKeys[g.rng.Intn(len(g.stateKeys))]
				recs = append(recs, base.Record{
					Key:   []byte(key),
					Kind:  base.KindPut,
					Value: []byte(fmt.Sprintf("state-%d", g.rng.Intn(1<<30))),
				})
			}
			continue
		}
		recs = append(recs, base.Record{
			Key:   []byte(fmt.Sprintf("app/1/e%06d", g.eventN)),
			Kind:  base.KindPut,
			Value: []byte(fmt.Sprintf("event-%d", g.eventN)),
		})
		g.eventN++
	}
	// occasionally include a delete of a state key
	if len(g.stateKeys) > 0 && g.rng.Intn(20) == 0 {
		key := g.stateKeys[g.rng.Intn(len(g.stateKeys))]
		recs = append(recs[:len(recs)-1], base.Record{Key: []byte(key), Kind: base.KindDelete})
	}
	return recs
}

// Keys returns the distinct user keys recorded (in sorted order).
func (m *Model) Keys() [][]byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	set := map[string]bool{}
	var out [][]byte
	for i := range m.recs {
		k := string(m.recs[i].Key)
		if !set[k] {
			set[k] = true
			out = append(out, []byte(k))
		}
	}
	sort.Slice(out, func(i, j int) bool { return base.CompareUser(out[i], out[j]) < 0 })
	return out
}
