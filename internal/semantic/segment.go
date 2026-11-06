// Package semantic implements Tier B of the engine: segmenting eligible
// records into batches (P8.2), computing the engine-side Stats (P8.3),
// rendering and parsing prompts (P8.4), validating LLM output (P8.5) and the
// map-reduce summarization pipeline (P8.6).
package semantic

import (
	"bytes"
	"sort"
	"time"

	"semlsm/internal/base"
)

// Segment is one batch of eligible records to summarize together into a
// single checkpoint tuple (P8.2). Records are ascending by (ts, key).
type Segment struct {
	Records []base.Record
}

// Segmenter cuts one group's records into summarizable segments per the
// policy (P8.2). The input is every record of one group from the merged job
// input; now is the engine clock used for the MinAge horizon.
//
// Eligibility: per key, versions ascending by ts form a prefix that is old
// enough (ts <= now-MinAge). In ModeState the newest version of every key is
// never eligible; in ModeEvents it may be. Eligible records are sorted by
// (ts, key) and cut into segments of at most MaxBatch records, but only
// while at least MinBatch remain; a same-ts run is never split (a segment
// may exceed MaxBatch when more than MaxBatch records share the cut ts).
// Everything not eligible, plus the final remainder below MinBatch, is
// returned raw.
func Segmenter(records []base.Record, pol base.Policy, now time.Time) (segs []Segment, raw []base.Record) {
	if pol.Mode == base.ModeDisabled || pol.MinBatch < 1 || pol.MaxBatch < 1 {
		return nil, append([]base.Record(nil), records...)
	}

	// horizon = now - MinAge, saturating: before the horizon everything
	// stays raw.
	horizon := uint64(0)
	if ns := now.UnixNano() - pol.MinAge.Nanoseconds(); ns > 0 {
		horizon = uint64(ns)
	}

	// group records by key, versions ascending by ts
	byKey := map[string][]base.Record{}
	keys := []string{}
	for _, r := range records {
		k := string(r.Key)
		if _, ok := byKey[k]; !ok {
			keys = append(keys, k)
		}
		byKey[k] = append(byKey[k], r)
	}
	for k := range byKey {
		list := byKey[k]
		sort.SliceStable(list, func(i, j int) bool { return list[i].Ts < list[j].Ts })
	}

	// collect the eligible prefix of every key's version list; everything
	// else (ModeState heads, versions newer than the horizon) stays raw
	ineligible := make([]base.Record, 0, len(records))
	eligible := make([]base.Record, 0, len(records))
	for _, k := range keys {
		list := byKey[k]
		limit := len(list)
		if pol.Mode == base.ModeState && limit > 0 {
			limit-- // the newest version is never dropped
		}
		i := 0
		for ; i < limit; i++ {
			if list[i].Ts > horizon {
				break
			}
		}
		eligible = append(eligible, list[:i]...)
		// versions too young (and the ModeState head) stay raw
		ineligible = append(ineligible, list[i:]...)
	}
	sort.SliceStable(eligible, func(i, j int) bool {
		if eligible[i].Ts != eligible[j].Ts {
			return eligible[i].Ts < eligible[j].Ts
		}
		return bytes.Compare(eligible[i].Key, eligible[j].Key) < 0
	})

	// cut into batches: never split a same-ts run (I7), stay raw below
	// MinBatch
	remaining := eligible
	for len(remaining) >= pol.MinBatch {
		cut := len(remaining) - 1
		if len(remaining) > pol.MaxBatch {
			cut = pol.MaxBatch - 1
		}
		cutTs := remaining[cut].Ts
		i := cut
		for i < len(remaining) && remaining[i].Ts == cutTs {
			i++
		}
		segs = append(segs, Segment{Records: remaining[:i:i]})
		remaining = remaining[i:]
	}
	if len(remaining) > 0 {
		ineligible = append(ineligible, remaining...)
	}
	return segs, ineligible
}
