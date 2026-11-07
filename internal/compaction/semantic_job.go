package compaction

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"sort"
	"time"

	"semlsm/internal/base"
	"semlsm/internal/checkpoint"
	"semlsm/internal/failpoint"
	"semlsm/internal/llm"
	"semlsm/internal/manifest"
	"semlsm/internal/merge"
	"semlsm/internal/semantic"
	"semlsm/internal/sstable"
	"semlsm/internal/version"
	"semlsm/internal/vfs"
)

// Semantic failpoints (P10.1 steps 7-9). after_manifest is hit by the engine
// driver right after the edit is durable.
var (
	FpSemanticAfterOutputs = failpoint.Define("semantic.after_outputs")
	FpSemanticAfterCkpt    = failpoint.Define("semantic.after_ckpt")
	FpSemanticAfterArchive = failpoint.Define("semantic.after_archive")
)

// SemanticDeps injects the engine facilities one semantic job needs. None of
// the callbacks may hold an engine lock while the job runs (I11): the job
// makes LLM calls and performs file I/O on its own goroutine.
type SemanticDeps struct {
	FS             vfs.FS
	Dir            string
	ArchiveDir     string // <dbdir>/archive
	GroupDepth     int
	GroupDelimiter byte

	// PolicyFor resolves a user key to its effective policy (B4).
	PolicyFor func(key []byte) base.Policy
	// Now is the engine clock for the MinAge horizon and CreatedUnixNano.
	Now func() time.Time
	// AllocNum allocates file numbers (shared across .sst/.ckpt/archive).
	AllocNum func() uint64
	// PrevTupleID returns the newest existing tuple id for the group from
	// the current Version (0 when none); called as each group's first tuple
	// is drafted.
	PrevTupleID func(group string) uint64

	TargetFileBytes int64
	ArchiveRaw      bool

	Model         string
	PromptVersion int

	// OpenInput opens one input SST; the release func must be called when
	// the job is done with it.
	OpenInput func(num uint64) (merge.Iter, func(), error)
	// Summarize runs one map-reduce summarization outside all locks (I11).
	Summarize func(ctx context.Context, seg semantic.Segment, group string) (semantic.Summary, semantic.Usage, error)

	Logger *slog.Logger
}

// SemanticOutcome classifies a finished job (P10.3).
type SemanticOutcome uint8

const (
	// SemanticCommitted: the job produced at least one tuple.
	SemanticCommitted SemanticOutcome = iota
	// SemanticRawOnly: every group stayed raw; the outputs rewrite the
	// inputs with no tuples (a structural move that keeps the data
	// summarizable by a later pass).
	SemanticRawOnly
	// SemanticFailed: an infrastructure failure; nothing is installed.
	SemanticFailed
)

func (o SemanticOutcome) String() string {
	switch o {
	case SemanticCommitted:
		return "committed"
	case SemanticRawOnly:
		return "raw-only"
	default:
		return "failed"
	}
}

// SemanticStats is the per-job accounting the engine folds into Stats (P10.3).
type SemanticStats struct {
	RecordsSummarized uint64
	RawBytesRemoved   uint64
	SummaryBytes      uint64
	LLMCalls          int64
	InputTokens       int64
	OutputTokens      int64
	Preempted         bool
	Failures          map[string]int // by class: validation, transient, permanent, budget, circuit, compression, canceled, other
}

func (s *SemanticStats) fail(class string) {
	if s.Failures == nil {
		s.Failures = map[string]int{}
	}
	s.Failures[class]++
}

// SemanticResult is a finished job's product; the driver turns it into one
// manifest edit (P10.1 steps 10-11).
type SemanticResult struct {
	Outputs  []*version.FileMeta // written at the plan's output level
	Ckpt     manifest.CkptFile   // the checkpoint file with its tuple refs
	Archives []uint64            // one archive SST per tuple, in tuple order
	Stats    SemanticStats
}

// recID identifies one input record version. (key, ts) is unique across the
// engine: the commit path assigns one ts per batch and dedups within it.
type recID struct {
	key string
	ts  uint64
}

// semGroup is one group's buffered records from the merged input.
type semGroup struct {
	name []byte
	recs []base.Record
}

// tupleDraft is one segment's product before the ckpt is written.
type tupleDraft struct {
	group   string
	meta    checkpoint.TupleMeta
	summary semantic.Summary
	entries []checkpoint.KeyEntry
	recs    []base.Record // the dropped records, in segment order
}

// RunSemantic executes steps 2-9 of P10.1 over the plan's inputs: stream
// groups, summarize eligible segments, assert I7, write output SSTs, one
// .ckpt, and (optionally) archive SSTs. It never takes an engine lock: the
// driver owns picking, busy marking, the manifest edit, and the install.
func RunSemantic(ctx context.Context, plan *Plan, d SemanticDeps) (*SemanticResult, SemanticStats, error) {
	stats := SemanticStats{Failures: map[string]int{}}
	log := d.Logger
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	res := &SemanticResult{}

	// Open every input (I/O on the job's goroutine, no engine locks held).
	files := plan.AllFiles()
	iters := make([]merge.Iter, 0, len(files))
	rels := make([]func(), 0, len(files))
	defer func() {
		for _, rel := range rels {
			rel()
		}
	}()
	for _, f := range files {
		it, rel, err := d.OpenInput(f.Num)
		if err != nil {
			return nil, stats, fmt.Errorf("semantic: open input %d: %w", f.Num, err)
		}
		iters = append(iters, it)
		rels = append(rels, rel)
	}
	src := merge.New(iters...)

	// Step 3: stream the merged input one group at a time (groups are
	// contiguous in internal-key order, B4). The buffers are bounded by the
	// driver's MaxSemanticJobBytes guard on total input size.
	var (
		groups   []semGroup
		cur      semGroup
		totalRec int
	)
	flush := func() {
		if len(cur.recs) > 0 {
			groups = append(groups, cur)
		}
		cur = semGroup{}
	}
	for src.First(); src.Valid(); src.Next() {
		user, ts, kind, ok := base.SplitInternalKey(src.Key())
		if !ok {
			return nil, stats, fmt.Errorf("semantic: bad internal key in input")
		}
		g := base.GroupOf(user, d.GroupDepth, d.GroupDelimiter)
		if len(cur.recs) > 0 && string(g) != string(cur.name) {
			flush()
		}
		if len(cur.recs) == 0 {
			cur.name = append([]byte(nil), g...)
		}
		cur.recs = append(cur.recs, base.Record{
			Key:   append([]byte(nil), user...),
			Ts:    ts,
			Kind:  kind,
			Value: append([]byte(nil), src.Value()...),
		})
		totalRec++
	}
	if err := src.Err(); err != nil {
		return nil, stats, fmt.Errorf("semantic: merge inputs: %w", err)
	}
	flush()
	if totalRec == 0 {
		return nil, stats, fmt.Errorf("semantic: empty input")
	}

	// Step 4: per group, partition by effective policy and summarize the
	// segmenter's eligible segments in time order. Any failure puts that
	// segment and every later one in the group raw; a preempted or timed-out
	// job stops calling the LLM entirely and commits what it has.
	drop := map[recID]struct{}{}
	var drafts []tupleDraft
	prevTupleInJob := map[string]uint64{} // group -> newest tuple id drafted by this job
	for gi := range groups {
		stayRaw := ctx.Err() != nil // preempted before this group: no LLM at all
		for _, part := range partitionByPolicy(groups[gi].recs, d.PolicyFor) {
			if part.pol.Mode == base.ModeDisabled {
				continue // never summarized (B4)
			}
			segs, _ := semantic.Segmenter(part.recs, part.pol, d.Now())
			for si := range segs {
				if stayRaw {
					break // this and all later segments in the group stay raw
				}
				group := string(groups[gi].name)
				sum, usage, err := d.Summarize(ctx, segs[si], group)
				stats.LLMCalls += usage.Calls
				stats.InputTokens += usage.InputTokens
				stats.OutputTokens += usage.OutputTokens
				if err != nil {
					stats.fail(failureClass(err))
					log.Warn("semlsm: semantic segment failed; the rest of the group stays raw",
						"group", group, "segment", si, "err", err)
					stayRaw = true // step 4's failure rule
					continue
				}
				dr, err := draftTuple(segs[si], sum, group, plan, d, prevTupleInJob)
				if err != nil {
					stats.fail("internal")
					return nil, stats, err
				}
				prevTupleInJob[group] = dr.meta.ID
				for _, r := range dr.recs {
					drop[recID{string(r.Key), r.Ts}] = struct{}{}
				}
				drafts = append(drafts, *dr)
				stats.RecordsSummarized += uint64(len(dr.recs))
				stats.RawBytesRemoved += dr.meta.SourceBytes
			}
		}
	}

	// Step 5: assert I7 for every dropped key — the dropped versions are
	// exactly the key's input versions inside [entry.MinTs, entry.MaxTs].
	// Failure aborts the job before anything is installed.
	if err := assertI7(groups, drafts); err != nil {
		return nil, stats, err
	}

	// Step 6: write every non-dropped record to output SSTs at the output
	// level (cut rules I5; every version and tombstone is kept).
	ow := NewOutputWriter(d.FS, d.Dir, d.TargetFileBytes, d.AllocNum)
	for gi := range groups {
		for _, r := range groups[gi].recs {
			if _, ok := drop[recID{string(r.Key), r.Ts}]; ok {
				continue
			}
			if err := ow.Add(base.MakeInternalKey(r.Key, r.Ts, r.Kind), r.Value); err != nil {
				ow.Abort()
				return nil, stats, fmt.Errorf("semantic: output: %w", err)
			}
		}
	}
	outputs, err := ow.Finish()
	if err != nil {
		ow.Abort()
		return nil, stats, fmt.Errorf("semantic: finish outputs: %w", err)
	}
	for _, fm := range outputs {
		fm.Level = plan.OutputLevel
	}
	res.Outputs = outputs
	if err := d.FS.SyncDir(d.Dir); err != nil {
		return nil, stats, fmt.Errorf("semantic: syncdir outputs: %w", err)
	}
	if err := failpoint.Hit(FpSemanticAfterOutputs); err != nil {
		return nil, stats, err
	}

	// Step 8: one .ckpt holding every tuple of this job. Archive file
	// numbers are allocated up front so the tuple metas inside the ckpt can
	// reference them; the archive files themselves land in step 9, still
	// before the manifest edit makes any of it visible.
	if len(drafts) > 0 {
		if d.ArchiveRaw {
			if err := d.FS.MkdirAll(d.ArchiveDir); err != nil {
				return nil, stats, fmt.Errorf("semantic: archive dir: %w", err)
			}
			for i := range drafts {
				drafts[i].meta.ArchiveFileNum = d.AllocNum()
				res.Archives = append(res.Archives, drafts[i].meta.ArchiveFileNum)
			}
		}
		ckptNum := d.AllocNum()
		cw, err := checkpoint.NewWriter(d.FS, filepath.Join(d.Dir, fmt.Sprintf("%06d.ckpt", ckptNum)))
		if err != nil {
			return nil, stats, err
		}
		for i := range drafts {
			sumJSON, err := json.Marshal(drafts[i].summary)
			if err != nil {
				cw.Abort()
				return nil, stats, fmt.Errorf("semantic: summary json: %w", err)
			}
			stats.SummaryBytes += uint64(len(sumJSON))
			if err := cw.AddTuple(drafts[i].meta, sumJSON, drafts[i].entries); err != nil {
				cw.Abort()
				return nil, stats, fmt.Errorf("semantic: ckpt tuple %d: %w", drafts[i].meta.ID, err)
			}
		}
		if err := cw.Finish(); err != nil {
			cw.Abort()
			return nil, stats, fmt.Errorf("semantic: ckpt finish: %w", err)
		}
		size := int64(0)
		if st, err := d.FS.Stat(filepath.Join(d.Dir, fmt.Sprintf("%06d.ckpt", ckptNum))); err == nil {
			size = st.Size
		}
		res.Ckpt = manifest.CkptFile{FileNum: ckptNum, Size: size}
		for i := range drafts {
			res.Ckpt.Tuples = append(res.Ckpt.Tuples, manifest.TupleRef{
				ID:     drafts[i].meta.ID,
				Group:  drafts[i].meta.Group,
				TsFrom: drafts[i].meta.TsFrom,
				TsTo:   drafts[i].meta.TsTo,
				Offset: cw.DirEntries()[i].Offset,
				Length: cw.DirEntries()[i].Length,
			})
		}
		if err := d.FS.SyncDir(d.Dir); err != nil {
			return nil, stats, fmt.Errorf("semantic: syncdir ckpt: %w", err)
		}
		if err := failpoint.Hit(FpSemanticAfterCkpt); err != nil {
			return nil, stats, err
		}
	}

	// Step 9: write the archive SSTs (one per tuple) and verify each by
	// reading it back and recomputing SourceCRC32 (B8).
	if d.ArchiveRaw && len(drafts) > 0 {
		for i := range drafts {
			if err := writeArchive(d, &drafts[i]); err != nil {
				return nil, stats, err
			}
		}
		if err := d.FS.SyncDir(d.ArchiveDir); err != nil {
			return nil, stats, fmt.Errorf("semantic: syncdir archive: %w", err)
		}
		if err := failpoint.Hit(FpSemanticAfterArchive); err != nil {
			return nil, stats, err
		}
	}

	res.Stats = stats
	return res, stats, nil
}

// HasTuples reports whether the job produced any checkpoint tuples.
func (r *SemanticResult) HasTuples() bool { return r != nil && len(r.Ckpt.Tuples) > 0 }

// partition is one policy's share of a group's records.
type partition struct {
	pol  base.Policy
	recs []base.Record
}

// partitionByPolicy splits a group's records by effective policy. Keys whose
// policy is ModeDisabled form their own partition and are never dropped.
// Sorted for deterministic tuple order.
func partitionByPolicy(recs []base.Record, policyFor func([]byte) base.Policy) []partition {
	byPol := map[base.Policy][]base.Record{}
	for _, r := range recs {
		p := policyFor(r.Key)
		byPol[p] = append(byPol[p], r)
	}
	out := make([]partition, 0, len(byPol))
	for p, rs := range byPol {
		out = append(out, partition{pol: p, recs: rs})
	}
	sort.Slice(out, func(i, j int) bool {
		a, b := out[i].pol, out[j].pol
		if a.Mode != b.Mode {
			return a.Mode < b.Mode
		}
		if a.MinAge != b.MinAge {
			return a.MinAge < b.MinAge
		}
		if a.MinBatch != b.MinBatch {
			return a.MinBatch < b.MinBatch
		}
		if a.MaxBatch != b.MaxBatch {
			return a.MaxBatch < b.MaxBatch
		}
		return a.Prefix < b.Prefix
	})
	return out
}

// draftTuple builds one tuple's metadata, key entries, and drop list from a
// summarized segment. Within a key the records arrive ts-ascending (P8.2),
// so the last record of a key is its newest dropped version.
func draftTuple(seg semantic.Segment, sum semantic.Summary, group string, plan *Plan,
	d SemanticDeps, prevInJob map[string]uint64) (*tupleDraft, error) {

	if len(seg.Records) == 0 {
		return nil, errors.New("semantic: empty segment")
	}
	entries := map[string]*checkpoint.KeyEntry{}
	var srcBytes uint64
	for _, r := range seg.Records {
		e := entries[string(r.Key)]
		if e == nil {
			e = &checkpoint.KeyEntry{Key: append([]byte(nil), r.Key...), MinTs: r.Ts}
			entries[string(r.Key)] = e
		}
		e.MaxTs = r.Ts
		e.Count++
		e.LastKind = r.Kind
		srcBytes += uint64(len(r.Key) + len(r.Value) + 9)
	}
	keys := make([]string, 0, len(entries))
	for k := range entries {
		keys = append(keys, k)
	}
	sort.Strings(keys) // the key index is sorted by key (B8)
	list := make([]checkpoint.KeyEntry, 0, len(keys))
	for _, k := range keys {
		list = append(list, *entries[k])
	}

	var prevTuple uint64
	if p, ok := prevInJob[group]; ok {
		prevTuple = p
	} else if d.PrevTupleID != nil {
		prevTuple = d.PrevTupleID(group)
	}

	tsFrom, tsTo := seg.Records[0].Ts, seg.Records[0].Ts
	for _, r := range seg.Records {
		if r.Ts < tsFrom {
			tsFrom = r.Ts
		}
		if r.Ts > tsTo {
			tsTo = r.Ts
		}
	}
	meta := checkpoint.TupleMeta{
		ID:              d.AllocNum(),
		Group:           group,
		TsFrom:          tsFrom,
		TsTo:            tsTo,
		SourceCount:     uint64(len(seg.Records)),
		SourceBytes:     srcBytes,
		SourceCRC32:     base.SourceCRC(seg.Records),
		PutCount:        sum.Stats.PutCount,
		DeleteCount:     sum.Stats.DeleteCount,
		KeyCount:        sum.Stats.DistinctKeys,
		FromLevel:       plan.Level,
		ToLevel:         plan.OutputLevel,
		Model:           d.Model,
		PromptVersion:   d.PromptVersion,
		CreatedUnixNano: d.Now().UnixNano(),
		PrevTupleID:     prevTuple,
	}
	return &tupleDraft{
		group:   group,
		meta:    meta,
		summary: sum,
		entries: list,
		recs:    seg.Records,
	}, nil
}

// assertI7 checks, for every dropped key of every draft, that the dropped
// versions are exactly the key's input versions inside the entry's
// [MinTs, MaxTs] (P10.1 step 5). A violation aborts the job.
func assertI7(groups []semGroup, drafts []tupleDraft) error {
	input := map[string][]uint64{} // key -> every input ts, ascending per key
	for gi := range groups {
		for _, r := range groups[gi].recs {
			input[string(r.Key)] = append(input[string(r.Key)], r.Ts)
		}
	}
	for di := range drafts {
		dropped := map[string][]uint64{}
		for _, r := range drafts[di].recs {
			dropped[string(r.Key)] = append(dropped[string(r.Key)], r.Ts)
		}
		for _, e := range drafts[di].entries {
			k := string(e.Key)
			var inRange []uint64
			for _, ts := range input[k] {
				if ts >= e.MinTs && ts <= e.MaxTs {
					inRange = append(inRange, ts)
				}
			}
			got := dropped[k]
			if len(inRange) != len(got) || len(inRange) != int(e.Count) {
				return fmt.Errorf("semantic: I7 violated for key %q in tuple %d: %d input versions in [%d,%d], %d dropped (count %d)",
					k, drafts[di].meta.ID, len(inRange), e.MinTs, e.MaxTs, len(got), e.Count)
			}
			sort.Slice(inRange, func(a, b int) bool { return inRange[a] < inRange[b] })
			for i := range inRange {
				if inRange[i] != got[i] {
					return fmt.Errorf("semantic: I7 violated for key %q in tuple %d: dropped set differs from the input range", k, drafts[di].meta.ID)
				}
			}
			if e.MinTs != got[0] || e.MaxTs != got[len(got)-1] {
				return fmt.Errorf("semantic: I7 violated for key %q in tuple %d: entry range [%d,%d] does not bound the dropped run [%d,%d]",
					k, drafts[di].meta.ID, e.MinTs, e.MaxTs, got[0], got[len(got)-1])
			}
		}
	}
	return nil
}

// failureClass maps a Summarize error to a Stats failure class (P10.3).
func failureClass(err error) string {
	var ve *semantic.ValidationError
	switch {
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return "canceled"
	case errors.Is(err, semantic.ErrPoorCompression):
		return "compression"
	case errors.As(err, &ve):
		return "validation"
	case errors.Is(err, llm.ErrTransient):
		return "transient"
	case errors.Is(err, llm.ErrPermanent):
		return "permanent"
	case errors.Is(err, llm.ErrBudgetExhausted):
		return "budget"
	case errors.Is(err, llm.ErrCircuitOpen):
		return "circuit"
	default:
		return "other"
	}
}

// writeArchive writes one tuple's dropped records to <archive>/<num>.sst and
// verifies SourceCRC32 by reading the file back (P10.1 step 9). The file
// number was allocated when the draft was built.
func writeArchive(d SemanticDeps, dr *tupleDraft) error {
	num := dr.meta.ArchiveFileNum
	if num == 0 {
		return errors.New("semantic: archive file number not allocated")
	}
	aw := NewOutputWriter(d.FS, d.ArchiveDir, 1<<30, func() uint64 { return num })
	// The SST requires internal-key order (key asc, ts desc, I3); the
	// segment arrives (ts asc, key asc), so reorder for the archive.
	ordered := append([]base.Record(nil), dr.recs...)
	sort.SliceStable(ordered, func(a, b int) bool {
		if c := base.CompareUser(ordered[a].Key, ordered[b].Key); c != 0 {
			return c < 0
		}
		return ordered[a].Ts > ordered[b].Ts
	})
	for _, r := range ordered {
		if err := aw.Add(base.MakeInternalKey(r.Key, r.Ts, r.Kind), r.Value); err != nil {
			aw.Abort()
			return fmt.Errorf("semantic: archive %d: %w", num, err)
		}
	}
	outs, err := aw.Finish()
	if err != nil {
		aw.Abort()
		return fmt.Errorf("semantic: archive %d finish: %w", num, err)
	}
	if len(outs) != 1 {
		aw.Abort()
		return fmt.Errorf("semantic: archive %d produced %d files", num, len(outs))
	}
	// Verify: read back, order by (ts asc, key asc), recompute the CRC.
	r, err := sstable.OpenReader(d.FS, filepath.Join(d.ArchiveDir, sstable.Name(num)))
	if err != nil {
		return fmt.Errorf("semantic: archive %d verify open: %w", num, err)
	}
	defer r.Close()
	var back []base.Record
	it := r.NewIterator()
	for it.First(); it.Valid(); it.Next() {
		user, ts, kind, ok := base.SplitInternalKey(it.Key())
		if !ok {
			it.Close()
			return fmt.Errorf("semantic: archive %d verify: bad internal key", num)
		}
		// Copy: the iterator's buffers are only valid until the next Next.
		back = append(back, base.Record{
			Key:   append([]byte(nil), user...),
			Ts:    ts,
			Kind:  kind,
			Value: append([]byte(nil), it.Value()...),
		})
	}
	if err := it.Err(); err != nil {
		it.Close()
		return fmt.Errorf("semantic: archive %d verify read: %w", num, err)
	}
	it.Close()
	if uint64(len(back)) != dr.meta.SourceCount {
		return fmt.Errorf("semantic: archive %d verify: %d records, want %d", num, len(back), dr.meta.SourceCount)
	}
	sort.Slice(back, func(a, b int) bool {
		if back[a].Ts != back[b].Ts {
			return back[a].Ts < back[b].Ts
		}
		return string(back[a].Key) < string(back[b].Key)
	})
	if crc := base.SourceCRC(back); crc != dr.meta.SourceCRC32 {
		return fmt.Errorf("semantic: archive %d verify: CRC %08x, want %08x", num, crc, dr.meta.SourceCRC32)
	}
	return nil
}
