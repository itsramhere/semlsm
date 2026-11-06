package semantic

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"semlsm/internal/base"
	"semlsm/internal/llm"
)

// ErrPoorCompression is returned when the summary is not small enough
// relative to its source records; the group stays raw until a later job
// (P8.6).
var ErrPoorCompression = errors.New("semantic: summary does not compress the source records enough")

// reduceFanIn is the number of partial summaries merged per reduce call.
const reduceFanIn = 8

// Usage accumulates the LLM consumption of one Summarize call (P8.6).
type Usage struct {
	InputTokens  int64
	OutputTokens int64
	Calls        int64
}

func (u *Usage) add(r llm.Response) {
	u.InputTokens += int64(r.InputTokens)
	u.OutputTokens += int64(r.OutputTokens)
	u.Calls++
}

// merge folds one call's usage delta into the total.
func (u *Usage) merge(o Usage) {
	u.InputTokens += o.InputTokens
	u.OutputTokens += o.OutputTokens
	u.Calls += o.Calls
}

// Summarizer runs the map-reduce summarization pipeline over one segment
// (P8.6). Every response is validated (P8.5); on a ValidationError there is
// exactly one repair retry, then the segment fails. The client is usually
// wrapped by the P9 reliability layer, which owns retries of transient
// errors and budget policy.
type Summarizer struct {
	Client        llm.Client
	MaxValueBytes int
	MaxChunkBytes int
	MinRatio      float64
	Concurrency   int
}

// NewSummarizer builds a Summarizer; concurrency below 1 becomes 1.
func NewSummarizer(c llm.Client, maxValueBytes, maxChunkBytes int, minRatio float64, concurrency int) *Summarizer {
	if concurrency < 1 {
		concurrency = 1
	}
	return &Summarizer{
		Client:        c,
		MaxValueBytes: maxValueBytes,
		MaxChunkBytes: maxChunkBytes,
		MinRatio:      minRatio,
		Concurrency:   concurrency,
	}
}

// Summarize maps the segment's chunks concurrently, reduces the partial
// summaries in groups of up to 8 until one remains, resolves every cited
// record id to (key, ts), and checks the compression ratio (P8.6).
func (s *Summarizer) Summarize(ctx context.Context, seg Segment, group string) (Summary, Usage, error) {
	var usage Usage
	if err := ctx.Err(); err != nil {
		return Summary{}, usage, err
	}
	if len(seg.Records) == 0 {
		return Summary{}, usage, errors.New("semantic: empty segment")
	}
	chunks := RenderChunks(seg.Records, group, s.MaxValueBytes, s.MaxChunkBytes)
	ids := map[string]bool{}
	for _, c := range chunks {
		for _, id := range c.IDs {
			ids[id] = true
		}
	}

	var final Narrative
	if len(chunks) == 1 {
		n, du, err := s.mapCall(ctx, group, 1, 1, chunks[0], ids)
		usage.merge(du)
		if err != nil {
			return Summary{}, usage, err
		}
		final = n
	} else {
		partials := make([]Narrative, len(chunks))
		opCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		sem := make(chan struct{}, s.Concurrency)
		var wg sync.WaitGroup
		var mu sync.Mutex
		var firstErr error
		for i := range chunks {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				select {
				case sem <- struct{}{}:
					defer func() { <-sem }()
				case <-opCtx.Done():
					return
				}
				n, du, err := s.mapCall(opCtx, group, i+1, len(chunks), chunks[i], ids)
				mu.Lock()
				defer mu.Unlock()
				usage.merge(du)
				if err != nil {
					if firstErr == nil {
						firstErr = err
						cancel() // stop the remaining map calls
					}
					return
				}
				partials[i] = n
			}(i)
		}
		wg.Wait()
		if firstErr != nil {
			return Summary{}, usage, firstErr
		}
		// reduce rounds: merge up to 8 partials per call until one remains
		round := partials
		for len(round) > 1 {
			next := make([]Narrative, 0, (len(round)+reduceFanIn-1)/reduceFanIn)
			for start := 0; start < len(round); start += reduceFanIn {
				end := min(start+reduceFanIn, len(round))
				n, du, err := s.reduceCall(ctx, group, round[start:end])
				usage.merge(du)
				if err != nil {
					return Summary{}, usage, err
				}
				next = append(next, n)
			}
			round = next
		}
		final = round[0]
	}

	if err := resolveRefs(&final, seg.Records); err != nil {
		return Summary{}, usage, err
	}
	out := Summary{
		SchemaVersion: 1,
		Stats:         ComputeStats(seg.Records),
		Narrative:     final,
	}
	if err := s.checkCompression(seg, out); err != nil {
		return Summary{}, usage, err
	}
	return out, usage, nil
}

// mapCall summarizes one chunk, with one repair retry on ValidationError.
// It returns the usage this call consumed; callers merge it, so the shared
// Usage is never written concurrently.
func (s *Summarizer) mapCall(ctx context.Context, group string, index, total int, c Chunk, ids map[string]bool) (Narrative, Usage, error) {
	req := llm.Request{
		System:          SystemPrompt(),
		User:            BuildUserPrompt(group, index, total, c),
		MaxOutputTokens: 2048,
		Metadata: map[string]string{
			"stage":          "map",
			"prompt_version": strconv.Itoa(PromptVersion),
			"group":          group,
		},
	}
	return s.callAndValidate(ctx, req, ids)
}

// reduceCall merges up to reduceFanIn partial summaries. The valid ids are
// the union of the input summaries' refs (P8.6).
func (s *Summarizer) reduceCall(ctx context.Context, group string, parts []Narrative) (Narrative, Usage, error) {
	valid := map[string]bool{}
	jsons := make([]string, len(parts))
	for i, p := range parts {
		data, err := NarrativeJSON(p)
		if err != nil {
			return Narrative{}, Usage{}, err
		}
		jsons[i] = string(data)
		for _, f := range p.KeyFacts {
			for _, r := range f.Refs {
				valid[r] = true
			}
		}
		for _, f := range p.Anomalies {
			for _, r := range f.Refs {
				valid[r] = true
			}
		}
	}
	req := llm.Request{
		System:          ReduceSystemPrompt(),
		User:            BuildReduceUserPrompt(group, jsons),
		MaxOutputTokens: 2048,
		Metadata: map[string]string{
			"stage":          "reduce",
			"prompt_version": strconv.Itoa(PromptVersion),
			"group":          group,
		},
	}
	return s.callAndValidate(ctx, req, valid)
}

// callAndValidate runs one LLM call, validates the response, and retries
// once with the repair instruction appended to the user prompt (P8.6). The
// returned Usage is the delta this call consumed; callers merge it, so the
// shared Usage is never written concurrently.
func (s *Summarizer) callAndValidate(ctx context.Context, req llm.Request, ids map[string]bool) (Narrative, Usage, error) {
	var u Usage
	resp, err := s.Client.Complete(ctx, req)
	if err != nil {
		return Narrative{}, u, err
	}
	u.add(resp)
	n, verr := ParseAndValidate(resp.Text, ids)
	if verr == nil {
		return n, u, nil
	}
	repair := req
	repair.User = req.User + "\nYour previous output was invalid: " + verr.Error() + ". Return only corrected JSON."
	resp2, err := s.Client.Complete(ctx, repair)
	if err != nil {
		return Narrative{}, u, err
	}
	u.add(resp2)
	n, verr = ParseAndValidate(resp2.Text, ids)
	if verr != nil {
		return Narrative{}, u, verr
	}
	return n, u, nil
}

// resolveRefs maps every cited record id to (key, ts) on the final
// narrative (P8.6). Ids are r<i> in segment order.
func resolveRefs(n *Narrative, records []base.Record) error {
	resolve := func(where string, facts []Fact) error {
		for i := range facts {
			refs := make([]RecordRef, 0, len(facts[i].Refs))
			for _, r := range facts[i].Refs {
				idx, err := strconv.Atoi(strings.TrimPrefix(r, "r"))
				if err != nil || idx < 0 || idx >= len(records) {
					return fmt.Errorf("semantic: %s[%d] cites %q outside the segment", where, i, r)
				}
				refs = append(refs, RecordRef{
					ID:  r,
					Key: append([]byte(nil), records[idx].Key...),
					Ts:  records[idx].Ts,
				})
			}
			facts[i].ResolvedRefs = refs
		}
		return nil
	}
	if err := resolve("key_facts", n.KeyFacts); err != nil {
		return err
	}
	return resolve("anomalies", n.Anomalies)
}

// checkCompression enforces SourceBytes / (summary JSON + key index bytes)
// >= MinRatio (P8.6). The key index size is estimated conservatively (len +
// 32 per distinct key), so marginal cases fail toward staying raw.
func (s *Summarizer) checkCompression(seg Segment, out Summary) error {
	var src uint64
	keys := map[string]bool{}
	for _, r := range seg.Records {
		src += uint64(len(r.Key) + len(r.Value) + 9) // klen+key + ts(8) + kind(1) + vlen+value
		keys[string(r.Key)] = true
	}
	if src == 0 {
		return nil
	}
	data, err := NarrativeJSON(out.Narrative)
	if err != nil {
		return err
	}
	outBytes := len(data)
	for k := range keys {
		outBytes += len(k) + 32
	}
	if float64(src)/float64(outBytes) < s.MinRatio {
		return fmt.Errorf("semantic: %w (source %d bytes, output %d bytes, ratio %.2f < %.2f)",
			ErrPoorCompression, src, outBytes, float64(src)/float64(outBytes), s.MinRatio)
	}
	return nil
}
