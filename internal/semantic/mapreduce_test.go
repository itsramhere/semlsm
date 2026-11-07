package semantic

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/itsramhere/semlsm/internal/base"
	"github.com/itsramhere/semlsm/internal/llm"
)

func segOf(n int) Segment {
	seg := Segment{}
	for i := 0; i < n; i++ {
		seg.Records = append(seg.Records, base.Record{
			Key:   []byte(fmt.Sprintf("g/key%04d", i)),
			Ts:    uint64(1_000_000 + i),
			Kind:  base.KindPut,
			Value: []byte(fmt.Sprintf("value payload number %04d %s", i, strings.Repeat("p", 570))),
		})
	}
	return seg
}

func testSummarizer(c llm.Client) *Summarizer {
	return NewSummarizer(c, 2000, 1<<20, 5.0, 4)
}

func TestSummarizeSingleChunk(t *testing.T) {
	mock := llm.NewMock(llm.MockValid)
	s := testSummarizer(mock)
	sum, usage, err := s.Summarize(context.Background(), segOf(5), "g/")
	if err != nil {
		t.Fatal(err)
	}
	if sum.SchemaVersion != 1 {
		t.Fatalf("schema version %d", sum.SchemaVersion)
	}
	if sum.Narrative.Summary == "" || len(sum.Narrative.KeyFacts) == 0 {
		t.Fatalf("narrative: %+v", sum.Narrative)
	}
	if usage.Calls != 1 {
		t.Fatalf("calls: %d", usage.Calls)
	}
	if sum.Stats.RecordCount != 5 || sum.Stats.DistinctKeys != 5 {
		t.Fatalf("stats: %+v", sum.Stats)
	}
	// refs resolved to (key, ts)
	f := sum.Narrative.KeyFacts[0]
	if len(f.ResolvedRefs) == 0 {
		t.Fatalf("refs not resolved: %+v", f)
	}
	for _, rr := range f.ResolvedRefs {
		idx := int(rr.Ts - 1_000_000)
		if idx < 0 || idx >= 5 || string(rr.Key) != fmt.Sprintf("g/key%04d", idx) {
			t.Fatalf("resolved ref %+v", rr)
		}
	}
}

func TestSummarizeEveryMockMode(t *testing.T) {
	injected := `{"summary":"injected but valid","key_facts":[{"text":"t","refs":["r0"]}],"anomalies":[],"final_state":""}`
	cases := []struct {
		name       string
		client     llm.Client
		wantSentin error // matched with errors.Is; nil means want success
		validation bool  // want a *ValidationError after the repair retry
	}{
		{"valid", llm.NewMock(llm.MockValid), nil, false},
		{"fenced", llm.NewMock(llm.MockFencedJSON), nil, false},
		{"injected", llm.NewMockInjected(injected), nil, false},
		{"bad json", llm.NewMock(llm.MockBadJSON), nil, true},
		{"hallucinated", llm.NewMock(llm.MockHallucinatedRef), nil, true},
		{"too long", llm.NewMock(llm.MockTooLong), nil, true},
		{"transient", llm.NewMockTransient(1), llm.ErrTransient, false},
		{"permanent", llm.NewMock(llm.MockPermanent), llm.ErrPermanent, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := testSummarizer(tc.client).Summarize(context.Background(), segOf(3), "g/")
			if tc.wantSentin == nil && !tc.validation {
				if err != nil {
					t.Fatalf("want success, got %v", err)
				}
				return
			}
			if err == nil {
				t.Fatal("want error")
			}
			if tc.validation {
				if _, ok := err.(*ValidationError); !ok {
					t.Fatalf("want ValidationError, got %v", err)
				}
				return
			}
			if !errors.Is(err, tc.wantSentin) {
				t.Fatalf("got %v, want %v", err, tc.wantSentin)
			}
		})
	}
}

func TestSummarizeTwoRepairCallsThenFail(t *testing.T) {
	mock := llm.NewMock(llm.MockBadJSON)
	_, _, err := testSummarizer(mock).Summarize(context.Background(), segOf(3), "g/")
	if err == nil {
		t.Fatal("BadJSON must fail the segment after the repair retry")
	}
	ve, ok := err.(*ValidationError)
	if !ok {
		t.Fatalf("want ValidationError, got %v", err)
	}
	if ve.Rule != "V2" {
		t.Fatalf("rule: %s", ve.Rule)
	}
	// one call + one repair call
	if mock.Calls() != 2 {
		t.Fatalf("calls: %d, want 2 (initial + repair)", mock.Calls())
	}
}

func TestSummarizeTenThousandRecords(t *testing.T) {
	mock := llm.NewMock(llm.MockValid)
	s := NewSummarizer(mock, 2000, 4096, 5.0, 8)
	sum, usage, err := s.Summarize(context.Background(), segOf(10000), "g/")
	if err != nil {
		t.Fatal(err)
	}
	if sum.Stats.RecordCount != 10000 || sum.Stats.DistinctKeys != 10000 {
		t.Fatalf("stats: %+v", sum.Stats)
	}
	if usage.Calls < 10 {
		t.Fatalf("expected many calls (map+reduce), got %d", usage.Calls)
	}
	if len(sum.Narrative.KeyFacts) == 0 || len(sum.Narrative.KeyFacts[0].ResolvedRefs) == 0 {
		t.Fatalf("final narrative incomplete: %+v", sum.Narrative)
	}
	// every cited ref resolves into the segment
	for _, f := range sum.Narrative.KeyFacts {
		for _, rr := range f.ResolvedRefs {
			if !strings.HasPrefix(string(rr.Key), "g/key") {
				t.Fatalf("bad resolved ref %+v", rr)
			}
		}
	}
}

// overlapClient answers with a fixed valid narrative and tracks the peak
// number of concurrent Complete calls (R10: no wall-clock assertions).
type overlapClient struct {
	mu  sync.Mutex
	cur int
	max int
}

const overlapResponse = `{"summary":"s","key_facts":[{"text":"t","refs":["r0"]}],"anomalies":[],"final_state":""}`

func (c *overlapClient) Complete(ctx context.Context, r llm.Request) (llm.Response, error) {
	c.mu.Lock()
	c.cur++
	if c.cur > c.max {
		c.max = c.cur
	}
	c.mu.Unlock()
	time.Sleep(20 * time.Millisecond)
	c.mu.Lock()
	c.cur--
	c.mu.Unlock()
	return llm.Response{Text: overlapResponse, Model: "overlap"}, nil
}

func TestSummarizeMapConcurrency(t *testing.T) {
	c := &overlapClient{}
	s := NewSummarizer(c, 2000, 700, 5.0, 4) // 700B chunks -> several chunks
	_, _, err := s.Summarize(context.Background(), segOf(10), "g/")
	if err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.max < 2 {
		t.Fatalf("map calls never overlapped (max in flight = %d)", c.max)
	}
}

func TestSummarizeCancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	start := time.Now()
	_, _, err := testSummarizer(llm.NewMock(llm.MockBlock)).Summarize(ctx, segOf(3), "g/")
	if err == nil {
		t.Fatal("cancelled ctx must fail")
	}
	if time.Since(start) > 50*time.Millisecond {
		t.Fatalf("cancelled ctx did not return quickly: %v", time.Since(start))
	}
	// a hanging client with a deadline also returns quickly
	ctx2, cancel2 := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel2()
	start = time.Now()
	_, _, err = testSummarizer(llm.NewMock(llm.MockBlock)).Summarize(ctx2, segOf(3), "g/")
	if err == nil {
		t.Fatal("deadline ctx must fail")
	}
	if time.Since(start) > 500*time.Millisecond {
		t.Fatalf("deadline did not stop the call: %v", time.Since(start))
	}
}

func TestSummarizePoorCompression(t *testing.T) {
	// a valid-length narrative over small records must fail the ratio check
	big := `{"summary":"` + strings.Repeat("x", 1900) + `","key_facts":[{"text":"t","refs":["r0"]}],"anomalies":[],"final_state":""}`
	tiny := Segment{Records: []base.Record{
		{Key: []byte("g/a"), Ts: 1, Kind: base.KindPut, Value: []byte(strings.Repeat("v", 150))},
		{Key: []byte("g/b"), Ts: 2, Kind: base.KindPut, Value: []byte(strings.Repeat("v", 150))},
	}}
	s := testSummarizer(llm.NewMockInjected(big))
	s.MinRatio = 5.0
	_, _, err := s.Summarize(context.Background(), tiny, "g/")
	if !errors.Is(err, ErrPoorCompression) {
		t.Fatalf("want ErrPoorCompression, got %v", err)
	}
}
