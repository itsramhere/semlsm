package llm

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"semlsm/internal/vfs"
)

// countingClient counts Complete calls and answers per the given modes.
type countingClient struct {
	calls    int64
	failWith error
	resp     Response
}

func (c *countingClient) Complete(ctx context.Context, r Request) (Response, error) {
	c.calls++
	if c.failWith != nil {
		return Response{}, c.failWith
	}
	return c.resp, nil
}

func stackConfig(dir string, maxTokens int64) Config {
	return Config{
		Model:             "test-model",
		PromptVersion:     3,
		FS:                vfs.NewMemFS(),
		Dir:               dir,
		MaxTokensTotal:    maxTokens,
		RequestsPerMinute: 600,
		TokensPerMinute:   600_000,
		Concurrency:       2,
		CacheMaxBytes:     1 << 20,
	}
}

func validResponse(text string) Response {
	return Response{Text: text, InputTokens: 10, OutputTokens: 5, Model: "upstream"}
}

func TestStackConstructorRejectsBadConfig(t *testing.T) {
	next := &countingClient{}
	if _, err := NewSummarizer(next, Config{Dir: "db"}); err == nil {
		t.Fatal("zero MaxTokensTotal must be rejected")
	}
	if _, err := NewSummarizer(nil, stackConfig("db", 1000)); err == nil {
		t.Fatal("nil client must be rejected")
	}
	if _, err := NewSummarizer(next, func() Config {
		c := stackConfig("db", 1000)
		c.Dir = ""
		return c
	}()); err == nil {
		t.Fatal("empty Dir must be rejected")
	}
	if _, err := NewSummarizer(next, func() Config {
		c := stackConfig("db", 1000)
		c.RequestsPerMinute = 0
		return c
	}()); err == nil {
		t.Fatal("zero request rate must be rejected")
	}
}

func TestStackHappyPathAndCacheHitSkipsAll(t *testing.T) {
	next := &countingClient{resp: validResponse(`{"summary":"ok"}`)}
	s, err := NewSummarizer(next, stackConfig("db", 100_000))
	if err != nil {
		t.Fatal(err)
	}
	req := Request{System: "sys", User: "usr", MaxOutputTokens: 30}
	if _, err := s.Complete(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	if next.calls != 1 {
		t.Fatalf("calls: %d", next.calls)
	}
	// The response was cached and the budget recorded actual usage.
	in, out, calls := s.budget.Used()
	if in != 10 || out != 5 || calls != 1 {
		t.Fatalf("budget: %d/%d calls=%d", in, out, calls)
	}

	// Break the budget and the breaker; the cache hit must bypass both.
	s.budget.pending = 1_000_000 // simulate an unrefundable backlog
	for i := 0; i < DefaultTripThreshold; i++ {
		s.breaker.RecordError(transientErr())
	}
	resp, err := s.Complete(context.Background(), req)
	if err != nil {
		t.Fatalf("cache hit must skip all layers: %v", err)
	}
	if resp.Text != `{"summary":"ok"}` {
		t.Fatalf("cache hit: %q", resp.Text)
	}
	if next.calls != 1 || s.CacheHits() != 1 {
		t.Fatalf("calls=%d hits=%d", next.calls, s.CacheHits())
	}
	// Model/PromptVersion accessors (Summarizer interface).
	if s.Model() != "test-model" || s.PromptVersion() != 3 {
		t.Fatalf("identity: %s/%d", s.Model(), s.PromptVersion())
	}
}

func TestStackBudgetExhaustedBeforeBreaker(t *testing.T) {
	next := &countingClient{resp: validResponse("v")}
	cfg := stackConfig("db", 10) // tiny: the first estimate already busts it
	s, err := NewSummarizer(next, cfg)
	if err != nil {
		t.Fatal(err)
	}
	// Estimate for this request: (3+3)/3 + 30 = 32 > 10.
	_, err = s.Complete(context.Background(), Request{System: "sys", User: "usr", MaxOutputTokens: 30})
	if !errors.Is(err, ErrBudgetExhausted) {
		t.Fatalf("want ErrBudgetExhausted, got %v", err)
	}
	if next.calls != 0 {
		t.Fatal("no call may reach the client")
	}
}

func TestStackBreakerOpen(t *testing.T) {
	next := &countingClient{failWith: fmt.Errorf("%w: down", ErrTransient)}
	cfg := stackConfig("db", 1_000_000)
	cfg.CacheMaxBytes = 0
	s, err := NewSummarizer(next, cfg)
	if err != nil {
		t.Fatal(err)
	}
	req := Request{System: "sys", User: "usr", MaxOutputTokens: 30}
	for i := 0; i < DefaultTripThreshold; i++ {
		if _, err := s.Complete(context.Background(), req); !errors.Is(err, ErrTransient) {
			t.Fatalf("call %d: %v", i, err)
		}
	}
	// Tripped: further calls are refused without reaching the client.
	if _, err := s.Complete(context.Background(), req); !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("want ErrCircuitOpen, got %v", err)
	}
	if next.calls != DefaultTripThreshold {
		t.Fatalf("calls: %d", next.calls)
	}
	state, _ := s.BreakerState()
	if state != "open" {
		t.Fatalf("state: %s", state)
	}
	// Failed attempts must not consume budget: the estimate is refunded.
	in, out, calls := s.budget.Used()
	if in != 0 || out != 0 || calls != 0 {
		t.Fatalf("budget after failures: %d/%d calls=%d", in, out, calls)
	}
}

func TestStackCacheStoresOnlyAcceptedResponses(t *testing.T) {
	next := &countingClient{resp: validResponse("garbage")}
	cfg := stackConfig("db", 1_000_000)
	cfg.Accept = func(req Request, resp Response) error {
		if strings.Contains(resp.Text, "garbage") {
			return errors.New("invalid response")
		}
		return nil
	}
	s, err := NewSummarizer(next, cfg)
	if err != nil {
		t.Fatal(err)
	}
	req := Request{System: "sys", User: "usr", MaxOutputTokens: 30}
	for i := 0; i < 2; i++ {
		if _, err := s.Complete(context.Background(), req); err != nil {
			t.Fatal(err)
		}
	}
	if next.calls != 2 || s.CacheHits() != 0 {
		t.Fatalf("rejected responses must not be cached: calls=%d hits=%d", next.calls, s.CacheHits())
	}

	// An accepted response is cached and served without the client.
	next.resp = validResponse("good")
	if _, err := s.Complete(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	next.failWith = fmt.Errorf("%w: down", ErrTransient)
	resp, err := s.Complete(context.Background(), req)
	if err != nil || resp.Text != "good" {
		t.Fatalf("cache hit after accept: %v %q", err, resp.Text)
	}
}

func TestStackCacheDisabledWithoutCap(t *testing.T) {
	next := &countingClient{resp: validResponse("v")}
	cfg := stackConfig("db", 1_000_000)
	cfg.CacheMaxBytes = 0
	s, err := NewSummarizer(next, cfg)
	if err != nil {
		t.Fatal(err)
	}
	req := Request{System: "sys", User: "usr", MaxOutputTokens: 30}
	for i := 0; i < 3; i++ {
		if _, err := s.Complete(context.Background(), req); err != nil {
			t.Fatal(err)
		}
	}
	if next.calls != 3 || s.CacheHits() != 0 {
		t.Fatalf("no cache layer expected: calls=%d hits=%d", next.calls, s.CacheHits())
	}
}

func TestStackLimiterApplies(t *testing.T) {
	clk := NewManualClock(time.Unix(1_000_000, 0))
	next := &countingClient{resp: validResponse("v")}
	cfg := stackConfig("db", 1_000_000)
	cfg.CacheMaxBytes = 0
	cfg.RequestsPerMinute = 1 // burst of exactly 1 request
	cfg.TokensPerMinute = 600_000
	cfg.Clock = clk
	s, err := NewSummarizer(next, cfg)
	if err != nil {
		t.Fatal(err)
	}
	req := Request{System: "sys", User: "usr", MaxOutputTokens: 30}
	if _, err := s.Complete(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := s.Complete(context.Background(), req)
		done <- err
	}()
	select {
	case err := <-done:
		t.Fatalf("second call must wait for the limiter: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	clk.Advance(61 * time.Second)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("second call never completed")
	}
	if next.calls != 2 {
		t.Fatalf("calls: %d", next.calls)
	}
}

func TestStackSurvivesReopenWithSameFS(t *testing.T) {
	fs := vfs.NewMemFS()
	req := Request{System: "sys", User: "usr", MaxOutputTokens: 30}

	cfg := stackConfig("db", 1_000_000)
	cfg.FS = fs
	next := &countingClient{resp: validResponse("v1")}
	s1, err := NewSummarizer(next, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s1.Complete(context.Background(), req); err != nil {
		t.Fatal(err)
	}

	// Reopen: usage.json persists, and the response is still cached.
	cfg2 := cfg
	next2 := &countingClient{failWith: fmt.Errorf("%w: down", ErrTransient)}
	s2, err := NewSummarizer(next2, cfg2)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := s2.Complete(context.Background(), req)
	if err != nil || resp.Text != "v1" {
		t.Fatalf("cache hit after reopen: %v %q", err, resp.Text)
	}
	in, out, calls := s2.budget.Used()
	if in != 10 || out != 5 || calls != 1 {
		t.Fatalf("persisted usage: %d/%d calls=%d", in, out, calls)
	}
	if next2.calls != 0 {
		t.Fatalf("upstream called %d times after reopen", next2.calls)
	}
}

func TestStackContextCancelReleasesEverything(t *testing.T) {
	clk := NewManualClock(time.Unix(1_000_000, 0))
	cfg := stackConfig("db", 1_000_000)
	cfg.CacheMaxBytes = 0
	cfg.RequestsPerMinute = 1 // burst of 1
	cfg.TokensPerMinute = 600_000
	cfg.Clock = clk
	next := &countingClient{resp: validResponse("v")}
	s, err := NewSummarizer(next, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Complete(context.Background(), Request{System: "sys", User: "usr", MaxOutputTokens: 30}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := s.Complete(ctx, Request{System: "sys", User: "usr", MaxOutputTokens: 30})
		done <- err
	}()
	cancel()
	if err := <-done; err == nil {
		t.Fatal("cancelled call must fail")
	}
	// The estimate was refunded and the limiter slot returned.
	if s.limiter.InFlight() != 0 {
		t.Fatalf("limiter slot leaked: %d", s.limiter.InFlight())
	}
	clk.Advance(61 * time.Second)
	if err := s.budget.Reserve(1000); err != nil {
		t.Fatalf("budget must be fully refundable: %v", err)
	}
}
