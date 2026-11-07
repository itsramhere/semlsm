package llm

import (
	"context"
	"sync"
	"testing"
	"time"
)

// waitDone runs Wait on a goroutine so a ManualClock can move while it
// blocks; returns a channel delivering the outcome.
func waitDone(l *Limiter, tokens int) <-chan error {
	ch := make(chan error, 1)
	go func() { ch <- l.Wait(context.Background(), tokens) }()
	return ch
}

// settled waits for one outcome without depending on timing for correctness:
// the ManualClock only moves when the test advances it explicitly.
func settled(t *testing.T, ch <-chan error) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("Wait did not return")
		return nil
	}
}

func TestLimiterImmediateWithinBurst(t *testing.T) {
	clk := NewManualClock(time.Unix(1_000_000, 0))
	l, err := NewLimiter(60, 6000, 4, clk)
	if err != nil {
		t.Fatal(err)
	}
	// The buckets start full: three requests fit without any time passing.
	for i := 0; i < 3; i++ {
		if err := l.Wait(context.Background(), 100); err != nil {
			t.Fatal(err)
		}
		l.Release()
	}
	if l.InFlight() != 0 {
		t.Fatalf("in flight after release: %d", l.InFlight())
	}
}

func TestLimiterBlocksUntilRefill(t *testing.T) {
	clk := NewManualClock(time.Unix(1_000_000, 0))
	l, err := NewLimiter(1, 6000, 4, clk) // burst of 1 request, 100 tokens/s
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Wait(context.Background(), 100); err != nil {
		t.Fatal(err)
	}
	l.Release()

	// The request bucket is empty (burst 1): the second Wait must block.
	ch := waitDone(l, 100)
	select {
	case err := <-ch:
		t.Fatalf("second Wait returned before refill: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	// 30 virtual seconds refill 0.5 requests and 3000 tokens: still short.
	clk.Advance(30 * time.Second)
	select {
	case err := <-ch:
		t.Fatalf("Wait returned at +30s, want block: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	// At +60s a full request has accrued.
	clk.Advance(30 * time.Second)
	if err := settled(t, ch); err != nil {
		t.Fatalf("Wait at +60s: %v", err)
	}
	l.Release()
}

func TestLimiterTokenBucketShortage(t *testing.T) {
	clk := NewManualClock(time.Unix(1_000_000, 0))
	l, err := NewLimiter(600, 120, 4, clk) // plenty of requests, 2 tokens/s
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Wait(context.Background(), 120); err != nil { // drains tokens
		t.Fatal(err)
	}
	l.Release()
	ch := waitDone(l, 120) // needs a full minute of tokens
	select {
	case err := <-ch:
		t.Fatalf("returned before refill: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	clk.Advance(59 * time.Second)
	select {
	case err := <-ch:
		t.Fatalf("returned at +59s: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	clk.Advance(1 * time.Second)
	if err := settled(t, ch); err != nil {
		t.Fatalf("Wait at +60s: %v", err)
	}
	l.Release()
}

func TestLimiterConcurrencySemaphore(t *testing.T) {
	clk := NewManualClock(time.Unix(1_000_000, 0))
	l, err := NewLimiter(6000, 1_000_000, 2, clk)
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{}, 4)
	release := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := l.Wait(context.Background(), 10); err != nil {
				t.Error(err)
				return
			}
			started <- struct{}{}
			<-release
			l.Release()
		}()
	}
	// Exactly 2 get slots; the other 2 stay out.
	<-started
	<-started
	select {
	case <-started:
		t.Fatal("more than 2 concurrent Wait holders")
	case <-time.After(20 * time.Millisecond):
	}
	if l.InFlight() != 2 {
		t.Fatalf("in flight: %d", l.InFlight())
	}
	close(release)
	wg.Wait()
}

func TestLimiterWaitContextCancelled(t *testing.T) {
	clk := NewManualClock(time.Unix(1_000_000, 0))
	l, err := NewLimiter(1, 1, 1, clk)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	if err := l.Wait(ctx, 1); err != nil { // takes the only slot and the burst
		t.Fatal(err)
	}
	l.Release()
	ch := waitDoneL(l, ctx, 1)
	cancel()
	if err := <-ch; err == nil {
		t.Fatal("cancelled Wait must fail")
	}
	if l.InFlight() != 0 {
		t.Fatalf("slot leaked on cancel: %d", l.InFlight())
	}
	// The cancelled Wait must not have charged tokens: after a refill of the
	// request bucket the next caller succeeds despite the same budget.
	clk.Advance(61 * time.Second)
	if err := l.Wait(context.Background(), 1); err != nil {
		t.Fatalf("next Wait after cancellation: %v", err)
	}
	l.Release()
}

func waitDoneL(l *Limiter, ctx context.Context, tokens int) <-chan error {
	ch := make(chan error, 1)
	go func() { ch <- l.Wait(ctx, tokens) }()
	return ch
}

func TestLimiterInvalidRates(t *testing.T) {
	if _, err := NewLimiter(0, 100, 1, nil); err == nil {
		t.Fatal("zero request rate accepted")
	}
	if _, err := NewLimiter(60, -1, 1, nil); err == nil {
		t.Fatal("negative token rate accepted")
	}
	// concurrency below 1 becomes 1, nil clock becomes RealClock
	l, err := NewLimiter(60, 100, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Wait(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	l.Release()
}

func TestEstimateTokens(t *testing.T) {
	r := Request{System: "abc", User: "def", MaxOutputTokens: 7}
	if got := EstimateTokens(r); got != 9 { // 6 bytes / 3 + 7
		t.Fatalf("estimate: %d, want 9", got)
	}
	if got := EstimateTokens(Request{}); got != 1 {
		t.Fatalf("empty estimate: %d, want 1", got)
	}
}
