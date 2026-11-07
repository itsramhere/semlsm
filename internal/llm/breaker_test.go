package llm

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

func transientErr() error { return fmt.Errorf("%w: boom", ErrTransient) }

func TestBreakerTripsAfterConsecutiveTransients(t *testing.T) {
	clk := NewManualClock(time.Unix(1_000_000, 0))
	b := NewBreaker(clk)
	for i := 0; i < DefaultTripThreshold-1; i++ {
		if err := b.Allow(); err != nil {
			t.Fatalf("call %d refused: %v", i, err)
		}
		b.RecordError(transientErr())
	}
	if err := b.Allow(); err != nil {
		t.Fatalf("closed below threshold, got %v", err)
	}
	// The 5th consecutive transient trips it.
	b.RecordError(transientErr())
	if err := b.Allow(); !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("want ErrCircuitOpen, got %v", err)
	}
	state, remain := b.State()
	if state != "open" || remain <= 0 || remain > DefaultOpenFor {
		t.Fatalf("state %s remain %v", state, remain)
	}
}

func TestBreakerHalfOpenTrial(t *testing.T) {
	clk := NewManualClock(time.Unix(1_000_000, 0))
	b := NewBreaker(clk)
	for i := 0; i < DefaultTripThreshold; i++ {
		b.RecordError(transientErr())
	}
	if err := b.Allow(); !errors.Is(err, ErrCircuitOpen) {
		t.Fatal("breaker should be open")
	}
	// Before the open window elapses: still refused.
	clk.Advance(59 * time.Second)
	if err := b.Allow(); !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("open window not elapsed, got %v", err)
	}
	// At +60s: one trial.
	clk.Advance(1 * time.Second)
	if err := b.Allow(); err != nil {
		t.Fatalf("trial refused: %v", err)
	}
	// A second concurrent caller gets nothing while the trial is out.
	if err := b.Allow(); !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("second call during trial: %v", err)
	}
	// Trial succeeds -> closed, streak reset, open duration back to base.
	b.RecordSuccess()
	state, _ := b.State()
	if state != "closed" {
		t.Fatalf("state after success: %s", state)
	}
	for i := 0; i < DefaultTripThreshold; i++ {
		b.RecordError(transientErr())
	}
	state, remain := b.State()
	if state != "open" || remain > DefaultOpenFor {
		t.Fatalf("retrip: state %s remain %v (want base 60s)", state, remain)
	}
}

func TestBreakerDoublingOpenFor(t *testing.T) {
	clk := NewManualClock(time.Unix(1_000_000, 0))
	b := NewBreaker(clk)
	want := DefaultOpenFor
	for range 12 { // 60s, 120s, ... capped at 30m
		for i := 0; i < DefaultTripThreshold; i++ {
			b.RecordError(transientErr())
		}
		if err := b.Allow(); !errors.Is(err, ErrCircuitOpen) {
			t.Fatal("want open")
		}
		_, remain := b.State()
		if remain > want {
			t.Fatalf("open for %v, want <= %v", remain, want)
		}
		if remain <= want/2 {
			t.Fatalf("open for %v, want about %v", remain, want)
		}
		clk.Advance(want) // reach half-open
		if err := b.Allow(); err != nil {
			t.Fatalf("trial refused: %v", err)
		}
		b.RecordError(transientErr()) // trial fails -> open again, doubled
		if want < MaxOpenFor {
			want = min(want*2, MaxOpenFor)
		}
	}
	state, remain := b.State()
	if state != "open" || remain > MaxOpenFor {
		t.Fatalf("cap exceeded: state %s remain %v", state, remain)
	}
}

func TestBreakerPermanentAndContextErrorsDoNotCount(t *testing.T) {
	clk := NewManualClock(time.Unix(1_000_000, 0))
	b := NewBreaker(clk)
	for i := 0; i < DefaultTripThreshold-1; i++ {
		b.RecordError(transientErr())
	}
	b.RecordError(fmt.Errorf("%w: bad request", ErrPermanent)) // ignored
	b.RecordError(context.Canceled)                            // ignored
	if err := b.Allow(); err != nil {
		t.Fatalf("non-transient errors must not count, got %v", err)
	}
	b.RecordError(transientErr()) // 5th transient trips
	if err := b.Allow(); !errors.Is(err, ErrCircuitOpen) {
		t.Fatal("want open")
	}
}

func TestBreakerSuccessResetsStreak(t *testing.T) {
	b := NewBreaker(NewManualClock(time.Unix(1_000_000, 0)))
	for i := 0; i < DefaultTripThreshold-1; i++ {
		b.RecordError(transientErr())
	}
	b.RecordSuccess()
	for i := 0; i < DefaultTripThreshold-1; i++ {
		b.RecordError(transientErr())
	}
	if err := b.Allow(); err != nil {
		t.Fatalf("streak must reset on success, got %v", err)
	}
}
