package llm

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

// Breaker states.
type breakerState int

const (
	breakerClosed breakerState = iota
	breakerOpen
	breakerHalfOpen
)

const (
	// DefaultTripThreshold is the number of consecutive transient failures
	// that trips the breaker (P9.2).
	DefaultTripThreshold = 5
	// DefaultOpenFor is the first open duration; it doubles on every failed
	// half-open trial up to MaxOpenFor.
	DefaultOpenFor = 60 * time.Second
	// MaxOpenFor caps the doubling (P9.2).
	MaxOpenFor = 30 * time.Minute
)

// Breaker is a circuit breaker over transient failures (P9.2): closed until
// DefaultTripThreshold consecutive transient failures, then open for
// DefaultOpenFor (doubling to MaxOpenFor on every failed trial), then
// half-open with exactly one trial call. A successful trial closes it again.
// All time comes from the injected Clock.
type Breaker struct {
	clock     Clock
	threshold int
	base      time.Duration

	mu         sync.Mutex
	state      breakerState
	streak     int // consecutive transient failures while closed
	openFor    time.Duration
	openUntil  time.Time
	trialTaken bool
}

// NewBreaker builds a Breaker with the plan's defaults. A nil clock means
// RealClock.
func NewBreaker(clock Clock) *Breaker {
	if clock == nil {
		clock = RealClock{}
	}
	return &Breaker{
		clock:     clock,
		threshold: DefaultTripThreshold,
		base:      DefaultOpenFor,
		state:     breakerClosed,
		openFor:   DefaultOpenFor,
	}
}

// Allow reports whether a call may start now. It reserves the half-open
// trial slot: after a true return in half-open state, further calls fail
// with ErrCircuitOpen until the trial's outcome is recorded.
func (b *Breaker) Allow() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.state {
	case breakerClosed:
		return nil
	case breakerHalfOpen:
		if b.trialTaken {
			return fmt.Errorf("%w: trial call in flight", ErrCircuitOpen)
		}
		b.trialTaken = true
		return nil
	default: // open
		now := b.clock.Now()
		if !now.Before(b.openUntil) {
			b.state = breakerHalfOpen
			b.trialTaken = true
			return nil
		}
		return fmt.Errorf("%w: retry at %s (%s from now)", ErrCircuitOpen,
			b.openUntil.Format(time.RFC3339), b.openUntil.Sub(now).Round(time.Millisecond))
	}
}

// RecordSuccess records a successful call: it closes the breaker and resets
// the failure streak and the open duration.
func (b *Breaker) RecordSuccess() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.close()
}

// RecordError records a call outcome. While closed, only ErrTransient
// failures count toward the trip threshold; permanent and context errors say
// nothing about availability and are ignored. In half-open state the trial
// always ends (M-025 item 1): a transient failure re-opens the breaker with
// a doubled duration, and ANY other error closes it — a 4xx proves the
// endpoint answered, and a client-side cancellation is not evidence of
// unhealth. Neither may wedge the breaker in half-open.
func (b *Breaker) RecordError(err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.state {
	case breakerClosed:
		if errors.Is(err, ErrTransient) {
			b.streak++
			if b.streak >= b.threshold {
				b.trip(b.base)
			}
		}
	case breakerHalfOpen:
		if errors.Is(err, ErrTransient) {
			b.trip(min(b.openFor*2, MaxOpenFor))
			return
		}
		b.close()
	default: // open: the call was not allowed; ignore
	}
}

// Cancel gives a half-open trial back when the call never reached the
// upstream client — ctx cancellation at the limiter, a budget or limiter
// refusal after Allow (M-025 item 2). The breaker returns to the open state
// it was in: if the cool-down window has since elapsed, the next Allow
// grants a fresh trial. Outside half-open it is a no-op.
func (b *Breaker) Cancel() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state == breakerHalfOpen {
		b.state = breakerOpen
		b.trialTaken = false
	}
}

// close returns the breaker to closed and resets all counters (must hold mu).
func (b *Breaker) close() {
	b.state = breakerClosed
	b.streak = 0
	b.trialTaken = false
	b.openFor = b.base
}

// trip opens the breaker for the given duration (must hold mu).
func (b *Breaker) trip(openFor time.Duration) {
	b.state = breakerOpen
	b.openFor = openFor
	b.openUntil = b.clock.Now().Add(openFor)
	b.trialTaken = false
	b.streak = 0
}

// State returns the current state and, when open, the remaining time (tests
// and P10.3 stats).
func (b *Breaker) State() (string, time.Duration) {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.state {
	case breakerClosed:
		return "closed", 0
	case breakerHalfOpen:
		return "half-open", 0
	default:
		return "open", b.openUntil.Sub(b.clock.Now())
	}
}
