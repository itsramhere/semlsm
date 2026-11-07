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
	b.state = breakerClosed
	b.streak = 0
	b.trialTaken = false
	b.openFor = b.base
}

// RecordError records a call outcome. Only ErrTransient errors count toward
// the breaker; a success is RecordSuccess, and permanent or context errors
// say nothing about service health, so they are ignored here. In half-open
// state a transient failure re-opens the breaker with a doubled duration.
func (b *Breaker) RecordError(err error) {
	if !errors.Is(err, ErrTransient) {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.state {
	case breakerClosed:
		b.streak++
		if b.streak >= b.threshold {
			b.trip(b.base)
		}
	case breakerHalfOpen:
		b.trip(min(b.openFor*2, MaxOpenFor))
	default: // open: the call was not allowed; ignore
	}
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
