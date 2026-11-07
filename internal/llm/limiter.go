package llm

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// Limiter is a dual token bucket — requests per minute and estimated tokens
// per minute — plus a concurrency semaphore (P9.1).
//
// Wait blocks until one request with the given estimated token cost may
// start: it takes one of the concurrency slots and charges both buckets.
// After the request finishes the caller MUST call Release exactly once. If
// Wait returns an error, Release must not be called. Buckets start full and
// refill continuously; capacity is one minute's worth of the rate. All time
// comes from the injected Clock, so tests never sleep (R10).
type Limiter struct {
	clock    Clock
	reqRate  float64 // requests per second
	tokRate  float64 // tokens per second
	reqBurst float64 // capacity: one minute of requests
	tokBurst float64 // capacity: one minute of tokens

	sem    chan struct{}
	mu     sync.Mutex
	reqTok float64
	tokTok float64
	last   time.Time
}

// NewLimiter builds a Limiter. Rates must be positive; concurrency below 1
// becomes 1. A nil clock means RealClock.
func NewLimiter(requestsPerMinute, tokensPerMinute, concurrency int, clock Clock) (*Limiter, error) {
	if requestsPerMinute <= 0 {
		return nil, fmt.Errorf("llm: requests per minute must be > 0, got %d", requestsPerMinute)
	}
	if tokensPerMinute <= 0 {
		return nil, fmt.Errorf("llm: tokens per minute must be > 0, got %d", tokensPerMinute)
	}
	if concurrency < 1 {
		concurrency = 1
	}
	if clock == nil {
		clock = RealClock{}
	}
	return &Limiter{
		clock:    clock,
		reqRate:  float64(requestsPerMinute) / 60,
		tokRate:  float64(tokensPerMinute) / 60,
		reqBurst: float64(requestsPerMinute),
		tokBurst: float64(tokensPerMinute),
		sem:      make(chan struct{}, concurrency),
		reqTok:   float64(requestsPerMinute),
		tokTok:   float64(tokensPerMinute),
		last:     clock.Now(),
	}, nil
}

// EstimateTokens estimates a request's token cost: len(bytes)/3 over the
// prompt bytes plus MaxOutputTokens (P9.1). The result is at least 1.
func EstimateTokens(r Request) int {
	n := (len(r.System) + len(r.User)) / 3
	n += r.MaxOutputTokens
	if n < 1 {
		n = 1
	}
	return n
}

// Wait takes one concurrency slot and blocks until both buckets can cover
// one request and its estimated token cost. On ctx cancellation the slot is
// returned and no tokens are charged.
func (l *Limiter) Wait(ctx context.Context, tokens int) error {
	if tokens < 1 {
		tokens = 1
	}
	select {
	case l.sem <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	// The slot is held from here on; only a successful return leaves it to
	// be given back by Release.
	for {
		l.mu.Lock()
		now := l.clock.Now()
		l.refillLocked(now)
		if l.reqTok >= 1 && float64(tokens) <= l.tokTok {
			l.reqTok--
			l.tokTok -= float64(tokens)
			l.mu.Unlock()
			return nil
		}
		d := l.delayLocked(tokens)
		l.mu.Unlock()

		timer := l.clock.After(d)
		select {
		case <-timer:
		case <-ctx.Done():
			<-l.sem // give the slot back; no tokens were charged
			return ctx.Err()
		}
	}
}

// Release gives back the concurrency slot taken by a successful Wait.
func (l *Limiter) Release() { <-l.sem }

// InFlight returns the number of currently held concurrency slots (tests).
func (l *Limiter) InFlight() int { return len(l.sem) }

// delayLocked computes how long until both buckets can satisfy the request.
// Rates are positive by construction, so the result is finite.
func (l *Limiter) delayLocked(tokens int) time.Duration {
	d := time.Duration(0)
	if l.reqTok < 1 {
		d = time.Duration((1 - l.reqTok) / l.reqRate * float64(time.Second))
	}
	if float64(tokens) > l.tokTok {
		need := time.Duration((float64(tokens) - l.tokTok) / l.tokRate * float64(time.Second))
		if need > d {
			d = need
		}
	}
	return d
}

// refillLocked adds elapsed-time capacity to both buckets, capped at burst.
func (l *Limiter) refillLocked(now time.Time) {
	el := now.Sub(l.last).Seconds()
	if el < 0 {
		el = 0
	}
	l.reqTok = min(l.reqBurst, l.reqTok+el*l.reqRate)
	l.tokTok = min(l.tokBurst, l.tokTok+el*l.tokRate)
	l.last = now
}
