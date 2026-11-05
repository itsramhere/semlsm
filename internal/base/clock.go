package base

import (
	"sync"
	"time"
)

// Clock supplies time to everything that needs it. Tests inject a ManualClock
// so correctness never depends on wall-clock sleeps (R10).
type Clock interface {
	Now() time.Time
}

// RealClock reads the system clock.
type RealClock struct{}

// Now implements Clock.
func (RealClock) Now() time.Time { return time.Now() }

// ManualClock is a mutable clock for tests, safe for concurrent use.
type ManualClock struct {
	mu sync.Mutex
	t  time.Time
}

// NewManualClock returns a ManualClock starting at t.
func NewManualClock(t time.Time) *ManualClock { return &ManualClock{t: t} }

// Now implements Clock.
func (c *ManualClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

// Set moves the clock to t.
func (c *ManualClock) Set(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = t
}

// Advance moves the clock forward by d.
func (c *ManualClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}
