package llm

import (
	"sync"
	"time"
)

// Clock supplies time to the reliability layers. Tests inject ManualClock so
// no correctness depends on wall-clock sleeps (R10).
type Clock interface {
	Now() time.Time
	// After returns a channel receiving one value once d has elapsed on this
	// clock's timeline. A d <= 0 delivers immediately.
	After(d time.Duration) <-chan time.Time
}

// RealClock is the wall clock.
type RealClock struct{}

// Now implements Clock.
func (RealClock) Now() time.Time { return time.Now() }

// After implements Clock.
func (RealClock) After(d time.Duration) <-chan time.Time {
	t := time.NewTimer(d)
	return t.C
}

// ManualClock is a virtual clock for tests. Timers created by After fire
// synchronously (into buffered channels) when Set or Advance crosses their
// deadline; no goroutine is involved, so tests stay deterministic. Safe for
// concurrent use.
type ManualClock struct {
	mu     sync.Mutex
	now    time.Time
	timers []*manualTimer
}

type manualTimer struct {
	deadline time.Time
	ch       chan time.Time
}

// NewManualClock returns a ManualClock starting at t.
func NewManualClock(t time.Time) *ManualClock { return &ManualClock{now: t} }

// Now implements Clock.
func (m *ManualClock) Now() time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.now
}

// After implements Clock. The channel is buffered(1); the value is delivered
// by a later Set/Advance on the caller's goroutine, never by a spawned one.
func (m *ManualClock) After(d time.Duration) <-chan time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	ch := make(chan time.Time, 1)
	if d <= 0 {
		ch <- m.now
		return ch
	}
	m.timers = append(m.timers, &manualTimer{deadline: m.now.Add(d), ch: ch})
	return ch
}

// Set moves the clock to t and fires timers that have come due.
func (m *ManualClock) Set(t time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.now = t
	m.fireLocked()
}

// Advance moves the clock forward by d and fires timers that have come due.
func (m *ManualClock) Advance(d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.now = m.now.Add(d)
	m.fireLocked()
}

// fireLocked delivers one value to every timer whose deadline has passed.
// The send is non-blocking: each channel is buffered and written at most once.
func (m *ManualClock) fireLocked() {
	kept := m.timers[:0]
	for _, t := range m.timers {
		if !t.deadline.After(m.now) {
			select {
			case t.ch <- m.now:
			default:
			}
			continue
		}
		kept = append(kept, t)
	}
	m.timers = kept
}
