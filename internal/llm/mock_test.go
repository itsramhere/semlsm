package llm

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestMockValid(t *testing.T) {
	m := NewMock(MockValid)
	r := Request{System: "sys", User: "r0 | PUT | k | v\nr1 | PUT | k2 | v2\nr2 | DELETE | k3 |"}
	resp, err := m.Complete(context.Background(), r)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(resp.Text, `"r0"`) || !strings.Contains(resp.Text, `"r1"`) {
		t.Fatalf("mock must cite input ids: %q", resp.Text)
	}
	if m.Calls() != 1 {
		t.Fatalf("calls=%d", m.Calls())
	}
}

func TestMockModes(t *testing.T) {
	ctx := context.Background()
	req := Request{User: "r0 | x"}
	t.Run("BadJSON", func(t *testing.T) {
		m := NewMock(MockBadJSON)
		resp, err := m.Complete(ctx, req)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(resp.Text, "not json") {
			t.Fatalf("got %q", resp.Text)
		}
	})
	t.Run("Fenced", func(t *testing.T) {
		m := NewMock(MockFencedJSON)
		resp, _ := m.Complete(ctx, req)
		if !strings.HasPrefix(resp.Text, "```json") {
			t.Fatalf("got %q", resp.Text)
		}
	})
	t.Run("HallucinatedRef", func(t *testing.T) {
		m := NewMock(MockHallucinatedRef)
		resp, _ := m.Complete(ctx, req)
		if !strings.Contains(resp.Text, "r9999") {
			t.Fatalf("got %q", resp.Text)
		}
	})
	t.Run("TooLong", func(t *testing.T) {
		m := NewMock(MockTooLong)
		resp, _ := m.Complete(ctx, req)
		if len(resp.Text) < 2000 {
			t.Fatalf("too short: %d", len(resp.Text))
		}
	})
	t.Run("Transient", func(t *testing.T) {
		m := NewMockTransient(2)
		for i := 0; i < 2; i++ {
			if _, err := m.Complete(ctx, req); !errors.Is(err, ErrTransient) {
				t.Fatalf("call %d: %v", i, err)
			}
		}
		if _, err := m.Complete(ctx, req); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("Permanent", func(t *testing.T) {
		m := NewMock(MockPermanent)
		if _, err := m.Complete(ctx, req); !errors.Is(err, ErrPermanent) {
			t.Fatalf("got %v", err)
		}
	})
	t.Run("Block", func(t *testing.T) {
		m := NewMock(MockBlock)
		cctx, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
		defer cancel()
		if _, err := m.Complete(cctx, req); err == nil {
			t.Fatal("blocked call must return ctx error")
		}
	})
	t.Run("Injected", func(t *testing.T) {
		m := NewMockInjected(`{"summary":"injected"}`)
		resp, _ := m.Complete(ctx, req)
		if resp.Text != `{"summary":"injected"}` {
			t.Fatalf("got %q", resp.Text)
		}
	})
	t.Run("SetMode", func(t *testing.T) {
		m := NewMock(MockValid)
		if _, err := m.Complete(ctx, req); err != nil {
			t.Fatal(err)
		}
		m.SetMode(MockBadJSON)
		if _, err := m.Complete(ctx, req); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("LatencyRespectsContext", func(t *testing.T) {
		m := NewMock(MockValid)
		m.Latency = time.Second
		cctx, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
		defer cancel()
		if _, err := m.Complete(cctx, req); err == nil {
			t.Fatal("latency must respect context cancellation")
		}
	})
}
