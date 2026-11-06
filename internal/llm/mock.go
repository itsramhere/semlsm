package llm

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// MockMode selects the mock client's failure behavior (P8.1).
type MockMode int

const (
	// MockValid always returns a valid JSON summary citing the first 3 ids.
	MockValid MockMode = iota
	// MockBadJSON returns syntactically invalid JSON.
	MockBadJSON
	// MockFencedJSON wraps the JSON in a code fence (must be stripped by V1).
	MockFencedJSON
	// MockHallucinatedRef cites a record id not in the input.
	MockHallucinatedRef
	// MockTooLong exceeds the length limits.
	MockTooLong
	// MockTransient fails n times with ErrTransient, then succeeds.
	MockTransient
	// MockPermanent always fails with ErrPermanent.
	MockPermanent
	// MockBlock blocks until the context is cancelled.
	MockBlock
	// MockInjected returns the configured Injected response verbatim.
	MockInjected
)

// MockClient is a deterministic mock for tests.
type MockClient struct {
	mu         sync.Mutex
	mode       MockMode
	transientN int
	injected   string
	calls      atomic.Int64
	// Latency, if set, sleeps before responding (respects ctx).
	Latency time.Duration
}

// NewMock builds a mock in the given mode.
func NewMock(mode MockMode) *MockClient { return &MockClient{mode: mode} }

// NewMockTransient fails the first n calls, then succeeds.
func NewMockTransient(n int) *MockClient {
	return &MockClient{mode: MockTransient, transientN: n}
}

// NewMockInjected returns injected verbatim (MockInjected).
func NewMockInjected(injected string) *MockClient {
	return &MockClient{mode: MockInjected, injected: injected}
}

// Calls returns the number of Complete invocations.
func (m *MockClient) Calls() int64 { return m.calls.Load() }

// SetMode switches the failure mode mid-test.
func (m *MockClient) SetMode(mode MockMode) {
	m.mu.Lock()
	m.mode = mode
	m.mu.Unlock()
}

var idPattern = regexp.MustCompile(`r[0-9]+`)

// Complete implements Client.
func (m *MockClient) Complete(ctx context.Context, r Request) (Response, error) {
	m.calls.Add(1)
	if m.Latency > 0 {
		select {
		case <-ctx.Done():
			return Response{}, ctx.Err()
		case <-time.After(m.Latency):
		}
	}
	m.mu.Lock()
	mode := m.mode
	m.mu.Unlock()

	switch mode {
	case MockBadJSON:
		return Response{Text: "not json at all {", Model: "mock"}, nil
	case MockFencedJSON:
		return Response{Text: "```json\n" + validJSON(r) + "\n```", Model: "mock"}, nil
	case MockHallucinatedRef:
		return Response{Text: `{"summary":"s","key_facts":[{"text":"t","refs":["r9999"]}],"anomalies":[],"final_state":""}`, Model: "mock"}, nil
	case MockTooLong:
		return Response{Text: `{"summary":"` + strings.Repeat("x", 3000) + `","key_facts":[],"anomalies":[],"final_state":""}`, Model: "mock"}, nil
	case MockTransient:
		m.mu.Lock()
		if m.transientN > 0 {
			m.transientN--
			m.mu.Unlock()
			return Response{}, fmt.Errorf("%w: mock transient", ErrTransient)
		}
		m.mu.Unlock()
		return Response{Text: validJSON(r), Model: "mock"}, nil
	case MockPermanent:
		return Response{}, fmt.Errorf("%w: mock permanent", ErrPermanent)
	case MockBlock:
		<-ctx.Done()
		return Response{}, ctx.Err()
	case MockInjected:
		return Response{Text: m.injected, Model: "mock"}, nil
	default:
		return Response{Text: validJSON(r), Model: "mock"}, nil
	}
}

// validJSON builds a valid response citing up to the first 3 record ids found
// in the user prompt.
func validJSON(r Request) string {
	ids := idPattern.FindAllString(r.User, 3)
	refs := make([]string, 0, len(ids))
	for _, id := range ids {
		refs = append(refs, `"`+id+`"`)
	}
	if len(refs) == 0 {
		refs = []string{`"r0"`}
	}
	return fmt.Sprintf(`{"summary":"mock summary of %d records","key_facts":[{"text":"fact","refs":[%s]}],"anomalies":[],"final_state":"done"}`,
		len(ids), strings.Join(refs, ","))
}

var _ Client = (*MockClient)(nil)
var _ = ErrMockNoIDs
var ErrMockNoIDs = errors.New("mock: no ids")
