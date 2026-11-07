// Package llm provides completion clients and the reliability stack used by
// SemLSM semantic compaction. The default filesystem is the OS filesystem.
package llm

import internal "github.com/itsramhere/semlsm/internal/llm"

// Completion interfaces and values also support custom transports.
type (
	Client      = internal.Client
	Summarizer  = internal.Summarizer
	Request     = internal.Request
	Response    = internal.Response
	Config      = internal.Config
	Stack       = internal.Stack
	HTTPOptions = internal.HTTPOptions
	HTTPClient  = internal.HTTPClient
	MockMode    = internal.MockMode
	MockClient  = internal.MockClient
)

const (
	DefaultModel        = internal.DefaultModel
	DefaultHTTPTimeout  = internal.DefaultHTTPTimeout
	MockValid           = internal.MockValid
	MockBadJSON         = internal.MockBadJSON
	MockFencedJSON      = internal.MockFencedJSON
	MockHallucinatedRef = internal.MockHallucinatedRef
	MockTooLong         = internal.MockTooLong
	MockTransient       = internal.MockTransient
	MockPermanent       = internal.MockPermanent
	MockBlock           = internal.MockBlock
	MockInjected        = internal.MockInjected
)

var (
	ErrTransient       = internal.ErrTransient
	ErrPermanent       = internal.ErrPermanent
	ErrBudgetExhausted = internal.ErrBudgetExhausted
	ErrCircuitOpen     = internal.ErrCircuitOpen
)

// NewSummarizer wraps a client with a cache, persisted budget, breaker and limiter.
func NewSummarizer(client Client, cfg Config) (*Stack, error) {
	return internal.NewSummarizer(client, cfg)
}

// HTTPOptionsFromEnv reads SEMLSM_LLM_BASE_URL, SEMLSM_LLM_API_KEY and
// SEMLSM_LLM_MODEL. The API key is never printed or included in errors.
func HTTPOptionsFromEnv() HTTPOptions { return internal.HTTPOptionsFromEnv() }

// NewHTTPClient creates an OpenAI-compatible chat-completions transport.
func NewHTTPClient(opts HTTPOptions) *HTTPClient { return internal.NewHTTPClient(opts) }

// NewMock creates a deterministic client for tests and local experiments.
func NewMock(mode MockMode) *MockClient { return internal.NewMock(mode) }

// NewMockTransient creates a client that fails the first n calls.
func NewMockTransient(n int) *MockClient { return internal.NewMockTransient(n) }

// NewMockInjected creates a client that returns text verbatim.
func NewMockInjected(text string) *MockClient { return internal.NewMockInjected(text) }
