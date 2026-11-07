package llm

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"semlsm/internal/vfs"
)

// Config configures the reliability stack (P9.6).
type Config struct {
	Model         string
	PromptVersion int
	FS            vfs.FS // nil -> OS filesystem
	Dir           string // DB directory; llm/usage.json and llm/cache/ live under it

	MaxTokensTotal    int64 // mandatory (> 0)
	RequestsPerMinute int
	TokensPerMinute   int
	Concurrency       int

	CacheMaxBytes int64 // <= 0 -> no cache layer

	// Accept decides whether a response may enter the cache; only
	// validated responses are stored (P9.4). nil stores everything.
	Accept func(Request, Response) error
	Clock  Clock // nil -> RealClock
	Logger *slog.Logger
}

// rateLimiter is the limiter surface the stack needs; *Limiter implements it
// and tests may stub it.
type rateLimiter interface {
	Wait(ctx context.Context, tokens int) error
	Release()
	TokensPerMinute() int
}

// Stack composes the reliability layers around a raw Client (P9.6):
//
//	cache -> budget -> breaker -> limiter -> client
//
// A cache hit returns immediately and skips every other layer. It implements
// the Summarizer interface, so the engine's Options.Summarizer takes it
// directly, and it is itself a Client for the semantic pipeline (P8.6).
type Stack struct {
	model         string
	promptVersion int

	cache   *Cache // nil if disabled
	budget  *Budget
	breaker *Breaker
	limiter rateLimiter
	next    Client
	accept  func(Request, Response) error
	log     *slog.Logger

	cacheHits atomic.Int64
}

// NewSummarizer builds the stack over next. MaxTokensTotal is mandatory; the
// constructor errors when it is not set, so an HTTP-backed engine cannot run
// without a budget (P9.3).
func NewSummarizer(next Client, cfg Config) (*Stack, error) {
	if next == nil {
		return nil, fmt.Errorf("llm: nil client")
	}
	if cfg.Dir == "" {
		return nil, fmt.Errorf("llm: Dir must be set")
	}
	if cfg.MaxTokensTotal <= 0 {
		return nil, fmt.Errorf("llm: MaxTokensTotal must be > 0 (mandatory)")
	}
	if cfg.FS == nil {
		cfg.FS = vfs.OSFS{}
	}
	clk := cfg.Clock
	if clk == nil {
		clk = RealClock{}
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	s := &Stack{
		model:         cfg.Model,
		promptVersion: cfg.PromptVersion,
		breaker:       NewBreaker(clk),
		next:          next,
		accept:        cfg.Accept,
		log:           logger,
	}
	budget, err := OpenBudget(cfg.FS, cfg.Dir, cfg.MaxTokensTotal)
	if err != nil {
		return nil, err
	}
	s.budget = budget
	limiter, err := NewLimiter(cfg.RequestsPerMinute, cfg.TokensPerMinute, cfg.Concurrency, clk)
	if err != nil {
		return nil, err
	}
	s.limiter = limiter
	if cfg.CacheMaxBytes > 0 {
		cache, err := OpenCache(cfg.FS, cfg.Dir, cfg.CacheMaxBytes, clk)
		if err != nil {
			return nil, err
		}
		s.cache = cache
	}
	return s, nil
}

// Model implements Summarizer.
func (s *Stack) Model() string { return s.model }

// PromptVersion implements Summarizer.
func (s *Stack) PromptVersion() int { return s.promptVersion }

// CacheHits returns the number of cache hits (P10.3 stats).
func (s *Stack) CacheHits() int64 { return s.cacheHits.Load() }

// BreakerState exposes the breaker state (P10.3 stats).
func (s *Stack) BreakerState() (string, time.Duration) {
	return s.breaker.State()
}

// TokensPerMinute exposes the token burst so Open can check that it covers a
// worst-case request (Options validation, M-025 item 3).
func (s *Stack) TokensPerMinute() int { return s.limiter.TokensPerMinute() }

// LLMHealthy reports whether the stack would currently admit a call: the
// breaker is not open and the budget still has room (P10.2: while unhealthy,
// Tier B runs raw structural compaction instead of LLM work).
func (s *Stack) LLMHealthy() bool {
	if state, _ := s.breaker.State(); state == "open" {
		return false
	}
	return s.budget.Remaining() > 0
}

// Complete runs one logical call through the layers, outermost first.
func (s *Stack) Complete(ctx context.Context, req Request) (Response, error) {
	if s.cache != nil {
		if resp, ok := s.cache.Get(s.model, s.promptVersion, req); ok {
			s.cacheHits.Add(1)
			return resp, nil
		}
	}
	estimate := int64(EstimateTokens(req))
	if err := s.budget.Reserve(estimate); err != nil {
		return Response{}, err
	}
	if err := s.breaker.Allow(); err != nil {
		s.budget.Release(estimate)
		return Response{}, err
	}
	if err := s.limiter.Wait(ctx, int(estimate)); err != nil {
		// The call never reached the upstream client, so the half-open
		// trial (if any) must be given back (M-025 item 2).
		s.budget.Release(estimate)
		s.breaker.Cancel()
		return Response{}, err
	}
	defer s.limiter.Release()

	resp, err := s.next.Complete(ctx, req)
	if err != nil {
		s.budget.Release(estimate)
		s.breaker.RecordError(err)
		return Response{}, err
	}
	s.breaker.RecordSuccess()
	if err := s.budget.Record(estimate, resp); err != nil {
		// The tokens are already spent and the response is valid, so the
		// caller keeps it (M-025 item 4): persisting usage is best-effort.
		// The in-memory totals were applied before the write; only the
		// on-disk file may lag until the next Record.
		s.log.Warn("llm: persisting usage.json failed; in-memory budget continues", "err", err)
	}
	if s.cache != nil && (s.accept == nil || s.accept(req, resp) == nil) {
		s.cache.Put(s.model, s.promptVersion, req, resp)
	}
	return resp, nil
}

var (
	_ Summarizer = (*Stack)(nil)
	_ Client     = (*Stack)(nil)
)
