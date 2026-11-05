// Package llm provides the summarizer abstraction and reliability layers for
// semantic compaction. This file defines the core interfaces and sentinel
// errors (P8.1); the HTTP client, limiter, breaker, budget and cache arrive
// in P9.
package llm

import (
	"context"
	"errors"
)

// Request is one completion request.
type Request struct {
	System          string
	User            string
	MaxOutputTokens int
	Metadata        map[string]string
}

// Response is one completion result.
type Response struct {
	Text         string
	InputTokens  int
	OutputTokens int
	Model        string
}

// Client is the raw completion transport.
type Client interface {
	Complete(ctx context.Context, r Request) (Response, error)
}

// Summarizer wraps a Client with all policy: rate limits, budget, breaker,
// and response cache. Tier B calls only this interface.
type Summarizer interface {
	Model() string
	PromptVersion() int
	Complete(ctx context.Context, r Request) (Response, error)
}

// Sentinel errors for the reliability layers (P9).
var (
	// ErrTransient marks retryable failures (429, 5xx, network, timeout).
	ErrTransient = errors.New("llm: transient error")
	// ErrPermanent marks non-retryable failures (other 4xx).
	ErrPermanent = errors.New("llm: permanent error")
	// ErrBudgetExhausted is returned once the token budget is spent.
	ErrBudgetExhausted = errors.New("llm: budget exhausted")
	// ErrCircuitOpen is returned while the breaker is open.
	ErrCircuitOpen = errors.New("llm: circuit open")
)
