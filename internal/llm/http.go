package llm

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

// Default per-call timeout (P9.5).
const DefaultHTTPTimeout = 120 * time.Second

// DefaultModel is used when SEMLSM_LLM_MODEL is unset (P9.5).
const DefaultModel = "glm-5.3-flash"

// HTTPOptions configures the OpenAI-compatible HTTP client. The API key is
// read from the environment (R6) or set programmatically and is never
// logged or included in errors.
type HTTPOptions struct {
	BaseURL string // e.g. https://api.example.com/v1
	APIKey  string
	Model   string

	Timeout    time.Duration // per call; 0 -> DefaultHTTPTimeout
	MaxRetries int           // retries after the first attempt; 0 -> 4
	RetryBase  time.Duration // first backoff; 0 -> 500ms
	RetryMax   time.Duration // backoff cap; 0 -> 30s
	Clock      Clock         // retry sleeps; nil -> RealClock
}

// HTTPOptionsFromEnv reads SEMLSM_LLM_BASE_URL, SEMLSM_LLM_API_KEY and
// SEMLSM_LLM_MODEL (default DefaultModel). The key stays in memory only
// (R6: never print, log, or write it).
func HTTPOptionsFromEnv() HTTPOptions {
	model := os.Getenv("SEMLSM_LLM_MODEL")
	if model == "" {
		model = DefaultModel
	}
	return HTTPOptions{
		BaseURL: os.Getenv("SEMLSM_LLM_BASE_URL"),
		APIKey:  os.Getenv("SEMLSM_LLM_API_KEY"),
		Model:   model,
	}
}

// HTTPClient is an OpenAI-compatible chat-completions client (P9.5). It
// retries transient failures (429, 5xx, network, timeout) up to MaxRetries
// times with exponential backoff and full jitter, honoring Retry-After.
// Other 4xx statuses are ErrPermanent and are not retried. Error strings
// contain status codes only — never response bodies and never the API key.
type HTTPClient struct {
	opt HTTPOptions
	hc  *http.Client
}

// NewHTTPClient applies defaults to opt and returns the client.
func NewHTTPClient(opt HTTPOptions) *HTTPClient {
	if opt.Model == "" {
		opt.Model = DefaultModel
	}
	if opt.Timeout <= 0 {
		opt.Timeout = DefaultHTTPTimeout
	}
	if opt.MaxRetries <= 0 {
		opt.MaxRetries = 4
	}
	if opt.RetryBase <= 0 {
		opt.RetryBase = 500 * time.Millisecond
	}
	if opt.RetryMax <= 0 {
		opt.RetryMax = 30 * time.Second
	}
	if opt.Clock == nil {
		opt.Clock = RealClock{}
	}
	return &HTTPClient{opt: opt, hc: &http.Client{Timeout: opt.Timeout}}
}

// Model returns the configured model name.
func (c *HTTPClient) Model() string { return c.opt.Model }

type chatRequest struct {
	Model       string        `json:"model"`
	Messages    []chatMessage `json:"messages"`
	Temperature float64       `json:"temperature"`
	MaxTokens   int           `json:"max_tokens,omitempty"`
}

type chatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type chatResponse struct {
	Choices []struct {
		Message struct {
			Content string `json:"content"`
		} `json:"message"`
	} `json:"choices"`
	// Usage is optional; when absent the tokens are estimated (P9.5).
	// Unknown fields (e.g. reasoning) are ignored.
	Usage *struct {
		PromptTokens     int `json:"prompt_tokens"`
		CompletionTokens int `json:"completion_tokens"`
	} `json:"usage"`
}

// Complete performs one logical call: up to MaxRetries+1 HTTP attempts.
func (c *HTTPClient) Complete(ctx context.Context, req Request) (Response, error) {
	payload := chatRequest{
		Model:       c.opt.Model,
		Messages:    []chatMessage{{Role: "system", Content: req.System}, {Role: "user", Content: req.User}},
		Temperature: 0,
		MaxTokens:   req.MaxOutputTokens,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return Response{}, fmt.Errorf("llm: encode request: %w", err)
	}
	url := strings.TrimRight(c.opt.BaseURL, "/") + "/chat/completions"

	var lastErr error
	var retryAfter time.Duration
	for attempt := 0; ; attempt++ {
		if attempt > 0 {
			if err := sleepCtx(ctx, retryAfter, c.opt.Clock); err != nil {
				return Response{}, err
			}
		}
		resp, ra, err := c.attempt(ctx, url, body, req)
		if err == nil {
			return resp, nil
		}
		lastErr = err
		if !errors.Is(err, ErrTransient) {
			return Response{}, err
		}
		if a := attempt + 1; a > c.opt.MaxRetries {
			return Response{}, lastErr
		}
		retryAfter = ra
		if retryAfter <= 0 {
			retryAfter = jitterBackoff(c.opt.RetryBase, c.opt.RetryMax, attempt)
		}
	}
}

// attempt does one POST and classifies the outcome.
func (c *HTTPClient) attempt(ctx context.Context, url string, body []byte, req Request) (Response, time.Duration, error) {
	hreq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return Response{}, 0, fmt.Errorf("llm: build request: %w", err)
	}
	hreq.Header.Set("Content-Type", "application/json")
	hreq.Header.Set("Authorization", "Bearer "+c.opt.APIKey)

	hresp, err := c.hc.Do(hreq)
	if err != nil {
		if cerr := ctx.Err(); cerr != nil {
			return Response{}, 0, cerr // caller's cancel/deadline, not retried
		}
		return Response{}, 0, fmt.Errorf("%w: %v", ErrTransient, err)
	}
	defer hresp.Body.Close()

	if hresp.StatusCode >= 200 && hresp.StatusCode < 300 {
		return c.parseBody(hresp, req)
	}
	ra := parseRetryAfter(hresp.Header.Get("Retry-After"))
	io.Copy(io.Discard, io.LimitReader(hresp.Body, 1<<20))
	if hresp.StatusCode == http.StatusTooManyRequests || hresp.StatusCode >= 500 {
		return Response{}, ra, fmt.Errorf("%w: status %d", ErrTransient, hresp.StatusCode)
	}
	return Response{}, 0, fmt.Errorf("%w: status %d", ErrPermanent, hresp.StatusCode)
}

// parseBody decodes a 2xx response. A malformed body is treated as
// transient: it is usually a broken proxy, worth one retry.
func (c *HTTPClient) parseBody(hresp *http.Response, req Request) (Response, time.Duration, error) {
	data, err := io.ReadAll(io.LimitReader(hresp.Body, 64<<20))
	if err != nil {
		return Response{}, 0, fmt.Errorf("%w: read body: %v", ErrTransient, err)
	}
	var cr chatResponse
	if err := json.Unmarshal(data, &cr); err != nil {
		return Response{}, 0, fmt.Errorf("%w: malformed body", ErrTransient)
	}
	if len(cr.Choices) == 0 || len(cr.Choices[0].Message.Content) == 0 {
		return Response{}, 0, fmt.Errorf("%w: empty choices", ErrTransient)
	}
	resp := Response{
		Text:  cr.Choices[0].Message.Content,
		Model: c.opt.Model,
	}
	if cr.Usage != nil {
		resp.InputTokens = cr.Usage.PromptTokens
		resp.OutputTokens = cr.Usage.CompletionTokens
	} else {
		// Estimate when the server omits usage (P9.5).
		resp.InputTokens = max(1, (len(req.System)+len(req.User))/3)
		resp.OutputTokens = max(1, len(resp.Text)/3)
	}
	return resp, 0, nil
}

// parseRetryAfter parses a delta-seconds Retry-After header. HTTP-date form
// or garbage returns 0 (fall back to jittered backoff).
func parseRetryAfter(v string) time.Duration {
	v = strings.TrimSpace(v)
	if v == "" {
		return 0
	}
	if secs, err := strconv.Atoi(v); err == nil && secs > 0 {
		return time.Duration(secs) * time.Second
	}
	return 0
}

// jitterBackoff returns a uniform random duration in [0, min(cap, base*2^attempt)].
func jitterBackoff(base, cap time.Duration, attempt int) time.Duration {
	exp := base << min(attempt, 20)
	if exp > cap || exp <= 0 {
		exp = cap
	}
	return time.Duration(rand.Int64N(int64(exp) + 1))
}

// sleepCtx waits on the injected clock, honoring ctx.
func sleepCtx(ctx context.Context, d time.Duration, clk Clock) error {
	if d <= 0 {
		return nil
	}
	timer := clk.After(d)
	select {
	case <-timer:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
