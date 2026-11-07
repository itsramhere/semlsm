package llm

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

const testKey = "sk-test-key-1234567890"

func httpTestClient(t *testing.T, url string, mutate func(*HTTPOptions)) *HTTPClient {
	t.Helper()
	opt := HTTPOptions{
		BaseURL:    url,
		APIKey:     testKey,
		Model:      "test-model",
		RetryBase:  time.Millisecond,
		RetryMax:   8 * time.Millisecond,
		MaxRetries: 4,
	}
	if mutate != nil {
		mutate(&opt)
	}
	return NewHTTPClient(opt)
}

func TestHTTPSuccess(t *testing.T) {
	var sawAuth, sawModel, sawTemp, sawRoles string
	var sawTempVal float64
	var sawMaxVal int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sawAuth = r.Header.Get("Authorization")
		var body struct {
			Model       string        `json:"model"`
			Messages    []chatMessage `json:"messages"`
			Temperature float64       `json:"temperature"`
			MaxTokens   int           `json:"max_tokens"`
		}
		data, _ := io.ReadAll(r.Body)
		json.Unmarshal(data, &body)
		sawModel, sawTempVal, sawMaxVal = body.Model, body.Temperature, body.MaxTokens
		if len(body.Messages) == 2 {
			sawRoles = body.Messages[0].Role + "," + body.Messages[1].Role
		}
		sawTemp = "set"
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"choices":[{"message":{"content":"{\"summary\":\"ok\"}"}}],"usage":{"prompt_tokens":11,"completion_tokens":7},"reasoning":{"x":1}}`)
	}))
	defer srv.Close()

	c := httpTestClient(t, srv.URL, nil)
	resp, err := c.Complete(context.Background(), Request{System: "sys", User: "usr", MaxOutputTokens: 2048})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Text != `{"summary":"ok"}` || resp.InputTokens != 11 || resp.OutputTokens != 7 || resp.Model != "test-model" {
		t.Fatalf("response: %+v", resp)
	}
	if sawAuth != "Bearer "+testKey {
		t.Fatalf("auth header: %q", sawAuth)
	}
	if sawModel != "test-model" || sawTemp != "set" || sawTempVal != 0 || sawMaxVal != 2048 {
		t.Fatalf("body: model=%s temp=%v max=%d", sawModel, sawTempVal, sawMaxVal)
	}
	if sawRoles != "system,user" {
		t.Fatalf("roles: %q", sawRoles)
	}
}

func TestHTTP429ThenOK(t *testing.T) {
	var calls atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		io.WriteString(w, `{"choices":[{"message":{"content":"ok"}}],"usage":{"prompt_tokens":1,"completion_tokens":1}}`)
	}))
	defer srv.Close()

	c := httpTestClient(t, srv.URL, nil)
	resp, err := c.Complete(context.Background(), Request{System: "s", User: "u", MaxOutputTokens: 16})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Text != "ok" || calls.Load() != 2 {
		t.Fatalf("resp=%q calls=%d", resp.Text, calls.Load())
	}
}

// TestHTTPRetryAfterHonored: Retry-After is 1s of virtual time while the
// jittered backoff is capped at 8ms, so success with >= 1s virtual elapsed
// proves the header was honored.
func TestHTTPRetryAfterHonored(t *testing.T) {
	clk := NewManualClock(time.Unix(1_000_000, 0))
	var calls atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		io.WriteString(w, `{"choices":[{"message":{"content":"ok"}}]}`)
	}))
	defer srv.Close()

	c := httpTestClient(t, srv.URL, func(o *HTTPOptions) { o.Clock = clk })
	start := clk.Now()
	done := make(chan error, 1)
	go func() {
		_, err := c.Complete(context.Background(), Request{System: "s", User: "u"})
		done <- err
	}()
	// Drive the virtual clock until the call completes or the bound is hit.
	deadline := time.Now().Add(5 * time.Second)
	for {
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
			if elapsed := clk.Now().Sub(start); elapsed < time.Second {
				t.Fatalf("Retry-After not honored: only %v virtual time elapsed", elapsed)
			}
			if calls.Load() != 2 {
				t.Fatalf("calls: %d", calls.Load())
			}
			return
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("call did not complete")
		}
		clk.Advance(250 * time.Millisecond)
		time.Sleep(time.Millisecond)
	}
}

func TestHTTP500Exhausted(t *testing.T) {
	var calls atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	c := httpTestClient(t, srv.URL, nil)
	_, err := c.Complete(context.Background(), Request{System: "s", User: "u"})
	if !errors.Is(err, ErrTransient) {
		t.Fatalf("want ErrTransient, got %v", err)
	}
	if got := calls.Load(); got != 5 { // 1 attempt + MaxRetries(4)
		t.Fatalf("attempts: %d, want 5", got)
	}
}

func TestHTTP401NoRetry(t *testing.T) {
	var calls atomic.Int64
	// The body echoes the key back: error strings must not leak it.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusUnauthorized)
		io.WriteString(w, "denied for "+r.Header.Get("Authorization"))
	}))
	defer srv.Close()

	c := httpTestClient(t, srv.URL, nil)
	_, err := c.Complete(context.Background(), Request{System: "s", User: "u"})
	if !errors.Is(err, ErrPermanent) {
		t.Fatalf("want ErrPermanent, got %v", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("4xx must not retry, calls: %d", calls.Load())
	}
	if strings.Contains(err.Error(), testKey) {
		t.Fatal("API key leaked into the error string")
	}
}

func TestHTTPMalformedBodyTransient(t *testing.T) {
	var calls atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		io.WriteString(w, "<html>gateway garbage</html>")
	}))
	defer srv.Close()

	c := httpTestClient(t, srv.URL, func(o *HTTPOptions) { o.MaxRetries = 2 })
	_, err := c.Complete(context.Background(), Request{System: "s", User: "u"})
	if !errors.Is(err, ErrTransient) {
		t.Fatalf("want ErrTransient, got %v", err)
	}
	if calls.Load() != 3 { // 1 + 2 retries
		t.Fatalf("calls: %d", calls.Load())
	}
	if strings.Contains(err.Error(), testKey) {
		t.Fatal("API key leaked into the error string")
	}
}

func TestHTTPMissingUsageEstimated(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, `{"choices":[{"message":{"content":"hello world, this is the answer"}}]}`)
	}))
	defer srv.Close()

	c := httpTestClient(t, srv.URL, nil)
	sys, usr := strings.Repeat("s", 30), strings.Repeat("u", 60)
	resp, err := c.Complete(context.Background(), Request{System: sys, User: usr})
	if err != nil {
		t.Fatal(err)
	}
	if want := (30 + 60) / 3; resp.InputTokens != want {
		t.Fatalf("input tokens %d, want %d", resp.InputTokens, want)
	}
	if want := len("hello world, this is the answer") / 3; resp.OutputTokens != want {
		t.Fatalf("output tokens %d, want %d", resp.OutputTokens, want)
	}
}

func TestHTTPCancelMidFlight(t *testing.T) {
	block := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-block // hold the response until the test lets go
	}))
	defer srv.Close()  // runs second
	defer close(block) // runs first: release the handler before Close waits on it

	c := httpTestClient(t, srv.URL, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := c.Complete(ctx, Request{System: "s", User: "u"})
	if err == nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want DeadlineExceeded, got %v", err)
	}
	if time.Since(start) > 2*time.Second {
		t.Fatalf("cancel did not return promptly: %v", time.Since(start))
	}
}

func TestHTTPNetworkErrorIsTransient(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	url := srv.URL
	srv.Close() // nothing listens anymore

	c := httpTestClient(t, url, func(o *HTTPOptions) { o.MaxRetries = 1 })
	_, err := c.Complete(context.Background(), Request{System: "s", User: "u"})
	if !errors.Is(err, ErrTransient) {
		t.Fatalf("want ErrTransient, got %v", err)
	}
}

func TestHTTPKeyNeverInErrors(t *testing.T) {
	calls := []func(c *HTTPClient) error{
		func(c *HTTPClient) error {
			_, err := c.Complete(context.Background(), Request{System: "s", User: "u"})
			return err
		},
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		io.WriteString(w, "bad request for "+r.Header.Get("Authorization"))
	}))
	defer srv.Close()
	for _, f := range calls {
		err := f(httpTestClient(t, srv.URL, nil))
		if err == nil {
			t.Fatal("want error")
		}
		if strings.Contains(err.Error(), testKey) {
			t.Fatal("API key leaked into an error string")
		}
	}
}

func TestHTTPOptionsDefaults(t *testing.T) {
	c := NewHTTPClient(HTTPOptions{BaseURL: "http://x", APIKey: "k"})
	if c.opt.Timeout != DefaultHTTPTimeout || c.opt.MaxRetries != 4 ||
		c.opt.RetryBase != 500*time.Millisecond || c.opt.RetryMax != 30*time.Second ||
		c.Model() != DefaultModel {
		t.Fatalf("defaults: %+v", c.opt)
	}
}

func TestHTTPOptionsFromEnv(t *testing.T) {
	t.Setenv("SEMLSM_LLM_BASE_URL", "https://llm.example/v1")
	t.Setenv("SEMLSM_LLM_API_KEY", "env-key")
	t.Setenv("SEMLSM_LLM_MODEL", "env-model")
	o := HTTPOptionsFromEnv()
	if o.BaseURL != "https://llm.example/v1" || o.APIKey != "env-key" || o.Model != "env-model" {
		t.Fatalf("env: %+v", o)
	}
	t.Setenv("SEMLSM_LLM_MODEL", "")
	if o := HTTPOptionsFromEnv(); o.Model != DefaultModel {
		t.Fatalf("default model: %q", o.Model)
	}
}

func TestParseRetryAfter(t *testing.T) {
	if d := parseRetryAfter(""); d != 0 {
		t.Fatalf("empty: %v", d)
	}
	if d := parseRetryAfter("3"); d != 3*time.Second {
		t.Fatalf("3: %v", d)
	}
	if d := parseRetryAfter("0"); d != 0 {
		t.Fatalf("0: %v", d)
	}
	if d := parseRetryAfter("-2"); d != 0 {
		t.Fatalf("-2: %v", d)
	}
	if d := parseRetryAfter("Wed, 21 Oct 2015 07:28:00 GMT"); d != 0 {
		t.Fatalf("http-date falls back: %v", d)
	}
}
