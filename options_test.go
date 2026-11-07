package semlsm

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"semlsm/internal/base"
	"semlsm/internal/llm"
	"semlsm/internal/vfs"
)

func TestOptionsDefaults(t *testing.T) {
	o, err := Options{}.withDefaults()
	if err != nil {
		t.Fatal(err)
	}
	if o.MemtableBytes != 64<<20 || o.MaxImmutableMemtables != 2 {
		t.Fatal("memtable defaults wrong")
	}
	if o.L0CompactTrigger != 4 || o.L0StallTrigger != 12 {
		t.Fatal("L0 defaults wrong")
	}
	if o.L1TargetBytes != 64<<20 || o.LevelMultiplier != 10 || o.TargetFileBytes != 16<<20 {
		t.Fatal("level defaults wrong")
	}
	if o.GroupDepth != 2 || o.GroupDelimiter != '/' {
		t.Fatal("group defaults wrong")
	}
	if o.DefaultPolicy.MinBatch != 1000 || o.DefaultPolicy.MaxBatch != 10000 {
		t.Fatal("policy defaults wrong")
	}
	if o.SemanticWorkers != 1 || o.MaxChunkBytes != 32<<10 {
		t.Fatal("semantic defaults wrong")
	}
	if o.HardScore != 2.0 || o.MinCompressionRatio != 5.0 {
		t.Fatal("score defaults wrong")
	}
	if o.BlockCacheBytes != 64<<20 || o.MaxOpenTables != 256 {
		t.Fatal("cache defaults wrong")
	}
	if o.SyncMode != SyncAlways {
		t.Fatal("sync default wrong")
	}
	if _, ok := o.Clock.(base.RealClock); !ok {
		t.Fatal("clock default wrong")
	}
}

func TestOptionsValidate(t *testing.T) {
	tests := []struct {
		name string
		mut  func(*Options)
		want bool // ok?
	}{
		{"stall below compact", func(o *Options) { o.L0StallTrigger = 2; o.L0CompactTrigger = 4 }, false},
		{"multiplier negative", func(o *Options) { o.LevelMultiplier = -1 }, true}, // defaults cover non-positive
		{"tiny file size", func(o *Options) { o.TargetFileBytes = 10 }, false},
		{"duplicate prefixes", func(o *Options) {
			o.Policies = []Policy{{Prefix: "a/", Mode: ModeState}, {Prefix: "a/", Mode: ModeEvents}}
		}, false},
		{"minbatch above maxbatch", func(o *Options) {
			o.Policies = []Policy{{Prefix: "a/", Mode: ModeState, MinBatch: 100, MaxBatch: 50}}
		}, false},
		{"negative minage", func(o *Options) {
			o.Policies = []Policy{{Prefix: "a/", Mode: ModeState, MinAge: -time.Second}}
		}, false},
		{"valid policies", func(o *Options) {
			o.Policies = []Policy{{Prefix: "a/", Mode: ModeState, MinBatch: 10, MaxBatch: 100}}
		}, true},
	}
	for _, tt := range tests {
		o := Options{}
		tt.mut(&o)
		_, err := o.withDefaults()
		if tt.want && err != nil {
			t.Errorf("%s: unexpected error %v", tt.name, err)
		}
		if !tt.want && err == nil {
			t.Errorf("%s: expected error", tt.name)
		}
	}
}

// M-025 item 3: Open must reject a Summarizer whose token rate cannot cover
// a worst-case request (MaxChunkBytes/3 + MaxOutputTokens), because such a
// request could never be admitted by the limiter.
func TestOptionsTokenRateCoversWorstCaseRequest(t *testing.T) {
	newStack := func(tokensPerMinute int) *llm.Stack {
		s, err := llm.NewSummarizer(llm.NewMock(llm.MockValid), llm.Config{
			Model:             "m",
			Dir:               t.TempDir(), // OS FS is fine for the stack's own files
			MaxTokensTotal:    1 << 40,
			RequestsPerMinute: 600,
			TokensPerMinute:   tokensPerMinute,
			CacheMaxBytes:     0,
		})
		if err != nil {
			t.Fatal(err)
		}
		return s
	}

	// Default MaxChunkBytes (32 KiB): worst case = 32768/3 + 2048 = 12970.
	tooSmall := newStack(100)
	o := Options{FS: vfs.NewMemFS(), Summarizer: tooSmall}
	_, err := o.withDefaults()
	if err == nil || !strings.Contains(err.Error(), "TokensPerMinute") {
		t.Fatalf("want TokensPerMinute rejection, got %v", err)
	}

	// A rate that covers the worst case passes validation.
	bigEnough := newStack(20_000)
	o = Options{FS: vfs.NewMemFS(), Summarizer: bigEnough}
	if _, err := o.withDefaults(); err != nil {
		t.Fatalf("sufficient rate rejected: %v", err)
	}

	// A non-Stack Summarizer without the capability is not checked.
	o = Options{FS: vfs.NewMemFS(), Summarizer: plainSummarizer{}}
	if _, err := o.withDefaults(); err != nil {
		t.Fatalf("capability-less summarizer must not be checked: %v", err)
	}
}

// plainSummarizer satisfies llm.Summarizer without exposing its rate, so the
// Open-time capability check must simply skip it.
type plainSummarizer struct{}

func (plainSummarizer) Model() string      { return "plain" }
func (plainSummarizer) PromptVersion() int { return 1 }
func (plainSummarizer) Complete(ctx context.Context, r llm.Request) (llm.Response, error) {
	return llm.Response{}, errors.New("not used")
}

func TestPolicyFor(t *testing.T) {
	o, _ := (Options{
		Policies: []Policy{
			{Prefix: "agent/", Mode: ModeState},
			{Prefix: "agent/7/", Mode: ModeEvents},
		},
		DefaultPolicy: Policy{Mode: ModeDisabled},
	}).withDefaults()
	tests := []struct {
		key  string
		mode Mode
	}{
		{"agent/7/events/1", ModeEvents}, // longest prefix wins
		{"agent/9/state", ModeState},     // shorter prefix
		{"other/1", ModeDisabled},        // default
		{"agent", ModeDisabled},          // not a prefix match (no slash)
		{"agentary/1", ModeState},        // "agent/" does not match "agentary"!
	}
	// "agentary/1" does not start with "agent/" -> default
	tests[4].mode = ModeDisabled
	for _, tt := range tests {
		p := policyFor(o, []byte(tt.key))
		if p.Mode != tt.mode {
			t.Errorf("policyFor(%q) = %v, want %v", tt.key, p.Mode, tt.mode)
		}
	}
}

func TestOptionsFileRoundTrip(t *testing.T) {
	fs := vfs.NewMemFS()
	dir := "db"
	fs.MkdirAll(dir)
	o, _ := (Options{
		FS:       fs,
		Policies: []Policy{{Prefix: "a/", Mode: ModeState, MinBatch: 5, MaxBatch: 50}},
	}).withDefaults()
	if err := writeOptionsFile(o, dir); err != nil {
		t.Fatal(err)
	}
	if err := checkOptionsFile(o, dir); err != nil {
		t.Fatalf("matching options must pass: %v", err)
	}
	// depth mismatch
	o2 := o
	o2.GroupDepth = 5
	if err := checkOptionsFile(o2, dir); !errors.Is(err, base.ErrOptionsMismatch) {
		t.Fatalf("depth mismatch: %v", err)
	}
	// delimiter mismatch
	o3 := o
	o3.GroupDelimiter = ':'
	if err := checkOptionsFile(o3, dir); !errors.Is(err, base.ErrOptionsMismatch) {
		t.Fatalf("delimiter mismatch: %v", err)
	}
	// policy mismatch
	o4 := o
	o4.Policies = []Policy{{Prefix: "b/", Mode: ModeState}}
	if err := checkOptionsFile(o4, dir); !errors.Is(err, base.ErrOptionsMismatch) {
		t.Fatalf("policy mismatch: %v", err)
	}
}
