package semlsm

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"path/filepath"
	"time"

	"semlsm/internal/base"
	"semlsm/internal/llm"
	"semlsm/internal/semantic"
	"semlsm/internal/vfs"
)

// SyncMode controls when the WAL is synced per commit (B5/P5.1).
type SyncMode uint8

const (
	// SyncAlways syncs every commit (default; I1 under crashes).
	SyncAlways SyncMode = iota
	// SyncEveryN syncs every N commits.
	SyncEveryN
	// SyncNone never syncs from the commit path (flush still syncs).
	SyncNone
)

// Mode is the summarization policy for a key prefix (B4). The canonical
// definition lives in internal/base (shared with internal/semantic, which
// may not import the root); this alias keeps the public API unchanged.
type Mode = base.Mode

const (
	// ModeDisabled never summarizes.
	ModeDisabled = base.ModeDisabled
	// ModeState is for repeatedly updated keys: the newest version is never dropped.
	ModeState = base.ModeState
	// ModeEvents is for append-only streams: the newest version may be dropped.
	ModeEvents = base.ModeEvents
)

// Policy selects which keys Tier B may summarize and under what conditions.
// Alias of base.Policy for the same reason as Mode.
type Policy = base.Policy

// Options configures Open. Zero fields take their defaults from withDefaults.
type Options struct {
	FS    vfs.FS
	Clock base.Clock

	SyncMode   SyncMode
	SyncEveryN int

	MemtableBytes         int64 // 64 MiB
	MaxImmutableMemtables int   // 2

	L0CompactTrigger int // 4
	L0StallTrigger   int // 12

	L1TargetBytes         int64 // 64 MiB
	LevelMultiplier       int   // 10
	TargetFileBytes       int64 // 16 MiB
	GroupDepth            int
	GroupDelimiter        byte // '/'
	Policies              []Policy
	DefaultPolicy         Policy
	Summarizer            llm.Summarizer // nil disables Tier B semantic work
	SemanticWorkers       int            // 1
	MaxChunkBytes         int            // 32 KiB
	MaxValueBytesInPrompt int            // 2 KiB
	MaxSemanticJobBytes   int64          // 256 MiB
	SemanticJobTimeout    time.Duration  // 30 min
	HardScore             float64        // 2.0
	SemanticSweepInterval time.Duration  // 10 min
	MinCompressionRatio   float64        // 5.0
	CkptMergeThreshold    int            // 32; negative disables automatic merging

	ArchiveRaw      bool
	ArchiveDir      string
	BlockCacheBytes int64 // 64 MiB
	MaxOpenTables   int   // 256

	DisableAutoCompaction bool
	Logger                *slog.Logger
}

// optionsFile is the persisted OPTIONS.json: group settings only (B4/P5.1).
type optionsFile struct {
	GroupDepth     int      `json:"group_depth"`
	GroupDelimiter string   `json:"group_delimiter"`
	Policies       []Policy `json:"policies"`
}

// withDefaults fills zero fields and validates.
func (o Options) withDefaults() (Options, error) {
	if o.FS == nil {
		o.FS = vfs.OSFS{}
	}
	if o.Clock == nil {
		o.Clock = base.RealClock{}
	}
	if o.SyncEveryN <= 0 {
		o.SyncEveryN = 1
	}
	if o.MemtableBytes <= 0 {
		o.MemtableBytes = 64 << 20
	}
	if o.MaxImmutableMemtables <= 0 {
		o.MaxImmutableMemtables = 2
	}
	if o.L0CompactTrigger <= 0 {
		o.L0CompactTrigger = 4
	}
	if o.L0StallTrigger <= 0 {
		o.L0StallTrigger = 12
	}
	if o.L1TargetBytes <= 0 {
		o.L1TargetBytes = 64 << 20
	}
	if o.LevelMultiplier <= 0 {
		o.LevelMultiplier = 10
	}
	if o.TargetFileBytes <= 0 {
		o.TargetFileBytes = 16 << 20
	}
	if o.GroupDepth == 0 {
		o.GroupDepth = 2
	}
	if o.GroupDelimiter == 0 {
		o.GroupDelimiter = '/'
	}
	if o.DefaultPolicy.MaxBatch == 0 {
		o.DefaultPolicy.MaxBatch = 10000
	}
	if o.DefaultPolicy.MinBatch == 0 {
		o.DefaultPolicy.MinBatch = 1000
	}
	if o.DefaultPolicy.MinAge == 0 && o.DefaultPolicy.Mode == ModeDisabled {
		o.DefaultPolicy.MinAge = time.Hour
	}
	if o.SemanticWorkers <= 0 {
		o.SemanticWorkers = 1
	}
	if o.MaxChunkBytes <= 0 {
		o.MaxChunkBytes = 32 << 10
	}
	if o.MaxValueBytesInPrompt <= 0 {
		o.MaxValueBytesInPrompt = 2 << 10
	}
	if o.MaxSemanticJobBytes <= 0 {
		o.MaxSemanticJobBytes = 256 << 20
	}
	if o.SemanticJobTimeout <= 0 {
		o.SemanticJobTimeout = 30 * time.Minute
	}
	if o.HardScore <= 0 {
		o.HardScore = 2.0
	}
	if o.SemanticSweepInterval <= 0 {
		o.SemanticSweepInterval = 10 * time.Minute
	}
	if o.MinCompressionRatio <= 0 {
		o.MinCompressionRatio = 5.0
	}
	if o.BlockCacheBytes <= 0 {
		o.BlockCacheBytes = 64 << 20
	}
	if o.MaxOpenTables <= 0 {
		o.MaxOpenTables = 256
	}
	if o.CkptMergeThreshold == 0 {
		o.CkptMergeThreshold = 32
	}
	if o.Logger == nil {
		o.Logger = slog.New(slog.DiscardHandler)
	}
	for i := range o.Policies {
		p := &o.Policies[i]
		if p.MinBatch == 0 {
			p.MinBatch = 1000
		}
		if p.MaxBatch == 0 {
			p.MaxBatch = 10000
		}
		if p.MinAge == 0 && p.Mode != ModeDisabled {
			p.MinAge = time.Hour
		}
	}
	if err := o.validate(); err != nil {
		return o, err
	}
	return o, nil
}

func (o Options) validate() error {
	if o.L0StallTrigger < o.L0CompactTrigger {
		return fmt.Errorf("options: L0StallTrigger (%d) < L0CompactTrigger (%d)", o.L0StallTrigger, o.L0CompactTrigger)
	}
	if o.LevelMultiplier < 1 {
		return fmt.Errorf("options: LevelMultiplier must be >= 1")
	}
	if o.TargetFileBytes < 4096 {
		return fmt.Errorf("options: TargetFileBytes too small")
	}
	if o.MemtableBytes < 4096 {
		return fmt.Errorf("options: MemtableBytes too small")
	}
	if o.GroupDepth < 0 {
		return fmt.Errorf("options: GroupDepth must be >= 0")
	}
	seen := map[string]bool{}
	for _, p := range o.Policies {
		if seen[p.Prefix] {
			return fmt.Errorf("options: duplicate policy prefix %q", p.Prefix)
		}
		seen[p.Prefix] = true
		if p.Mode != ModeDisabled && p.MinBatch > p.MaxBatch {
			return fmt.Errorf("options: policy %q MinBatch (%d) > MaxBatch (%d)", p.Prefix, p.MinBatch, p.MaxBatch)
		}
		if p.MinAge < 0 {
			return fmt.Errorf("options: policy %q MinAge negative", p.Prefix)
		}
	}
	if o.DefaultPolicy.Mode != ModeDisabled && o.DefaultPolicy.MinBatch > o.DefaultPolicy.MaxBatch {
		return fmt.Errorf("options: default policy MinBatch > MaxBatch")
	}
	// The LLM rate limit must be able to admit a worst-case request
	// (M-025 item 3): EstimateTokens of a MaxChunkBytes-sized prompt plus
	// the completion budget. Otherwise semantic compaction would fail on
	// every call with "request exceeds TokensPerMinute".
	if o.Summarizer != nil {
		if r, ok := o.Summarizer.(interface{ TokensPerMinute() int }); ok {
			worst := o.MaxChunkBytes/3 + semantic.MaxOutputTokens
			if worst > r.TokensPerMinute() {
				return fmt.Errorf("options: TokensPerMinute (%d) cannot cover a worst-case request of %d estimated tokens (MaxChunkBytes %d / 3 + MaxOutputTokens %d); raise the token rate or lower MaxChunkBytes",
					r.TokensPerMinute(), worst, o.MaxChunkBytes, semantic.MaxOutputTokens)
			}
		}
	}
	return nil
}

// policyFor returns the policy for a key: the longest matching prefix wins,
// else the default.
func policyFor(opts Options, key []byte) Policy {
	var best *Policy
	for i := range opts.Policies {
		p := &opts.Policies[i]
		if len(p.Prefix) == 0 {
			continue
		}
		if len(key) >= len(p.Prefix) && string(key[:len(p.Prefix)]) == p.Prefix {
			if best == nil || len(p.Prefix) > len(best.Prefix) {
				best = p
			}
		}
	}
	if best != nil {
		return *best
	}
	return opts.DefaultPolicy
}

// writeOptionsFile persists group settings at creation.
func writeOptionsFile(opts Options, dir string) error {
	of := optionsFile{
		GroupDepth:     opts.GroupDepth,
		GroupDelimiter: string(opts.GroupDelimiter),
		Policies:       append([]Policy(nil), opts.Policies...),
	}
	data, err := json.Marshal(of)
	if err != nil {
		return fmt.Errorf("options: encode: %w", err)
	}
	f, err := opts.FS.Create(filepath.Join(dir, "OPTIONS.json"))
	if err != nil {
		return fmt.Errorf("options: write: %w", err)
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return fmt.Errorf("options: write: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("options: write: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("options: write: %w", err)
	}
	return opts.FS.SyncDir(dir)
}

// checkOptionsFile compares persisted group settings with opts.
func checkOptionsFile(opts Options, dir string) error {
	f, err := opts.FS.Open(filepath.Join(dir, "OPTIONS.json"))
	if err != nil {
		// Missing file: a DB created before this format; treat as mismatch-safe
		// only if there is no data. Recovery handles file absence separately;
		// here absence means legacy DB: accept.
		return nil
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return fmt.Errorf("options: read: %w", err)
	}
	buf := make([]byte, st.Size)
	if _, err := f.Read(buf); err != nil && err.Error() != "EOF" {
		return fmt.Errorf("options: read: %w", err)
	}
	var of optionsFile
	if err := json.Unmarshal(buf, &of); err != nil {
		return fmt.Errorf("options: parse: %w", base.ErrCorruption)
	}
	if of.GroupDepth != opts.GroupDepth {
		return fmt.Errorf("options: group_depth %d != %d: %w", of.GroupDepth, opts.GroupDepth, base.ErrOptionsMismatch)
	}
	wantDelim := string(opts.GroupDelimiter)
	if of.GroupDelimiter != wantDelim {
		return fmt.Errorf("options: group_delimiter %q != %q: %w", of.GroupDelimiter, wantDelim, base.ErrOptionsMismatch)
	}
	if len(of.Policies) != len(opts.Policies) {
		return fmt.Errorf("options: %d policies != %d: %w", len(of.Policies), len(opts.Policies), base.ErrOptionsMismatch)
	}
	for i := range of.Policies {
		if of.Policies[i] != opts.Policies[i] {
			return fmt.Errorf("options: policy %d differs: %w", i, base.ErrOptionsMismatch)
		}
	}
	return nil
}
