package llm

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"sync"

	"github.com/itsramhere/semlsm/internal/vfs"
)

// persistedUsage is the on-disk shape of llm/usage.json (P9.3).
type persistedUsage struct {
	InputTokens  int64 `json:"input_tokens"`
	OutputTokens int64 `json:"output_tokens"`
	Calls        int64 `json:"calls"`
}

// Budget caps the cumulative LLM token spend across restarts (P9.3). The
// running total is persisted at <dbDir>/llm/usage.json with an atomic
// write. Reserve holds an in-memory reservation for one estimated call;
// Record reconciles it with the response's actual usage and persists;
// Release gives a reservation back (failed call). Once
// used+pending+next-estimate exceeds MaxTokensTotal, Reserve returns
// ErrBudgetExhausted and stays exhausted until the process reopens the DB.
type Budget struct {
	mu      sync.Mutex
	fs      vfs.FS
	file    string // <dbDir>/llm/usage.json
	max     int64
	used    persistedUsage
	pending int64
}

// OpenBudget opens (or initializes) the budget for a DB directory.
// MaxTokensTotal is mandatory: <= 0 is an error, so an HTTP-backed stack
// cannot run without a budget.
func OpenBudget(fs vfs.FS, dbDir string, maxTokensTotal int64) (*Budget, error) {
	if maxTokensTotal <= 0 {
		return nil, errors.New("llm: MaxTokensTotal must be > 0 (mandatory for HTTP clients)")
	}
	dir := filepath.Join(dbDir, "llm")
	if err := fs.MkdirAll(dir); err != nil {
		return nil, fmt.Errorf("llm: create %s: %w", dir, err)
	}
	file := filepath.Join(dir, "usage.json")
	b := &Budget{fs: fs, file: file, max: maxTokensTotal}
	f, err := fs.Open(file)
	if err != nil {
		if errors.Is(err, vfs.ErrNotExist) {
			return b, nil // first open: no usage yet
		}
		return nil, fmt.Errorf("llm: open %s: %w", file, err)
	}
	defer f.Close()
	data, err := io.ReadAll(f)
	if err != nil {
		return nil, fmt.Errorf("llm: read %s: %w", file, err)
	}
	if err := json.Unmarshal(data, &b.used); err != nil {
		// A corrupt usage file must not silently reset the spend.
		return nil, fmt.Errorf("llm: corrupt %s: %w", file, err)
	}
	return b, nil
}

// Reserve checks that one call of the given estimated size still fits the
// budget and holds the estimate until Record or Release.
func (b *Budget) Reserve(tokens int64) error {
	if tokens < 1 {
		tokens = 1
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if total := b.used.InputTokens + b.used.OutputTokens + b.pending + tokens; total > b.max {
		return fmt.Errorf("%w: %d of %d tokens spent, next call needs %d",
			ErrBudgetExhausted, b.used.InputTokens+b.used.OutputTokens+b.pending, b.max, tokens)
	}
	b.pending += tokens
	return nil
}

// Record reconciles a finished call: the estimate is returned and the
// response's actual usage added, then the total is persisted.
func (b *Budget) Record(estimate int64, r Response) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.returnPending(estimate)
	b.used.InputTokens += int64(r.InputTokens)
	b.used.OutputTokens += int64(r.OutputTokens)
	b.used.Calls++
	return b.persistLocked()
}

// Release gives back a reservation made by Reserve for a call that never
// completed. The in-memory pending count is the source of truth for
// reservations, so nothing is persisted here.
func (b *Budget) Release(estimate int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.returnPending(estimate)
}

// Used returns the persisted totals (P10.3 stats).
func (b *Budget) Used() (input, output, calls int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.used.InputTokens, b.used.OutputTokens, b.used.Calls
}

// Remaining returns how many tokens the budget can still admit, counting
// pending reservations (P10.2 health checks).
func (b *Budget) Remaining() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	r := b.max - b.used.InputTokens - b.used.OutputTokens - b.pending
	if r < 0 {
		r = 0
	}
	return r
}

func (b *Budget) returnPending(estimate int64) {
	b.pending -= estimate
	if b.pending < 0 {
		b.pending = 0
	}
}

// persistLocked atomically writes usage.json (must hold mu).
func (b *Budget) persistLocked() error {
	data, err := json.Marshal(b.used)
	if err != nil {
		return err
	}
	return writeFileAtomic(b.fs, b.file, data)
}
