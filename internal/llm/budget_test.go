package llm

import (
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"semlsm/internal/vfs"
)

func openBudget(t *testing.T, fs vfs.FS, dir string, max int64) *Budget {
	t.Helper()
	b, err := OpenBudget(fs, dir, max)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestBudgetZeroMaxRejected(t *testing.T) {
	fs := vfs.NewMemFS()
	if _, err := OpenBudget(fs, "db", 0); err == nil {
		t.Fatal("MaxTokensTotal=0 must be rejected")
	}
	if _, err := OpenBudget(fs, "db", -5); err == nil {
		t.Fatal("negative MaxTokensTotal must be rejected")
	}
}

func TestBudgetReserveRecordPersist(t *testing.T) {
	fs := vfs.NewMemFS()
	b := openBudget(t, fs, "db", 10_000)

	if err := b.Reserve(1000); err != nil {
		t.Fatal(err)
	}
	if err := b.Record(1000, Response{InputTokens: 300, OutputTokens: 200}); err != nil {
		t.Fatal(err)
	}
	in, out, calls := b.Used()
	if in != 300 || out != 200 || calls != 1 {
		t.Fatalf("used: %d/%d calls=%d", in, out, calls)
	}

	// The file holds the cumulative totals, and no temp file is left.
	data, err := readFileAll(fs, filepath.Join("db", "llm", "usage.json"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), `"input_tokens":300`) ||
		!strings.Contains(string(data), `"output_tokens":200`) ||
		!strings.Contains(string(data), `"calls":1`) {
		t.Fatalf("usage.json: %s", data)
	}
	entries, err := fs.List(filepath.Join("db", "llm"))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name != "usage.json" {
		t.Fatalf("llm dir: %+v", entries)
	}
}

func TestBudgetExhausted(t *testing.T) {
	fs := vfs.NewMemFS()
	b := openBudget(t, fs, "db", 1000)
	if err := b.Reserve(900); err != nil {
		t.Fatal(err)
	}
	// 900 pending + 200 would exceed 1000.
	if err := b.Reserve(200); !errors.Is(err, ErrBudgetExhausted) {
		t.Fatalf("want ErrBudgetExhausted, got %v", err)
	}
	// Release frees the reservation, so the 200 fits again.
	b.Release(900)
	if err := b.Record(900, Response{InputTokens: 900}); err != nil {
		t.Fatal(err)
	}
	if err := b.Reserve(100); err != nil {
		t.Fatal(err)
	}
	if err := b.Reserve(1); !errors.Is(err, ErrBudgetExhausted) {
		t.Fatalf("want exhausted at the cap, got %v", err)
	}
}

func TestBudgetSurvivesReopen(t *testing.T) {
	fs := vfs.NewMemFS()
	b := openBudget(t, fs, "db", 5000)
	if err := b.Reserve(600); err != nil {
		t.Fatal(err)
	}
	if err := b.Record(600, Response{InputTokens: 1234, OutputTokens: 567}); err != nil {
		t.Fatal(err)
	}

	b2 := openBudget(t, fs, "db", 5000) // same FS: restart
	in, out, calls := b2.Used()
	if in != 1234 || out != 567 || calls != 1 {
		t.Fatalf("reopened: %d/%d calls=%d", in, out, calls)
	}
	// Reservations do not survive (in-memory), but the recorded spend does:
	// 1801 spent + 3199 pending would be the cap.
	if err := b2.Reserve(3199); err != nil {
		t.Fatalf("reserve at exactly the remaining cap: %v", err)
	}
	if err := b2.Reserve(1); !errors.Is(err, ErrBudgetExhausted) {
		t.Fatal("want exhausted past the cap")
	}
}

func TestBudgetCorruptFileIsAnError(t *testing.T) {
	fs := vfs.NewMemFS()
	dir := filepath.Join("db", "llm")
	if err := fs.MkdirAll(dir); err != nil {
		t.Fatal(err)
	}
	f, err := fs.Create(filepath.Join(dir, "usage.json"))
	if err != nil {
		t.Fatal(err)
	}
	f.Write([]byte("{not json"))
	f.Close()
	if _, err := OpenBudget(fs, "db", 1000); err == nil {
		t.Fatal("corrupt usage.json must be an error, not a silent reset")
	}
}
