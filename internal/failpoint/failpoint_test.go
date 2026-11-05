package failpoint

import (
	"errors"
	"sort"
	"testing"
)

func TestFailpointUnset(t *testing.T) {
	Reset()
	defer Reset()
	if err := Hit("nothing"); err != nil {
		t.Fatalf("unset hook must return nil, got %v", err)
	}
}

func TestFailpointSetHit(t *testing.T) {
	Reset()
	defer Reset()
	sentinel := errors.New("boom")
	SetError("compact.before_manifest", sentinel)
	if err := Hit("compact.before_manifest"); err != sentinel {
		t.Fatalf("got %v", err)
	}
	Set("flush.after_sst_sync", func() error { return nil })
	if err := Hit("flush.after_sst_sync"); err != nil {
		t.Fatalf("fn hook returning nil: %v", err)
	}
	// nil fn removes the hook
	Set("flush.after_sst_sync", nil)
	if err := Hit("flush.after_sst_sync"); err != nil {
		t.Fatalf("removed hook: %v", err)
	}
}

func TestFailpointResetAndList(t *testing.T) {
	Reset()
	defer Reset()
	SetError("a", errors.New("a"))
	SetError("b", errors.New("b"))
	names := List()
	sort.Strings(names)
	if len(names) != 2 || names[0] != "a" || names[1] != "b" {
		t.Fatalf("list = %v", names)
	}
	Reset()
	if len(List()) != 0 {
		t.Fatal("reset must clear all")
	}
}
