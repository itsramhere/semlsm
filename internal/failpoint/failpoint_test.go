package failpoint

import (
	"errors"
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
	// nil fn disarms the hook
	Set("flush.after_sst_sync", nil)
	if err := Hit("flush.after_sst_sync"); err != nil {
		t.Fatalf("disarmed hook: %v", err)
	}
}

func TestFailpointResetClearsHooksKeepsDefinitions(t *testing.T) {
	Reset()
	defer Reset()
	Define("a")
	Define("b")
	SetError("a", errors.New("a"))
	SetError("b", errors.New("b"))
	Reset()
	if err := Hit("a"); err != nil {
		t.Fatalf("reset must disarm: %v", err)
	}
	if err := Hit("b"); err != nil {
		t.Fatalf("reset must disarm: %v", err)
	}
	names := List()
	got := map[string]bool{}
	for _, n := range names {
		got[n] = true
	}
	if !got["a"] || !got["b"] {
		t.Fatalf("reset must keep definitions, List = %v", names)
	}
}

func TestFailpointListOnlyDefinedNames(t *testing.T) {
	Reset()
	defer Reset()
	n1 := Define("pkg.first_hook")
	n2 := Define("pkg.second_hook")
	if n1 != "pkg.first_hook" || n2 != "pkg.second_hook" {
		t.Fatalf("Define must return the name")
	}
	names := List()
	got := map[string]bool{}
	for _, n := range names {
		got[n] = true
	}
	if !got["pkg.first_hook"] || !got["pkg.second_hook"] {
		t.Fatalf("List = %v, want defined names", names)
	}
	if got["pkg.never_defined"] {
		t.Fatal("List must not contain undefined names")
	}
	// Arming an undefined name is a programming error but must not add it to List.
	SetError("pkg.never_defined", errors.New("x"))
	defer Set("pkg.never_defined", nil)
	for _, n := range List() {
		if n == "pkg.never_defined" {
			t.Fatal("armed-but-undefined must not appear in List")
		}
	}
}
