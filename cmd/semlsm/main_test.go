package main

import (
	"bytes"
	"os"
	"strings"
	"testing"
)

// TestCLIGolden runs the CLI through run() against a temp dir and checks the
// golden outputs (P14.1). Exit codes: 0 ok, 1 on error.
func TestCLIGolden(t *testing.T) {
	dir := t.TempDir()
	var out bytes.Buffer
	runWith := func(args ...string) (string, int) {
		out.Reset()
		code := run(append([]string{"--dir", dir}, args...), &out, &out)
		return out.String(), code
	}

	// put / get / del round trip.
	if _, code := runWith("put", "agent/1/state", "hello"); code != 0 {
		t.Fatal("put failed")
	}
	s, code := runWith("get", "agent/1/state")
	if code != 0 || !strings.Contains(s, "hello @") {
		t.Fatalf("get: code %d out %q", code, s)
	}
	if _, code := runWith("del", "agent/1/state"); code != 0 {
		t.Fatalf("del: code %d", code)
	}
	if s, code := runWith("get", "agent/1/state"); code != 0 || !strings.Contains(s, "<not found>") {
		t.Fatalf("get after del: code %d out %q", code, s)
	}

	// A few keys, then scan.
	for _, kv := range [][2]string{{"agent/2/a", "va"}, {"agent/2/b", "vb"}, {"other/1/c", "vc"}} {
		if _, code := runWith("put", kv[0], kv[1]); code != 0 {
			t.Fatalf("put %s", kv[0])
		}
	}
	s, code = runWith("scan", "--start", "agent/", "--end", "agent/3")
	if code != 0 {
		t.Fatalf("scan: code %d out %q", code, s)
	}
	if strings.Count(s, "\n") != 2 || !strings.Contains(s, "agent/2/a\tva") || !strings.Contains(s, "agent/2/b\tvb") {
		t.Fatalf("scan output: %q", s)
	}

	// History lists the del and the puts of agent/1/state oldest first.
	s, code = runWith("history", "agent/1/state")
	if code != 0 || !strings.Contains(s, "DEL") || !strings.Contains(s, "PUT hello") {
		t.Fatalf("history output: %q", s)
	}

	// Checkpoints lists nothing before compaction, works after.
	s, code = runWith("checkpoints", "agent/2/")
	if code != 0 {
		t.Fatalf("checkpoints: code %d out %q", code, s)
	}
	if strings.Contains(s, "tuple") {
		t.Fatalf("checkpoints before compaction should be empty: %q", s)
	}

	// compact --level N runs; --level is required.
	if _, code := runWith("compact"); code != 1 {
		t.Fatalf("compact without --level must exit 1")
	}
	if _, code := runWith("compact", "--level", "0"); code != 0 {
		t.Fatalf("compact 0: code %d", code)
	}

	// stats and verify.
	s, code = runWith("stats")
	if code != 0 || !strings.Contains(s, "visible_ts") || !strings.Contains(s, "level_0") {
		t.Fatalf("stats output: %q", s)
	}
	s, code = runWith("verify")
	if code != 0 || !strings.Contains(s, "problems 0") {
		t.Fatalf("verify output: %q", s)
	}

	// Errors exit 1: unknown command, bad usage, missing key.
	if _, code := runWith("frobnicate"); code != 1 {
		t.Fatal("unknown command must exit 1")
	}
	if _, code := runWith("put", "onlykey"); code != 1 {
		t.Fatal("put with one arg must exit 1")
	}
	if _, code := runWith("get"); code != 1 {
		t.Fatal("get without a key must exit 1")
	}

	// --llm without the environment fails cleanly (exit 1, no key printed).
	out.Reset()
	code = run([]string{"--dir", dir, "--llm", "stats"}, &out, &out)
	if code != 1 || !strings.Contains(out.String(), "SEMLSM_LLM_BASE_URL") {
		t.Fatalf("--llm without env: code %d out %q", code, out.String())
	}
	if strings.Contains(out.String(), os.Getenv("SEMLSM_LLM_API_KEY")) && os.Getenv("SEMLSM_LLM_API_KEY") != "" {
		t.Fatal("the API key must never appear in CLI output")
	}
}
