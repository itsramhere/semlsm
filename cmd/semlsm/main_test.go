package main

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"semlsm/internal/base"
	"semlsm/internal/checkpoint"
	"semlsm/internal/manifest"
	"semlsm/internal/vfs"
)

func TestCLICompactCheckpointsGolden(t *testing.T) {
	dir := t.TempDir()
	var out, stderr bytes.Buffer
	if code := run([]string{"--dir", dir, "put", "raw/key", "value"}, &out, &stderr); code != 0 {
		t.Fatalf("put: %d %s", code, stderr.String())
	}
	fs := vfs.OSFS{}
	name, err := manifest.ReadCurrent(fs, dir)
	if err != nil {
		t.Fatal(err)
	}
	edits, err := manifest.ReadAll(fs, filepath.Join(dir, name))
	if err != nil {
		t.Fatal(err)
	}
	edit := manifest.Edit{NextFileNum: 3000}
	for i := uint64(0); i < 2; i++ {
		num := 1000 + i
		path := filepath.Join(dir, fmt.Sprintf("%06d.ckpt", num))
		w, err := checkpoint.NewWriter(fs, path)
		if err != nil {
			t.Fatal(err)
		}
		meta := checkpoint.TupleMeta{ID: 2000 + i, Group: "g/", TsFrom: i + 1, TsTo: i + 1}
		if i != 0 {
			meta.PrevTupleID = 2000
		}
		if err := w.AddTuple(meta, []byte(`{}`), []checkpoint.KeyEntry{{Key: []byte("g/key"), MinTs: i + 1, MaxTs: i + 1, Count: 1, LastKind: base.KindPut}}); err != nil {
			t.Fatal(err)
		}
		if err := w.Finish(); err != nil {
			t.Fatal(err)
		}
		loc := w.DirEntries()[0]
		edit.AddCkpts = append(edit.AddCkpts, manifest.CkptFile{FileNum: num, ArchiveNums: []uint64{}, Tuples: []manifest.TupleRef{{ID: meta.ID, Group: meta.Group, TsFrom: meta.TsFrom, TsTo: meta.TsTo, Offset: loc.Offset, Length: loc.Length}}})
	}
	w, err := manifest.NewWriter(fs, filepath.Join(dir, name))
	if err != nil {
		t.Fatal(err)
	}
	for _, edit := range append(edits, edit) {
		if err := w.Append(&edit); err != nil {
			t.Fatal(err)
		}
	}
	w.Close()
	out.Reset()
	stderr.Reset()
	if code := run([]string{"--dir", dir, "compact", "--checkpoints"}, &out, &stderr); code != 0 || out.String() != "checkpoints compacted\n" || stderr.Len() != 0 {
		t.Fatalf("compact golden: %d %q %q", code, out.String(), stderr.String())
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), ".ckpt") {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("CLI did not merge: %d checkpoint files", count)
	}
}

func TestCLIRepairGolden(t *testing.T) {
	dir := t.TempDir()
	var out, stderr bytes.Buffer
	if code := run([]string{"--dir", dir, "put", "agent/1/k", "value"}, &out, &stderr); code != 0 {
		t.Fatalf("put: %d %s", code, stderr.String())
	}
	name, err := manifest.ReadCurrent(vfs.OSFS{}, dir)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, name)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// Add one complete edit, then damage its payload.
	// Use the manifest writer on another file to produce its canonical frame.
	temp := filepath.Join(dir, "edit.tmp")
	w, err := manifest.NewWriter(vfs.OSFS{}, temp)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Append(&manifest.Edit{NextFileNum: 900}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	frame, err := os.ReadFile(temp)
	if err != nil {
		t.Fatal(err)
	}
	frame[len(frame)-1] ^= 1
	if err := os.WriteFile(path, append(data, frame...), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "999999.sst"), []byte("preserve me"), 0600); err != nil {
		t.Fatal(err)
	}
	out.Reset()
	stderr.Reset()
	if code := run([]string{"repair", "--dir", dir}, &out, &stderr); code != 0 {
		t.Fatalf("repair: %d %s", code, stderr.String())
	}
	want := fmt.Sprintf("truncated %s bytes_dropped %d\nquarantined %s\nrepair truncated 1 quarantined 1\n", name, len(frame), filepath.Join("lost+found", "999999.sst"))
	if out.String() != want || stderr.Len() != 0 {
		t.Fatalf("repair golden:\ngot %q stderr %q\nwant %q", out.String(), stderr.String(), want)
	}
	out.Reset()
	stderr.Reset()
	if code := run([]string{"--dir", dir, "get", "agent/1/k"}, &out, &stderr); code != 0 || !strings.Contains(out.String(), "value @") {
		t.Fatalf("after repair: %d %s %s", code, out.String(), stderr.String())
	}
	out.Reset()
	stderr.Reset()
	if code := run([]string{"--dir", t.TempDir(), "repair"}, &out, &stderr); code != 1 || !strings.Contains(stderr.String(), "semlsm: repair:") {
		t.Fatalf("repair error: %d %q", code, stderr.String())
	}
}

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
