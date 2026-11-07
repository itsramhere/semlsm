// Command semlsm is the engine's CLI (P14.1): put/get/del/scan/history/
// checkpoints/stats/verify/compact against a database directory. The LLM is
// the deterministic mock unless --llm is set, in which case the client is
// configured from SEMLSM_LLM_BASE_URL / SEMLSM_LLM_API_KEY / SEMLSM_LLM_MODEL
// (R6: the key is read from the environment and never printed).
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"time"

	"semlsm"
	"semlsm/internal/base"
	"semlsm/internal/llm"
	"semlsm/internal/vfs"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

// run executes one CLI invocation and returns the process exit code
// (0 ok, 1 on any error). Separated from main for golden tests.
func run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprintln(stderr, "usage: semlsm [--dir DIR] [--llm] [--auto-repair] <command> [args]")
		return 1
	}
	dir := "semlsm-data"
	useLLM := false
	autoRepair := false
	var archiveRetention, summaryRetention time.Duration
	var rest []string
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--dir":
			if i+1 >= len(args) {
				fmt.Fprintln(stderr, "--dir needs a value")
				return 1
			}
			i++
			dir = args[i]
		case "--llm":
			useLLM = true
		case "--auto-repair":
			autoRepair = true
		case "--archive-retention", "--summary-retention":
			name := args[i]
			if i+1 >= len(args) {
				fmt.Fprintf(stderr, "%s needs a duration\n", name)
				return 1
			}
			i++
			age, err := time.ParseDuration(args[i])
			if err != nil {
				fmt.Fprintf(stderr, "%s: %v\n", name, err)
				return 1
			}
			if name == "--archive-retention" {
				archiveRetention = age
			} else {
				summaryRetention = age
			}
		default:
			rest = append(rest, args[i])
		}
	}
	if len(rest) == 0 {
		fmt.Fprintln(stderr, "usage: semlsm [--dir DIR] [--llm] [--auto-repair] <command> [args]")
		return 1
	}

	if rest[0] == "repair" {
		report, err := semlsm.Repair(dir, cliDBOptions())
		for _, f := range report.TruncatedFiles {
			fmt.Fprintf(stdout, "truncated %s bytes_dropped %d\n", f.Name, f.BytesDropped)
		}
		for _, name := range report.QuarantinedFiles {
			fmt.Fprintf(stdout, "quarantined %s\n", name)
		}
		if err != nil {
			fmt.Fprintf(stderr, "semlsm: repair: %v\n", err)
			return 1
		}
		fmt.Fprintf(stdout, "repair truncated %d quarantined %d\n", len(report.TruncatedFiles), len(report.QuarantinedFiles))
		return 0
	}
	opts, err := cliOptions(dir, useLLM)
	if err != nil {
		fmt.Fprintf(stderr, "semlsm: %v\n", err)
		return 1
	}
	opts.AutoRepair = autoRepair
	opts.ArchiveRetention, opts.SummaryRetention = archiveRetention, summaryRetention
	if autoRepair {
		opts.Logger = slog.New(slog.NewTextHandler(stderr, nil))
	}
	db, err := semlsm.Open(dir, opts)
	if err != nil {
		fmt.Fprintf(stderr, "semlsm: open: %v\n", err)
		return 1
	}
	defer db.Close()

	cmd, args2 := rest[0], rest[1:]
	switch cmd {
	case "put":
		if len(args2) != 2 {
			fmt.Fprintln(stderr, "usage: semlsm put <key> <value>")
			return 1
		}
		if _, err := db.Put([]byte(args2[0]), []byte(args2[1])); err != nil {
			fmt.Fprintf(stderr, "semlsm: put: %v\n", err)
			return 1
		}
	case "del":
		if len(args2) != 1 {
			fmt.Fprintln(stderr, "usage: semlsm del <key>")
			return 1
		}
		if _, err := db.Delete([]byte(args2[0])); err != nil {
			fmt.Fprintf(stderr, "semlsm: del: %v\n", err)
			return 1
		}
	case "get":
		return cmdGet(db, args2, stdout, stderr)
	case "scan":
		return cmdScan(db, args2, stdout, stderr)
	case "history":
		return cmdHistory(db, args2, stdout, stderr)
	case "checkpoints":
		return cmdCheckpoints(db, args2, stdout, stderr)
	case "stats":
		st := db.Stats()
		fmt.Fprintf(stdout, "visible_ts %d\nlast_ts %d\nmemtable_bytes %d\nsnapshots_open %d\ncompaction_errors %d\n",
			st.VisibleTs, st.LastTs, st.MemtableBytes, st.SnapshotsOpen, st.CompactionErrors)
		for l, n := range st.LevelFiles {
			fmt.Fprintf(stdout, "level_%d files %d bytes %d\n", l, n, st.LevelBytes[l])
		}
		fmt.Fprintf(stdout, "tuples_created %d\nrecords_summarized %d\nraw_bytes_removed %d\nsummary_bytes %d\n",
			st.SemanticTuplesCreated, st.SemanticRecordsSummarized, st.SemanticRawBytesRemoved, st.SemanticSummaryBytes)
		fmt.Fprintf(stdout, "llm_calls %d llm_input_tokens %d llm_output_tokens %d llm_cache_hits %d\n",
			st.LLMCalls, st.LLMInputTokens, st.LLMOutputTokens, st.LLMCacheHits)
		fmt.Fprintf(stdout, "breaker %s\n", st.LLMBreakerState)
	case "verify":
		rep, err := db.VerifyIntegrity(context.Background())
		if err != nil {
			fmt.Fprintf(stderr, "semlsm: verify: %v\n", err)
			return 1
		}
		fmt.Fprintf(stdout, "sst_files %d ckpt_files %d tuples %d entries %d problems %d\n",
			rep.SSTFiles, rep.CkptFiles, rep.Tuples, rep.Entries, len(rep.Problems))
		for _, p := range rep.Problems {
			fmt.Fprintf(stdout, "problem: %s\n", p)
		}
		if len(rep.Problems) > 0 {
			return 1
		}
	case "compact":
		fs := flag.NewFlagSet("compact", flag.ContinueOnError)
		fs.SetOutput(stderr)
		level := fs.Int("level", -1, "level to compact")
		checkpoints := fs.Bool("checkpoints", false, "merge checkpoint files")
		retention := fs.Bool("retention", false, "apply tuple retention")
		if err := fs.Parse(args2); err != nil {
			return 1
		}
		if *retention {
			if err := db.ApplyRetention(context.Background()); err != nil {
				fmt.Fprintf(stderr, "semlsm: retention: %v\n", err)
				return 1
			}
			fmt.Fprintln(stdout, "retention applied")
			return 0
		}
		if *checkpoints {
			if err := db.CompactCheckpoints(context.Background()); err != nil {
				fmt.Fprintf(stderr, "semlsm: compact checkpoints: %v\n", err)
				return 1
			}
			fmt.Fprintln(stdout, "checkpoints compacted")
			return 0
		}
		if *level < 0 {
			fmt.Fprintln(stderr, "usage: semlsm compact --level N | --checkpoints | --retention [--archive-retention AGE] [--summary-retention AGE]")
			return 1
		}
		if err := db.CompactLevel(context.Background(), *level); err != nil {
			fmt.Fprintf(stderr, "semlsm: compact: %v\n", err)
			return 1
		}
	default:
		fmt.Fprintf(stderr, "unknown command %q\n", cmd)
		return 1
	}
	return 0
}

// cmdGet handles `get <key> [--at ts]`.
func cmdGet(db *semlsm.DB, args []string, stdout, stderr io.Writer) int {
	at := int64(-1)
	fs := flag.NewFlagSet("get", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Int64Var(&at, "at", -1, "read at this timestamp")
	if len(args) < 1 {
		fmt.Fprintln(stderr, "usage: semlsm get <key> [--at ts]")
		return 1
	}
	if err := fs.Parse(args[1:]); err != nil {
		return 1
	}
	key := []byte(args[0])
	var res semlsm.Result
	var err error
	if at >= 0 {
		res, err = db.GetAt(key, uint64(at))
	} else {
		res, err = db.Get(key)
	}
	if err != nil {
		fmt.Fprintf(stderr, "semlsm: get: %v\n", err)
		return 1
	}
	switch res.Kind {
	case semlsm.ResultNotFound:
		fmt.Fprintln(stdout, "<not found>")
	case semlsm.ResultExact:
		fmt.Fprintf(stdout, "%s @%d\n", res.Value, res.VersionTs)
	case semlsm.ResultSummarized:
		c := res.Checkpoint
		fmt.Fprintf(stdout, "<summarized tuple %d group %s ts %d..%d count %d> %s\n",
			c.ID, c.Group, c.TsFrom, c.TsTo, c.SourceCount, c.Summary.Narrative.Summary)
	case semlsm.ResultExpired:
		c := res.Checkpoint
		fmt.Fprintf(stdout, "<expired tuple %d group %s ts %d..%d>\n", c.ID, c.Group, c.TsFrom, c.TsTo)
	}
	return 0
}

// cmdScan handles `scan [--start s] [--end e]`.
func cmdScan(db *semlsm.DB, args []string, stdout, stderr io.Writer) int {
	var start, end string
	fs := flag.NewFlagSet("scan", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.StringVar(&start, "start", "", "inclusive start key")
	fs.StringVar(&end, "end", "", "exclusive end key")
	if err := fs.Parse(args); err != nil {
		return 1
	}
	it := db.NewIterator(semlsm.IterOptions{
		Start: []byte(start),
		End:   []byte(end),
	})
	defer it.Close()
	for it.Next() {
		r := it.Result()
		switch r.Kind {
		case semlsm.ResultExact:
			fmt.Fprintf(stdout, "%s\t%s\t@%d\n", r.Key, r.Value, r.VersionTs)
		case semlsm.ResultSummarized:
			c := r.Checkpoint
			fmt.Fprintf(stdout, "%s\t<summarized tuple %d ts %d..%d>\n", r.Key, c.ID, c.TsFrom, c.TsTo)
		case semlsm.ResultExpired:
			c := r.Checkpoint
			fmt.Fprintf(stdout, "%s\t<expired tuple %d ts %d..%d>\n", r.Key, c.ID, c.TsFrom, c.TsTo)
		}
	}
	if err := it.Err(); err != nil {
		fmt.Fprintf(stderr, "semlsm: scan: %v\n", err)
		return 1
	}
	return 0
}

// cmdHistory handles `history <key> [--from ts] [--to ts]`.
func cmdHistory(db *semlsm.DB, args []string, stdout, stderr io.Writer) int {
	if len(args) < 1 {
		fmt.Fprintln(stderr, "usage: semlsm history <key> [--from ts] [--to ts]")
		return 1
	}
	var from, to int64
	fs := flag.NewFlagSet("history", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Int64Var(&from, "from", 0, "from ts (inclusive)")
	fs.Int64Var(&to, "to", -1, "to ts (inclusive; -1 = newest)")
	if err := fs.Parse(args[1:]); err != nil {
		return 1
	}
	toU := ^uint64(0)
	if to >= 0 {
		toU = uint64(to)
	}
	items, err := db.History([]byte(args[0]), uint64(from), toU)
	if err != nil {
		fmt.Fprintf(stderr, "semlsm: history: %v\n", err)
		return 1
	}
	for _, it := range items {
		if it.Raw != nil {
			kind := "PUT"
			if it.Raw.Kind == base.KindDelete {
				kind = "DEL"
			}
			fmt.Fprintf(stdout, "@%d %s %s\n", it.Raw.Ts, kind, it.Raw.Value)
			continue
		}
		c := it.Checkpoint
		if c.RetiredUnixNano != 0 {
			fmt.Fprintf(stdout, "@%d..%d EXPIRED tuple %d (%d versions)\n", c.KeyMinTs, c.KeyMaxTs, c.ID, c.KeyCount)
			continue
		}
		fmt.Fprintf(stdout, "@%d..%d SUMMARIZED tuple %d (%d versions) %s\n",
			c.KeyMinTs, c.KeyMaxTs, c.ID, c.KeyCount, c.Summary.Narrative.Summary)
	}
	return 0
}

// cmdCheckpoints handles `checkpoints <group> [--from ts] [--to ts]`.
func cmdCheckpoints(db *semlsm.DB, args []string, stdout, stderr io.Writer) int {
	if len(args) < 1 {
		fmt.Fprintln(stderr, "usage: semlsm checkpoints <group> [--from ts] [--to ts]")
		return 1
	}
	var from, to int64
	fs := flag.NewFlagSet("checkpoints", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Int64Var(&from, "from", 0, "from ts")
	fs.Int64Var(&to, "to", -1, "to ts (-1 = newest)")
	if err := fs.Parse(args[1:]); err != nil {
		return 1
	}
	toU := ^uint64(0)
	if to >= 0 {
		toU = uint64(to)
	}
	cks, err := db.Checkpoints(args[0], uint64(from), toU)
	if err != nil {
		fmt.Fprintf(stderr, "semlsm: checkpoints: %v\n", err)
		return 1
	}
	for _, c := range cks {
		fmt.Fprintf(stdout, "tuple %d ts %d..%d sources %d bytes %d model %s\n",
			c.ID, c.TsFrom, c.TsTo, c.SourceCount, c.SourceBytes, "mock-model")
	}
	return 0
}

// cliOptions builds the CLI's Options: the deterministic mock LLM unless
// --llm, then the environment-configured HTTP stack.
func cliOptions(dir string, useLLM bool) (semlsm.Options, error) {
	opts := cliDBOptions()
	var client llm.Client
	if useLLM {
		httpOpt := llm.HTTPOptionsFromEnv()
		if httpOpt.BaseURL == "" || httpOpt.APIKey == "" {
			return opts, fmt.Errorf("--llm needs SEMLSM_LLM_BASE_URL and SEMLSM_LLM_API_KEY")
		}
		client = llm.NewHTTPClient(httpOpt)
	} else {
		client = llm.NewMock(llm.MockValid)
	}
	stack, err := llm.NewSummarizer(client, llm.Config{
		Model:             modelName(client),
		PromptVersion:     1,
		FS:                vfs.OSFS{},
		Dir:               dir,
		MaxTokensTotal:    2_000_000,
		RequestsPerMinute: 60,
		TokensPerMinute:   200_000,
		Concurrency:       2,
		CacheMaxBytes:     16 << 20,
	})
	if err != nil {
		return opts, err
	}
	opts.Summarizer = stack
	return opts, nil
}

func cliDBOptions() semlsm.Options {
	return semlsm.Options{
		GroupDepth:     2,
		GroupDelimiter: '/',
		Policies: []semlsm.Policy{
			{Prefix: "agent/", Mode: semlsm.ModeState, MinAge: time.Hour, MinBatch: 100, MaxBatch: 10000},
			{Prefix: "app/", Mode: semlsm.ModeEvents, MinAge: time.Hour, MinBatch: 100, MaxBatch: 10000},
		},
		DefaultPolicy:       semlsm.Policy{Mode: semlsm.ModeDisabled},
		MinCompressionRatio: 5.0,
	}
}

// modelName reads the model name from a client that knows it.
func modelName(client llm.Client) string {
	type namer interface{ Model() string }
	if n, ok := client.(namer); ok {
		return n.Model()
	}
	return "unknown"
}
