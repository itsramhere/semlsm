# SemLSM

An embeddable Go LSM storage engine that replaces old key versions with validated LLM summaries.

Append-heavy key histories, such as agent state and telemetry, can grow forever even when only the latest value needs to stay exact. SemLSM uses deep-level compaction to hand eligible old versions to an LLM and store a validated summary in their place. With a state policy, the latest value stays exact; dropped versions can also be kept in a separate cold archive for exact recovery.

**Status:** experimental, with no stable API yet. Not used in production. Never yet run against a live LLM endpoint: validation has used the deterministic mock and an HTTP client tested with `httptest` only.

## Features

- Exact latest reads with `ModeState`, deletes, batches, and MVCC snapshots.
- Semantic compaction with validated LLM output, engine-computed statistics, reference checks, and a minimum compression ratio.
- Optional cold archive and exact historical recovery through `GetArchived`.
- Archive and summary retention, with `ResultExpired` stubs that preserve historical range boundaries.
- Checkpoint file merging without changing tuple contents.
- Crash-safe atomic installs and CRC checks throughout WALs, manifests, SSTs, and checkpoints.
- Explicit `Repair` and opt-in `AutoRepair`, with quarantine and recovery reports.
- LLM response cache, persisted token budget, circuit breaker, and rate limiter.
- CLI, benchmark tool, and telemetry demo; standard library only.

## How it works

```text
writes -> WAL -> memtable -> L0 -> L1 -> L2 -> L3 -> L4
                            |----------| |-----------|
                            Tier A       Tier B + L4 sweep
                            structural   semantic when eligible
                                             |
                                      checkpoint summary
                                      + optional cold archive

read(key, timestamp) -> newest visible raw version + covering checkpoint
                    -> Exact / Summarized / Expired / NotFound
```

Tier A preserves raw history. Tier B selects old, contiguous version runs according to policy, validates the LLM's narrative, and atomically installs raw survivors and checkpoint tuples. If semantic work cannot proceed, raw structural compaction remains available. Reads consult both raw records and checkpoint ranges so a summarized version cannot reveal an older raw value as exact.

See [Architecture](docs/ARCHITECTURE.md) and [On-disk formats](docs/FORMATS.md) for details.

## Install

Requires Go 1.26+ and no third-party dependencies. The current `go.mod` specifies Go 1.26.3, so use that patch release or newer.

```sh
go get github.com/itsramhere/semlsm
```

## Quick start

This complete example is also [`ExampleOpen` in example_test.go](example_test.go). Run it with `go test -run '^ExampleOpen$' .`. The mock requires no credentials. Two fresh writes do not trigger semantic compaction; the switch shows how to handle all four result kinds when reading an older database too.

```go
package semlsm_test

import (
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/itsramhere/semlsm"
	"github.com/itsramhere/semlsm/llm"
)

func ExampleOpen() {
	dir, err := os.MkdirTemp("", "semlsm-example-*")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)

	// The mock makes this example safe to run without an API key.
	client := llm.NewMock(llm.MockValid)
	stack, err := llm.NewSummarizer(client, llm.Config{
		Model: "mock", PromptVersion: 1, Dir: dir,
		MaxTokensTotal: 2_000_000, RequestsPerMinute: 60,
		TokensPerMinute: 200_000, Concurrency: 2, CacheMaxBytes: 16 << 20,
	})
	if err != nil {
		panic(err)
	}
	db, err := semlsm.Open(dir, semlsm.Options{
		Policies: []semlsm.Policy{{
			Prefix: "agent/", Mode: semlsm.ModeState,
			MinAge: time.Hour, MinBatch: 100, MaxBatch: 10_000,
		}},
		Summarizer: stack,
		ArchiveRaw: true,
	})
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			panic(err)
		}
	}()

	show := func(r semlsm.Result, err error) {
		if err != nil {
			panic(err)
		}
		switch r.Kind {
		case semlsm.ResultExact:
			fmt.Printf("Exact: %s\n", r.Value)
		case semlsm.ResultSummarized:
			fmt.Println("Summarized:", r.Checkpoint.Summary.Narrative.Summary)
		case semlsm.ResultExpired:
			fmt.Println("Expired tuple:", r.Checkpoint.ID)
		case semlsm.ResultNotFound:
			fmt.Println("NotFound")
		}
	}
	key := []byte("agent/1/state")
	ts, err := db.Put(key, []byte("hello"))
	if err != nil {
		panic(err)
	}
	snapshot := db.NewSnapshot()
	defer snapshot.Release()
	if _, err := db.Put(key, []byte("updated")); err != nil {
		panic(err)
	}
	show(db.Get(key))
	show(db.GetAt(key, ts))
	show(snapshot.Get(key))
	show(db.Get([]byte("missing")))

	items, err := db.History(key, 0, ^uint64(0))
	if err != nil {
		panic(err)
	}
	fmt.Println("History entries:", len(items))

	// After semantic compaction, a summarized run can be read exactly
	// from its archive. These two fresh versions have not been compacted.
	archived, err := db.GetArchived(key, ts)
	if errors.Is(err, semlsm.ErrNotArchived) {
		fmt.Println("No archived run yet")
	} else {
		show(archived, err)
	}
	// Output:
	// Exact: updated
	// Exact: hello
	// Exact: hello
	// NotFound
	// History entries: 2
	// No archived run yet
}
```

The example prints:

```text
Exact: updated
Exact: hello
Exact: hello
NotFound
History entries: 2
No archived run yet
```

`GetAt` takes the timestamp returned by a write; a timestamp beyond the latest visible commit returns `ErrFutureTimestamp`. `History` returns raw versions and covering checkpoint runs in chronological order. `GetArchived` uses an archive for a covered run; it can return `ErrNotArchived` if none exists, or `ErrArchiveRetired` (which wraps `ErrNotArchived`) after retention. Beyond a dropped run it delegates to `GetAt`.

### Connect an LLM

The public `github.com/itsramhere/semlsm/llm` package exposes the mock, OpenAI-compatible HTTP client, and reliability stack. This second example uses the same imports as above and is compiled as `ExampleOpen_http` in [example_test.go](example_test.go). It has no output directive, so `go test` compiles it without running it or contacting a provider.

Set `SEMLSM_LLM_BASE_URL` to the provider's API base (for example, a URL ending in `/v1`), `SEMLSM_LLM_API_KEY` to the secret, and `SEMLSM_LLM_MODEL` to the desired model. `HTTPOptionsFromEnv` reads these variables; the key is never printed. If the model is unset, the client defaults to `glm-5.3-flash`; select a model supported by your endpoint.

```go
func ExampleOpen_http() {
	// Configure the environment before using this with a live endpoint.
	opts := llm.HTTPOptionsFromEnv()
	if opts.BaseURL == "" || opts.APIKey == "" {
		return
	}
	client := llm.NewHTTPClient(opts)
	stack, err := llm.NewSummarizer(client, llm.Config{
		Model: client.Model(), PromptVersion: 1, Dir: "mydb",
		MaxTokensTotal: 2_000_000, RequestsPerMinute: 60,
		TokensPerMinute: 200_000, Concurrency: 2, CacheMaxBytes: 16 << 20,
	})
	if err != nil {
		panic(err)
	}
	db, err := semlsm.Open("mydb", semlsm.Options{
		Policies: []semlsm.Policy{{
			Prefix: "agent/", Mode: semlsm.ModeState,
			MinAge: time.Hour, MinBatch: 100, MaxBatch: 10_000,
		}},
		Summarizer: stack,
		ArchiveRaw: true,
	})
	if err != nil {
		panic(err)
	}
	if err := db.Close(); err != nil {
		panic(err)
	}
}
```

The stack applies cache -> budget -> breaker -> limiter -> client. The engine validates responses before installing summaries, including responses served from cache. `Config.Accept` can filter cache admission; when nil, the stack caches transport responses without that filter. Token usage persists under the database directory across restarts; reopening does not reset spent tokens. `Summarizer: nil` disables semantic work entirely.

## Concepts

**Groups.** `GroupDepth` and `GroupDelimiter` derive contiguous key groups. The defaults, depth 2 and `/`, group `agent/42/state` and `agent/42/events` under `agent/42/`. Group settings and the explicit policy list are persisted and must match on reopen.

**Policies.** The longest matching `Prefix` wins; unmatched keys use `DefaultPolicy`, whose default mode is `ModeDisabled`. `ModeState` preserves each key's newest version. `ModeEvents` may summarize even the newest version, so use it only when a summary is an acceptable latest answer. `ModeDisabled` keeps raw history. `MinAge` protects recent records; `MinBatch` and `MaxBatch` bound eligible records per segment. Explicit enabled policies default to a one-hour minimum age, 1,000 minimum records, and 10,000 maximum records.

**Checkpoint tuples.** A tuple contains narrative text, engine-computed counts and statistics, per-key timestamp ranges, source CRC, and a predecessor link. The engine validates JSON shape, length limits, and source references; it does not prove that the prose is factually complete or correct. Checkpoint merging combines files while preserving tuples.

**Archive.** `ArchiveRaw` keeps dropped versions in separate SSTs, under `<db>/archive` by default or `ArchiveDir` if set. Exact latest state, surviving raw history, snapshot reads, and available archived values keep their bytes. Summarized intermediate values are lossy unless their archive is retained. Keeping an archive can make total storage larger even while the live tree shrinks.

**Retention.** `ArchiveRetention` releases old archives; `SummaryRetention` removes old summaries and any remaining archives, leaving `ResultExpired` stubs. Ages are measured from a tuple's latest source timestamp, not its creation time. If both ages are nonzero, archive retention must be no longer than summary retention. Snapshots keep their original summaries and archives until released. Stubs retain full key ranges forever to prevent stale exact reads; retirement cannot be undone by changing options.

## Configuration

Zero-valued `Options` use the defaults below. Byte sizes use binary units. These are runtime settings unless noted above.

| Option | Default | Purpose |
|---|---|---|
| `Summarizer` | `nil` | Disable semantic work until a summarizer is supplied |
| `DefaultPolicy.Mode` | `ModeDisabled` | Preserve unmatched keys as raw records |
| `GroupDepth`, `GroupDelimiter` | `2`, `'/'` | Key grouping; persisted |
| `SyncMode`, `SyncEveryN` | `SyncAlways`, `1` | WAL sync on every commit; weaker modes trade durability for throughput |
| `MemtableBytes`, `MaxImmutableMemtables` | 64 MiB, `2` | Write buffers |
| `L0CompactTrigger`, `L0StallTrigger` | `4`, `12` | Start compaction / apply write backpressure |
| `L1TargetBytes`, `LevelMultiplier` | 64 MiB, `10` | Level size targets |
| `TargetFileBytes` | 16 MiB | SST target size, cut at key boundaries |
| `SemanticWorkers` | `1` | Background semantic workers |
| `SemanticSweepInterval` | 10 minutes | Minimum interval between L4 sweep attempts |
| `MaxChunkBytes`, `MaxValueBytesInPrompt` | 32 KiB, 2 KiB | Prompt chunk and individual value limits |
| `MaxSemanticJobBytes`, `SemanticJobTimeout` | 256 MiB, 30 minutes | Semantic job bounds |
| `MinCompressionRatio` | `5.0` | Minimum raw-to-summary byte ratio to accept |
| `HardScore` | `2.0` | Structural fallback threshold when semantic work is unavailable |
| `ArchiveRaw` | `false` | Keep dropped raw versions in cold storage |
| `ArchiveDir` | empty, resolves to `<db>/archive` | Override the archive location |
| `ArchiveRetention`, `SummaryRetention` | `0`, `0` | Keep archives / summaries forever |
| `RetentionInterval` | 1 hour | Interval between automatic retention attempts |
| `CkptMergeThreshold` | `32` | Automatic merge when checkpoint file count exceeds this; negative disables it |
| `AutoRepair` | `false` | Opt in to rollback of damaged complete final log records |
| `BlockCacheBytes`, `MaxOpenTables` | 64 MiB, `256` | Read cache limits |
| `DisableAutoCompaction` | `false` | Disable automatic compaction |
| `Logger` | discard logger | Supply a logger to receive warnings and diagnostics |

`DB.CompactCheckpoints(ctx)` requests a merge even when automatic merging is disabled. `DB.ApplyRetention(ctx)` applies configured retention without waiting for `RetentionInterval`. See [options.go](options.go) for all fields and validation rules. The LLM stack separately requires a positive `MaxTokensTotal`; its token rate must admit a worst-case prompt plus completion.

## CLI

Run from a checkout, or install with `go install github.com/itsramhere/semlsm/cmd/semlsm@latest` once published. Global flags are `--dir DIR` (default `semlsm-data`), `--llm`, `--auto-repair`, `--archive-retention AGE`, and `--summary-retention AGE`. Durations use Go syntax such as `24h`.

| Command | Arguments / flags |
|---|---|
| `put` | `<key> <value>` |
| `get` | `<key> [--at timestamp]` |
| `del` | `<key>` |
| `scan` | `[--start inclusive-key] [--end exclusive-key]` |
| `history` | `<key> [--from timestamp] [--to timestamp]` |
| `checkpoints` | `<group> [--from timestamp] [--to timestamp]` |
| `stats` | Print storage and semantic counters |
| `verify` | Check file integrity and checkpoint invariants |
| `repair` | Quarantine unreferenced files and roll back a repairable damaged final frame |
| `compact` | `--level N`, `--checkpoints`, or `--retention` |

For `get`, `history`, and `checkpoints`, put the key/group before the command's flags, as shown above. Choose one compact operation per invocation. Read timestamps are unsigned write timestamps; history/checkpoint ranges default to the full history.

POSIX shell example with a fresh database outside the checkout:

```sh
dir=$(mktemp -d)
go run ./cmd/semlsm --dir "$dir" put agent/1/state hello
go run ./cmd/semlsm --dir "$dir" get agent/1/state
# hello @<write-timestamp> (the numeric timestamp varies)
go run ./cmd/semlsm --dir "$dir" scan --start agent/ --end agent0
# agent/1/state<TAB>hello<TAB>@<write-timestamp>
go run ./cmd/semlsm --dir "$dir" history agent/1/state
# @<write-timestamp> PUT hello
go run ./cmd/semlsm --dir "$dir" checkpoints agent/1/
# No output: no versions have been summarized yet.
go run ./cmd/semlsm --dir "$dir" stats
go run ./cmd/semlsm --dir "$dir" verify
# sst_files 1 ckpt_files 0 tuples 0 entries 1 problems 0
go run ./cmd/semlsm --dir "$dir" compact --checkpoints
# checkpoints compacted
go run ./cmd/semlsm --dir "$dir" compact --retention --archive-retention 24h --summary-retention 168h
# retention applied
go run ./cmd/semlsm --dir "$dir" --auto-repair get agent/1/state
# hello @<write-timestamp>
go run ./cmd/semlsm --dir "$dir" repair
# repair truncated 0 quarantined 0
go run ./cmd/semlsm --dir "$dir" del agent/1/state
go run ./cmd/semlsm --dir "$dir" get agent/1/state
# <not found>
```

On Windows, use PowerShell and set `$dir = Join-Path ([System.IO.Path]::GetTempPath()) ('semlsm-' + [guid]::NewGuid())`; the commands above work with the same `--dir "$dir"` argument.

Use `get agent/1/state --at <write-timestamp>` with the numeric timestamp printed by `get` to read that version, or `compact --level 4` to request deep-level compaction. CLI policies use `ModeState` for `agent/`, `ModeEvents` for `app/`, a one-hour age, and batches of 100-10,000. Other keys stay raw. The CLI uses a deterministic mock by default and does not enable `ArchiveRaw`; use the Go API to retain dropped raw values.

`--llm` switches to the HTTP stack and requires `SEMLSM_LLM_BASE_URL` and `SEMLSM_LLM_API_KEY`; `SEMLSM_LLM_MODEL` selects the model. The key is read from the environment and never printed. The CLI stack has a persisted 2,000,000-token budget, 60 requests/minute, 200,000 estimated tokens/minute, concurrency 2, and a 16 MiB response cache. `--auto-repair` emits recovery warnings to stderr.

## Demo and benchmarks

From a checkout, run these in a POSIX shell; benchmark JSON goes outside the repository:

```sh
go run ./cmd/semlsm-demo
dir=$(mktemp -d)
go run ./cmd/semlsm-bench --records 20000 --out "$dir"
```

The demo defaults to an in-memory filesystem and 100 agents x 1,000 versions. It shows an exact latest read, a summarized historical read, the same historical timestamp still exact in a pinned snapshot, and recovery of planted anomalies through `GetArchived`. `--agents`, `--per-agent`, and `--dir` adjust the workload or select a disk directory. The demo exits unsuccessfully if its summarized-range reduction is below 10x.

The recorded measurements in [FINAL_REPORT.md](FINAL_REPORT.md) used Windows, an in-memory filesystem, and the deterministic mock:

| Measurement | Recorded result |
|---|---|
| Summarized range, 99,900 records | 9,969,000 raw bytes -> 57,305 summary bytes: **174x** |
| Live store | 1,276,520 bytes (~1.28 MB) -> 117,071 bytes (~117 KB), about 11x |
| Archive, kept separately | 1,289,480 bytes after compaction |
| Get-latest, 20,000-record benchmark | p50 ~0.5-0.9 us; p99 ~2-4 us; historical reads similar |
| Write throughput | ~750-1,200 MB/s over batch spans |
| Benchmark directory size, archive off | 384 KB semantic off; 25-38 KB semantic on |

**The 174x figure uses a mock whose summaries are fixed-size regardless of input.** A real LLM writes larger summaries; its ratio is expected to be lower and remains unmeasured. The live store shrinks, while live store plus archive is slightly larger than the original raw store. The mock's narrative is canned; source counts and ranges are engine-computed.

These are development measurements, not production disk benchmarks. The semantic-off size is after writing without compaction; semantic-on is measured after compaction, and both disable archiving. The machine's monotonic clock resolved only roughly 1 ms spans, so shorter batch measurements were reported as zero rather than meaningful per-operation timings. The quoted throughput does not establish durable disk write performance. Results have not been remeasured for this README.

## Guarantees

- **Atomic installs:** one durable manifest edit publishes a flush or compaction; readers see the old or new state.
- **Snapshot stability:** a snapshot pins its read state and keeps the same results until released.
- **No stale exact reads:** covering checkpoint ranges prevent an older raw value from masquerading as the visible version.
- **Contiguous history replacement:** each checkpoint covers exactly the dropped versions in each key's recorded range.
- **LLM isolation:** calls run outside engine locks and the read/write critical path; failed summaries leave the affected data raw.
- **CRC integrity:** framed logs, table blocks, and checkpoint sections are checked; detected corruption returns an error.

WAL durability follows `SyncMode`; weaker sync settings can lose unsynced writes. Directory sync is a no-op on Windows, so the simulated crash results do not establish power-loss durability on every filesystem.

## Limitations

- No live-LLM validation yet; provider behavior, summary quality, latency, and real compression ratios remain unmeasured.
- Summaries are LLM prose, not exact data. Structural validation and valid references do not prove factual correctness.
- `ModeEvents` may summarize the newest version; use `ModeState` when latest values must remain exact.
- `AutoRepair` is opt-in and can roll back a damaged final edit/write. It reports the rollback; default recovery rejects complete bad-CRC frames.
- Non-prefix disk tears need `Repair`, or opt-in `AutoRepair` for an eligible final frame. Middle corruption and malformed good-CRC payloads still fail recovery.
- An exhausted token budget pauses semantic work. Usage persists across restarts; increase the configured total deliberately to allow more spending.
- Single-process access: the database directory is protected by an exclusive `LOCK`.
- Stub metadata and key ranges remain forever. Retention releases payloads, not every byte of historical metadata.
- Unreadable legacy archive references conservatively prevent archive deletion; `Repair` refuses to guess their ownership.
- The crash simulator models prefix tears, and Windows directory sync is a no-op; real-disk power-loss behavior needs further validation.

## Project layout

```text
semlsm.go, options.go     Public database API and configuration
db_*.go, repair.go        Reads, writes, snapshots, compaction, retention, repair
llm/                      Public completion clients and reliability stack
internal/                 WAL, SSTs, manifest, checkpoints, archive, semantic pipeline
cmd/semlsm/               CLI
cmd/semlsm-demo/          Telemetry and archive demo
cmd/semlsm-bench/         Latency, throughput, and size measurements
docs/                     Architecture and on-disk formats
scripts/verify.ps1        Formatting, vet, tests, optional race checks
example_test.go           Compiling README examples
```

## Development

```sh
go test ./...
go test -race ./...
go test -tags soak -run TestSoak -timeout 15m .
```

The soak test runs for ten minutes by default. Race tests require CGO and a supported C toolchain.

On Windows, the PowerShell verification script runs these checks together:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/verify.ps1
powershell -ExecutionPolicy Bypass -File scripts/verify.ps1 -NoRace
```

The script runs formatting checks, vet, and tests. Race checks are on by default; `-NoRace` disables them, and `CGO_ENABLED=0` automatically falls back to ordinary tests. It writes no log files.

## License

No license has been chosen yet. All rights reserved until a LICENSE file is added.
