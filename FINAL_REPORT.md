# SemLSM Final Report

Run completed through P14 under the A9/A10 supervision protocol (resume
per M-024 after the withdrawn M-023 stop). P15 (live LLM smoke) was not run:
`SEMLSM_LLM_API_KEY` is unset, and P15 is optional.

## Gate-by-gate status

| Gate | Status | Commit (tag) | Verdict |
|---|---|---|---|
| 0 bootstrap | PASSED | 69c77b3 (phase-0-done) | |
| 1 base/vfs/failpoint | PASSED | 3d6d047 → 0b9a320 → de588b6 | M-008/M-009 |
| 2 wal/memtable | PASSED | bc88dbb → 3abe324 | M-006 |
| 3 sstable | PASSED | 53a62f2 | + M-008/M-009/M-011/M-012 fixes |
| 4 manifest/version | PASSED | 7533df9 → 851da6a → 0b9a320 | |
| 5 db core | PASSED | f039828 → cd6c627 | M-013 (4 real bugs found by own tests) |
| 6 structural compaction | PASSED (re-run) | fb81ad7 (phase-6-done) | M-021 (M-015 closed by fix(P6)) |
| 7 checkpoint + resolution | PASSED | 499daf4 + f40fdcf (phase-7-done) | |
| 8 semantic pipeline | PASSED | 8a1e3ff + d76b556 (phase-8-done) | M-022 — approved on gate evidence only (no line-by-line review of internal/semantic) |
| 9 LLM reliability | PASSED | 82b77ca → 28a0659 (phase-9-done) | M-025 CHANGES → M-026 |
| 10 Tier B wiring | PASSED | 90b8200 → 7b024e1 (phase-10-done) | M-027 CHANGES → M-028 |
| 11 iteration/history | PASSED | a79ec54 (phase-11-done) | M-029 |
| 12 cold archive | PASSED | e811be1 → f02d444 (phase-12-done) | M-030 CHANGES → M-031 |
| 13 hardening | PASSED | 3ca75d7 (phase-13-done) | M-032 |
| 14 tools/docs/report | PASSED | this commit (phase-14-done) | pending |
| 15 live LLM | NOT RUN | — | optional; SEMLSM_LLM_API_KEY unset |

## Verification at report time

`gofmt -l .` empty; `go vet ./...` clean; `go test ./... -race -count=1`
passes on all 17 packages (root, cmd/semlsm goldens, internal/{archive,base,
bloom,checkpoint,compaction,failpoint,llm,manifest,memtable,merge,resolve,
semantic,sstable,version,vfs,wal}). No `TODO|FIXME|not implemented` in Go
code. Heavy tests are reduced under `testing.Short()`; gates run full.
A build-tag `soak` test ran once for 600 s (8 writers, 8 readers, snapshot
churn, chaos LLM, 9 reopens, periodic VerifyIntegrity): PASS with zero
integrity problems.

## P14 numbers (honest, no tuning)

- **Demo** (`cmd/semlsm-demo`, 100 agents × 1000 telemetry versions =
  100 000 records, deterministic mock LLM): raw record bytes 9 978 990;
  summarized raw bytes 2 989 764 → summary bytes 17 201 = **173.8x
  reduction** of the summarized range (29 970 records summarized). The demo
  prints an Exact latest read, a Summarized mid-range read, a pinned
  snapshot read, and recovers planted anomalies exactly from the archive
  tier (`GetArchived`); it exits 1 below 10x.
- **Bench** (`cmd/semlsm-bench`, 20 000 records/phase, in-memory FS):
  get-latest p50 ≈ 0.5–0.9 µs, p99 ≈ 2–4 µs; historical reads similar;
  write throughput ≈ 750–1200 MB/s (batch spans; see below); on-disk size
  384 KB (semantic off) vs 25–38 KB (semantic on, compacted). NOTE: this
  Windows box's monotonic clock resolves only ~1 ms spans, so batch spans
  shorter than that report as 0 ns; per-op numbers below the clock floor
  are not measurable here and are reported as 0 rather than invented.
  Reports land in `bench-results/*.json` (git-ignored).

## Bugs found and fixed during P9–P14 (all caught by the gated tests)

- fix(P6) closed all M-015 items, including the L0 range-union bug and a
  latent clean-reopen readState bug (details in the P6 section of the
  history; APPROVED M-021).
- fix(P8): concurrent writes to the shared Usage in the map phase (-race).
- fix(P9): M-025 — the breaker's half-open trial could wedge forever on a
  non-transient outcome; `Breaker.Cancel` added for calls that never
  reached the client; limiter fails fast on requests larger than the token
  burst; Open validates `TokensPerMinute` against a worst-case request; a
  budget-persist failure no longer discards the paid-for response. Also
  fixed a limiter slot leak on ctx cancellation while waiting for buckets.
- fix(P10): M-027 — failed semantic jobs now delete their outputs/ckpt/
  archives; the Open sweep removes unreferenced archive segments.
- P11: `Snapshot.NewIterator` shared the snapshot's pinned read state, so
  every snapshot iterator consumed one of the snapshot's version references
  (refcount below zero at Close). Fixed; the master MVCC model test
  (20 seeds) found it.
- fix(P12): M-030 — `GetArchived` at a ts past the key's dropped run now
  delegates to `GetAt` (the archive is not authoritative above the run);
  dropped tombstones return NotFound instead of an Exact empty value.
- P13 (hardening) found four more: `DB.Close` never released the
  checkpoint cache (leaked reader handles on Windows); `prevTupleID` used
  newest-data instead of newest-created, giving several tuples the same
  predecessor (soak); `PickFast` starved L1→L2 under continuous L0 traffic
  (now alternates when both levels are over target); a corrupted CURRENT
  surfaced as not-exist instead of `ErrCorruption`.

## Deviations from the plan (all logged in the gate messages)

- `llm` has its own `Clock` interface (Now+After) with a deterministic
  `ManualClock` — `base.Clock` lacks `After` and adding a method would
  break the public `Options.Clock` contract (R8).
- `Limiter.Wait` pairs with `Release()` so the stack can hold a concurrency
  slot across the upstream call.
- Archives live outside the manifest (the B8 manifest format has no archive
  field): referenced via `TupleMeta.ArchiveFileNum`, orphan-swept at Open,
  deleted with their checkpoint. Checkpoint files accumulate one per
  semantic job (no tuple GC).
- `PickFast` alternates L0 and L1 when both are over target (starvation
  fix; the plan mandates "either scores >= 1", not a fixed priority).
- `GetArchived` delegates to `GetAt` for ts above the key's dropped run
  (M-030, documented choice).
- CompactLevel runs semantic jobs on deep levels when a healthy Summarizer
  is configured (the manual trigger the tests drive).

## Known limitations

- **Manifest torn tail rolls back silently**: a corrupted final manifest
  frame is treated as a torn write per the plan's WAL-like rule; the last
  committed edit (e.g. a whole semantic job) rolls back to the previous
  state and the next Open sweeps its files as orphans. No wrong answer is
  ever served, but the rollback is silent. A stricter variant — treat a
  full-length final frame with a bad CRC as `ErrCorruption` and tolerate
  only short frames — was noted for the supervisor in M-032 and is not
  implemented (it would change recovery semantics the plan fixes).
- Checkpoint files accumulate (no tuple GC); archive segments from an
  unreadable dying checkpoint leak until the next Open sweep can read the
  live ckpts.
- The soak's Windows handle check runs only where the OS FS is used
  (`cmd` tests, soak); in-memory tests rely on the goroutine-settle check.
- P15 (live LLM smoke) was not run: no API key in the environment. The
  engine has never talked to a real endpoint; the HTTP client is
  httptest-tested only.

## Process notes

- The run resumed per M-024 after the withdrawn M-023 stop; every gate
  waited with bounded A10 waits and full-tail mailbox reads. Two CHANGES
  verdicts (M-025, M-027) and one medium CHANGES (M-030) were fixed as
  `fix(P*)` commits with the supervisor's tests ported permanently.
- Supervisor tests ported as permanent tests: three llm tests (M-025), the
  I10-over-older-raw-generation and failed-job-cleanup tests (M-027), the
  trailing-delimiter iterator test (M-029, optional, kept), and the two
  archive tests (M-030).
- FINAL_REPORT.md supersedes the earlier stop report (M-023) and the P6/P8
  sections of the previous version; the gate table above is complete.
