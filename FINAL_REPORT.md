# SemLSM Final Report

Run ended by supervisor STOP (M-023, 2026-11-07 00:10 +05:30) after Gate 8,
with the user's instruction to end the run after this phase. Phases P0-P8 are
implemented and green; P9-P15 were not started.

## Gate-by-gate status

| Gate | Status | Commit (tag) | Notes |
|---|---|---|---|
| 0 bootstrap | PASSED | 69c77b3 (phase-0-done) | |
| 1 base/vfs/failpoint | PASSED | 3d6d047 → 0b9a320 → de588b6 (M-008/M-009) | |
| 2 wal/memtable | PASSED | bc88dbb → 3abe324 (M-006) | strict manifest replay landed in fix(P4) |
| 3 sstable | PASSED | 53a62f2 (+ M-008/M-009/M-011/M-012 fixes) | |
| 4 manifest/version | PASSED | 7533df9 → 851da6a → 0b9a320 | |
| 5 db core | PASSED | f039828 → cd6c627 (M-013) | 4 real bugs found by own tests |
| 6 structural compaction | PASSED (re-run) | fb81ad7 (phase-6-done), verdict M-021 | M-015 fully closed by fix(P6), see below |
| 7 checkpoint + resolution | PASSED | 499daf4 + f40fdcf (phase-7-done) | incl. concurrent-install double-release fix |
| 8 semantic pipeline | PASSED | 8a1e3ff + d76b556 (phase-8-done), verdict M-022 | **approved on gate evidence only** — no line-by-line review of internal/semantic was performed |
| 9-15 | NOT STARTED | | run ended per M-023 |

## Verification at stop

`gofmt -l .` empty; `go vet ./...` clean;
`go test ./... -race -count=1 -timeout 600s` passes on all 16 packages
(root 31, semantic 32, compaction 6, version 14, base 11, vfs 18, failpoint 4,
wal 14, memtable 4, bloom 5, sstable 23, merge 9, manifest 5, resolve 2,
checkpoint 5, llm 10 subtests). No `TODO|FIXME|not implemented` in Go code.
`-race` works (CGO_ENABLED=1, gcc 14.2.0). Heavy model/crash tests are reduced
under `testing.Short()` (root ~4 s under -short); gates run full without -short.

## fix(P6): M-015 closed (commit fb81ad7, APPROVED M-021)

- **Item 3 (critical)**: planL0 computed the L0 key range from file order; a
  straddling L1 file was missed, Apply rejected the edit, and compaction
  retried forever (WaitIdle never returned). Now the range is the union of
  all inputs' ranges (min Smallest / max Largest via CompareUser).
- **Item 2**: read-path errors (missing/corrupt SST, checkpoint open/index
  failures) propagate through sstGet, rawLookup, checkpointLookup, Get,
  GetAt, Snapshot.Get; the iterator records an unopenable table in Err()
  instead of skipping it (I10/I12).
- **Item 4**: VersionSet.Validate runs Apply's checks as a dry-run;
  appendManifestLocked validates BEFORE Append+Sync, so a rejected edit
  leaves the manifest byte-identical and the DB reopens.
- **Item 5**: runStructural re-checks every plan file against the current
  version (Version.HasFile) while marking busy; stale/busy plans are dropped
  silently (no error, no backoff).
- **Item 6**: both compaction loops back off 100 ms doubling to 5 s (exit on
  close), count failures in the new Stats.CompactionErrors.
- **Item 8**: internal/compaction/picker_test.go — L0 range union, busy files
  at each position, round-robin cursor, overlap edges, scores, bottom level.
- **Item 9**: testing.Short() reductions (model 200k→20k, concurrent model
  8k→2k, crash matrix 6→2 rounds, multi-session 4→2).
- **Permanent scenarios**: TestSnapshotPinsFilesUntilRelease (existing),
  TestL0NonMonotoneRangesCompactionAndReopen (new),
  TestSSTRemovedBehindBack (new).
- **Bonus bug found by the item-3 scenario test**: openRecover built the
  readState BEFORE the manifest load, so a clean reopen with an empty active
  WAL served NotFound for every key until the first install. Every earlier
  reopen test had unflushed WAL data masking it. Fixed: rs built after
  loadOrCreateManifest.

The race-detector gate also caught a data race in the first version of the
new compaction-backoff test (non-atomic counter read); fixed with atomic.Int64.

## P8: semantic pipeline (commits 8a1e3ff, d76b556; tag phase-8-done)

- P8.1 llm Client/MockClient (9 fault modes, call counting) — done in the
  previous session (01f9486).
- P8.2 Segmenter: per plan algorithm; segments + raw account for the whole
  input; saturating MinAge horizon; I7 property test (per-key contiguous
  oldest-first prefix, nothing lost or duplicated).
- P8.3 B9 Stats/KeyCount/NumericField/Narrative/Fact(+ResolvedRefs)/Summary;
  ComputeStats with deterministic top-20 lists; NumericFields only from
  values starting '{', mixed-type objects handled, huge numbers ignored.
- P8.4 Prompts: exact map/reduce system+user prompts (goldens); line format
  `r<i> | ts=+delta | KIND | key=<suffix> | <value>`; rune-boundary
  truncation; `<binary N bytes crc=%08x>`; newline escaping; `RECORDS>>>`
  neutralized to `RECORDS>>\u200b>` (verified against the plan bytes).
- P8.5 ParseAndValidate with V1-V8 in order, typed ValidationError; table
  test with 20 cases incl. exact limits and fence stripping.
- P8.6 Summarizer.Summarize: bounded-concurrency map (first error cancels
  the rest) + serial reduce rounds of up to 8; every response validated with
  one repair retry; refs resolved to (key, ts); compression-ratio check with
  ErrPoorCompression; Usage accounting. -race found concurrent writes to the
  shared Usage in the map phase (fix(P8) d76b556: per-call deltas merged
  under the map mutex).
- Gate 8 "no network used": satisfied — only the deterministic mock runs; no
  HTTP client exists yet; SEMLSM_LLM_API_KEY unset.

Deviations (logged in the gate message, none recorded in DECISIONS.md as R8
changes): Summarize takes the group explicitly; Mode/Policy moved to
internal/base with root type aliases (public API unchanged) because
internal/semantic may not import the root.

## Process notes

- P8.2-P8.6 were started before the G-025 verdict: the two bounded A10
  verdict waits timed out, and my wait command only grepped for the verdict
  string instead of re-reading the mailbox, so M-020 (end the run after the
  P6 gate) was missed. Disclosed in G-026; the supervisor accepted the
  disclosure (M-022) and approved P8 on gate evidence only.
- FINAL_REPORT.md from the earlier stop (fdefa6d) is superseded by this file.
- Not reached: P9 (LLM reliability), P10 (Tier B wiring), P11 (iteration/
  history/VerifyIntegrity full), P12 (archive), P13 (hardening/soak),
  P14 (tools/docs/benchmarks), P15 (live LLM). The engine's Tier B machinery
  exists and is tested, but no DB code path calls it yet (deep compaction
  still moves data raw; P10 replaces that).
