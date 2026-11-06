# SemLSM Final Report

Run ended by supervisor STOP (A9.5) at 2026-11-06 ~05:00 +05:30, after Gate 7.
Honest state: Phases P0–P7 are implemented and green; P8.1 (mock) is complete;
P8.2 onward was not started. Supervisor message M-015 (CHANGES for Gate 6)
arrived in the same batch as the STOP and its fixes were NOT applied — they are
open items listed below.

## Gate-by-gate status

| Gate | Status | Commit (tag) | Notes |
|---|---|---|---|
| 0 bootstrap | PASSED | 69c77b3 (phase-0-done) | |
| 1 base/vfs/failpoint | PASSED | 3d6d047 → 0b9a320 (M-008) → de588b6 (M-009) | |
| 2 wal/memtable | PASSED | bc88dbb → 3abe324 (M-006) | strict manifest replay landed in fix(P4) |
| 3 sstable | PASSED | 53a62f2 → 851da6a-era fixes (M-008/M-009/M-011/M-012) | |
| 4 manifest/version | PASSED | 7533df9 → 851da6a (M-011) → 0b9a320 (M-012) | |
| 5 db core | PASSED | f039828 → cd6c627 (M-013) | 4 real bugs found by own tests (log_num advance, readState immutability, per-batch ts, manifest writer close) |
| 6 structural compaction | PASSED | 97b3812 (phase-6-done) | M-015 CHANGES arrived later; items below are OPEN |
| 7 checkpoint + resolution | PASSED | 499daf4 + f40fdcf (phase-7-done) | incl. concurrent-install double-release fix |
| 8 semantic pipeline | PARTIAL | 01f9486 | P8.1 mock only; P8.2–P8.6 not started |
| 9–15 | NOT STARTED | | |

Verification at STOP: `gofmt -l .` empty; `go vet ./...` clean;
`go test ./... -race -count=1 -timeout 600s` passes, 13 packages, 144 tests
(root 26, base 11, vfs 18, failpoint 4, wal 14, memtable 4, bloom 5, sstable 23,
merge 9, manifest 5, version 13, resolve 2, checkpoint 5, llm 10 subtests).
No `TODO|FIXME|not implemented` in Go code. `-race` works (CGO_ENABLED=1,
gcc 14.2.0); no `-count=20` fallback needed.

## What works (verified by tests)

- WAL with torn-tail replay and strict manifest replay (M-012 scan-ahead rule).
- Lock-free-read skiplist memtable; group commit with per-batch strictly
  increasing ts (I2); sync policies; sticky write-failure state.
- SST format per B8 (prefix compression, restarts, bloom, props, footer) with
  byte-flip matrices that never return wrong data (I12).
- Recovery: OPTIONS.json check, manifest replay + fresh snapshot rewrite,
  WAL replay bounded by the active WAL, orphan cleanup, allocator raised above
  every existing file number (M-013 multi-session data-loss fix).
- Structural compaction (Tier A + interim raw deep), OutputWriter cutting only
  at user-key boundaries (I5), pickers with busy-file handling.
- Checkpoint (.ckpt) format + transparent MVCC resolution via
  `internal/resolve` (B6 truth table), handmade-checkpoint end-to-end tests
  incl. the v49/v50–60/v61 example and row 7 (delete-tail).
- Snapshots stable across flushes and compactions (I9); obsolete deletion
  driven exclusively by version death.
- Crash matrix on MemFS (both clone modes) with SyncAlways acknowledged-write
  survival (I1); 200k-op model tests with historical reads.

## Open items (supervisor M-015, NOT applied)

1. `sstGet` (db_read.go) turns every error into "not found": a missing or
   corrupt SST can yield a stale Exact from a deeper level (I10/I12 risk).
   Error propagation through Get/GetAt/Snapshot.Get/iterator is required.
2. `planL0` computes the L0 key range from file order instead of
   min(Smallest)/max(Largest) — non-monotone L0 ranges make the compaction
   retry forever and can brick the manifest (see item 4).
3. `appendManifestLocked` appends+syncs BEFORE validation; a rejected edit is
   durable in the manifest and bricks the next Open. Needs
   VersionSet.Prepare/Validate before Append.
4. runStructural must re-check plan files are still current while marking busy.
5. Compaction error loops retry every 10 ms; need growing backoff and Stats.
6. Picker tests on scripted LSM shapes (non-monotone L0 ranges, busy files,
   round-robin, boundary overlaps) — internal/compaction has no test file.
7. Heavy model/crash tests should be reduced under `testing.Short()`
   (root package ~185 s under -race).

Items already resolved by the P7 work before STOP: M-015 item 1 (the
`enqueueObsolete` loop was removed; deletion is driven only by version death
via `drainObsolete`) and item 7 (`DB.Flush()` exists, committed).

## Other notes

- Live LLM was never exercised (SEMLSM_LLM_API_KEY not set; P15 skipped by
  design per A9). Only the deterministic mock ran; no network was touched by
  any test (httptest-based tests never written — P9 not reached).
- Approximate token use is not reported by the harness; based on session
  length this run is well under the 270M A8 threshold.
- Benchmarks (P14.2), demo (P14.3), docs (P14.4) and the 10x-reduction proof
  were not reached.
