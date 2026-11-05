# SemLSM Progress

Source of truth for task state. `[ ]` unchecked, `[x]` done (append one line: what was built, deviations).
Plan reference: SEMLSM_IMPLEMENTATION_PLAN.md (the spec). Phases end with a gate; gates tag `phase-N-done`.

## P0. Bootstrap
- [x] **P0.1** Run `go version` (need >= 1.22; if it's older, use its features only, set `go.mod` accordingly, and log it). Record OS, Go version, cgo, gcc, race availability, and whether `SEMLSM_LLM_API_KEY` is set (yes or no only, never the value) in `.agent/ENV.md`.
  -> ENV.md written. go1.26.3, cgo=1, gcc 14.2.0; race probe passed; key not set.
- [x] **P0.2** `git init` if needed, plus `git config user.name "semlsm-agent"` and `git config user.email "agent@localhost"` (local to the repo only). `.gitattributes`: `* text=auto eol=lf`. `.gitignore`: `.agent/logs/`, `.agent/supervision/`, `shared-memory.json`, `.mcp.json`, `.zcode/`, `.claude/`, `.env`, `*.test`, `*.exe`, `*.prof`, `/tmp/`, `bench-results/`.
  -> repo on branch main, local identity, both files written.
- [x] **P0.3** `go mod init semlsm`. Create the directory tree and `.agent/` files, without touching `.agent/supervision/TO_GLM.md`. Copy every checkbox task in this plan into `PROGRESS.md`. Append the `START` message to `FROM_GLM.md`, and create the `semlsm-status` entity in `shared-memory`.
  -> module semlsm; tree + .agent files created; PROGRESS.md has all tasks; G-001 START appended; entity created.
- [x] **P0.4** `scripts/verify.ps1` runs A5's commands with exit-code checks, with the race step gated on `.agent/ENV.md`. Add a trivial test.
  -> verify.ps1 (race gated on ENV.md); bootstrap_test.go; go test + -race probe pass; gofmt/vet clean.

**Gate 0:** `go test ./...` passes; first commit exists. — PASSED, commit 69c77b3, tag phase-0-done.

## P1. Base and VFS
- [x] **P1.1** `internal/base`: ... (full text in plan)
  -> key.go (Kind/Record/MakeInternalKey/SplitInternalKey/UserKey/Compare/SeekKey), errors.go (sentinels, size limits, NumLevels=5, ValidateKV), clock.go (Clock/RealClock/ManualClock), crc.go (CRC-32C, SourceCRC order-independent), group.go (GroupOf). 11 tests pass: ordering table, transitivity, round trips, GroupOf B4 examples, CRC known-vector, SourceCRC field coverage.
- [x] **P1.2** `internal/vfs` OSFS + interfaces
  -> vfs.go (FS/File/LockHandle/FileInfo), os.go (OSFS), syncdir_windows.go (no-op) + syncdir_unix.go (dir fsync) with build tags, lock_windows.go (CreateFile share mode 0) + lock_unix.go (flock LOCK_EX|LOCK_NB).
- [x] **P1.3** MemFS crash sim + FaultFS
  -> memfs.go: synced-vs-unsynced bytes, durable dirs, rename/remove revert until SyncDir, CrashClone(DropUnsynced|TornTail(rng)); faultfs.go: nth-occurrence injection on FS ops and file Write/Sync/Truncate. 11 tests: unsynced lost, torn prefix, rename revert, remove durability, dir durability, read semantics, list/stat, fault surfacing, OSFS basic, lock exclusivity.
- [x] **P1.4** `internal/failpoint`
  -> Hit/Set/SetError/Reset/List, RWMutex-guarded, near-zero cost when unset. 3 tests.

**Gate 1:** all pass, `gofmt`, `vet`. — see G-003; CHANGES verdict M-004 fixed in commit 935ef6c7:
  1 Remove now hides the file from Open/Stat/List immediately (graveyard only affects CrashClone).
  2 Write honors and advances the per-handle offset (overwrite in place, then extend).
  3 Graveyard is a slice of displaced states; durable entries are never overwritten; crash tests for remove/create/remove and double-rename sequences.
  4 Close returns os.ErrClosed after close (all methods); MemFS.OpenHandles() added.
  5 memState tracks the current name; Stat/handle-Stat use it.
  6 failpoint.Define permanent registry; List returns defined names; Reset keeps definitions.
  7 Hit short-circuits on an atomic counter of armed hooks.
  8 TestFSContract runs one scenario against OSFS and MemFS (M-004 item 8).

## P2. WAL and memtable
- [x] **P2.1** `internal/wal`: ... (full text in plan)
  -> wal.go (Writer Append/Sync/Close/Size, Replay with last-WAL torn-tail rule, size check before allocation), batch.go (EncodeBatch/DecodeBatch, copies on decode). 11 tests: round trip, torn at every offset of final frame (last=true ok / last=false ErrCorruption), torn header offsets, flipped byte, max-size frame, oversized length field, size accounting, codec table + malformed.
- [x] **P2.2** `internal/memtable`: skiplist per B7
  -> memtable.go: max height 12, branching 1/4, deterministic RNG (seed 0x5EED5EED, mutex-guarded); Add copies key/value; Get via SeekKey; Iterator SeekGE/First/Next/Valid/Key/Value; ApproxBytes/Len; atomic.Pointer towers, nodes immutable. 4 tests incl. 100k-op model comparison (seed logged on failure) and 1-writer/8-readers concurrency (order + no torn values), passed with -race.

**Gate 2:** pass, including the concurrent test with `-race` or `-count=20`. — see G-006.

## P3. SSTable
- [x] **P3.1** bloom per B8 -> bloom.go: 10 bits/key, 7 probes, FNV-1a 64 double hashing; Encode/Decode with corruption checks. 5 tests: no false negatives 100k, FPR 0.75% < 2%, round trip, decode corruption, empty filter.
- [x] **P3.2** block builder/reader -> block.go: uvarint shared/unshared/vlen entries, restart every 16, restart binary-search SeekGE, onDiskBlock codec 0/1 flate-if-smaller with CRC. 7 tests incl fuzz seeks and structural corruption.
- [x] **P3.3** Writer -> writer.go: strict ascending keys, props + distinct-user tracking, 16 KiB block target, footer 64B (CRC over first 52, magic), Sync/Close, Abort removes.
- [x] **P3.4** Reader -> reader.go (footer/index/bloom/props validation, Get, MayContain) + iterator.go (sequential block iterator). 6 tests: 100k vs model, many versions, byte-flip matrix never returns wrong data, footer corruption.
- [x] **P3.5** caches -> cache.go: TableCache (ref-counted LRU, Evict closes at 0), BlockCache (byte-bounded LRU). 3 tests incl Evict-then-delete on Windows OSFS and concurrent use under -race.
- [x] **P3.6** merge -> merge.go: min-heap, stable source tie-break, first-error propagation. 5 tests: 1..64 sources incl empty/nil, SeekGE, error propagation.

**Gate 3:** pass; corruption tests pass.


## P4. Manifest and versions
- [ ] **P4.1** `internal/manifest`: `Edit` struct per B8, `Writer`, `ReadAll`, `SetCurrent`, `ReadCurrent`. Test: round trip; torn tail tolerance; `CURRENT` atomicity under `MemFS` crash.
- [ ] **P4.2** `internal/version`: `FileMeta`, immutable `Version{Levels [NumLevels][]*FileMeta; Ckpts *checkpoint index snapshot}`, `Ref/Unref`. `VersionSet.Apply(edit)` returns a new Version. For L1+, keep files sorted by smallest key and check for no overlap on apply (return an error on violation). L0 sorted by file number. Files track a live-reference count across Versions. On last unref, enqueue the file to the obsolete list. For checkpoint data this phase only stores opaque tuple directory entries; P7 adds reading. Tests: apply sequence; overlap rejected; an old Version keeps files alive until unref; the obsolete list is correct.
- [ ] **P4.3** Obsolete file deleter: runs in the background; `TableCache.Evict`, then delete with up to 5 retries at growing delays, leaving a failure for startup cleanup. Test with `FaultFS`.

**Gate 4:** pass.

## P5. Database core (no compaction yet)
- [ ] **P5.1** `options.go`: full Options struct per plan section P5.1 (FS, Clock, SyncMode, memtable/level/file sizes, GroupDepth/GroupDelimiter, Policies, Summarizer, semantic knobs, archive, caches, DisableAutoCompaction, Logger). `withDefaults` and `validate`. Write and check `OPTIONS.json` (group settings only; others may change).
- [ ] **P5.2** `db_recover.go` / `Open`: acquire `LOCK`; create or load manifest; delete files not in the manifest (`.sst`, `.ckpt`, stale `.wal`, `.tmp`); replay WALs with number >= `log_num` ascending, only the last is tolerant of a torn tail, and truncate it to valid bytes; flush replayed data to L0 if any exists (simple, avoids WAL reuse); set `lastTs = max(manifest.last_ts, replayed)`; also bump to `clock.Now()` via the max rule. Start goroutines.
- [ ] **P5.3** `db_write.go`: commit goroutine per B7, batch validation, rotation at `MemtableBytes` (new WAL, sync the old one, push to `imms`, signal flush). Stall when `imms` is full or L0 >= `L0StallTrigger` using a `sync.Cond` wait without holding the commit path's other waiters incorrectly. Record stall time in `Stats`. On WAL write or sync error, the DB goes to a sticky failed state: all further writes return the error.
- [ ] **P5.4** `db_flush.go`: pops the oldest immutable memtable, writes L0 SST, sync, sync dir, manifest edit (`log_num` advances past flushed WALs), install per B7, delete old WAL. Failpoints: `flush.after_sst_sync`, `flush.after_manifest`.
- [ ] **P5.5** `db_read.go`: read state acquire and release, `Get`, `GetAt`, raw lookup per B6. Use `resolve.Resolve` from the start (write `internal/resolve` with truth-table tests now; the entry is always not-found until P7).
- [ ] **P5.6** `db_snapshot.go`, and raw-only `db_iter.go`: merge memtables and all levels; for each user key take the newest version <= S; skip tombstones; respect `Start`/`End`. Use-after-release returns `ErrReleased`. `Close` waits for workers, syncs the WAL, and releases `LOCK`. Leaked snapshots at `Close` are forcibly released and a warning is logged.
- [ ] **P5.7** `internal/testmodel`: `Model` records every committed `(key, ts, kind, value)`, offering `VisibleAt(key, S)`, `ScanAt(start, end, S)`, and later `MarkDropped`. Workload generator with seeded mixes of state-update keys and event-stream keys. Tests at root: Put/Get/Delete; batch atomicity (a concurrent reader never sees half a batch); snapshots stable across flushes; reopen; time travel; a model test with 200k random ops, flushes, reopens, and reads at random historical ts; 8 concurrent writers yield strictly increasing unique timestamps; clock going backwards still yields increasing ts (`ManualClock`).

**Gate 5:** a working raw engine with durability. A `MemFS` crash test: run a workload, crash at random points with both clone modes, reopen, every write acknowledged under `SyncAlways` is present, and nothing is corrupt.

## P6. Structural compaction (Tier A and raw fallback)
- [ ] **P6.1** `internal/compaction/picker.go`: score: L0 = `fileCount/L0CompactTrigger`; L_n = `bytes/target(n)`. Skip busy files. For level `L>=1` pick the file following a persisted-in-memory round-robin cursor. `PickFast()` covers L0->L1 and L1->L2. `PickDeep()` covers L2->L3 and L3->L4, and returns inputs plus overlapping L+1 files. L0 input takes all L0 files present at pick time (preserving I4) plus overlapping L1. Tests on scripted LSM shapes.
- [ ] **P6.2** `compaction/run.go`: merges inputs via `merge`, retains ALL versions and tombstones (history is removed only by semantic compaction), writes outputs via an `OutputWriter` that cuts at `TargetFileBytes` only when the user key changes (I5). Atomic manifest edit. Failpoints: `compact.before_manifest`, `compact.after_manifest`. On error: abort outputs, unmark, back off, retry.
- [ ] **P6.3** `db_compact.go`: fast worker goroutine (one) loops `PickFast`. Deep raw work runs on a separate goroutine, only as directed in P10 (for now, if `Summarizer == nil`, runs raw L2+ when score >= 1). Fast worker never waits on deep. `WaitIdle` and `CompactLevel` implemented. Conflict rule: never select a file already busy.
- [ ] **P6.4** Tests: bulk load triggers L0..L3; all invariants checked by a `checkLSM()` test helper (I3, I4, I5, level non-overlap, each key's versions fully retained) after every compaction; model test with compactions, snapshots, and reopens running concurrently; pinned snapshot still reads deleted files, and disk files disappear after `Release` (assert with `List`); crash at each failpoint and verify.

**Gate 6:** full raw LSM passes all, soak of 60 seconds of concurrent mixed load clean.

## P7. Checkpoint format and transparent resolution
- [ ] **P7.1** `internal/checkpoint`: `Writer{AddTuple(meta, summary, entries)}`, `Finish`; `Reader{Tuples(), ReadMeta(id), ReadSummary(id), LookupKey(id, key) (Entry, bool), IterKeys(id)}`, per B8. Readers are cached and ref-counted, the same as SSTs. Corruption -> `ErrCorruption`. Tests: round trip at 1 and 50,000 keys; flip samples yield `ErrCorruption`; prefix-compressed lookup vs a map model.
- [ ] **P7.2** Version integration: `CkptIndex` is an immutable `map[group][]TupleRef`, with each slice sorted by `TsFrom`. New Versions share unchanged slices. `Apply` handles `add_ckpts` and `del_ckpts`.
- [ ] **P7.3** Entry lookup: for `(K, S)`: `refs := index[GroupOf(K)]`; for every ref with `TsFrom <= S`, check bloom then `LookupKey`, keep the entry with the largest `MinTs <= S`. Skip the checkpoint lookup entirely if the group has no tuples.
- [ ] **P7.4** Wire `Get`, `GetAt`, and `Snapshot.Get` to `Resolve`: build `CheckpointInfo` lazily (read summary only if the outcome is `UseSummary`).
- [ ] **P7.5** Test-only helper `installHandmadeCheckpoint(t, db, group, tsCut)`, which rewrites the relevant SSTs and installs a tuple atomically, via the same edit path. Tests: every row of the truth table end to end; the v49/v50–60/v61 example from B6; a delete as last dropped record, row 7; a snapshot taken before install reads Exact, and one after reads Summarized; `db.VerifyIntegrity` catches a flipped checkpoint byte.

**Gate 7:** pass.

## P8. Semantic pipeline with mock only
- [ ] **P8.1** `internal/llm`: `Request`/`Response`/`Client` interface, `ErrTransient/ErrPermanent/ErrBudgetExhausted/ErrCircuitOpen`, `Summarizer` interface, `MockClient` (deterministic, parses record ids, returns valid JSON citing the first 3; modes Valid/BadJSON/FencedJSON/HallucinatedRef/TooLong/Transient(n)/Permanent/Block(ctx)/Injected; counts calls).
- [ ] **P8.2** `semantic/segment.go`: exact algorithm per plan P8.2 (group by key, ts ascending, MinAge horizon, ModeState keeps newest, sort eligible by (ts,key), cut by ts at MaxBatch, leftover raw). Tests: property test asserting I7 per key per segment, ModeState head retained, ModeEvents head droppable, same-ts batch never split, MinBatch/MaxBatch respected except same-ts overflow, MinAge honored.
- [ ] **P8.3** `semantic/stats.go`: computes `Stats`. `NumericFields`: parse a value as JSON only if the first byte is `{`, take only top-level numeric fields, ignore errors. Deterministic ordering. Tests: including non-JSON values and huge numbers.
- [ ] **P8.4** `semantic/prompt.go`: `PromptVersion = 1`; record line format with relative ts, value truncation, binary values rendered, newlines escaped; chunk by MaxChunkBytes; map/reduce system+user prompts exact per plan; `RECORDS>>>` neutralized. Tests: golden test per prompt; truncation; binary values; neutralization.
- [ ] **P8.5** `semantic/validate.go`: `ParseAndValidate(text, validIDs) (Narrative, error)`; rules V1–V8 in order; typed `ValidationError`. Table test per rule.
- [ ] **P8.6** `semantic/mapreduce.go`: `Summarize(ctx, seg) (Summary, usage, error)`. Single chunk: one call; else map + reduce in groups of up to 8 until one remains. Validate every response; one repair retry; ref resolution; compression ratio check (`ErrPoorCompression`). Tests: every mock mode; 10,000 records across many chunks produces one Summary; cancelled ctx returns quickly.

**Gate 8:** pass, no network used.

## P9. LLM reliability layer
- [ ] **P9.1** `llm/limiter.go`: dual token bucket (requests/min, estimated tokens/min) + concurrency semaphore. Token estimate `len(bytes)/3 + MaxOutputTokens`. `Wait(ctx, tokens)`. Injected clock. Tests with `ManualClock`, no real sleeping.
- [ ] **P9.2** `llm/breaker.go`: closed -> open after 5 consecutive transient failures; open 60s doubling to max 30 min; half-open permits one trial call. Tests.
- [ ] **P9.3** `llm/budget.go`: persisted `llm/usage.json` (atomic write), cumulative input/output/calls; `MaxTokensTotal` mandatory for HTTP clients (zero -> constructor error); exceeded -> `ErrBudgetExhausted`, Tier B stops until reopen; survives restarts. Tests.
- [ ] **P9.4** `llm/cache.go`: on-disk response cache; key = hex SHA-256 of `model|promptVersion|system|user`; stores only validated responses; atomic writes; size cap with oldest-first eviction. Tests.
- [ ] **P9.5** `llm/http.go`: OpenAI-compatible client (`POST {BaseURL}/chat/completions`, Bearer key, `{model, messages, temperature:0, max_tokens}`); parse `choices[0].message.content` + optional usage (estimate if absent); ignore extra fields; env config (`SEMLSM_LLM_BASE_URL`, `SEMLSM_LLM_API_KEY`, `SEMLSM_LLM_MODEL` default `glm-5.3-flash`); 120s timeout; 429/5xx/network/timeout -> ErrTransient, other 4xx -> ErrPermanent; up to 4 retries with exponential backoff + full jitter, honoring Retry-After; never leak the key in errors/logs. Tests only with `httptest.Server`.
- [ ] **P9.6** `llm/stack.go`: `NewSummarizer(client, cfg)` composes cache -> budget -> breaker -> limiter -> client. Cache hits skip all others. Tests for order and interplay.

**Gate 9:** pass.

## P10. Tier B semantic compaction
- [ ] **P10.1** Job state and flow (`internal/compaction/semantic_job.go`): 12-step order per plan (pick, size guard, stream per group, segmenter, Summarize outside locks, drop-set I7 assert, output SSTs, .ckpt, optional archive, one manifest edit, install, obsolete deletion). `PrevTupleID` chain; tuple IDs from file allocator.
- [ ] **P10.2** Scheduling (`db_compact.go`): SemanticWorkers loop on PickDeep (score >= 1 or sweep timer); Summarizer nil/breaker open/budget exhausted -> no LLM, raw L2+ at score >= HardScore; per-job context with SemanticJobTimeout and preempt cancel; anti-stall rule (preempt when fast worker needs L2 files held by a semantic job and L1 score >= HardScore); preempted job commits what it has.
- [ ] **P10.3** `Stats` additions: tuples created, records summarized, raw bytes removed, summary bytes, ratio, LLM calls, tokens, cache hits, failures by class, breaker state, preemptions, jobs by outcome, stall time, per-level file counts/bytes, snapshots open.
- [ ] **P10.4** Bottom level sweep: every `SemanticSweepInterval` and `CompactLevel(bottom)`; pick eligible L4 file (MinAge, MinBatch, `nextTry` backoff map); rewrite in place L4->L4; newest ModeState version stays.
- [ ] **P10.5** Integration tests with the mock (small sizes, ManualClock): 1 headline (10k mutations of `agent/1/state` -> one tuple, SourceCount 9,999, CRC match, latest Exact, mid-range Summarized, raw gone); 2 events mode; 3 mixed policies / ModeDisabled never summarized; 4 snapshot before compaction reads Exact, files removed after Release; 5 fault modes leave nothing lost; 6 Block(ctx) client: writes flow, Tier A proceeds, preempt fires, no deadlock; 7 budget/breaker fallback, reopen resumes; 8 crash at every semantic failpoint, cache-backed retry (mock call count unchanged); 9 repeated compaction, `checkInvariants()` walks I7 for every key.

**Gate 10:** pass. This is the core milestone. Tag it.

## P11. MVCC iteration and history
- [ ] **P11.1** `db_iter.go` full version: merge raw stream with tuple key entries; per key Resolve per B6; keys only in checkpoints emitted; `CollapseSummaries` emits each tuple once at first key.
- [ ] **P11.2** `History`: raw versions in ts range from all levels plus tuples with overlapping run, oldest first, non-overlapping. Test interleaving.
- [ ] **P11.3** `VerifyIntegrity`: read all SST blocks, tuples, manifest/files; check I7 for all; recompute SourceCRC32 from archives; validate PrevTupleID chains; return report.
- [ ] **P11.4** Master model-based MVCC test (`mvcc_model_test.go`): >= 20 seeds, concurrent workload/compaction/reopen/snapshots; rules at random (key, S) per plan; pinned snapshots byte-identical; iterator results equal per-key GetAt.

**Gate 11:** pass across all seeds.

## P12. Cold archive tier
- [ ] **P12.1** `internal/archive`: dropped segment raw records in standard SST at `archive/%06d.sst` (or ArchiveDir); written before manifest commit; orphans removed at startup; different volume -> copy with sync.
- [ ] **P12.2** `GetArchived(key, ts)`: tuple lookup -> archive reader -> newest version <= ts in run, else `ErrNotArchived`. `CheckpointInfo.ArchiveAvailable` set. Tests: exact recoverability; VerifyIntegrity recomputes SourceCRC32 and detects tampering; missing archive file -> ErrNotArchived without harming normal reads.

**Gate 12:** pass.

## P13. Hardening
- [ ] **P13.1** Crash matrix: every failpoint (`failpoint.List()`) x both crash modes: scripted workload with semantic compaction, crash, reopen under MemFS, model checks + VerifyIntegrity; repeat a crash during recovery itself once.
- [ ] **P13.2** Corruption matrix: flip bytes in each file type at sampled offsets; result is ErrCorruption, exact correct result, never a wrong answer, never a panic.
- [ ] **P13.3** I/O error matrix with FaultFS: fail each kind of write/sync at flush/compaction/ckpt/manifest; DB enters failed state or retries; no acknowledged write lost after reopen.
- [ ] **P13.4** Soak test under build tag `soak`, duration `SEMLSM_SOAK_SECONDS` default 600: real OS FS, 8 writers, 8 readers, snapshot churn, mock LLM with random faults, reopen every 60s, periodic VerifyIntegrity, goroutine leak check, heap-growth report. Run once at this gate.
- [ ] **P13.5** Goroutine leak check helper in all DB tests: after Close, `runtime.NumGoroutine()` settles within bounded polling; Windows temp cleanup failure treated as a bug.

**Gate 13:** pass, including one full soak.

## P14. Tools, docs, report
- [ ] **P14.1** `cmd/semlsm`: `put, get [--at ts], del, scan, history, checkpoints, stats, verify, compact --level N`; mock unless `--llm`; exit code 1 on error; golden tests.
- [ ] **P14.2** `cmd/semlsm-bench`: write throughput, point reads latest + historical, scan, semantic on/off; p50/p99; space before/after; output `bench-results/*.json` + summary tables.
- [ ] **P14.3** `cmd/semlsm-demo`: 100k realistic agent-telemetry events with planted anomalies, mock by default; prints raw size, compacted size, ratio, Exact/Summarized/pinned-snapshot reads; at least 10x reduction in summarized range.
- [ ] **P14.4** `README.md`, `docs/ARCHITECTURE.md`, `docs/FORMATS.md` describing the code as built. Then `FINAL_REPORT.md`: gate-by-gate status, bench numbers, deviations, blockers, race status, live-LLM status, token use; plainly state anything not working.

## P15. Live smoke test (OPTIONAL, only if SEMLSM_LLM_API_KEY is set)
- [ ] **P15.1** `cmd/semlsm-eval`: env-gated, 2M token hard cap; 2,000 records with 20 planted facts; real compaction at MaxChunkBytes 32 KiB; `eval-results.json` with recall/validation/tokens/latency/ratio; max 3 attempts on endpoint rejection then mock-only status.

## Final acceptance (Part F)
- [ ] All Part F items true; FINAL_REPORT.md written; work committed; stop.

## Phase gates
- [ ] Gate 0 (P0) — tagged phase-0-done
- [ ] Gate 1 (P1) — tagged phase-1-done
- [ ] Gate 2 (P2) — tagged phase-2-done
- [ ] Gate 3 (P3) — tagged phase-3-done
- [ ] Gate 4 (P4) — tagged phase-4-done
- [ ] Gate 5 (P5) — tagged phase-5-done
- [ ] Gate 6 (P6) — tagged phase-6-done
- [ ] Gate 7 (P7) — tagged phase-7-done
- [ ] Gate 8 (P8) — tagged phase-8-done
- [ ] Gate 9 (P9) — tagged phase-9-done
- [ ] Gate 10 (P10) — tagged phase-10-done
- [ ] Gate 11 (P11) — tagged phase-11-done
- [ ] Gate 12 (P12) — tagged phase-12-done
- [ ] Gate 13 (P13) — tagged phase-13-done
- [ ] Gate 14 (P14) — tagged phase-14-done
