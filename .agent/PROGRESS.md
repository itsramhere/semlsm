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
- [x] **P4.1** manifest -> manifest.go: Edit (JSON per B8, []byte keys base64), Writer (wal.Frame shared), ReadAll (wal.Replay last-mode; JSON decode errors are ErrCorruption), SetCurrent/ReadCurrent (tmp+sync+rename+syncdir), ManifestName/ParseManifestNum. 5 tests: round trip, torn tail tolerance, valid-CRC bad-JSON corruption, CURRENT atomicity (crash before/after SyncDir), name parsing.
- [x] **P4.2** version -> version.go: FileMeta (manifest.FileMeta + atomic refs), immutable Version (Levels + CkptIndex clone-on-write), VersionSet.Apply (L0 num-desc, L1+ smallest-sorted with overlap rejection), membership-ref accounting: old versions release file refs only at death; obsolete list on zero. 7 tests: apply sequence, overlap/boundary rejection, refcount + obsolete timing, instance sharing.
- [x] **P4.3** obsolete deleter -> obsolete.go: background worker, TableCache.Evict then Remove with 5 retries (base delay injectable), failures to Failed() for startup cleanup; FaultFS InjectRange added for retry tests. 3 tests incl. retry-then-success and fail-after-5.

**Gate 4:** pass.


## P5. Database core (no compaction yet)
- [x] **P5.1** options.go -> full Options struct per plan, SyncMode/Mode/Policy, withDefaults + validate, policyFor (longest prefix), OPTIONS.json write/check (group settings only) with ErrOptionsMismatch. 4 tests.
- [x] **P5.2** db_recover (semlsm.go) -> LOCK, OPTIONS check/write, manifest load (replay via Apply + fresh snapshot rewrite per M-011 item 7) or create, active WAL created + SyncDir before replay, WAL replay [logNum, walNum) ascending with last-torn truncate, recovery flush to L0 (log_num advances to active WAL), orphan cleanup (never the active WAL), lastTs = max(manifest, replayed, clock).
- [x] **P5.3** db_write.go -> Batch (last-write-wins, copied), commit goroutine with group commit (max 256), per-batch strictly-increasing ts = max(lastTs+1, now) (I2), SyncAlways/EveryN/None, memtable apply, rotation at MemtableBytes (new WAL + SyncDir + new immutable readState), sync.Cond stall with stall-time stats, sticky failed state on WAL errors.
- [x] **P5.4** db_flush.go -> flush worker pops oldest immutable, writes L0 SST (sync + syncdir + failpoints flush.after_sst_sync / flush.after_manifest), manifest edit advancing log_num ONLY past the flushed WAL (I1), installs fresh readState per B7, closes+deletes old WAL.
- [x] **P5.5** db_read.go + internal/resolve -> resolve.Resolve truth table (16-case test), rawLookup per B6 (mem, imms newest-first, L0 num-desc, L1+ range), Get/GetAt with ErrFutureTimestamp, checkpoint entry not-found until P7.
- [x] **P5.6** db_snapshot.go + db_iter.go -> Snapshot (pins readState, ErrReleased after Release, idempotent), raw iterator (merge over mem+imms+levels, newest <= S per key, tombstones skipped, Start/End), Close waits for workers, syncs WAL, releases LOCK, forcibly releases leaked snapshots with warning.
- [x] **P5.7** internal/testmodel + root tests -> Model (VisibleAt/ScanAt/MarkDropped/Keys), Gen (state+event mix); 14 root tests: Put/Get/Delete, batch atomicity, snapshots stable across flushes, reopen, time travel, 200k-op model test with historical probes, 8-writer unique ts, clock-backwards, MemFS crash matrix (6 rounds x 2 modes, SyncAlways acknowledged writes present).

**Gate 5:** a working raw engine with durability. — PASSED, see G-018.

## P6. Structural compaction (Tier A and raw fallback)
- [x] **P6.1** internal/compaction/picker.go -> Score (L0 count-based, L1+ bytes/target), PickFast/PickDeep/PickLevel, planL0 takes ALL L0 (I4), planLevel round-robin cursor + overlapping outputs, overlapRange with busy detection, OutputWriter cutting at TargetFileBytes only at user-key boundaries (I5), retaining all versions/tombstones.
- [x] **P6.2** compaction run (db_compact.go runStructural) -> merge over inputs, outputs synced + syncdir before manifest edit, failpoints compact.before_manifest/after_manifest, appendManifestLocked for the atomic edit, Abort on error, fresh readState install per B7, busy-file marking with conflict detection.
- [x] **P6.3** db_compact.go workers -> fast worker (L0->L1, L1->L2) never waiting on deep; deep raw worker (L2+ at score >= 1, interim until P10); WaitIdle (queues + scores, with obsolete draining); CompactLevel (manual trigger); obsolete deletion driven ONLY by version death (drainObsolete from workers + WaitIdle), keeping snapshot-pinned files on disk.
- [x] **P6.4** compaction_test.go -> checkLSM helper (I3 order over files+memtables, L1+ non-overlap, full version multiset vs model); bulk load drives L0..L3; model test with concurrent compactions + snapshot churn + reopen + historical probes; pinned snapshot reads consumed files and they disappear after Release; crash at both compaction failpoints and reopen verify.

**Gate 6:** fix(P6) re-run after M-015; re-review pending (unchecked until APPROVED, M-016 item 5). See G-024.

fix(P6) applied (M-015 items 3,2,4,5,6,8,9 + supervisor scenarios):
- item 3 (critical): planL0 now computes the L0 range as the union of all inputs (min Smallest / max Largest via CompareUser); the old file-order range missed a straddling L1 file, Apply rejected the edit and compaction retried forever.
- NEW BUG found by the item-3 scenario test: openRecover built the readState BEFORE the manifest load, so a clean reopen (active WAL empty at Close, nothing to replay) served NotFound for every key until the first install. rs is now built after loadOrCreateManifest. Every earlier reopen test had unflushed WAL data, whose recovered flush masked it.
- item 2: sstGet/rawLookup/checkpointLookup/Get/GetAt/Snapshot.Get propagate storage errors; iterator records unopenable tables in Err() instead of skipping. TestSSTRemovedBehindBack.
- item 4: VersionSet.Validate runs Apply's checks dry-run; appendManifestLocked validates BEFORE Append+Sync, so a rejected edit leaves the manifest byte-identical. TestManifestEditValidatedBeforeAppend.
- item 5: runStructural re-checks plan files against the current version (Version.HasFile) while marking busy; stale plans and busy files drop silently (no error, no backoff). TestStalePlanDroppedSilently.
- item 6: both compaction loops back off 100ms doubling to 5s (exit on close), count failures in Stats.CompactionErrors, log per retry. TestCompactionBackoffOnPersistentError (failpoint-counted attempts).
- item 8: internal/compaction/picker_test.go - L0 range union, busy at each position, round-robin cursor, overlap edges (lo==hi touching), scores, bottom-level boundary.
- item 9: testing.Short() reductions - model 200k->20k, concurrent model 8000->2000, crash matrix 6->2 rounds, multi-session 4->2 (root: ~30s -> ~4s under -short).
- permanent scenarios: TestSnapshotPinsFilesUntilRelease (already existed), TestL0NonMonotoneRangesCompactionAndReopen, TestSSTRemovedBehindBack.
- removed the temporary dbgLogCompaction stack-capture hook (unused since the pin-race investigation).

## P7. Checkpoint format and transparent resolution
- [x] **P7.1** internal/checkpoint -> Writer (SLCK header, tuples: meta JSON+CRC, flate-if-smaller summary/index sections with codec byte in CRC, prefix-compressed key index with restarts every 32 + bloom + fixed trailer, framed JSON directory, 24B footer) + Reader (footer/dir validation, lazy per-tuple parse cache, LookupKey with restart binary search, IterKeys) + ref-counted CkptCache. 5 tests: round trip, 50k keys, single key/empty summary, byte-flip matrix, prefix compression.
- [x] **P7.2** version CkptIndex -> ByNum + ByGroup (map[string][]TupleLoc sorted by TsFrom, rebuilt on clone/Apply). Existing version tests extended.
- [x] **P7.3** entry lookup -> db.checkpointLookup walks group refs with TsFrom <= S, keeps the entry with the largest MinTs <= S (B6/P7.3).
- [x] **P7.4** Get/GetAt/Snapshot.Get wired to Resolve via checkpointLookup; CheckpointInfo built lazily (summary read only on UseSummary); ref handling on every outcome path.
- [x] **P7.5** p7_test.go -> installHandmadeCheckpoint (Flush first, rewrite SSTs without dropped records at the same level, one atomic manifest edit + install); truth-table rows end to end incl. B6's v49/v50-60/v61 example; row 7 with the run ending in a delete; snapshot-before-install reads Exact from pinned raw while live reads Summarized (I9); minimal VerifyIntegrity catches a flipped ckpt byte.
- [x] **BUG FIX (found by the P6.4 pin test under -race)**: concurrent installs (flush worker vs CompactLevel vs handmade) each captured old = db.rs.version separately and could double-release the readState's version ref, killing a snapshot-pinned version and deleting its files. Fix: Apply->readState swap now happens under manifestMu (nested db.mu; lock order manifestMu->mu per B7) in finishFlush and runStructural, so exactly one installer releases the readState ref.

**Gate 7:** pass. — PASSED, see G-022.

## P8. Semantic pipeline with mock only
- [x] **P8.1** `internal/llm`: `Request`/`Response`/`Client` interface, `ErrTransient/ErrPermanent/ErrBudgetExhausted/ErrCircuitOpen`, `Summarizer` interface, `MockClient` (deterministic, parses record ids, returns valid JSON citing the first 3; modes Valid/BadJSON/FencedJSON/HallucinatedRef/TooLong/Transient(n)/Permanent/Block(ctx)/Injected; counts calls). — DONE (commit 01f9486, STOP received; 1 test file, 10 subtests). P8.2-P8.6 done in the resumed run (commit follows).
- [x] **P8.2** `internal/semantic/segment.go` — DONE: `Segmenter(records, pol, now)` per the plan algorithm; returns segments + raw (ineligible + leftover) so segments+raw account for the whole input; saturating MinAge horizon. 7 tests incl. I7 property (per-key contiguous oldest-first prefix, nothing lost/duplicated).: exact algorithm per plan P8.2 (group by key, ts ascending, MinAge horizon, ModeState keeps newest, sort eligible by (ts,key), cut by ts at MaxBatch, leftover raw). Tests: property test asserting I7 per key per segment, ModeState head retained, ModeEvents head droppable, same-ts batch never split, MinBatch/MaxBatch respected except same-ts overflow, MinAge honored.
- [x] **P8.3** `internal/semantic/stats.go` — DONE: B9 `Stats`/`KeyCount`/`NumericField`/`Narrative`/`Fact`(+ResolvedRefs)/`Summary` in types.go; `ComputeStats` with top-20 caps, ties deterministic; NumericFields only for values starting '{' via UseNumber+type-assert (mixed-type objects handled), huge numbers ignored. 4 tests.: computes `Stats`. `NumericFields`: parse a value as JSON only if the first byte is `{`, take only top-level numeric fields, ignore errors. Deterministic ordering. Tests: including non-JSON values and huge numbers.
- [x] **P8.4** `internal/semantic/prompt.go` — DONE: PromptVersion=1; exact map/reduce system prompts (reduce = map + merge sentence) and map/reduce user prompts; line format `r<i> | ts=+delta | KIND | key=<suffix> | <value>`; truncation at rune boundary, `<binary N bytes crc=%08x>` for non-UTF-8, newlines escaped, `RECORDS>>>` neutralized to `RECORDS>>​>` (verified against the plan bytes). Chunking never splits a record. 8 tests incl. goldens.: `PromptVersion = 1`; record line format with relative ts, value truncation, binary values rendered, newlines escaped; chunk by MaxChunkBytes; map/reduce system+user prompts exact per plan; `RECORDS>>>` neutralized. Tests: golden test per prompt; truncation; binary values; neutralization.
- [x] **P8.5** `internal/semantic/validate.go` — DONE: `ParseAndValidate(text, validIDs)` with V1-V8 in order, typed `ValidationError{Rule,Msg}`; DisallowUnknownFields + trailing-data rejection; rune-based limits; escaped control chars () caught at V8. Table test with 20 cases + exact-limit + fence tests. `NarrativeJSON` helper.: `ParseAndValidate(text, validIDs) (Narrative, error)`; rules V1–V8 in order; typed `ValidationError`. Table test per rule.
- [x] **P8.6** `internal/semantic/mapreduce.go` — DONE: `Summarizer.Summarize(ctx, seg, group)`; single chunk one call; else bounded-concurrency map (first error cancels the rest) + serial reduce rounds of up to 8; every response validated with ONE repair retry (`Your previous output was invalid: ...`); refs resolved to (key,ts) into Fact.ResolvedRefs; compression check `SourceBytes/(narrative JSON + est. key index) >= MinRatio` else `ErrPoorCompression`; `Usage` accumulates tokens/calls. Note: signature takes the group explicitly (Segment does not carry it). 8 tests: all 9 mock modes, repair-call count, 10k records, deterministic overlap probe for concurrency, cancelled ctx, poor compression.: `Summarize(ctx, seg) (Summary, usage, error)`. Single chunk: one call; else map + reduce in groups of up to 8 until one remains. Validate every response; one repair retry; ref resolution; compression ratio check (`ErrPoorCompression`). Tests: every mock mode; 10,000 records across many chunks produces one Summary; cancelled ctx returns quickly.

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
- [x] Gate 4 (P4) — PASSED, commit 7533df9, tag phase-4-done
- [x] Gate 5 (P5) — PASSED, commit f039828, tag phase-5-done
- [ ] Gate 6 (P6) — 97b3812 passed G-021 but M-015 reopened it; fix(P6) posted, awaiting verdict
- [x] Gate 7 (P7) — PASSED, commit 499daf4, tag phase-7-done
- [ ] Gate 8 (P8) — tagged phase-8-done
- [ ] Gate 9 (P9) — tagged phase-9-done
- [ ] Gate 10 (P10) — tagged phase-10-done
- [ ] Gate 11 (P11) — tagged phase-11-done
- [ ] Gate 12 (P12) — tagged phase-12-done
- [ ] Gate 13 (P13) — tagged phase-13-done
- [ ] Gate 14 (P14) — tagged phase-14-done
