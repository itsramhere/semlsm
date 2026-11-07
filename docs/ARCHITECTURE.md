# SemLSM Architecture

This document describes the code **as built** (through P14). The normative
spec is `SEMLSM_IMPLEMENTATION_PLAN.md`; where implementation choices were
made they are marked.

## 1. The tree

Five levels. Writes go: commit goroutine → WAL append (+sync per `SyncMode`)
→ active memtable; at `MemtableBytes` the memtable rotates to an immutable
one and a fresh WAL starts; the flush worker writes L0 SSTs and advances
`log_num`. Auto compaction:

- **Tier A (fast worker)**: L0→L1 and L1→L2. L0 files are always taken *all
  together* (I4: newest-first visibility). When both L0 and L1 are over
  target the picker alternates so continuous L0 traffic cannot starve L1→L2.
- **Tier B (semantic workers)**: L2→L3, L3→L4 picks and the L4 sweep.
  Structural fallback runs raw when the LLM is unhealthy (breaker open /
  budget exhausted) at score ≥ `HardScore`.

## 2. Keys, ordering, grouping

Internal key = `userKey || ts(8B BE) || kind(1B)`; order is user key asc, ts
desc, kind desc (I3). `GroupOf(key)` takes the `GroupDepth`-th delimiter
prefix (`agent/42/events/9` → `agent/42/` at depth 2); groups are contiguous
in key order, which is what makes per-group streaming in a merged scan safe.

Policies (longest prefix wins): `ModeState` never drops a key's newest
version; `ModeEvents` may drop everything in the run; `ModeDisabled` is
never summarized.

## 3. Reads (B6)

`Get`/`GetAt`/snapshot reads: newest raw version ≤ S from memtables and
levels, plus the covering checkpoint entry (largest `MinTs ≤ S` among the
group's tuples), fed to `resolve.Resolve`. The truth table guarantees I10:
a dropped run answers `UseSummary` (or an exact NotFound when the run ends
in a tombstone); a raw version inside a run's range is an I7 violation and
surfaces as `ErrCorruption`. The iterator performs the same resolution per
key, merging the raw stream with tuple key entries;
`CollapseSummaries` emits each tuple once at its first key.

## 4. Semantic jobs (P10)

`RunSemantic` executes plan steps 3–9 with **no engine locks held** (I11):

1. Stream the merged inputs one group at a time (bounded by
   `MaxSemanticJobBytes`; bigger jobs fall back to raw structural).
2. Per group, partition by effective policy; run the Segmenter (eligible
   oldest-first prefixes per key, same-ts runs never split, MinBatch/MaxBatch
   honored, MinAge horizon on the engine clock).
3. Summarize each segment in time order via the map-reduce pipeline
   (P8): bounded-concurrency map, reduce rounds of 8, one repair retry on
   validation failure, refs resolved to (key, ts), compression-ratio gate.
   Any failure puts that segment and all later ones in the group raw;
   preempted jobs stop calling the LLM entirely and commit what they have.
4. Assert I7 per key (dropped set == input versions inside
   `[entry.MinTs, entry.MaxTs]`) — violation aborts the job.
5. Write output SSTs (all non-dropped records; cut only on key change),
   one `.ckpt` with all tuples, optional per-tuple archive SSTs (each
   verified by reading back and recomputing `SourceCRC32`).
6. The driver appends ONE manifest edit under `manifestMu` (del inputs,
   add outputs, add ckpt), installs the new read state under `mu` nested
   inside `manifestMu`, and obsolete deletion happens through version death.

`PrevTupleID` is the group's most recently created tuple (largest id; the
allocator is monotonic across restarts). Jobs can drop progressively older
ranges, so insertion order — not ts order — defines the chain.

## 5. Reliability layers (P9)

`llm.Stack` composes **cache → budget → breaker → limiter → client**. Cache
hits skip everything (and never spend tokens). The budget persists
cumulative tokens at `llm/usage.json` (atomic writes; `MaxTokensTotal` is
mandatory for the HTTP stack). The breaker trips after 5 consecutive
transient failures, re-opens with doubling cool-down (60s → 30min), and
allows exactly one half-open trial — the trial always resolves (a
non-transient outcome closes the breaker; a call that never reached the
client is given back via `Cancel`). The limiter is a dual token bucket
(requests/min, estimated tokens/min) plus a concurrency semaphore.
The OpenAI-compatible client retries 429/5xx/network/timeout up to 4 times
with full jitter, honoring `Retry-After`; other 4xx are permanent; the API
key never appears in errors.

Responses enter the cache only through the `Accept` hook — the engine wires
semantic validation (V1–V8, ids extracted from the prompt) so only validated
responses are cached.

## 6. Concurrency model (B7)

`readState` is immutable once built (memtable + immutables + version);
readers acquire/release it under `mu`. Manifest writes serialize on
`manifestMu`; the install swaps the read state under `mu` nested inside
`manifestMu`. Versions are ref-counted; obsolete deletion is driven
exclusively by version death (a snapshot keeps its files alive). No lock is
held across I/O or LLM calls.

## 7. Recovery

Open: LOCK → OPTIONS.json check → manifest replay (torn final frame ignored
per plan; anything else `ErrCorruption`) → delete files not in the manifest
(SSTs/ckpts/WALs/tmps) plus archive segments no live tuple references →
replay WALs ≥ log_num (only the last WAL tolerates a torn tail) → flush
replayed data to L0 → start workers.

## 8. Known limitations

- **Silent torn-tail rollback**: a byte flip in the manifest's final frame
  is treated as a torn write (per the plan's WAL-like rule), rolling back
  the last committed edit to the previous state. No wrong answer is served
  and the next Open sweeps the orphan files, but the rollback is silent. A
  stricter variant would treat a full-length frame with a bad CRC as
  `ErrCorruption` and tolerate only short frames.
- Checkpoint files accumulate one per semantic job (no tuple GC yet).
- `Picker` round-robin cursors are guarded by a mutex shared with manual
  `CompactLevel` callers.
