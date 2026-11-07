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
   add outputs, add ckpt with its archive numbers), installs the new read state
   under `mu` nested inside `manifestMu`, and obsolete deletion happens through
   version death.

`PrevTupleID` is the group's most recently created tuple (largest id; the
allocator is monotonic across restarts). Jobs can drop progressively older
ranges, so insertion order — not ts order — defines the chain.

### Checkpoint merging

`Options.CkptMergeThreshold` defaults to 32; a negative value disables
background merges. When current has more files than the threshold, the fast
compaction loop performs at most one merge per iteration alongside its SST
work. Semantic workers continue independently. `DB.CompactCheckpoints(ctx)`
(and CLI `compact --checkpoints`) performs one merge immediately, including
when automatic merging is disabled. Its input cap is max(2, threshold).

Inputs are the oldest available file numbers, skipping busy files. One merge
runs at a time; its inputs and output are reserved in the existing busy map.
Semantic output numbers stay busy through installation and job completion.
Semantic jobs consult checkpoint metadata for predecessor IDs; they do not
read checkpoint files, and unchanged tuple IDs make this safe across merges.

A pinned input version protects files throughout copying. Outside engine
locks, `checkpoint.Writer.CopyTuple` freshly reads and verifies all section
CRCs and the key index, then writes the original tuple bytes. Only directory
offsets change. The output manifest record carries every tuple reference and
the union of archive numbers. Under manifestMu, the merge rechecks current
membership and its busy reservations, then appends one AddCkpts + DelCkpts
edit and installs under mu. Old files die only after all versions release
them; checkpoint cache eviction releases dead readers before deletion.

`ckptmerge.after_write` runs after output sync and directory sync but before
any edit; precommit errors remove the output, while a crash leaves an orphan
for Open to sweep. `ckptmerge.after_manifest` runs after durable installation:
errors there retain the referenced output. If manifest append/sync fails
before Apply, a fresh snapshot of the unchanged state is published before
removing the candidate: failed syncs can leave complete frames readable.
If restoring that snapshot also fails, the output is retained because its
durability is uncertain. The new checkpoint protects its
archives during input version death. Snapshot Get, iteration, and
GetArchived continue using the original pinned version. Close stops new
merge admission and waits for in-flight manual and background merges.

Merges retain every tuple and summary. They bound file count; they do not
implement tuple retention or shrink historical summary contents.

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

Open takes LOCK, checks OPTIONS.json, replays the manifest and WALs numbered
at least log_num (only the newest WAL tolerates a torn tail), creates a fresh
WAL, flushes replayed data to L0, sweeps unreferenced SSTs/ckpts/replayed
WALs/tmps and archives, then starts workers. A failed Open closes its writers
and releases LOCK; it does not create an empty newer WAL before discovering
a bad older one.

A short header, declared length past EOF, or permitted zero tail is torn.
Complete frames with bad CRCs are `ErrCorruption`, even in the final manifest
frame or last WAL. Manifest recovery still searches past an invalid length
for valid later frames, which makes it corruption rather than a tail tear.

`Repair(dir, opts)` / `semlsm --dir DIR repair` acquires the same LOCK. It
validates CURRENT's manifest and all WALs Open would replay; only a complete
final bad-CRC frame in that manifest or the newest eligible WAL can be
explicitly discarded. Middle corruption, valid frames hidden after damage,
and invalid JSON/batches with valid CRCs remain errors. Repair recovers the
preceding version, moves every unreferenced data file into `lost+found/`
(archives under `lost+found/archive/`), syncs the directories, then truncates
and syncs the damaged logs. It never deletes data files or overwrites an
existing quarantine entry. A following Open ignores `lost+found/` entirely.

Each checkpoint's manifest record carries `archive_nums`. Version membership
counts keep checkpoints alive across snapshots; the obsolete record retains
the dying version's checkpoint metadata. Archive deletion uses those numbers
and excludes archives referenced by ANY live version, including snapshot-only
checkpoints. The locked VersionSet.LiveCheckpoints accessor supplies independent
metadata copies for that union. Legacy records
without numbers fall back to checked tuple reads. If a live legacy archive
set cannot be determined, Open logs and skips archive deletion entirely;
Repair returns an error rather than guessing which archives to quarantine.

MemFS tracks every Create/Open/OpenReadWrite handle, exposes sorted names
for diagnostics, and counts a handle's Close only once. Crash clones start
with no handles or process locks. Root and internal MemFS test fixtures check
for zero handles after DB, cache, and stack use has ended.

## 8. Known limitations

- Real disks may tear a write in a non-prefix pattern (for example sector
  reordering). A complete final frame with a bad CRC is now `ErrCorruption`;
  recovery requires explicit Repair. The MemFS crash model only keeps prefixes.
- Tuple retention (dropping old summaries) is not implemented; merges keep all tuples.
- `Picker` round-robin cursors are guarded by a mutex shared with manual
  `CompactLevel` callers.
