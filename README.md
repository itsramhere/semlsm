# SemLSM

An embeddable Go LSM storage engine whose deep-level compaction replaces old
key versions with LLM-written **checkpoint tuples** while keeping raw reads
exact.

## What it is

A five-level LSM tree (WAL → memtable → L0 → L1 → L2/L3/L4) where structural
compaction (Tier A) never drops data, and semantic compaction (Tier B) — run
on deep levels only — hands contiguous runs of old versions of one key to an
LLM. The LLM returns a validated narrative summary; the run's exact versions
are removed from the tree, one `.ckpt` file records the summary plus a key
index (per-key min/max ts/count/last-kind), and every read consults both the
raw data and the tuples through a truth-table resolution (`internal/resolve`)
so no reader ever sees a stale exact value.

- **Raw reads stay exact**: the newest version of every key is never dropped
  (ModeState), deletes stay deletes, and reads at any timestamp resolve
  through the same table.
- **History is recoverable**: with `ArchiveRaw` the dropped versions are
  archived in plain SSTs; `GetArchived(key, ts)` returns any dropped version
  exactly, and `VerifyIntegrity` recomputes the archives' CRCs.
- **The LLM is policy, not truth**: only the narrative is LLM-written, it is
  validated against strict rules (fences, JSON shape, length caps, record-id
  refs that must resolve), and a poor compression ratio leaves the group raw.

## Quick start

```go
db, err := semlsm.Open("mydb", semlsm.Options{
    GroupDepth:     2,
    GroupDelimiter: '/',
    Policies: []semlsm.Policy{
        {Prefix: "agent/", Mode: semlsm.ModeState, MinAge: time.Hour,
            MinBatch: 100, MaxBatch: 10000},
    },
    DefaultPolicy: semlsm.Policy{Mode: semlsm.ModeDisabled},
    Summarizer:    stack, // nil = Tier B never runs; pure structural LSM
})
if err != nil { ... }
defer db.Close()

ts, err := db.Put([]byte("agent/7/state"), []byte(`{"hp":42}`))
res, _ := db.Get([]byte("agent/7/state"))        // ResultExact
old, _ := db.GetAt([]byte("agent/7/state"), ts)  // Exact or Summarized
items, _ := db.History([]byte("agent/7/state"), 0, ^uint64(0))
```

## CLI

```
go run ./cmd/semlsm --dir demo put agent/1/state '{"hp":42}'
go run ./cmd/semlsm --dir demo get agent/1/state --at 1767225600000000001
go run ./cmd/semlsm --dir demo scan --start agent/
go run ./cmd/semlsm --dir demo history agent/1/state
go run ./cmd/semlsm --dir demo checkpoints agent/1/
go run ./cmd/semlsm --dir demo stats
go run ./cmd/semlsm --dir demo verify
go run ./cmd/semlsm --dir demo compact --level 4
```

The CLI uses the deterministic mock LLM; `--llm` switches to an
OpenAI-compatible endpoint configured from `SEMLSM_LLM_BASE_URL`,
`SEMLSM_LLM_API_KEY`, `SEMLSM_LLM_MODEL` (the key is read from the
environment and never logged).

## Benchmarks and demo

```
go run ./cmd/semlsm-bench --records 20000   # write/read/scan p50-p99, sizes, JSON report
go run ./cmd/semlsm-demo                    # 100k telemetry records, >= 10x reduction
```

The demo prints the before/after sizes and demonstrates an Exact read, a
Summarized read, a pinned-snapshot read of the SAME mid-range timestamp
(still Exact while the live read is Summarized — I9), and exact anomaly
recovery from the archive tier. Measured on the development machine
(Windows, in-memory FS) at 100k records: **174x reduction of the summarized
range** — with the deterministic mock, whose summaries are fixed-size; a
real LLM writes larger summaries, so the live-LLM ratio will be lower and
stays unmeasured until the optional P15 run. The mock's summary text
("mock summary of 3 records") is canned; the tuple's real numbers
(999 versions covered) are engine-computed.

Store sizes are reported honestly: the whole store does NOT shrink by the
summarization ratio — the archive tier intentionally keeps every dropped
raw record (recoverability), and checkpoint files add fixed overhead. The
demo prints the live store and the archive separately; the live store
(1.28 MB raw → 117 KB) is where the compaction wins live.

## Guarantees

- **I1 atomicity**: one fsynced manifest record per flush/compaction; a crash
  leaves the old or the new state, never a mix.
- **I3 internal-key order**, **I4 newest-first L0**, **I5 files cut on key
  boundaries**, **I6 MVCC reads via the newest version <= S**.
- **I7 contiguous drops**: a checkpoint's dropped versions are exactly the
  key's versions inside `[minTs, maxTs]` — asserted when a semantic job
  commits and verified by `VerifyIntegrity`.
- **I8 atomic install**, **I9 snapshot stability** (pinned read states;
  reads byte-identical for the snapshot's life).
- **I10 no stale exact**: resolution never returns an older raw version when
  the true visible version was dropped.
- **I11 LLM isolation**: no lock is held across an LLM call; Tier A never
  waits on Tier B; LLM failure leaves data raw, never lost.
- **I12 integrity**: CRCs on every block/frame/section; corruption returns
  `ErrCorruption`, never silent wrong data.

## What is lossy and what is not

- **Not lossy**: latest values, raw history below and above dropped runs,
  tombstones, the archived raw records when `ArchiveRaw` is on.
- **Lossy**: the *intermediate* versions inside a checkpoint's range are
  replaced by a narrative summary. Their exact values exist only in the
  archive (if enabled). The summary is LLM prose and is not a substitute for
  the data.

## Limits

- **Manifest torn tail**: a corrupted *final* manifest frame is treated as a
  torn write per the plan, which silently rolls back the last committed edit
  (e.g. a whole semantic job) to the previous state — orphan files are swept
  at the next Open. No wrong answer is ever served, but the rollback is
  silent; see `docs/FORMATS.md` for the possible tightening.
- Checkpoint files accumulate one per semantic job (no tuple GC yet).
- The LLM path is policy-limited (rate limiter, breaker, persisted token
  budget); an exhausted budget stops semantic work until the process reopens
  the DB.

## Layout

`semlsm.go` (public API) · `db_write/db_read/db_iter/db_snapshot/db_flush/
db_compact/db_recover/db_history/db_archive` · `internal/{base,vfs,failpoint,
wal,memtable,bloom,sstable,merge,manifest,version,resolve,checkpoint,semantic,
llm,compaction,archive}` · `cmd/{semlsm,semlsm-bench,semlsm-demo}`.

See `docs/ARCHITECTURE.md` for the design and `docs/FORMATS.md` for every
on-disk format.
