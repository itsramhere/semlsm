# SemLSM On-Disk Formats

All integers little-endian unless stated otherwise; the internal key's
timestamp is big-endian. CRC is CRC-32C (Castagnoli) via the shared
`base.CRC`. Files live in one DB directory; a shared file-number allocator
names them `%06d.{sst,ckpt,wal}`.

## Internal key

`userKey || ts(8 bytes BE) || kind(1B)`. Kinds: `Delete=1`, `Put=2`.
`Compare`: user key asc, then larger ts first, then larger kind first.
The seek key for "newest version ≤ S" is `(key, S, 0xFF)`.

## WAL frame

`u32 payloadLen | u32 crc(payload) | payload`.
Payload: `u64 ts | uvarint count | repeat(u8 kind, uvarint klen, key,
uvarint vlen, value)`. Max payload 64 MiB. Replay: a short header, declared
length past EOF, or zero-length tail in the LAST WAL is a torn tail (stop
and truncate). A complete frame with a bad CRC is `ErrCorruption`, even at
EOF. Earlier WALs reject torn tails as corruption. Existing zero-tail rules
are unchanged; the manifest additionally requires all bytes following a
zero-length header to be zero.

## SST

```
[data block]* [bloom block] [props block] [index block] [footer 64B]
```

- On-disk block: `payload | codec(1) | crc32(payload||codec)`. Codec
  `0`=none, `1`=flate level 1 (used only when it shrinks).
- Block payload entries: `uvarint shared | uvarint unshared | uvarint vlen |
  keyDelta | value`, restart every 16, trailing `u32 restarts[] |
  u32 numRestarts`. Target data block 16 KiB.
- Index entries: key = block's last internal key; value = `u64 offset |
  u64 length`.
- Bloom: 10 bits per distinct user key, 7 probes, double hashing from
  FNV-1a 64; payload `bits | u8 k`.
- Props (JSON): `{count, deletes, raw_key_bytes, raw_value_bytes, min_ts,
  max_ts, smallest, largest}`.
- Footer, exactly 64 bytes: `index off,len | bloom off,len | props off,len
  (6 x u64) | u32 version=1 | u32 crc32(first 52) |
  u64 magic=0x53454D4C534D5354`.

## Manifest

`MANIFEST-%06d` frames like the WAL, payload JSON:

```json
{"next_file_num":0,"last_ts":0,"log_num":0,
 "add_files":[{"level":0,"num":0,"size":0,"smallest":"","largest":"",
               "min_ts":0,"max_ts":0,"count":0}],
 "del_files":[{"level":0,"num":0}],
 "add_ckpts":[{"file_num":0,"size":0,"archive_nums":[0],
               "tuples":[{"id":0,"group":"","ts_from":0,"ts_to":0,
                          "offset":0,"length":0}]}],
 "del_ckpts":[0]}
```

Keys are `[]byte` (base64 in JSON). The first record of a new manifest is a
full snapshot. `CURRENT` holds the manifest file name (written via
tmp+sync+rename+syncdir). Roll at 4 MiB. **Torn tail**: ignore a short
header, declared length past EOF (unless a valid frame follows), or an
all-zero tail. A complete frame with a bad CRC is `ErrCorruption`, including
at EOF; good-CRC frames with invalid JSON are also corruption.

`archive_nums` is additive: new checkpoint records always write an array,
including `[]` for a known-empty set. JSON decoding and version/snapshot
copies preserve that distinction. An absent field (or legacy null) on a
checkpoint with tuples means unknown; readers recover its archive set from
those tuples for compatibility.
If any live legacy checkpoint is unreadable, Open skips the entire archive
orphan sweep and logs the reason.

`Repair(dir, opts)` (CLI: `semlsm --dir DIR repair`) takes the same exclusive
LOCK as Open. It may truncate a bad-CRC complete final frame in CURRENT's
manifest or the newest WAL numbered at least the recovered `log_num`.
Earlier WALs, middle corruption, hidden later valid frames, and bad payloads
in good-CRC frames are errors. All logs and archive references are checked
before changes. Unreferenced `.sst`/`.ckpt` files and archive SSTs are moved
into `lost+found/` (`lost+found/archive/` for archives), with directory syncs;
existing quarantine names receive numeric suffixes instead of being
replaced. Quarantine is made durable before the log rollback is published,
so an interrupted quarantine leaves Open blocked by the damaged frame.
Repair reports truncated names/bytes dropped and quarantine destinations.
Open never descends into `lost+found/`.

AutoRepair is a runtime opt-in (default false): Open shares repairLocked with
Repair under the LOCK it already holds, closes recovery handles, quarantines,
truncates, and retries. With AutoRepair, the newest WAL uses Repair's strict validation.
Only ErrFinalFrameCRC is repairable; bad JSON or damage
with later valid frames remains an error. A Warn and RecoveryReport expose the
rollback, and Stats.RepairsOnOpen counts it. --auto-repair enables this in the
CLI. The trade-off is automatic loss of the damaged final edit/write instead
of an operator-approved rollback. No AutoRepair setting is persisted.


The crash simulator keeps only a prefix of unsynced bytes, and frame appends
use one Write. A complete frame with a failed CRC therefore means damage.
On real disks a non-prefix tear (for example sector reordering) also surfaces
as `ErrCorruption`; explicit Repair or opt-in AutoRepair can discard that final frame.

## Checkpoint file (.ckpt)

```
"SLCK" u32 version=1
tuple*
tuple directory JSON (framed: u32 len, u32 crc)
footer: u64 dirOffset | u32 tupleCount | u32 crc32(first 12) | "SLCKEND\0"
```

Each tuple: `u32 metaLen | meta JSON | u32 metaCRC | u32 summaryLen |
summary JSON | u32 summaryCRC | u32 indexLen | key index | u32 indexCRC`.
Summary and key index may be flate-compressed; the first byte of those
sections is the codec byte and is included in the CRC.

Tuple meta (B8):

```go
type TupleMeta struct {
    ID uint64; Group string; TsFrom, TsTo uint64
    SourceCount, SourceBytes uint64; SourceCRC32 uint32
    PutCount, DeleteCount, KeyCount uint64
    FromLevel, ToLevel int
    Model string; PromptVersion int; CreatedUnixNano int64
    PrevTupleID uint64    // previous tuple in the group (largest existing id at commit)
    ArchiveFileNum uint64 // 0 = no archive
    RetiredUnixNano int64 // JSON retired_unix_nano, omitted/0 = live summary
    ArchiveRetiredUnixNano int64 // JSON archive_retired_unix_nano, omitted/0 = not released
    InputTokens, OutputTokens uint64
    Degraded bool // always false in this version
}
```

Key index: entries sorted by key, `uvarint shared | uvarint unshared |
keyDelta | uvarint minTs | uvarint maxTs-minTs | uvarint count |
u8 lastKind`, restart every 32, trailing restart array and a bloom filter
(same construction as SST), lengths in a fixed trailer.

`SourceCRC32`: CRC over the segment's records sorted by (ts asc, key asc);
each record contributes `uvarint klen | key | u64 BE ts | u8 kind |
uvarint vlen | value`. `base.SourceCRC` computes it; the archive verify path
recomputes it from the archived records.

## Retired tuple stubs

The checkpoint version stays 1. Two optional metadata JSON fields are additive:
retired_unix_nano and archive_retired_unix_nano (int64, absent decodes as 0).
A summary-retired stub keeps all identity/count/range/predecessor metadata and
its complete encoded key index. ArchiveFileNum becomes 0. Its summary section
encodes an empty payload (u32 section length 5, codec 0 and CRC), rather than
summary JSON. Archive-only retirement preserves the original summary bytes.

Metadata and key-index CRCs remain mandatory; stub readers skip the unavailable
summary payload and its CRC. VerifyIntegrity retains I7 and tuple-chain checks,
counts stubs and archive-retired tuples, and skips their unavailable content.
ResultExpired is appended after existing ResultKind values; it carries tuple
and key-range information in CheckpointInfo with VersionTs 0. A retained range
continues to mask older raw data, preventing stale Exact answers (I10).

A retention rewrite atomically swaps affected files through AddCkpts/DelCkpts,
just like a merge. Untouched tuples remain byte-identical. Its archive_nums
array is rebuilt from surviving tuple references so released segments die when
no live version pins them. Stubs are never removed or un-retired. Unchanged
files are not rewritten. Runtime retention durations use Options.Clock and
are not stored in OPTIONS.json; automatic attempts default to a 1h interval.

## Checkpoint merge publication

Merging uses the same version-1 checkpoint format. Tuple metadata, encoded
summary and index sections, CRCs, and tuple IDs are copied byte-for-byte after
verification; directory offsets and the file directory/footer are rewritten.
The manifest publishes one edit with the new CkptFile (all tuple references
and the union of archive_nums) and DelCkpts for every input. A pure merge rewrites or deletes no archive file. Older versions retain the original files until
snapshots/readers release them. Pre-edit crash outputs are unreferenced and
swept at Open; a durable edit always keeps its output. Retention may remove payloads, but tuple range fences are never discarded.

## Summary JSON (B9)

```json
{"SchemaVersion":1,
 "Stats":{"RecordCount":0,"PutCount":0,"DeleteCount":0,"DistinctKeys":0,
          "FirstTs":0,"LastTs":0,"TopKeys":[{"Key":"","Count":0}],
          "NumericFields":[{"Name":"","Count":0,"Min":0,"Max":0,"Sum":0}],
          "ValueBytes":0},
 "Narrative":{"summary":"","key_facts":[{"text":"","refs":["r0"]}],
              "anomalies":[],"final_state":""}}
```

`Stats` is engine-computed; `Narrative` is the validated LLM part. Fact refs
(`r<i>`) are resolved to `(key, ts)` before storage. Stored per tuple as the
"summary JSON" section.

## Supporting files

- `CURRENT` — the manifest file name; `OPTIONS.json` — group settings
  (`GroupDepth`, `GroupDelimiter`, `Policies`); mismatch on reopen is
  `ErrOptionsMismatch`.
- `LOCK` — exclusive DB lock (process-death released).
- `llm/usage.json` — `{"input_tokens":N,"output_tokens":N,"calls":N}`,
  written atomically (tmp+sync+rename+syncdir).
- `llm/cache/<sha256-hex>.json` — one cached LLM response per
  `model|promptVersion|system|user`; entries carry the model and prompt
  version and are re-checked on hit; size-capped with oldest-first eviction.
- `archive/%06d.sst` (or `ArchiveDir`) — dropped segments as plain SSTs in
  internal-key order; referenced through `TupleMeta.ArchiveFileNum` and the
  checkpoint's manifest `archive_nums`. Unreferenced segments are removed
  at Open only when every live archive set is known. On checkpoint version
  death, archives are removed using the retained manifest metadata, excluding
  any still referenced by ANY live version, including snapshot-only checkpoints. Legacy checkpoints require a
  successful fallback read.
- Directories are synced (dentry fsync) after every rename/create that must
  survive a crash; the Windows build treats directory fsync as a no-op.
