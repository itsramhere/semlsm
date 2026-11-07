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
uvarint vlen, value)`. Max payload 64 MiB. Replay: a short header/payload or
CRC mismatch in the LAST WAL is a torn tail (stop and truncate); in an
earlier WAL it is `ErrCorruption`.

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
 "add_ckpts":[{"file_num":0,"size":0,
               "tuples":[{"id":0,"group":"","ts_from":0,"ts_to":0,
                          "offset":0,"length":0}]}],
 "del_ckpts":[0]}
```

Keys are `[]byte` (base64 in JSON). The first record of a new manifest is a
full snapshot. `CURRENT` holds the manifest file name (written via
tmp+sync+rename+syncdir). Roll at 4 MiB. **Torn tail**: a short or CRC-broken
final frame is ignored, like the WAL — see the caveat in ARCHITECTURE.md §8
(the last committed edit can be silently rolled back).

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
  internal-key order; referenced only through `TupleMeta.ArchiveFileNum`;
  unreferenced segments are removed at Open.
- Directories are synced (dentry fsync) after every rename/create that must
  survive a crash; the Windows build treats directory fsync as a no-op.
