package semlsm

import (
	"encoding/json"
	"errors"

	"semlsm/internal/base"
	"semlsm/internal/checkpoint"
	"semlsm/internal/manifest"
	"semlsm/internal/resolve"
	"semlsm/internal/version"
)

// acquireReadState returns the current read state with its version pinned,
// plus the visible timestamp (B7: acquire under mu).
func (db *DB) acquireReadState() (*readState, uint64) {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.rs == nil {
		return nil, 0
	}
	db.rs.version.Ref()
	return db.rs, db.visibleTs
}

func releaseReadState(rs *readState) {
	rs.release()
}

// Result is the answer to a point read (B5).
type Result struct {
	Kind       ResultKind
	Key        []byte
	Value      []byte          // set only for ResultExact
	VersionTs  uint64          // Exact: the version's ts; Summarized: 0
	Checkpoint *CheckpointInfo // set only for ResultSummarized
}

// ResultKind classifies a read result (B5).
type ResultKind uint8

const (
	// ResultNotFound: no data visible at the read timestamp.
	ResultNotFound ResultKind = iota
	// ResultExact: an exact raw version.
	ResultExact
	// ResultSummarized: covered by a checkpoint tuple.
	ResultSummarized
)

// CheckpointInfo describes the tuple covering a summarized read (B5).
type CheckpointInfo struct {
	ID               uint64
	Group            string
	TsFrom, TsTo     uint64
	SourceCount      uint64
	SourceBytes      uint64
	SourceCRC32      uint32
	KeyMinTs         uint64
	KeyMaxTs         uint64
	KeyCount         uint64
	Summary          Summary
	ArchiveAvailable bool
}

// Get returns the latest visible version of key.
func (db *DB) Get(key []byte) (Result, error) {
	if err := base.ValidateKV(key, nil); err != nil {
		return Result{}, err
	}
	rs, s := db.acquireReadState()
	if rs == nil {
		return Result{}, ErrClosed
	}
	defer releaseReadState(rs)
	return db.getAtState(key, s, rs)
}

// GetAt returns the version of key visible at timestamp s. A timestamp newer
// than the last visible commit returns ErrFutureTimestamp.
func (db *DB) GetAt(key []byte, s uint64) (Result, error) {
	if err := base.ValidateKV(key, nil); err != nil {
		return Result{}, err
	}
	rs, visible := db.acquireReadState()
	if rs == nil {
		return Result{}, ErrClosed
	}
	defer releaseReadState(rs)
	if s > visible {
		return Result{}, ErrFutureTimestamp
	}
	return db.getAtState(key, s, rs)
}

// getAtState resolves one read against a pinned read state (B6).
func (db *DB) getAtState(key []byte, s uint64, rs *readState) (Result, error) {
	raw, value := db.rawLookup(key, s, rs)
	entry, hit := db.checkpointLookup(key, s, rs)
	outcome := resolve.Resolve(s, raw, entry)
	switch outcome {
	case resolve.NotFound:
		if hit != nil {
			hit.rel()
		}
		return Result{Kind: ResultNotFound, Key: copyBytes(key)}, nil
	case resolve.UseRaw:
		if hit != nil {
			hit.rel()
		}
		return Result{
			Kind:      ResultExact,
			Key:       copyBytes(key),
			Value:     copyBytes(value),
			VersionTs: raw.Ts,
		}, nil
	case resolve.UseSummary:
		info, err := db.checkpointInfoFor(key, entry, hit)
		if err != nil {
			return Result{}, err
		}
		return Result{
			Kind:       ResultSummarized,
			Key:        copyBytes(key),
			Checkpoint: info,
		}, nil
	default:
		if hit != nil {
			hit.rel()
		}
		return Result{}, resolve.ErrInvariant(key, s, entry)
	}
}

// rawLookup finds the newest raw version of key with ts <= s (B6 lookup)
// together with its value. The value aliases engine buffers.
func (db *DB) rawLookup(key []byte, s uint64, rs *readState) (resolve.Raw, []byte) {
	// 1. active memtable, then immutable memtables newest to oldest
	if ts, kind, value, ok := rs.mem.Get(key, s); ok {
		return resolve.Raw{Found: true, Ts: ts, Kind: kind}, value
	}
	for _, im := range rs.imms {
		if ts, kind, value, ok := im.mem.Get(key, s); ok {
			return resolve.Raw{Found: true, Ts: ts, Kind: kind}, value
		}
	}
	// 2. L0 files in descending file number (already sorted newest first)
	v := rs.version
	for _, f := range v.Levels[0] {
		if f.MinTs > s {
			continue // every version in the file is newer than s
		}
		if !fileMayContain(f, key) {
			continue
		}
		if ts, kind, value, ok := db.sstGet(f.Num, key, s); ok {
			return resolve.Raw{Found: true, Ts: ts, Kind: kind}, value
		}
	}
	// 3. L1..L4: the one file whose range may contain the key
	for l := 1; l < base.NumLevels; l++ {
		for _, f := range v.Levels[l] {
			if f.MinTs > s {
				continue
			}
			if !fileMayContain(f, key) {
				continue
			}
			if ts, kind, value, ok := db.sstGet(f.Num, key, s); ok {
				return resolve.Raw{Found: true, Ts: ts, Kind: kind}, value
			}
		}
	}
	return resolve.Raw{}, nil
}

// sstGet reads one file through the table cache.
func (db *DB) sstGet(num uint64, key []byte, s uint64) (ts uint64, kind base.Kind, value []byte, ok bool) {
	r, rel, err := db.tableCache.Get(num)
	if err != nil {
		return 0, 0, nil, false
	}
	defer rel()
	ts, kind, value, ok, err = r.Get(key, s)
	if err != nil {
		return 0, 0, nil, false
	}
	return ts, kind, value, ok
}

// fileMayContain checks the user-key range of a file's metadata.
func fileMayContain(f *version.FileMeta, key []byte) bool {
	smallest := base.UserKey(f.Smallest)
	largest := base.UserKey(f.Largest)
	return base.CompareUser(smallest, key) <= 0 && base.CompareUser(largest, key) >= 0
}

// ckptHit carries the covering tuple ref and reader for lazy summary loads.
type ckptHit struct {
	ref    manifest.TupleRef
	reader *checkpoint.Reader
	rel    func()
}

// checkpointLookup implements P7.3: for (K, S), walk the group's tuple refs
// with TsFrom <= S, checking each tuple's key index; keep the entry with the
// largest MinTs <= S.
func (db *DB) checkpointLookup(key []byte, s uint64, rs *readState) (resolve.Entry, *ckptHit) {
	group := string(base.GroupOf(key, db.opts.GroupDepth, db.opts.GroupDelimiter))
	refs := rs.version.Ckpts.GroupRefs(group)
	if len(refs) == 0 {
		return resolve.Entry{}, nil
	}
	var best resolve.Entry
	var hit *ckptHit
	for _, loc := range refs {
		ref := loc.Ref
		if ref.TsFrom > s {
			break // sorted by TsFrom
		}
		r, rel, err := db.ckptCache.Get(loc.FileNum)
		if err != nil {
			continue
		}
		ke, ok, lerr := r.LookupKey(ref.ID, key)
		if lerr != nil || !ok {
			rel()
			continue
		}
		// B6: only entries with MinTs <= S are eligible; keep the one with
		// the largest MinTs.
		if ke.MinTs <= s && (!best.Found || ke.MinTs >= best.MinTs) {
			if hit != nil {
				hit.rel()
			}
			best = resolve.Entry{
				Found:    true,
				MinTs:    ke.MinTs,
				MaxTs:    ke.MaxTs,
				Count:    ke.Count,
				LastKind: ke.LastKind,
			}
			hit = &ckptHit{ref: ref, reader: r, rel: rel}
			continue
		}
		rel()
	}
	return best, hit
}

// checkpointEntry returns the covering checkpoint entry for (key, s).
func (db *DB) checkpointEntry(key []byte, s uint64, rs *readState) resolve.Entry {
	e, hit := db.checkpointLookup(key, s, rs)
	if hit != nil {
		hit.rel()
	}
	return e
}

// checkpointInfoFor builds the CheckpointInfo for a summarized read (P7.4):
// the summary is read only when the outcome is UseSummary.
func (db *DB) checkpointInfoFor(key []byte, entry resolve.Entry, hit *ckptHit) (*CheckpointInfo, error) {
	if hit == nil {
		return nil, errors.New("semlsm: no covering checkpoint")
	}
	meta, err := hit.reader.ReadMeta(hit.ref.ID)
	if err != nil {
		hit.rel()
		return nil, err
	}
	info := &CheckpointInfo{
		ID:               meta.ID,
		Group:            meta.Group,
		TsFrom:           meta.TsFrom,
		TsTo:             meta.TsTo,
		SourceCount:      meta.SourceCount,
		SourceBytes:      meta.SourceBytes,
		SourceCRC32:      meta.SourceCRC32,
		KeyMinTs:         entry.MinTs,
		KeyMaxTs:         entry.MaxTs,
		KeyCount:         entry.Count,
		ArchiveAvailable: meta.ArchiveFileNum != 0,
	}
	if sumJSON, err := hit.reader.ReadSummary(hit.ref.ID); err == nil && len(sumJSON) > 0 {
		_ = json.Unmarshal(sumJSON, &info.Summary)
	}
	hit.rel()
	return info, nil
}

func copyBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	out := make([]byte, len(b))
	copy(out, b)
	return out
}
