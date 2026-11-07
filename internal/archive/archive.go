// Package archive stores a dropped segment's raw records in a standard SST
// under <dir>/<num>.sst (P12.1). Archives live outside the manifest: they
// are written before the semantic job's manifest edit and unreferenced ones
// are removed at startup. A different volume needs no special handling —
// the segment is created in place (never renamed across volumes).
package archive

import (
	"fmt"
	"path/filepath"
	"sort"

	"semlsm/internal/base"
	"semlsm/internal/sstable"
	"semlsm/internal/vfs"
)

// Write writes one tuple's dropped records to <dir>/<num>.sst and verifies
// the segment by reading it back and recomputing SourceCRC32 (P10.1 step 9).
// The file number was allocated by the caller; sourceCount and sourceCRC
// come from the tuple meta being written. One segment is one file: no
// cutting, so the sstable writer is used directly.
func Write(fs vfs.FS, dir string, num uint64, records []base.Record, sourceCount uint64, sourceCRC uint32) error {
	w, err := sstable.NewWriter(fs, Path(dir, num))
	if err != nil {
		return fmt.Errorf("archive: segment %d: %w", num, err)
	}
	// The SST requires internal-key order (key asc, ts desc, I3); the
	// segment arrives (ts asc, key asc), so reorder for the archive.
	ordered := append([]base.Record(nil), records...)
	sort.SliceStable(ordered, func(a, b int) bool {
		if c := base.CompareUser(ordered[a].Key, ordered[b].Key); c != 0 {
			return c < 0
		}
		return ordered[a].Ts > ordered[b].Ts
	})
	for _, r := range ordered {
		if err := w.Add(base.MakeInternalKey(r.Key, r.Ts, r.Kind), r.Value); err != nil {
			w.Abort()
			return fmt.Errorf("archive: segment %d: %w", num, err)
		}
	}
	if err := w.Finish(); err != nil {
		w.Abort()
		return fmt.Errorf("archive: segment %d finish: %w", num, err)
	}
	// Verify: read back, order by (ts asc, key asc), recompute the CRC.
	r, err := Open(fs, Path(dir, num))
	if err != nil {
		return fmt.Errorf("archive: segment %d verify open: %w", num, err)
	}
	defer r.Close()
	var back []base.Record
	it := r.NewIterator()
	for it.First(); it.Valid(); it.Next() {
		u, ts, kind, ok := base.SplitInternalKey(it.Key())
		if !ok {
			it.Close()
			return fmt.Errorf("archive: segment %d verify: bad internal key", num)
		}
		// Copy: the iterator's buffers are only valid until the next Next.
		back = append(back, base.Record{
			Key:   append([]byte(nil), u...),
			Ts:    ts,
			Kind:  kind,
			Value: append([]byte(nil), it.Value()...),
		})
	}
	if e := it.Err(); e != nil {
		it.Close()
		return fmt.Errorf("archive: segment %d verify read: %w", num, e)
	}
	it.Close()
	if uint64(len(back)) != sourceCount {
		return fmt.Errorf("archive: segment %d verify: %d records, want %d", num, len(back), sourceCount)
	}
	sort.Slice(back, func(a, b int) bool {
		if back[a].Ts != back[b].Ts {
			return back[a].Ts < back[b].Ts
		}
		return string(back[a].Key) < string(back[b].Key)
	})
	if crc := base.SourceCRC(back); crc != sourceCRC {
		return fmt.Errorf("archive: segment %d verify: CRC %08x, want %08x", num, crc, sourceCRC)
	}
	return nil
}

// Path returns the archive file path for a segment number.
func Path(dir string, num uint64) string { return filepath.Join(dir, sstable.Name(num)) }

// Open opens one archived segment for reading (P12.2).
func Open(fs vfs.FS, path string) (*sstable.Reader, error) {
	return sstable.OpenReader(fs, path)
}

// Lookup returns the newest version of key with ts <= s in one archived
// segment (P12.2). The archive holds exactly the tuple's dropped run, so
// "in the run" needs no extra bound.
func Lookup(r *sstable.Reader, key []byte, s uint64) (base.Record, bool, error) {
	ts, kind, value, ok, err := r.Get(key, s)
	if err != nil {
		return base.Record{}, false, err
	}
	if !ok {
		return base.Record{}, false, nil
	}
	return base.Record{Key: key, Ts: ts, Kind: kind, Value: value}, true, nil
}
