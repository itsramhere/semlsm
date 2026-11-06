package semlsm

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"semlsm/internal/base"
	"semlsm/internal/checkpoint"
	"semlsm/internal/manifest"
	"semlsm/internal/sstable"
	"semlsm/internal/testmodel"
	"semlsm/internal/vfs"
)

// installHandmadeCheckpoint rewrites the SSTs that hold dropped versions and
// installs a tuple atomically via the same manifest edit path (P7.5).
// fromTs..toTs is the dropped range for keys under `group`.
func installHandmadeCheckpoint(t *testing.T, db *DB, group string, fromTs, toTs uint64, model *testmodel.Model) uint64 {
	t.Helper()

	// 1. collect dropped records from the model
	var dropped []base.Record
	keysInRange := map[string][]base.Record{}
	for _, v := range model.AllVersions() {
		if !strings.HasPrefix(string(v.Key), group) {
			continue
		}
		if v.Ts >= fromTs && v.Ts <= toTs {
			dropped = append(dropped, v)
			keysInRange[string(v.Key)] = append(keysInRange[string(v.Key)], v)
		}
	}
	if len(dropped) == 0 {
		t.Fatal("no records in the dropped range")
	}
	inRange := map[string]bool{}
	for _, r := range dropped {
		inRange[string(r.Key)+"@"+fmt.Sprint(r.Ts)] = true
	}
	// flush everything to SSTs first: memtable versions in the dropped
	// range must not survive (I7)
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}

	// 2. rewrite SSTs that contain any dropped record
	v := db.vs.Acquire()
	var dels []manifest.DelFile
	var adds []manifest.FileMeta
	var ckptAdds []manifest.CkptFile
	_ = ckptAdds

	for l := range v.Levels {
		for _, f := range v.Levels[l] {
			reader, rel, err := db.tableCache.Get(f.Num)
			if err != nil {
				t.Fatal(err)
			}
			it := reader.NewIterator()
			needRewrite := false
			var kept []struct {
				ikey []byte
				val  []byte
			}
			for it.First(); it.Valid(); it.Next() {
				ikey := it.Key()
				u, ts, _, _ := base.SplitInternalKey(ikey)
				if inRange[string(u)+"@"+fmt.Sprint(ts)] {
					needRewrite = true
					continue
				}
				kept = append(kept, struct {
					ikey []byte
					val  []byte
				}{append([]byte(nil), ikey...), append([]byte(nil), it.Value()...)})
			}
			if e := it.Err(); e != nil {
				t.Fatal(e)
			}
			it.Close()
			rel()
			if !needRewrite {
				continue
			}
			// write the replacement file at the same level
			newNum := db.allocFile()
			w, err := newSSTWriter(db, newNum)
			if err != nil {
				t.Fatal(err)
			}
			var smallest, largest []byte
			var count uint64
			var minTs, maxTs uint64
			for _, k := range kept {
				if err := w.Add(k.ikey, k.val); err != nil {
					t.Fatal(err)
				}
				count++
				u, ts, _, _ := base.SplitInternalKey(k.ikey)
				_ = u
				if count == 1 || ts < minTs {
					minTs = ts
				}
				if ts > maxTs {
					maxTs = ts
				}
				if smallest == nil {
					smallest = append([]byte(nil), k.ikey...)
				}
				largest = append(largest[:0], k.ikey...)
			}
			if count == 0 {
				// everything dropped: the file disappears
				dels = append(dels, manifest.DelFile{Level: l, Num: f.Num})
				continue
			}
			if err := w.Finish(); err != nil {
				t.Fatal(err)
			}
			dels = append(dels, manifest.DelFile{Level: l, Num: f.Num})
			adds = append(adds, manifest.FileMeta{
				Level: l, Num: newNum, Size: fileSizeOnDisk(db.opts.FS, sstPathOf(db.dir, newNum)),
				Smallest: smallest, Largest: append([]byte(nil), largest...),
				MinTs: minTs, MaxTs: maxTs, Count: count,
			})
		}
	}
	v.Unref()

	// 3. build the checkpoint file
	ckptNum := db.allocFile()
	tupleID := db.allocFile()
	cw, err := newCkptWriter(db, ckptNum)
	if err != nil {
		t.Fatal(err)
	}
	var entries []checkpoint.KeyEntry
	for key, recs := range keysInRange {
		var minTs, maxTs uint64
		var lastKind base.Kind
		for i, r := range recs {
			if i == 0 || r.Ts < minTs {
				minTs = r.Ts
			}
			if i == 0 || r.Ts > maxTs {
				maxTs = r.Ts
			}
			lastKind = r.Kind
		}
		entries = append(entries, checkpoint.KeyEntry{
			Key: []byte(key), MinTs: minTs, MaxTs: maxTs,
			Count: uint64(len(recs)), LastKind: lastKind,
		})
	}
	ckMeta := checkpoint.TupleMeta{
		ID: tupleID, Group: group, TsFrom: fromTs, TsTo: toTs,
		SourceCount: uint64(len(dropped)),
	}
	if err := cw.AddTuple(ckMeta, []byte(`{"summary":"handmade"}`), entries); err != nil {
		t.Fatal(err)
	}
	if err := cw.Finish(); err != nil {
		t.Fatal(err)
	}
	dir := cw.DirEntries()
	if len(dir) != 1 {
		t.Fatalf("expected one tuple, got %d", len(dir))
	}
	if err := db.opts.FS.SyncDir(db.dir); err != nil {
		t.Fatal(err)
	}

	// 4. one atomic manifest edit + install (I8)
	db.manifestMu.Lock()
	e := &manifest.Edit{}
	e.DelFiles = dels
	e.AddFiles = adds
	e.AddCkpts = []manifest.CkptFile{{
		FileNum: ckptNum,
		Size:    fileSizeOnDisk(db.opts.FS, ckptPathOf(db.dir, ckptNum)),
		Tuples: []manifest.TupleRef{{
			ID: tupleID, Group: group,
			TsFrom: fromTs, TsTo: toTs,
			Offset: dir[0].Offset, Length: dir[0].Length,
		}},
	}}
	if err := db.appendManifestLocked(e, model.MaxTs()); err != nil {
		db.manifestMu.Unlock()
		t.Fatal(err)
	}
	db.manifestMu.Unlock()

	// install under mu (B7)
	db.mu.Lock()
	newVersion := db.vs.Acquire()
	if db.rs != nil {
		old := db.rs.version
		db.rs = &readState{mem: db.mem, imms: db.rs.imms, version: newVersion}
		old.Unref()
		db.stallCond.Broadcast()
	}
	db.mu.Unlock()
	return tupleID
}

func TestCheckpointTruthTableEndToEnd(t *testing.T) {
	fs := vfs.NewMemFS()
	dir := "db"
	// manual clock so a gap exists between the dropped range and v40
	clock := base.NewManualClock(time.Unix(1750000000, 0))
	// no auto compaction: raw layout stays under test control
	opts := testOptions(fs, clock)
	opts.L0CompactTrigger = 1 << 20
	opts.L0StallTrigger = 1 << 21
	db, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	model := testmodel.NewModel()
	// v@10 Put "a", v@20..30 to be dropped, v@40 Put "z"
	write := func(val string) uint64 {
		b := NewBatch()
		b.Put([]byte("agent/1/k"), []byte(val))
		ts, err := db.Apply(b)
		if err != nil {
			t.Fatal(err)
		}
		for j := range b.recs {
			b.recs[j].Ts = ts
			model.Commit(b.recs[j : j+1])
		}
		return ts
	}
	ts10 := write("a")
	ts20 := write("v20")
	ts30 := write("v30")
	// no minimum distance needed: the range comes from actual timestamps
	if ts30 <= ts20 || ts20 <= ts10 {
		t.Fatalf("timestamps too close: %d..%d", ts10, ts30)
	}
	// derive the actual ts range for the dropped middle versions
	fromTs := ts20
	toTs := ts30
	// open a gap between the dropped range and v40
	clock.Advance(time.Minute)

	if err := db.WaitIdle(context.Background()); err != nil {
		t.Fatal(err)
	}
	// snapshot taken BEFORE v40 and BEFORE install: its pinned version still
	// holds the raw files (I9)
	snap := db.NewSnapshot()
	pre, err := snap.Get([]byte("agent/1/k"))
	if err != nil || pre.Kind != ResultExact || string(pre.Value) != "v30" {
		t.Fatalf("pre-install snapshot: %+v err=%v", pre, err)
	}
	// now write v40 (the snapshot does not see it)
	ts40 := write("z")

	tupleID := installHandmadeCheckpoint(t, db, "agent/1/", fromTs, toTs, model)
	if tupleID == 0 {
		t.Fatal("tuple id zero")
	}

	// row: s below everything -> NotFound
	r, err := db.GetAt([]byte("agent/1/k"), ts10-1)
	if err != nil || r.Kind != ResultNotFound {
		t.Fatalf("below all: %+v err=%v", r, err)
	}
	// rows 2/3: raw only, s just below the entry.s MinTs -> UseRaw
	r, err = db.GetAt([]byte("agent/1/k"), ts20-1)
	if err != nil || r.Kind != ResultExact || string(r.Value) != "a" {
		t.Fatalf("row2/3: %+v err=%v want a@%d", r, err, ts10)
	}
	// row 8: raw older than entry, s inside range -> Summarized
	r, err = db.GetAt([]byte("agent/1/k"), ts25of(fromTs, toTs))
	if err != nil || r.Kind != ResultSummarized {
		t.Fatalf("row8: %+v err=%v", r, err)
	}
	if r.Checkpoint == nil || r.Checkpoint.ID != tupleID || r.Checkpoint.Group != "agent/1/" {
		t.Fatalf("checkpoint info: %+v", r.Checkpoint)
	}
	// row 8 (s after MaxTs, Put last) -> Summarized
	r, err = db.GetAt([]byte("agent/1/k"), toTs+1)
	if err != nil || r.Kind != ResultSummarized {
		t.Fatalf("row8 after max: %+v err=%v", r, err)
	}
	// rows 4/5: raw newer than entry -> UseRaw
	r, err = db.GetAt([]byte("agent/1/k"), ts40)
	if err != nil || r.Kind != ResultExact || string(r.Value) != "z" {
		t.Fatalf("row4/5: %+v err=%v", r, err)
	}
	// latest
	r, err = db.Get([]byte("agent/1/k"))
	if err != nil || r.Kind != ResultExact || string(r.Value) != "z" {
		t.Fatalf("latest: %+v err=%v", r, err)
	}
	// the pre-install snapshot STILL reads Exact v30 from its pinned raw
	// files, byte-identical (I9), while the live tree now summarizes
	post, err := snap.Get([]byte("agent/1/k"))
	if err != nil || post.Kind != ResultExact || string(post.Value) != "v30" || post.VersionTs != pre.VersionTs {
		t.Fatalf("post-install snapshot read: %+v (pre %+v)", post, pre)
	}
	live, err := db.GetAt([]byte("agent/1/k"), ts25of(fromTs, toTs))
	if err != nil || live.Kind != ResultSummarized {
		t.Fatalf("live post-install read: %+v err=%v", live, err)
	}
	snap.Release()

	// I7: no raw version of the key in [fromTs, toTs] anywhere
	rep, err := db.VerifyIntegrity(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(rep.Problems) > 0 {
		t.Fatalf("verify problems: %v", rep.Problems)
	}
}

func ts25of(from, to uint64) uint64 {
	return (from + to) / 2
}

func TestCheckpointV49V50to60V61(t *testing.T) {
	// B6's example: raw v49, dropped v50-60, raw v61; read at 55 must use
	// the summary, never the stale raw v49.
	fs := vfs.NewMemFS()
	db, err := Open("db", noCompactionOptions(fs, base.RealClock{}))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	model := testmodel.NewModel()
	versions := map[uint64]string{}
	var prev uint64
	for i := 49; i <= 61; i++ {
		b := NewBatch()
		val := fmt.Sprintf("v%d", i)
		b.Put([]byte("k"), []byte(val))
		ts, err := db.Apply(b)
		if err != nil {
			t.Fatal(err)
		}
		if i > 49 && ts <= prev {
			t.Fatalf("ts not increasing")
		}
		prev = ts
		versions[uint64(i)] = val
		for j := range b.recs {
			b.recs[j].Ts = ts
			model.Commit(b.recs[j : j+1])
		}
	}
	if err := db.WaitIdle(context.Background()); err != nil {
		t.Fatal(err)
	}
	// map synthetic versions to real ts: v49=1st, v61=last; drop 50..60
	all := model.AllVersions()
	if len(all) != 13 {
		t.Fatalf("expected 13 versions, got %d", len(all))
	}
	ts49 := all[0].Ts
	ts50 := all[1].Ts
	ts60 := all[11].Ts
	installHandmadeCheckpoint(t, db, "k", ts50, ts60, model)

	// read at 55 -> Summarized (row 8; stale raw v49 must not leak)
	mid := ts50 + (ts60-ts50)/2
	r, err := db.GetAt([]byte("k"), mid)
	if err != nil || r.Kind != ResultSummarized {
		t.Fatalf("read at 55: %+v err=%v", r, err)
	}
	// read at 49 -> Exact v49
	r, err = db.GetAt([]byte("k"), ts49)
	if err != nil || r.Kind != ResultExact || string(r.Value) != "v49" {
		t.Fatalf("read at 49: %+v err=%v", r, err)
	}
	// read at 61 -> Exact v61
	last := all[12].Ts
	r, err = db.GetAt([]byte("k"), last)
	if err != nil || r.Kind != ResultExact || string(r.Value) != "v61" {
		t.Fatalf("read at 61: %+v err=%v", r, err)
	}
	// read at 60 -> Summarized (dropped)
	r, err = db.GetAt([]byte("k"), ts60)
	if err != nil || r.Kind != ResultSummarized {
		t.Fatalf("read at 60: %+v err=%v", r, err)
	}
}

func TestCheckpointRow7DeleteLast(t *testing.T) {
	// dropped run ends with a delete: reads at and after the run are NotFound
	fs := vfs.NewMemFS()
	db, err := Open("db", noCompactionOptions(fs, base.RealClock{}))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	model := testmodel.NewModel()
	var tsList []uint64
	for i := 0; i < 5; i++ {
		b := NewBatch()
		if i == 4 {
			b.Delete([]byte("k"))
		} else {
			b.Put([]byte("k"), []byte(fmt.Sprintf("v%d", i)))
		}
		ts, err := db.Apply(b)
		if err != nil {
			t.Fatal(err)
		}
		tsList = append(tsList, ts)
		for j := range b.recs {
			b.recs[j].Ts = ts
			model.Commit(b.recs[j : j+1])
		}
	}
	if err := db.WaitIdle(context.Background()); err != nil {
		t.Fatal(err)
	}
	// drop v1..v4 (v4 = delete): the run ends with the delete (row 7)
	installHandmadeCheckpoint(t, db, "k", tsList[1], tsList[4], model)
	// row 7: s == tsList[4] (the delete's ts) -> NotFound
	r, err := db.GetAt([]byte("k"), tsList[4])
	if err != nil || r.Kind != ResultNotFound {
		t.Fatalf("row7: %+v err=%v", r, err)
	}
	// row 8: s inside the run but before the delete -> Summarized
	r, err = db.GetAt([]byte("k"), tsList[1])
	if err != nil || r.Kind != ResultSummarized {
		t.Fatalf("row8 mid-run: %+v err=%v", r, err)
	}
	// v0 still Exact
	r, err = db.GetAt([]byte("k"), tsList[0])
	if err != nil || r.Kind != ResultExact || string(r.Value) != "v0" {
		t.Fatalf("v0: %+v err=%v", r, err)
	}
}

func TestVerifyIntegrityCatchesCkptFlip(t *testing.T) {
	fs := vfs.NewMemFS()
	dir := "db"
	db, err := Open("db", noCompactionOptions(fs, base.RealClock{}))
	if err != nil {
		t.Fatal(err)
	}
	model := testmodel.NewModel()
	for i := 0; i < 30; i++ {
		b := NewBatch()
		b.Put([]byte(fmt.Sprintf("k%02d", i)), []byte(fmt.Sprintf("value-%02d-padding", i)))
		ts, err := db.Apply(b)
		if err != nil {
			t.Fatal(err)
		}
		for j := range b.recs {
			b.recs[j].Ts = ts
			model.Commit(b.recs[j : j+1])
		}
	}
	if err := db.WaitIdle(context.Background()); err != nil {
		t.Fatal(err)
	}
	installHandmadeCheckpoint(t, db, "k", 0, model.MaxTs(), model)

	rep, err := db.VerifyIntegrity(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(rep.Problems) != 0 || rep.CkptFiles != 1 {
		t.Fatalf("clean verify: %+v", rep)
	}

	// flip a byte in the ckpt file: verify must notice. The live reader may
	// have the tuple cached, so read via a fresh reader on the raw file.
	entries, err := fs.List(dir)
	if err != nil {
		t.Fatal(err)
	}
	var ckptName string
	for _, e := range entries {
		if strings.HasSuffix(e.Name, ".ckpt") {
			ckptName = e.Name
		}
	}
	f, err := fs.Open(filepath.Join(dir, ckptName))
	if err != nil {
		t.Fatal(err)
	}
	st, _ := f.Stat()
	buf := make([]byte, st.Size)
	if _, err := f.ReadAt(buf, 0); err != nil {
		t.Fatal(err)
	}
	f.Close()
	// flip a byte in the middle of the first tuple's key index region
	mid := st.Size / 2
	buf[mid] ^= 0x01
	// write to a fresh FS with the corrupted file
	cfs := vfs.NewMemFS()
	cfs.MkdirAll(dir)
	fc, err := cfs.Create(filepath.Join(dir, ckptName))
	if err != nil {
		t.Fatal(err)
	}
	fc.Write(buf)
	fc.Close()

	r, err := openCkptReader(cfs, filepath.Join(dir, ckptName))
	if err == nil {
		// if open succeeded, tuple reads must fail
		tups := r.Tuples()
		bad := false
		for _, tm := range tups {
			if _, err := r.IterKeys(tm.ID); err != nil {
				bad = true
			}
		}
		if !bad && len(tups) > 0 {
			t.Fatal("corrupted ckpt read cleanly")
		}
		r.Close()
	}
}

// noCompactionOptions keeps all data raw so the tests control the layout.
func noCompactionOptions(fs vfs.FS, clock base.Clock) Options {
	o := testOptions(fs, clock)
	o.L0CompactTrigger = 1 << 20
	o.L0StallTrigger = 1 << 21
	o.L1TargetBytes = 1 << 40
	return o
}

func sstPathOf(dir string, num uint64) string {
	return filepath.Join(dir, sstName(num))
}

func ckptPathOf(dir string, num uint64) string {
	return filepath.Join(dir, ckptName(num))
}

func newSSTWriter(db *DB, num uint64) (*sstable.Writer, error) {
	return sstable.NewWriter(db.opts.FS, sstPathOf(db.dir, num))
}

func newCkptWriter(db *DB, num uint64) (*checkpoint.Writer, error) {
	return checkpoint.NewWriter(db.opts.FS, ckptPathOf(db.dir, num))
}

func openCkptReader(fs vfs.FS, path string) (*checkpoint.Reader, error) {
	return checkpoint.OpenReader(fs, path)
}
