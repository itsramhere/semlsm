package version

import (
	"errors"
	"sync"
	"testing"

	"semlsm/internal/base"
	"semlsm/internal/manifest"
)

func fm(level int, num uint64, smallest, largest string) manifest.FileMeta {
	return manifest.FileMeta{
		Level:    level,
		Num:      num,
		Size:     100,
		Smallest: base.MakeInternalKey([]byte(smallest), 10, base.KindPut),
		Largest:  base.MakeInternalKey([]byte(largest), 5, base.KindPut),
		MinTs:    5,
		MaxTs:    10,
		Count:    2,
	}
}

func TestVersionApplySequence(t *testing.T) {
	vs := NewVersionSet()
	v0 := vs.Acquire()
	if v0 == nil || v0.Refs() != 2 { // set's ref + ours
		t.Fatalf("initial version refs = %d, want 2", v0.Refs())
	}
	v0.Unref() // release ours; the set keeps its own

	// add L0 file
	v1, err := vs.Apply(&manifest.Edit{
		NextFileNum: 2,
		AddFiles:    []manifest.FileMeta{fm(0, 1, "a", "c")},
	})
	if err != nil {
		t.Fatal(err)
	}
	cur := vs.Acquire()
	if len(cur.Levels[0]) != 1 || cur.Levels[0][0].Num != 1 {
		t.Fatalf("v1 L0 wrong: %+v", cur.Levels[0])
	}
	cur.Unref()
	if v1 == nil {
		t.Fatal("Apply must return the new version handle for tests")
	}

	// add L1 files out of key order; must come back sorted
	_, err = vs.Apply(&manifest.Edit{
		NextFileNum: 4,
		AddFiles: []manifest.FileMeta{
			fm(1, 3, "x", "z"),
			fm(1, 2, "d", "f"),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	cur = vs.Acquire()
	if len(cur.Levels[1]) != 2 {
		t.Fatalf("v2 L1 wrong")
	}
	if base.UserKey(cur.Levels[1][0].Smallest)[0] != 'd' {
		t.Fatalf("L1 not sorted by smallest: %q", cur.Levels[1][0].Smallest)
	}
	cur.Unref()

	// L0 sorted by file number descending (newest first)
	_, err = vs.Apply(&manifest.Edit{
		NextFileNum: 6,
		AddFiles: []manifest.FileMeta{
			fm(0, 5, "aa", "ab"),
			fm(0, 4, "aa", "ab"),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	cur = vs.Acquire()
	if len(cur.Levels[0]) != 3 || cur.Levels[0][0].Num != 5 || cur.Levels[0][1].Num != 4 || cur.Levels[0][2].Num != 1 {
		t.Fatalf("L0 order wrong: %d,%d,%d", cur.Levels[0][0].Num, cur.Levels[0][1].Num, cur.Levels[0][2].Num)
	}
	cur.Unref()

	// delete file 1
	_, err = vs.Apply(&manifest.Edit{
		NextFileNum: 7,
		DelFiles:    []manifest.DelFile{{Level: 0, Num: 1}},
	})
	if err != nil {
		t.Fatal(err)
	}
	cur = vs.Acquire()
	for _, f := range cur.Levels[0] {
		if f.Num == 1 {
			t.Fatal("file 1 must be deleted")
		}
	}
	cur.Unref()

	// delete a checkpoint
	_, err = vs.Apply(&manifest.Edit{
		NextFileNum: 8,
		AddCkpts: []manifest.CkptFile{{
			FileNum: 10,
			Tuples:  []manifest.TupleRef{{ID: 1, Group: "g/", TsFrom: 1, TsTo: 2, Offset: 3, Length: 4}},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	cur = vs.Acquire()
	if len(cur.Ckpts.ByNum) != 1 {
		t.Fatalf("ckpt add failed: %+v", cur.Ckpts.ByNum)
	}
	cur.Unref()
	_, err = vs.Apply(&manifest.Edit{
		NextFileNum: 9,
		DelCkpts:    []uint64{10},
	})
	if err != nil {
		t.Fatal(err)
	}
	cur = vs.Acquire()
	if len(cur.Ckpts.ByNum) != 0 {
		t.Fatal("ckpt delete failed")
	}
	cur.Unref()
}

func TestVersionOverlapRejected(t *testing.T) {
	vs := NewVersionSet()
	_, err := vs.Apply(&manifest.Edit{
		NextFileNum: 2,
		AddFiles:    []manifest.FileMeta{fm(1, 1, "a", "f")},
	})
	if err != nil {
		t.Fatal(err)
	}
	// overlapping range on L1
	_, err = vs.Apply(&manifest.Edit{
		NextFileNum: 3,
		AddFiles:    []manifest.FileMeta{fm(1, 2, "e", "g")},
	})
	if err == nil {
		t.Fatal("overlapping L1 files must be rejected")
	}
	// touching boundaries overlap on the user key
	_, err = vs.Apply(&manifest.Edit{
		NextFileNum: 4,
		AddFiles:    []manifest.FileMeta{fm(1, 3, "f", "h")},
	})
	if err == nil {
		t.Fatal("boundary overlap must be rejected")
	}
	// L0 may overlap freely
	_, err = vs.Apply(&manifest.Edit{
		NextFileNum: 5,
		AddFiles: []manifest.FileMeta{
			fm(0, 10, "a", "z"),
			fm(0, 11, "a", "z"),
		},
	})
	if err != nil {
		t.Fatalf("L0 overlap must be allowed: %v", err)
	}
}

func TestVersionRefCountAndObsolete(t *testing.T) {
	vs := NewVersionSet()
	v1, err := vs.Apply(&manifest.Edit{
		NextFileNum: 2,
		AddFiles:    []manifest.FileMeta{fm(0, 1, "a", "c")},
	})
	if err != nil {
		t.Fatal(err)
	}
	// an old Version keeps files alive until unref (reader holds it)
	v1.Ref()

	_, err = vs.Apply(&manifest.Edit{
		NextFileNum: 3,
		DelFiles:    []manifest.DelFile{{Level: 0, Num: 1}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := vs.TakeObsolete(); len(got) != 0 {
		t.Fatalf("file 1 is still referenced by v1: obsolete = %+v", got)
	}

	v1.Unref() // drop the reader ref; Apply already dropped the set's ref
	obs := vs.TakeObsolete()
	if len(obs) != 1 || obs[0].Num != 1 {
		t.Fatalf("obsolete = %+v, want file 1", obs)
	}
	if vs.VersionCount() != 0 {
		t.Fatalf("live files = %d, want 0", vs.VersionCount())
	}
}

func TestObsoleteCheckpointRetainsArchivesAcrossVersions(t *testing.T) {
	vs := NewVersionSet()
	cf := manifest.CkptFile{FileNum: 10, ArchiveNums: []uint64{20, 21}}
	if _, err := vs.Apply(&manifest.Edit{AddCkpts: []manifest.CkptFile{cf}}); err != nil {
		t.Fatal(err)
	}
	a := vs.Acquire()
	if _, err := vs.Apply(&manifest.Edit{}); err != nil {
		t.Fatal(err)
	}
	b := vs.Acquire()
	if _, err := vs.Apply(&manifest.Edit{DelCkpts: []uint64{10}}); err != nil {
		t.Fatal(err)
	}
	if got := vs.TakeObsolete(); len(got) != 0 {
		t.Fatalf("pinned checkpoint obsolete: %+v", got)
	}
	live := vs.LiveCheckpoints()
	if len(live) != 1 || live[0].FileNum != 10 {
		t.Fatalf("snapshot-only checkpoint omitted: %+v", live)
	}
	live[0].ArchiveNums[0] = 999
	if got := vs.LiveCheckpoints(); got[0].ArchiveNums[0] != 20 {
		t.Fatal("accessor exposed mutable archive metadata")
	}
	b.Unref()
	if got := vs.TakeObsolete(); len(got) != 0 {
		t.Fatalf("older pin lost: %+v", got)
	}
	a.Unref()
	got := vs.TakeObsolete()
	if len(got) != 1 || got[0].Level != -1 || got[0].Num != 10 || got[0].Ckpt == nil || len(got[0].Ckpt.ArchiveNums) != 2 || got[0].Ckpt.ArchiveNums[1] != 21 {
		t.Fatalf("obsolete metadata: %+v", got)
	}
	if got := vs.TakeObsolete(); len(got) != 0 {
		t.Fatalf("checkpoint queued twice: %+v", got)
	}
}

func TestVersionUnchangedFilesShared(t *testing.T) {
	vs := NewVersionSet()
	v1, _ := vs.Apply(&manifest.Edit{NextFileNum: 2, AddFiles: []manifest.FileMeta{fm(0, 1, "a", "c")}})
	v2, err := vs.Apply(&manifest.Edit{NextFileNum: 3, AddFiles: []manifest.FileMeta{fm(0, 2, "x", "z")}})
	if err != nil {
		t.Fatal(err)
	}
	// file 1 instance is shared between v1 and v2 (found at different index
	// after the L0 num-descending sort)
	var shared *FileMeta
	for _, f := range v2.Levels[0] {
		if f.Num == 1 {
			shared = f
		}
	}
	if shared == nil || shared != v1.Levels[0][0] {
		t.Fatal("unchanged file instance must be shared")
	}
	if vs.VersionCount() != 2 {
		t.Fatalf("live = %d, want 2", vs.VersionCount())
	}
}

// M-010 focus: a version held across several Applies, unref in either order,
// obsolete exactly once.
func TestVersionHeldAcrossApplies(t *testing.T) {
	vs := NewVersionSet()
	v0, err := vs.Apply(&manifest.Edit{NextFileNum: 2, AddFiles: []manifest.FileMeta{fm(0, 1, "a", "c")}})
	if err != nil {
		t.Fatal(err)
	}
	v0.Ref() // reader pin

	// several applies that delete file 1 and add others
	for i := uint64(2); i <= 5; i++ {
		if _, err := vs.Apply(&manifest.Edit{
			NextFileNum: i + 1,
			AddFiles:    []manifest.FileMeta{fm(0, i, "aa", "az")},
			DelFiles:    []manifest.DelFile{{Level: 0, Num: i - 1}},
		}); err != nil {
			t.Fatal(err)
		}
	}
	// after these edits, files 1..4 are gone from the tree; the unpinned
	// intermediate versions (2,3,4) died immediately and are obsolete; file 1
	// is still pinned by v0
	if got := vs.TakeObsolete(); len(got) != 3 {
		t.Fatalf("intermediate files should be obsolete: %+v", got)
	}

	// file 5 stays live in the current version
	cur := vs.Acquire()
	if len(cur.Levels[0]) != 1 || cur.Levels[0][0].Num != 5 {
		t.Fatalf("current L0 wrong: %+v", cur.Levels[0])
	}
	cur.Unref()

	// release the pin: file 1 becomes obsolete exactly once
	v0.Unref() // reader pin; the set's ref was dropped when v0 was replaced
	obs := vs.TakeObsolete()
	if len(obs) != 1 || obs[0].Num != 1 {
		t.Fatalf("obsolete = %+v, want exactly file 1", obs)
	}
	defer func() {
		if recover() == nil {
			t.Fatal("double unref must panic")
		}
	}()
	v0.Unref() // v0 is dead: must panic
}

func TestVersionUnrefEitherOrder(t *testing.T) {
	// Two pinned versions sharing files, released in the opposite order.
	vs := NewVersionSet()
	vA, err := vs.Apply(&manifest.Edit{NextFileNum: 2, AddFiles: []manifest.FileMeta{fm(0, 1, "a", "c")}})
	if err != nil {
		t.Fatal(err)
	}
	vA.Ref()
	vB, err := vs.Apply(&manifest.Edit{NextFileNum: 3, AddFiles: []manifest.FileMeta{fm(1, 2, "d", "f")}})
	if err != nil {
		t.Fatal(err)
	}
	vB.Ref() // pinned while file 1 is already deleted from the tree

	if got := vs.TakeObsolete(); len(got) != 0 {
		t.Fatalf("both pins hold their files: %+v", got)
	}
	// release in reverse creation order. vB holds set ref + reader pin (2);
	// releasing both kills it. vA lost its set ref when replaced, so one
	// Unref kills it.
	vB.Unref()
	vB.Unref()
	if got := vs.TakeObsolete(); len(got) != 1 || got[0].Num != 2 {
		t.Fatalf("obsolete = %+v, want file 2", got)
	}
	vA.Unref()
	obs := vs.TakeObsolete()
	if len(obs) != 1 || obs[0].Num != 1 {
		t.Fatalf("obsolete = %+v, want file 1", obs)
	}
}

func TestVersionRefPanicsOnDead(t *testing.T) {
	vs := NewVersionSet()
	v, _ := vs.Apply(&manifest.Edit{NextFileNum: 2, AddFiles: []manifest.FileMeta{fm(0, 1, "a", "c")}})
	// replace v: the set's ref moves to the new version and v dies (refs 0)
	if _, err := vs.Apply(&manifest.Edit{NextFileNum: 3, AddFiles: []manifest.FileMeta{fm(0, 2, "d", "f")}}); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if recover() == nil {
			t.Fatal("Ref on dead version must panic")
		}
	}()
	v.Ref()
}

func TestVersionRejectsBadEdits(t *testing.T) {
	// M-011 item 5
	vs := NewVersionSet()
	if _, err := vs.Apply(&manifest.Edit{NextFileNum: 2, AddFiles: []manifest.FileMeta{fm(0, 1, "a", "c")}}); err != nil {
		t.Fatal(err)
	}
	// add of a live num
	if _, err := vs.Apply(&manifest.Edit{NextFileNum: 3, AddFiles: []manifest.FileMeta{fm(0, 1, "a", "c")}}); !errors.Is(err, base.ErrCorruption) {
		t.Fatalf("add of live file: err=%v", err)
	}
	// del of an unknown num
	if _, err := vs.Apply(&manifest.Edit{NextFileNum: 3, DelFiles: []manifest.DelFile{{Level: 0, Num: 99}}}); !errors.Is(err, base.ErrCorruption) {
		t.Fatalf("del of unknown file: err=%v", err)
	}
	// del with mismatched level
	if _, err := vs.Apply(&manifest.Edit{NextFileNum: 3, DelFiles: []manifest.DelFile{{Level: 1, Num: 1}}}); !errors.Is(err, base.ErrCorruption) {
		t.Fatalf("del level mismatch: err=%v", err)
	}
	// nothing changed
	cur := vs.Acquire()
	if len(cur.Levels[0]) != 1 || cur.Levels[0][0].Num != 1 {
		t.Fatalf("failed edits must change nothing: %+v", cur.Levels[0])
	}
	cur.Unref()
}

func TestVersionCountersTakeMax(t *testing.T) {
	// M-011 item 2
	vs := NewVersionSet()
	if _, err := vs.Apply(&manifest.Edit{NextFileNum: 100, LastTs: 500, LogNum: 7}); err != nil {
		t.Fatal(err)
	}
	if _, err := vs.Apply(&manifest.Edit{NextFileNum: 90, LastTs: 400, LogNum: 5}); err != nil {
		t.Fatal(err)
	}
	if vs.NextFileNum != 100 || vs.LastTs != 500 || vs.LogNum != 7 {
		t.Fatalf("counters regressed: %d/%d/%d", vs.NextFileNum, vs.LastTs, vs.LogNum)
	}
	if _, err := vs.Apply(&manifest.Edit{NextFileNum: 200, LastTs: 900, LogNum: 9}); err != nil {
		t.Fatal(err)
	}
	if vs.NextFileNum != 200 || vs.LastTs != 900 || vs.LogNum != 9 {
		t.Fatalf("counters did not advance: %d/%d/%d", vs.NextFileNum, vs.LastTs, vs.LogNum)
	}
}

// M-012 item 1: concurrent Unrefs must release exactly once. A file still
// in the current version must never become obsolete.
func TestVersionConcurrentUnrefSingleRelease(t *testing.T) {
	const iters = 50000
	for i := 0; i < iters; i++ {
		vs := NewVersionSet()
		if _, err := vs.Apply(&manifest.Edit{NextFileNum: uint64(i*10 + 2), AddFiles: []manifest.FileMeta{fm(0, uint64(i*10+1), "a", "c")}}); err != nil {
			t.Fatal(err)
		}
		v := vs.Acquire()
		v.Ref()
		if _, err := vs.Apply(&manifest.Edit{NextFileNum: uint64(i*10 + 3), AddFiles: []manifest.FileMeta{fm(0, uint64(i*10+2), "d", "f")}}); err != nil {
			t.Fatal(err)
		}
		// v now holds the only two refs (Acquire + Ref) on the old version
		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)
		for g := 0; g < 2; g++ {
			go func() {
				defer wg.Done()
				<-start
				v.Unref()
			}()
		}
		close(start)
		wg.Wait()
		if got := vs.TakeObsolete(); len(got) != 0 {
			t.Fatalf("iter %d: live file marked obsolete: %+v", i, got)
		}
	}
}

// M-015 item 4: Validate runs Apply's checks without changing anything.
func TestValidateDryRun(t *testing.T) {
	vs := NewVersionSet()
	e1 := &manifest.Edit{AddFiles: []manifest.FileMeta{
		{Level: 0, Num: 1, Smallest: base.MakeInternalKey([]byte("a"), 5, base.KindPut), Largest: base.MakeInternalKey([]byte("m"), 5, base.KindPut), MinTs: 5, MaxTs: 5, Count: 1},
	}}
	if _, err := vs.Apply(e1); err != nil {
		t.Fatal(err)
	}
	// an L1 file so the overlap case has something to overlap
	eL1 := &manifest.Edit{AddFiles: []manifest.FileMeta{
		{Level: 1, Num: 4, Smallest: base.MakeInternalKey([]byte("c"), 6, base.KindPut), Largest: base.MakeInternalKey([]byte("g"), 6, base.KindPut), MinTs: 6, MaxTs: 6, Count: 1},
	}}
	if _, err := vs.Apply(eL1); err != nil {
		t.Fatal(err)
	}

	// good edit validates
	e2 := &manifest.Edit{AddFiles: []manifest.FileMeta{
		{Level: 0, Num: 2, Smallest: base.MakeInternalKey([]byte("n"), 6, base.KindPut), Largest: base.MakeInternalKey([]byte("z"), 6, base.KindPut), MinTs: 6, MaxTs: 6, Count: 1},
	}}
	if err := vs.Validate(e2); err != nil {
		t.Fatalf("valid edit rejected: %v", err)
	}
	if n := vs.VersionCount(); n != 2 {
		t.Fatalf("Validate changed the set: %d live files", n)
	}

	// every rejected shape fails Validate too
	bad := []struct {
		name string
		e    *manifest.Edit
	}{
		{"del unknown", &manifest.Edit{DelFiles: []manifest.DelFile{{Level: 0, Num: 99}}}},
		{"del level mismatch", &manifest.Edit{DelFiles: []manifest.DelFile{{Level: 1, Num: 1}}}},
		{"add of live", &manifest.Edit{AddFiles: []manifest.FileMeta{
			{Level: 0, Num: 1, Smallest: base.MakeInternalKey([]byte("a"), 5, base.KindPut), Largest: base.MakeInternalKey([]byte("m"), 5, base.KindPut)},
		}}},
		{"L1 overlap", &manifest.Edit{AddFiles: []manifest.FileMeta{
			{Level: 1, Num: 3, Smallest: base.MakeInternalKey([]byte("b"), 7, base.KindPut), Largest: base.MakeInternalKey([]byte("z"), 7, base.KindPut), MinTs: 7, MaxTs: 7, Count: 1},
		}}},
	}
	for _, tc := range bad {
		if err := vs.Validate(tc.e); err == nil {
			t.Fatalf("%s: Validate accepted", tc.name)
		}
		if n := vs.VersionCount(); n != 2 {
			t.Fatalf("%s: Validate changed the set", tc.name)
		}
	}
	// after all the rejected validates, the good edit still applies
	if _, err := vs.Apply(e2); err != nil {
		t.Fatalf("apply after validates: %v", err)
	}
}
