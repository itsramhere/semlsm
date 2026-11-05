package version

import (
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
	v0 := vs.Current()
	if v0 == nil || v0.Refs() != 1 {
		t.Fatal("initial version must exist with 1 ref")
	}

	// add L0 file
	v1, err := vs.Apply(&manifest.Edit{
		NextFileNum: 2,
		AddFiles:    []manifest.FileMeta{fm(0, 1, "a", "c")},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(v1.Levels[0]) != 1 || v1.Levels[0][0].Num != 1 {
		t.Fatalf("v1 L0 wrong: %+v", v1.Levels[0])
	}
	if vs.Current() != v1 {
		t.Fatal("current must advance")
	}

	// add L1 files out of key order; must come back sorted
	v2, err := vs.Apply(&manifest.Edit{
		NextFileNum: 4,
		AddFiles: []manifest.FileMeta{
			fm(1, 3, "x", "z"),
			fm(1, 2, "d", "f"),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(v2.Levels[1]) != 2 {
		t.Fatalf("v2 L1 wrong")
	}
	if base.UserKey(v2.Levels[1][0].Smallest)[0] != 'd' {
		t.Fatalf("L1 not sorted by smallest: %q", v2.Levels[1][0].Smallest)
	}

	// L0 sorted by file number descending (newest first)
	v3, err := vs.Apply(&manifest.Edit{
		NextFileNum: 6,
		AddFiles: []manifest.FileMeta{
			fm(0, 5, "aa", "ab"),
			fm(0, 4, "aa", "ab"),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(v3.Levels[0]) != 3 || v3.Levels[0][0].Num != 5 || v3.Levels[0][1].Num != 4 || v3.Levels[0][2].Num != 1 {
		t.Fatalf("L0 order wrong: %d,%d,%d", v3.Levels[0][0].Num, v3.Levels[0][1].Num, v3.Levels[0][2].Num)
	}

	// delete file 1
	v4, err := vs.Apply(&manifest.Edit{
		NextFileNum: 7,
		DelFiles:    []manifest.DelFile{{Level: 0, Num: 1}},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range v4.Levels[0] {
		if f.Num == 1 {
			t.Fatal("file 1 must be deleted")
		}
	}

	// delete a checkpoint
	v5, err := vs.Apply(&manifest.Edit{
		NextFileNum: 8,
		AddCkpts: []manifest.CkptFile{{
			FileNum: 10,
			Tuples:  []manifest.TupleRef{{ID: 1, Group: "g/", TsFrom: 1, TsTo: 2, Offset: 3, Length: 4}},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(v5.Ckpts.ByNum) != 1 {
		t.Fatalf("ckpt add failed: %+v", v5.Ckpts.ByNum)
	}
	v6, err := vs.Apply(&manifest.Edit{
		NextFileNum: 9,
		DelCkpts:    []uint64{10},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(v6.Ckpts.ByNum) != 0 {
		t.Fatal("ckpt delete failed")
	}
	_ = v0
	_ = v2
	_ = v4
	_ = v6
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
	// touching boundaries: largest(a..f) == smallest(f..) overlaps on the user key
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
	// an old Version keeps files alive until unref
	v1.Ref() // simulate a reader holding it

	_, err = vs.Apply(&manifest.Edit{
		NextFileNum: 3,
		DelFiles:    []manifest.DelFile{{Level: 0, Num: 1}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(vs.TakeObsolete()) != 0 {
		t.Fatal("file 1 is still referenced by v1: not obsolete")
	}

	v1.Unref() // drop the reader ref
	v1.Unref() // drop the set's ref -> version dead -> file obsolete
	obs := vs.TakeObsolete()
	if len(obs) != 1 || obs[0].Num != 1 {
		t.Fatalf("obsolete = %+v, want file 1", obs)
	}
	if vs.VersionCount() != 0 {
		t.Fatalf("live files = %d, want 0", vs.VersionCount())
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
	// v2 holds 2 files, v1 holds 1
	if vs.VersionCount() != 2 {
		t.Fatalf("live = %d, want 2", vs.VersionCount())
	}
}
