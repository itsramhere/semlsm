package compaction

import (
	"testing"

	"github.com/itsramhere/semlsm/internal/base"
	"github.com/itsramhere/semlsm/internal/manifest"
	"github.com/itsramhere/semlsm/internal/version"
)

// fm builds one scripted file metadata entry. Smallest/Largest are internal
// keys, as the flush path produces them.
func fm(num uint64, level int, lo, hi string) *version.FileMeta {
	return &version.FileMeta{FileMeta: manifest.FileMeta{
		Num:      num,
		Level:    level,
		Size:     1000,
		Smallest: base.MakeInternalKey([]byte(lo), 100, base.KindPut),
		Largest:  base.MakeInternalKey([]byte(hi), 100, base.KindPut),
	}}
}

// scriptedVersion builds a Version with the given per-level file lists.
func scriptedVersion(levels ...[]*version.FileMeta) *version.Version {
	v := &version.Version{}
	for i := 0; i < base.NumLevels && i < len(levels); i++ {
		v.Levels[i] = levels[i]
	}
	return v
}

func busySet(nums ...uint64) map[uint64]bool {
	out := map[uint64]bool{}
	for _, n := range nums {
		out[n] = true
	}
	return out
}

// M-015 item 3: L0 files are ordered by file number, not key, so the L0
// key range is the union of all inputs' ranges. Older file m..z, newer file
// a..c: the union is a..z and L1 file e..o (which straddles m) must be
// selected as an overlap. The old file-order range m..c missed it, the
// output then straddled the L1 file, Apply rejected the edit and the
// compaction retried forever.
func TestPickerL0RangeUnion(t *testing.T) {
	v := scriptedVersion(
		[]*version.FileMeta{fm(2, 0, "a", "c"), fm(1, 0, "m", "z")}, // num desc
		[]*version.FileMeta{fm(10, 1, "e", "o")},
	)
	p := NewPicker(2, 1<<40, 2, 1<<20)
	pl := p.PickFast(v, nil)
	if pl == nil {
		t.Fatal("expected an L0 plan")
	}
	if pl.Level != 0 || pl.OutputLevel != 1 {
		t.Fatalf("plan level: %d->%d", pl.Level, pl.OutputLevel)
	}
	if len(pl.Inputs) != 2 {
		t.Fatalf("inputs: %d", len(pl.Inputs))
	}
	if len(pl.Overlaps) != 1 || pl.Overlaps[0].Num != 10 {
		t.Fatalf("L1 file e..o missed by the union range: overlaps=%v", pl.Overlaps)
	}
}

// M-015 item 8: busy files at each position abort the plan.
func TestPickerBusyFiles(t *testing.T) {
	l0 := []*version.FileMeta{fm(2, 0, "a", "c"), fm(1, 0, "m", "z")}
	l1 := []*version.FileMeta{fm(10, 1, "d", "g"), fm(11, 1, "q", "t")}
	p := NewPicker(2, 1<<40, 2, 1<<20)

	// a busy L0 file: a partial L0 input would violate I4
	if pl := p.PickFast(scriptedVersion(l0, l1), busySet(2)); pl != nil {
		t.Fatalf("busy L0 input accepted: %+v", pl)
	}
	// a busy L1 overlap: the plan must be abandoned, not shrunk
	if pl := p.PickFast(scriptedVersion(l0, l1), busySet(10)); pl != nil {
		t.Fatalf("busy L1 overlap accepted: %+v", pl)
	}
	// all output-level candidates busy in planLevel
	v := scriptedVersion(nil, []*version.FileMeta{fm(10, 1, "a", "c")}, []*version.FileMeta{fm(20, 2, "a", "z")})
	if pl := p.PickLevel(v, 1, busySet(20)); pl != nil {
		t.Fatalf("busy deep overlap accepted: %+v", pl)
	}
	// every input busy
	if pl := p.PickLevel(v, 1, busySet(10)); pl != nil {
		t.Fatalf("busy input accepted: %+v", pl)
	}
}

// M-015 item 8: the round-robin cursor cycles through input files.
func TestPickerRoundRobin(t *testing.T) {
	l1 := []*version.FileMeta{fm(10, 1, "a", "c"), fm(11, 1, "e", "g"), fm(12, 1, "i", "k")}
	v := scriptedVersion(nil, l1, nil)
	p := NewPicker(1<<20, 1<<40, 2, 1<<20)
	want := []uint64{10, 11, 12, 10}
	for i, w := range want {
		pl := p.PickLevel(v, 1, nil)
		if pl == nil {
			t.Fatalf("pick %d: no plan", i)
		}
		if len(pl.Inputs) != 1 || pl.Inputs[0].Num != w {
			t.Fatalf("pick %d: input %v, want %d", i, pl.Inputs, w)
		}
		if len(pl.Overlaps) != 0 {
			t.Fatalf("pick %d: unexpected overlaps %v", i, pl.Overlaps)
		}
	}
	// a busy file at the cursor position is skipped, not blocking
	p2 := NewPicker(1<<20, 1<<40, 2, 1<<20)
	pl := p2.PickLevel(v, 1, busySet(10))
	if pl == nil || pl.Inputs[0].Num != 11 {
		t.Fatalf("busy cursor position not skipped: %+v", pl)
	}
}

// M-015 item 8: overlap edges — touching ranges overlap (lo == other.hi).
func TestPickerOverlapEdges(t *testing.T) {
	// input k..m touches L1 file m..o at key m: it is an overlap
	v := scriptedVersion(nil,
		[]*version.FileMeta{fm(1, 1, "k", "m")},
		[]*version.FileMeta{fm(20, 2, "m", "o"), fm(21, 2, "a", "c")})
	p := NewPicker(1<<20, 1<<40, 2, 1<<20)
	pl := p.PickLevel(v, 1, nil)
	if pl == nil {
		t.Fatal("no plan")
	}
	if len(pl.Overlaps) != 1 || pl.Overlaps[0].Num != 20 {
		t.Fatalf("touching range m..o not selected: %v", pl.Overlaps)
	}
	// a disjoint L1 file just below the input is not an overlap
	v2 := scriptedVersion(nil,
		[]*version.FileMeta{fm(1, 1, "k", "m")},
		[]*version.FileMeta{fm(20, 2, "a", "c")})
	pl2 := p.PickLevel(v2, 1, nil)
	if pl2 == nil || len(pl2.Overlaps) != 0 {
		t.Fatalf("disjoint file selected: %+v", pl2)
	}
	// input k..m touches L1 file i..k at key k: it is an overlap
	v3 := scriptedVersion(nil,
		[]*version.FileMeta{fm(1, 1, "k", "m")},
		[]*version.FileMeta{fm(20, 2, "i", "k")})
	pl3 := p.PickLevel(v3, 1, nil)
	if pl3 == nil || len(pl3.Overlaps) != 1 {
		t.Fatalf("lower touching range not selected: %+v", pl3)
	}
}

// Scores: L0 by file count, deeper levels by bytes over target with the
// multiplier growing per level.
func TestPickerScores(t *testing.T) {
	l0 := []*version.FileMeta{fm(1, 0, "a", "b"), fm(2, 0, "c", "d"), fm(3, 0, "e", "f")}
	l1 := []*version.FileMeta{fm(10, 1, "a", "z")}
	l1[0].Size = 4000
	v := scriptedVersion(l0, l1, nil, nil, nil)
	p := NewPicker(4, 1000, 10, 1<<20)
	s := p.Score(v)
	if s[0] != 3.0/4.0 {
		t.Fatalf("L0 score %v", s[0])
	}
	if s[1] != 4.0 {
		t.Fatalf("L1 score %v", s[1])
	}
	if s[2] != 0 {
		t.Fatalf("L2 score %v", s[2])
	}
}

// PickLevel boundary: the bottom level has no deeper level.
func TestPickerBottomLevel(t *testing.T) {
	p := NewPicker(2, 1<<40, 2, 1<<20)
	v := scriptedVersion(nil, nil, nil, nil, []*version.FileMeta{fm(1, 4, "a", "z")})
	for _, l := range []int{-1, base.NumLevels - 1, base.NumLevels, 99} {
		if pl := p.PickLevel(v, l, nil); pl != nil {
			t.Fatalf("level %d produced a plan", l)
		}
	}
}
