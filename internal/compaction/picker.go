// Package compaction implements compaction planning (the picker) and the
// output writer shared by structural and semantic compaction.
package compaction

import (
	"path/filepath"
	"sync"

	"github.com/itsramhere/semlsm/internal/base"
	"github.com/itsramhere/semlsm/internal/manifest"
	"github.com/itsramhere/semlsm/internal/sstable"
	"github.com/itsramhere/semlsm/internal/version"
	"github.com/itsramhere/semlsm/internal/vfs"
)

// Picker computes level scores and compaction plans (P6.1).
type Picker struct {
	L0CompactTrigger int
	L1TargetBytes    int64
	LevelMultiplier  int
	TargetFileBytes  int64
	cursors          [base.NumLevels]uint64 // round-robin per input level
	fastParity       bool                   // alternates L0/L1 when both are over target

	// mu guards the cursors and fastParity: PickFast runs on the fast
	// worker while PickLevel runs on manual CompactLevel callers
	// (M-032 note 3).
	mu sync.Mutex
}

// NewPicker builds a picker.
func NewPicker(l0Trigger int, l1Target int64, multiplier int, targetFile int64) *Picker {
	return &Picker{
		L0CompactTrigger: l0Trigger,
		L1TargetBytes:    l1Target,
		LevelMultiplier:  multiplier,
		TargetFileBytes:  targetFile,
	}
}

// Score returns the compaction score of every level: L0 by file count,
// deeper levels by bytes over target.
func (p *Picker) Score(v *version.Version) [base.NumLevels]float64 {
	var out [base.NumLevels]float64
	out[0] = float64(len(v.Levels[0])) / float64(p.L0CompactTrigger)
	target := p.L1TargetBytes
	for l := 1; l < base.NumLevels; l++ {
		var bytes int64
		for _, f := range v.Levels[l] {
			bytes += f.Size
		}
		out[l] = float64(bytes) / float64(target)
		target *= int64(p.LevelMultiplier)
	}
	return out
}

// Plan is one compaction: inputs from Level merge into OutputLevel together
// with the overlapping files there.
type Plan struct {
	Level       int
	OutputLevel int
	Inputs      []*version.FileMeta
	Overlaps    []*version.FileMeta
}

// AllFiles returns inputs plus overlaps.
func (pl *Plan) AllFiles() []*version.FileMeta {
	out := make([]*version.FileMeta, 0, len(pl.Inputs)+len(pl.Overlaps))
	out = append(out, pl.Inputs...)
	out = append(out, pl.Overlaps...)
	return out
}

// PickFast returns a plan for L0->L1 or L1->L2 when either scores >= 1,
// or nil. busy holds file numbers already used by running compactions.
// When both levels are over target the worker alternates between them:
// strict L0-first lets continuous L0 traffic starve L1->L2 (the growing L1
// gets rewritten forever and L2 never sees data — found by the P13
// hardening work).
func (p *Picker) PickFast(v *version.Version, busy map[uint64]bool) *Plan {
	p.mu.Lock()
	defer p.mu.Unlock()
	scores := p.Score(v)
	l0 := scores[0] >= 1
	l1 := scores[1] >= 1
	if l0 && l1 {
		p.fastParity = !p.fastParity
		if p.fastParity {
			if pl := p.planLevel(v, 1, busy); pl != nil {
				return pl
			}
			return p.planL0(v, busy)
		}
		if pl := p.planL0(v, busy); pl != nil {
			return pl
		}
		return p.planLevel(v, 1, busy)
	}
	if l0 {
		if pl := p.planL0(v, busy); pl != nil {
			return pl
		}
	}
	if l1 {
		if pl := p.planLevel(v, 1, busy); pl != nil {
			return pl
		}
	}
	return nil
}

// PickDeep returns a plan for L2->L3 or L3->L4 when either scores >= 1,
// or nil.
func (p *Picker) PickDeep(v *version.Version, busy map[uint64]bool) *Plan {
	p.mu.Lock()
	defer p.mu.Unlock()
	scores := p.Score(v)
	if scores[2] >= 1 {
		if pl := p.planLevel(v, 2, busy); pl != nil {
			return pl
		}
	}
	if scores[3] >= 1 {
		if pl := p.planLevel(v, 3, busy); pl != nil {
			return pl
		}
	}
	return nil
}

// PickLevel builds a plan for an explicit level (CompactLevel). The bottom
// level has no deeper level; callers handle self-compaction.
func (p *Picker) PickLevel(v *version.Version, level int, busy map[uint64]bool) *Plan {
	p.mu.Lock()
	defer p.mu.Unlock()
	if level < 0 || level >= base.NumLevels-1 {
		return nil
	}
	if level == 0 {
		return p.planL0(v, busy)
	}
	return p.planLevel(v, level, busy)
}

// planL0 takes ALL L0 files (I4) plus the overlapping L1 files.
func (p *Picker) planL0(v *version.Version, busy map[uint64]bool) *Plan {
	l0 := v.Levels[0]
	if len(l0) == 0 {
		return nil
	}
	inputs := make([]*version.FileMeta, 0, len(l0))
	for _, f := range l0 {
		if busy[f.Num] {
			return nil // a partial L0 input would violate I4
		}
		inputs = append(inputs, f)
	}
	// The L0 range is the UNION of every input's range (M-015 item 3): L0
	// files overlap and are ordered by file number, not by key, so neither
	// the first nor the last file bounds the set.
	lo := base.UserKey(inputs[0].Smallest)
	hi := base.UserKey(inputs[0].Largest)
	for _, f := range inputs[1:] {
		if base.CompareUser(base.UserKey(f.Smallest), lo) < 0 {
			lo = base.UserKey(f.Smallest)
		}
		if base.CompareUser(base.UserKey(f.Largest), hi) > 0 {
			hi = base.UserKey(f.Largest)
		}
	}
	ovs, ok := overlapRange(v.Levels[1], lo, hi, busy)
	if !ok {
		return nil
	}
	return &Plan{Level: 0, OutputLevel: 1, Inputs: inputs, Overlaps: ovs}
}

// planLevel picks one input file by round-robin cursor plus the overlapping
// output-level files.
func (p *Picker) planLevel(v *version.Version, level int, busy map[uint64]bool) *Plan {
	files := v.Levels[level]
	if len(files) == 0 {
		return nil
	}
	var input *version.FileMeta
	for i := 0; i < len(files); i++ {
		idx := (p.cursors[level] + uint64(i)) % uint64(len(files))
		cand := files[idx]
		if !busy[cand.Num] {
			input = cand
			break
		}
	}
	if input == nil {
		return nil // all busy
	}
	p.cursors[level]++ // advance round-robin regardless of which file won

	ovs, ok := overlapRange(v.Levels[level+1],
		base.UserKey(input.Smallest), base.UserKey(input.Largest), busy)
	if !ok {
		return nil
	}
	return &Plan{Level: level, OutputLevel: level + 1,
		Inputs: []*version.FileMeta{input}, Overlaps: ovs}
}

// overlapRange returns the files whose user-key range intersects [lo, hi].
// ok is false when any of them is busy (the plan must be abandoned).
func overlapRange(files []*version.FileMeta, lo, hi []byte, busy map[uint64]bool) (ovs []*version.FileMeta, ok bool) {
	var out []*version.FileMeta
	for _, f := range files {
		fLo := base.UserKey(f.Smallest)
		fHi := base.UserKey(f.Largest)
		if base.CompareUser(fHi, lo) < 0 || base.CompareUser(fLo, hi) > 0 {
			continue
		}
		if busy[f.Num] {
			return nil, false
		}
		out = append(out, f)
	}
	return out, true
}

// OutputWriter writes compaction outputs, cutting files at TargetFileBytes
// only when the user key changes (I5). All versions and tombstones are
// retained: structural compaction drops nothing (Pitfall 8).
type OutputWriter struct {
	fs          vfs.FS
	dir         string
	targetBytes int64
	nextNum     func() uint64

	w        *sstable.Writer
	curNum   uint64
	curKey   []byte
	curBytes int64
	outputs  []*version.FileMeta
}

// NewOutputWriter builds a writer over dir.
func NewOutputWriter(fs vfs.FS, dir string, targetBytes int64, nextNum func() uint64) *OutputWriter {
	return &OutputWriter{fs: fs, dir: dir, targetBytes: targetBytes, nextNum: nextNum}
}

// Add appends one entry, cutting to a new file when the size target is
// passed and the user key changes.
func (ow *OutputWriter) Add(ikey, value []byte) error {
	user := base.UserKey(ikey)
	if ow.w != nil && ow.curBytes >= ow.targetBytes && base.CompareUser(ow.curKey, user) != 0 {
		if err := ow.closeCurrent(); err != nil {
			return err
		}
	}
	if ow.w == nil {
		num := ow.nextNum()
		w, err := sstable.NewWriter(ow.fs, filepath.Join(ow.dir, sstable.Name(num)))
		if err != nil {
			return err
		}
		ow.w = w
		ow.curNum = num
		ow.curBytes = 0
	}
	if err := ow.w.Add(ikey, value); err != nil {
		return err
	}
	ow.curBytes += int64(len(ikey) + len(value) + 16)
	ow.curKey = append(ow.curKey[:0], user...)
	return nil
}

// closeCurrent finishes the open output file and records its metadata.
func (ow *OutputWriter) closeCurrent() error {
	if ow.w == nil {
		return nil
	}
	props := ow.w.Props()
	path := ow.w.Path()
	if err := ow.w.Finish(); err != nil {
		return err
	}
	size := int64(0)
	if st, err := ow.fs.Stat(path); err == nil {
		size = st.Size
	}
	ow.outputs = append(ow.outputs, &version.FileMeta{
		FileMeta: manifest.FileMeta{
			Num:      ow.curNum,
			Size:     size,
			Smallest: props.Smallest,
			Largest:  props.Largest,
			MinTs:    props.MinTs,
			MaxTs:    props.MaxTs,
			Count:    props.Count,
		},
	})
	ow.w = nil
	ow.curBytes = 0
	return nil
}

// Finish closes the last output and returns all output file metadata.
func (ow *OutputWriter) Finish() ([]*version.FileMeta, error) {
	if err := ow.closeCurrent(); err != nil {
		return nil, err
	}
	return ow.outputs, nil
}

// Abort closes and removes all outputs written so far.
func (ow *OutputWriter) Abort() {
	if ow.w != nil {
		ow.w.Abort()
		ow.w = nil
	}
	for _, fm := range ow.outputs {
		_ = ow.fs.Remove(filepath.Join(ow.dir, sstable.Name(fm.Num)))
	}
	ow.outputs = nil
}
