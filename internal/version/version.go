// Package version holds the immutable LSM tree state: levels of file metadata,
// checkpoint references, and live-reference counting for obsolete tracking.
package version

import (
	"fmt"
	"sync"
	"sync/atomic"

	"semlsm/internal/base"
	"semlsm/internal/manifest"
)

// FileMeta is a file in the tree. Instances are shared between Versions that
// reference the same file; the refcount tracks live Versions.
type FileMeta struct {
	manifest.FileMeta
	refs atomic.Int64
}

// Level index constants for readability.
const (
	Level0 = 0
	// BottomLevel is L4.
	BottomLevel = base.NumLevels - 1
)

// CkptIndex holds checkpoint file references (opaque until P7).
type CkptIndex struct {
	ByNum map[uint64]*manifest.CkptFile
}

func (c *CkptIndex) clone() *CkptIndex {
	if c == nil {
		return &CkptIndex{ByNum: map[uint64]*manifest.CkptFile{}}
	}
	out := &CkptIndex{ByNum: make(map[uint64]*manifest.CkptFile, len(c.ByNum))}
	for k, v := range c.ByNum {
		out.ByNum[k] = v
	}
	return out
}

// Version is an immutable snapshot of the LSM tree.
type Version struct {
	Levels [base.NumLevels][]*FileMeta
	Ckpts  *CkptIndex

	refs atomic.Int64
	vs   *VersionSet // for obsolete enqueuing on last unref
}

// Ref pins the version.
func (v *Version) Ref() {
	v.refs.Add(1)
}

// Unref releases the version; when the last reference drops, the files it
// holds are dereferenced and files reaching zero are enqueued as obsolete.
func (v *Version) Unref() {
	if v.refs.Add(-1) != 0 {
		return
	}
	if v.vs != nil {
		v.vs.releaseVersion(v)
	}
}

// Refs returns the current reference count (tests).
func (v *Version) Refs() int64 { return v.refs.Load() }

// VersionSet applies edits and tracks file liveness.
type VersionSet struct {
	mu      sync.Mutex
	current *Version
	live    map[uint64]*FileMeta // num -> current instance
	refs    map[uint64]int64     // num -> live reference count
	// obsolete lists files whose refcount reached zero (num and level hint).
	Obsolete    []manifest.DelFile
	NextFileNum uint64
	LastTs      uint64
	LogNum      uint64
}

// NewVersionSet returns an empty set holding one empty version.
func NewVersionSet() *VersionSet {
	vs := &VersionSet{
		live: map[uint64]*FileMeta{},
		refs: map[uint64]int64{},
	}
	v := &Version{Ckpts: &CkptIndex{ByNum: map[uint64]*manifest.CkptFile{}}, vs: vs}
	v.refs.Store(1)
	vs.current = v
	return vs
}

// Current returns the current version (already referenced by the set).
func (vs *VersionSet) Current() *Version {
	vs.mu.Lock()
	defer vs.mu.Unlock()
	return vs.current
}

// Apply validates and applies an edit, returning the new Version (refcount 1,
// held by the set). On error nothing changes.
func (vs *VersionSet) Apply(e *manifest.Edit) (*Version, error) {
	vs.mu.Lock()
	defer vs.mu.Unlock()

	old := vs.current

	// Convert adds to instances, reusing live ones.
	adds := make(map[uint64]*FileMeta, len(e.AddFiles))
	for i := range e.AddFiles {
		fm := &e.AddFiles[i]
		if inst, ok := vs.live[fm.Num]; ok {
			// Re-adding a live file must describe the same file.
			if inst.Level != fm.Level || inst.Size != fm.Size {
				return nil, fmt.Errorf("version: re-add mismatch for %d: %w", fm.Num, base.ErrCorruption)
			}
			adds[fm.Num] = inst
		} else {
			adds[fm.Num] = &FileMeta{FileMeta: *fm}
		}
	}

	// Build new levels.
	newVersion := &Version{
		Ckpts: old.Ckpts.clone(),
		vs:    vs,
	}
	for l := range old.Levels {
		kept := make([]*FileMeta, 0, len(old.Levels[l]))
		for _, fm := range old.Levels[l] {
			deleted := false
			for _, d := range e.DelFiles {
				if d.Num == fm.Num {
					deleted = true
					break
				}
			}
			if deleted {
				continue
			}
			kept = append(kept, fm)
		}
		for _, fm := range adds {
			if fm.Level == l {
				kept = append(kept, fm)
			}
		}
		newVersion.Levels[l] = kept
	}

	// L0: sort by file number descending (newer first, I4).
	sortL0(newVersion.Levels[0])
	// L1+: sort by smallest user key and reject overlap.
	for l := 1; l < base.NumLevels; l++ {
		sortBySmallest(newVersion.Levels[l])
		if err := checkNoOverlap(newVersion.Levels[l]); err != nil {
			return nil, fmt.Errorf("version: level %d: %w", l, err)
		}
	}

	// Checkpoints.
	for _, d := range e.DelCkpts {
		delete(newVersion.Ckpts.ByNum, d)
	}
	for i := range e.AddCkpts {
		cf := e.AddCkpts[i]
		newVersion.Ckpts.ByNum[cf.FileNum] = &cf
	}

	// Bookkeeping.
	vs.NextFileNum = e.NextFileNum
	vs.LastTs = e.LastTs
	vs.LogNum = e.LogNum

	// Reference accounting (M-010 era fix): file.refs counts live VERSION
	// memberships. The new version takes membership on its files; the old
	// version releases membership only when it dies (refs reach 0), which
	// may be long after Apply if readers still hold it.
	newVersion.refs.Store(1)
	for _, level := range newVersion.Levels {
		for _, fm := range level {
			fm.refs.Add(1)
			vs.live[fm.Num] = fm
			vs.refs[fm.Num] = fm.refs.Load()
		}
	}

	vs.current = newVersion
	if old.refs.Add(-1) == 0 {
		vs.releaseVersionLocked(old)
	}
	return newVersion, nil
}

// releaseVersionLocked releases a dead version's file memberships and
// enqueues files that reach zero. Callers must hold vs.mu.
func (vs *VersionSet) releaseVersionLocked(v *Version) {
	for _, level := range v.Levels {
		for _, fm := range level {
			if fm.refs.Add(-1) == 0 {
				vs.Obsolete = append(vs.Obsolete, manifest.DelFile{Level: fm.Level, Num: fm.Num})
				delete(vs.live, fm.Num)
				delete(vs.refs, fm.Num)
			}
		}
	}
}

// TakeObsolete drains the obsolete list.
func (vs *VersionSet) TakeObsolete() []manifest.DelFile {
	vs.mu.Lock()
	defer vs.mu.Unlock()
	out := vs.Obsolete
	vs.Obsolete = nil
	return out
}

// VersionCount returns the number of distinct live files (tests).
func (vs *VersionSet) VersionCount() int {
	vs.mu.Lock()
	defer vs.mu.Unlock()
	return len(vs.live)
}

func sortL0(files []*FileMeta) {
	// newest first: larger file number first
	for i := 1; i < len(files); i++ {
		for j := i; j > 0 && files[j].Num > files[j-1].Num; j-- {
			files[j], files[j-1] = files[j-1], files[j]
		}
	}
}

func sortBySmallest(files []*FileMeta) {
	for i := 1; i < len(files); i++ {
		for j := i; j > 0 && base.CompareUser(base.UserKey(files[j].Smallest), base.UserKey(files[j-1].Smallest)) < 0; j-- {
			files[j], files[j-1] = files[j-1], files[j]
		}
	}
}

func checkNoOverlap(files []*FileMeta) error {
	for i := 1; i < len(files); i++ {
		prev := base.UserKey(files[i-1].Largest)
		cur := base.UserKey(files[i].Smallest)
		if base.CompareUser(prev, cur) >= 0 {
			return fmt.Errorf("overlap between %q and %q: %w", prev, cur, base.ErrCorruption)
		}
	}
	return nil
}

// releaseVersion is the locked entry point used by Version.Unref.
func (vs *VersionSet) releaseVersion(v *Version) {
	vs.mu.Lock()
	defer vs.mu.Unlock()
	vs.releaseVersionLocked(v)
}
