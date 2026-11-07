// Package version holds the immutable LSM tree state: levels of file metadata,
// checkpoint references, and live-reference counting for obsolete tracking.
package version

import (
	"fmt"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/itsramhere/semlsm/internal/base"
	"github.com/itsramhere/semlsm/internal/manifest"
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

// CkptIndex holds checkpoint file references and the per-group index (P7.2).
type CkptIndex struct {
	ByNum   map[uint64]*manifest.CkptFile
	ByGroup map[string][]TupleLoc // sorted by TsFrom
}

// TupleLoc locates one tuple: its .ckpt file number plus the manifest ref.
type TupleLoc struct {
	FileNum uint64
	Ref     manifest.TupleRef
}

func (c *CkptIndex) clone() *CkptIndex {
	if c == nil {
		c = &CkptIndex{ByNum: map[uint64]*manifest.CkptFile{}}
	}
	out := &CkptIndex{ByNum: make(map[uint64]*manifest.CkptFile, len(c.ByNum))}
	for k, v := range c.ByNum {
		out.ByNum[k] = v
	}
	out.rebuildGroups()
	return out
}

// rebuildGroups reconstructs the group index from ByNum. Each group's
// tuple refs are sorted by TsFrom (P7.2).
func (c *CkptIndex) rebuildGroups() {
	c.ByGroup = make(map[string][]TupleLoc, len(c.ByNum))
	for fileNum, cf := range c.ByNum {
		for _, t := range cf.Tuples {
			c.ByGroup[t.Group] = append(c.ByGroup[t.Group], TupleLoc{FileNum: fileNum, Ref: t})
		}
	}
	for g := range c.ByGroup {
		refs := c.ByGroup[g]
		for i := 1; i < len(refs); i++ {
			for j := i; j > 0 && refs[j].Ref.TsFrom < refs[j-1].Ref.TsFrom; j-- {
				refs[j], refs[j-1] = refs[j-1], refs[j]
			}
		}
		c.ByGroup[g] = refs
	}
}

// GroupRefs returns a copy of the tuple locations for a group.
func (c *CkptIndex) GroupRefs(group string) []TupleLoc {
	refs := c.ByGroup[group]
	out := make([]TupleLoc, len(refs))
	copy(out, refs)
	return out
}

// Version is an immutable snapshot of the LSM tree.
type Version struct {
	Levels [base.NumLevels][]*FileMeta
	Ckpts  *CkptIndex

	refs atomic.Int64
	vs   *VersionSet // for obsolete enqueuing on last unref
}

// Ref pins the version. Panics on a dead version: reviving a released
// Version may observe deleted files (M-011 item 4). The Add result decides
// liveness atomically (M-012 item 1): n <= 1 means the version had no
// references left.
func (v *Version) Ref() {
	n := v.refs.Add(1)
	if n <= 1 {
		panic("version: Ref on a released version")
	}
}

// Unref releases the version; when the last reference drops, the files it
// holds are dereferenced and files reaching zero are enqueued as obsolete.
// A double Unref is a programming error and panics (M-011 item 4). The Add
// result is the only liveness check (M-012 item 1): a separate Load would
// let two concurrent Unrefs both see zero and double-release files.
func (v *Version) Unref() {
	n := v.refs.Add(-1)
	if n < 0 {
		panic("version: Unref below zero")
	}
	if n != 0 {
		return
	}
	if v.vs != nil {
		v.vs.releaseVersion(v)
	}
}

// Refs returns the current reference count (tests).
func (v *Version) Refs() int64 { return v.refs.Load() }

// HasFile reports whether num is a member of this version's levels (M-015
// item 5): plans picked from an older version must be re-checked against the
// current one before their files are marked busy.
func (v *Version) HasFile(num uint64) bool {
	for _, level := range v.Levels {
		for _, fm := range level {
			if fm.Num == num {
				return true
			}
		}
	}
	return false
}

// ObsoleteRecord retains a dying checkpoint's metadata even when its file
// can no longer be read. Level -1 denotes a checkpoint.
type ObsoleteRecord struct {
	Level int
	Num   uint64
	Ckpt  *manifest.CkptFile
}

// VersionSet applies edits and tracks file liveness.
type VersionSet struct {
	mu      sync.Mutex
	current *Version
	live    map[uint64]*FileMeta // num -> current instance
	refs    map[uint64]int64     // num -> live reference count
	// obsolete lists files whose refcount reached zero (num and level hint).
	Obsolete    []ObsoleteRecord
	ckptRefs    map[uint64]int64
	ckptLive    map[uint64]*manifest.CkptFile
	NextFileNum uint64
	LastTs      uint64
	LogNum      uint64
}

// NewVersionSet returns an empty set holding one empty version.
func NewVersionSet() *VersionSet {
	vs := &VersionSet{
		live:     map[uint64]*FileMeta{},
		refs:     map[uint64]int64{},
		ckptRefs: map[uint64]int64{},
		ckptLive: map[uint64]*manifest.CkptFile{},
	}
	v := &Version{Ckpts: &CkptIndex{ByNum: map[uint64]*manifest.CkptFile{}, ByGroup: map[string][]TupleLoc{}}, vs: vs}
	v.refs.Store(1)
	vs.current = v
	return vs
}

// Acquire returns the current version with a reference held under vs.mu
// (M-011 item 3). Every reader (including readState) must hold its own
// acquired reference; a bare Current() would let Apply obsolete files in
// use. Release with Unref.
func (vs *VersionSet) Acquire() *Version {
	vs.mu.Lock()
	defer vs.mu.Unlock()
	v := vs.current
	v.Ref()
	return v
}

// currentForTest returns the current version without a reference.
// Test-only introspection; readers must use Acquire.
func (vs *VersionSet) currentForTest() *Version {
	vs.mu.Lock()
	defer vs.mu.Unlock()
	return vs.current
}

// Apply validates and applies an edit, returning the new Version. The set
// holds the new version's reference; readers take their own via Acquire.
// On error nothing changes.
func (vs *VersionSet) Apply(e *manifest.Edit) (*Version, error) {
	vs.mu.Lock()
	defer vs.mu.Unlock()

	old := vs.current
	newVersion, err := vs.buildVersionLocked(old, e)
	if err != nil {
		return nil, err
	}

	// Bookkeeping: monotone max of each counter (M-011 item 2). An edit built
	// earlier must never roll back the allocator, the timestamp, or the WAL
	// pointer — reusing file numbers would overwrite live files and repeated
	// timestamps would break I2.
	// New file numbers in the edit also raise the allocator (M-013 item 1c):
	// edits without NextFileNum must not let the allocator restart below
	// numbers now in use.
	for i := range e.AddFiles {
		if e.AddFiles[i].Num >= vs.NextFileNum {
			vs.NextFileNum = e.AddFiles[i].Num + 1
		}
	}
	for i := range e.AddCkpts {
		if e.AddCkpts[i].FileNum >= vs.NextFileNum {
			vs.NextFileNum = e.AddCkpts[i].FileNum + 1
		}
	}
	if e.NextFileNum > vs.NextFileNum {
		vs.NextFileNum = e.NextFileNum
	}
	if e.LastTs > vs.LastTs {
		vs.LastTs = e.LastTs
	}
	if e.LogNum > vs.LogNum {
		vs.LogNum = e.LogNum
	}

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
	for num, cf := range newVersion.Ckpts.ByNum {
		vs.ckptRefs[num]++
		vs.ckptLive[num] = cf
	}
	if old.refs.Add(-1) == 0 {
		vs.releaseVersionLocked(old)
	}
	return newVersion, nil
}

// Validate runs every check Apply performs without changing anything
// (M-015 item 4): callers can reject a bad edit before it becomes durable in
// the manifest, where it would brick the next Open.
func (vs *VersionSet) Validate(e *manifest.Edit) error {
	vs.mu.Lock()
	defer vs.mu.Unlock()
	_, err := vs.buildVersionLocked(vs.current, e)
	return err
}

// buildVersionLocked constructs the Version the edit would produce and
// validates it against old, mutating neither the set nor any refcount.
// Apply installs the result; Validate discards it. Callers must hold vs.mu.
func (vs *VersionSet) buildVersionLocked(old *Version, e *manifest.Edit) (*Version, error) {
	// Validate dels against the current version first (M-011 item 5):
	// deleting an unknown file, or with a mismatched level, is corruption.
	delSet := map[uint64]manifest.DelFile{}
	for _, d := range e.DelFiles {
		found := false
		for _, level := range old.Levels {
			for _, fm := range level {
				if fm.Num == d.Num {
					if fm.Level != d.Level {
						return nil, fmt.Errorf("version: del level mismatch for %d: edit %d != %d: %w",
							d.Num, d.Level, fm.Level, base.ErrCorruption)
					}
					found = true
					break
				}
			}
		}
		if !found {
			return nil, fmt.Errorf("version: del of unknown file %d: %w", d.Num, base.ErrCorruption)
		}
		delSet[d.Num] = d
	}

	// Convert adds to instances, reusing live ones. Re-adding a num that is
	// still live would list it twice (M-011 item 5).
	adds := make(map[uint64]*FileMeta, len(e.AddFiles))
	for i := range e.AddFiles {
		fm := &e.AddFiles[i]
		if _, ok := vs.live[fm.Num]; ok {
			return nil, fmt.Errorf("version: add of live file %d: %w", fm.Num, base.ErrCorruption)
		}
		adds[fm.Num] = &FileMeta{FileMeta: *fm}
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
		cf.Tuples = append([]manifest.TupleRef(nil), cf.Tuples...)
		if cf.ArchiveNums != nil {
			cf.ArchiveNums = append([]uint64{}, cf.ArchiveNums...)
		}
		newVersion.Ckpts.ByNum[cf.FileNum] = &cf
	}
	newVersion.Ckpts.rebuildGroups()
	return newVersion, nil
}

// releaseVersionLocked releases a dead version's file memberships and
// enqueues files that reach zero. Callers must hold vs.mu.
func (vs *VersionSet) releaseVersionLocked(v *Version) {
	for _, level := range v.Levels {
		for _, fm := range level {
			if fm.refs.Add(-1) == 0 {
				if dbgTraceObsolete != nil {
					dbgTraceObsolete(v, fm.Num)
				}
				vs.Obsolete = append(vs.Obsolete, ObsoleteRecord{Level: fm.Level, Num: fm.Num})
				delete(vs.live, fm.Num)
				delete(vs.refs, fm.Num)
			}
		}
	}
	for num, cf := range v.Ckpts.ByNum {
		vs.ckptRefs[num]--
		if vs.ckptRefs[num] == 0 {
			vs.Obsolete = append(vs.Obsolete, ObsoleteRecord{Level: -1, Num: num, Ckpt: cf})
			delete(vs.ckptRefs, num)
			delete(vs.ckptLive, num)
		}
	}
}

// dbgTraceObsolete is a test-only hook for obsolete tracing.
var dbgTraceObsolete func(v *Version, num uint64)

// TakeObsolete drains the obsolete list.
func (vs *VersionSet) TakeObsolete() []ObsoleteRecord {
	vs.mu.Lock()
	defer vs.mu.Unlock()
	out := vs.Obsolete
	vs.Obsolete = nil
	return out
}

// LiveCheckpoints returns independent metadata copies for checkpoints held by
// ANY live version, including versions pinned only by snapshots or jobs.
func (vs *VersionSet) LiveCheckpoints() []manifest.CkptFile {
	vs.mu.Lock()
	defer vs.mu.Unlock()
	out := make([]manifest.CkptFile, 0, len(vs.ckptLive))
	for _, cf := range vs.ckptLive {
		copyCF := *cf
		copyCF.Tuples = append([]manifest.TupleRef(nil), cf.Tuples...)
		if cf.ArchiveNums != nil {
			copyCF.ArchiveNums = append([]uint64{}, cf.ArchiveNums...)
		}
		out = append(out, copyCF)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].FileNum < out[j].FileNum })
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

// IsCkpt reports whether the file is a checkpoint file (never for SSTs).
func (f *FileMeta) IsCkpt() bool { return false }

// CurrentLastTs returns the set's LastTs under its lock.
func (vs *VersionSet) CurrentLastTs() uint64 {
	vs.mu.Lock()
	defer vs.mu.Unlock()
	return vs.LastTs
}

// CurrentLogNum returns the set's LogNum under its lock.
func (vs *VersionSet) CurrentLogNum() uint64 {
	vs.mu.Lock()
	defer vs.mu.Unlock()
	return vs.LogNum
}

// ExportedVersionRef is a read-only view for test tracing.
type ExportedVersionRef struct {
	V   *Version
	Num uint64
}

// SetTraceObsolete installs a test-only trace hook for obsolete enqueues.
// Nil disables it.
func SetTraceObsolete(fn func(v *Version, num uint64)) {
	dbgTraceObsolete = fn
}
