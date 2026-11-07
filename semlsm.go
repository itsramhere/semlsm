// Package semlsm is the public API of the SemLSM storage engine: an
// embeddable LSM tree whose deep-level compaction replaces old key versions
// with LLM-written checkpoint tuples while keeping raw reads exact.
package semlsm

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"semlsm/internal/base"
	"semlsm/internal/checkpoint"
	"semlsm/internal/compaction"
	"semlsm/internal/failpoint"
	"semlsm/internal/manifest"
	"semlsm/internal/memtable"
	"semlsm/internal/semantic"
	"semlsm/internal/sstable"
	"semlsm/internal/version"
	"semlsm/internal/vfs"
	"semlsm/internal/wal"
)

// Re-exported sentinel errors (B5).
var (
	ErrCorruption      = base.ErrCorruption
	ErrClosed          = base.ErrClosed
	ErrFutureTimestamp = base.ErrFutureTimestamp
	ErrOptionsMismatch = base.ErrOptionsMismatch
	ErrNotArchived     = base.ErrNotArchived
	ErrKeyEmpty        = base.ErrKeyEmpty
	ErrKeyTooLarge     = base.ErrKeyTooLarge
	ErrValueTooLarge   = base.ErrValueTooLarge
	ErrReleased        = base.ErrReleased
)

// Failpoint names used by flush and recovery (P5.4). Compaction and semantic
// names are defined in P6/P10.
var (
	fpFlushAfterSSTSync  = failpoint.Define("flush.after_sst_sync")
	fpFlushAfterManifest = failpoint.Define("flush.after_manifest")
)

// Stats is the engine snapshot returned by DB.Stats.
type Stats struct {
	VisibleTs             uint64
	LastTs                uint64
	MemtableBytes         int64
	ImmutableMemtables    int
	StallTime             time.Duration
	SnapshotsOpen         int
	LevelFiles            [base.NumLevels]int
	LevelBytes            [base.NumLevels]int64
	CompactionErrors      uint64 // failed compaction attempts since Open (M-015 item 6)
	CheckpointMerges      uint64
	CheckpointFilesMerged uint64 // number of input files rewritten by merges

	// Tier B semantic accounting (P10.3).
	SemanticTuplesCreated     uint64
	SemanticRecordsSummarized uint64
	SemanticRawBytesRemoved   uint64
	SemanticSummaryBytes      uint64
	SemanticCompressionRatio  float64 // raw bytes removed / summary bytes
	LLMCalls                  uint64
	LLMInputTokens            uint64
	LLMOutputTokens           uint64
	LLMCacheHits              uint64
	LLMBreakerState           string
	LLMFailures               map[string]uint64 // by class since Open
	SemanticPreemptions       uint64
	SemanticJobs              map[string]uint64 // by outcome since Open
}

// immEntry is one immutable memtable awaiting flush, with the WAL number
// that holds its data.
type immEntry struct {
	mem    *memtable.Memtable
	walNum uint64
	wal    *wal.Writer
}

// readState is an immutable snapshot of the read path (B7).
type readState struct {
	mem     *memtable.Memtable
	imms    []immEntry // newest first
	version *version.Version
}

func (rs *readState) release() {
	if rs.version != nil {
		rs.version.Unref()
	}
}

// commitReq is one write request flowing to the commit goroutine.
type commitReq struct {
	recs  []base.Record // last-write-wins batch, ts assigned by the committer
	done  chan error
	ts    uint64
	flush bool // rotate the active memtable and wait (Flush)
}

// DB is a SemLSM instance.
type DB struct {
	dir  string
	opts Options

	lockH vfs.LockHandle

	// mu guards the read-state pointer, visibleTs, queues, and scheduling
	// state. Never held over I/O or LLM calls (B7).
	mu        sync.Mutex
	rs        *readState
	visibleTs uint64

	// manifestMu serializes manifest writes; lock order is manifestMu
	// then mu.
	manifestMu  sync.Mutex
	manifest    *manifest.Writer
	manifestNum uint64
	vs          *version.VersionSet

	// Commit goroutine owns wal, walNum, mem, lastTs, and the sync counter.
	wal     *wal.Writer
	walNum  uint64
	mem     *memtable.Memtable
	lastTs  uint64
	syncCnt uint64

	commitQ     chan *commitReq
	closeCh     chan struct{}
	compactWake chan struct{}
	commitWG    sync.WaitGroup
	stallCond   *sync.Cond

	flushWake chan struct{} // cap 1
	flushWG   sync.WaitGroup

	blockCache *sstable.BlockCache
	tableCache *sstable.TableCache
	ckptCache  *checkpoint.CkptCache
	obsolete   *version.ObsoleteDeleter

	closed    atomic.Bool
	failedErr error // sticky commit-path failure (guarded by mu)
	closeOnce sync.Once

	stallNanos    atomic.Int64
	snapshotsOpen atomic.Int64
	compactErrs   atomic.Uint64 // failed compaction attempts (Stats)

	snapsMu sync.Mutex
	snaps   map[*Snapshot]struct{}

	logger *slog.Logger

	nextFileNum uint64 // guarded by manifestMu

	picker              *compaction.Picker
	compactWG           sync.WaitGroup
	compactionsInFlight int
	ckptMergeRunning    bool // guarded by mu; one manual/background merge at a time
	ckptMergeDone       chan struct{}
	ckptMergeWG         sync.WaitGroup // registrations under mu, before Close closes admission
	ckptMerges          atomic.Uint64
	ckptFilesMerged     atomic.Uint64
	busy                map[uint64]bool // file numbers used by running compactions

	// Tier B semantic state (P10.2). semJobs is guarded by mu; the counters
	// are atomics folded into Stats.
	semSum    *semantic.Summarizer
	semModel  string
	semPV     int
	semJobs   map[*semJobHandle]struct{}
	nextTry   map[uint64]time.Time // L4 file -> earliest retry (sweep backoff)
	lastSweep atomic.Int64         // unix nano of the last sweep attempt

	semTuples      atomic.Uint64
	semRecs        atomic.Uint64
	semRawBytes    atomic.Uint64
	semSumBytes    atomic.Uint64
	semCalls       atomic.Uint64
	semInTokens    atomic.Uint64
	semOutTokens   atomic.Uint64
	semPreemptions atomic.Uint64

	semStatsMu sync.Mutex
	semFail    map[string]uint64 // LLM failures by class
	semJobsOut map[string]uint64 // jobs by outcome
}

// semJobHandle tracks one running semantic job for the anti-stall rule (I11).
type semJobHandle struct {
	cancel context.CancelFunc
	files  map[uint64]bool // input file numbers the job holds busy
}

// file name helpers (B8).
func sstName(num uint64) string  { return fmt.Sprintf("%06d.sst", num) }
func ckptName(num uint64) string { return fmt.Sprintf("%06d.ckpt", num) }
func walName(num uint64) string  { return fmt.Sprintf("%06d.wal", num) }

func parseFileNum(name string) (uint64, bool, string) {
	// returns (num, ok, suffix)
	i := strings.IndexByte(name, '.')
	if i < 0 {
		return 0, false, ""
	}
	var n uint64
	if _, err := fmt.Sscanf(name[:i], "%d", &n); err != nil {
		return 0, false, ""
	}
	return n, true, name[i:]
}

// Open creates or opens a database in dir (P5.2).
func Open(dir string, opts Options) (*DB, error) {
	opts, err := opts.withDefaults()
	if err != nil {
		return nil, err
	}
	if err := opts.FS.MkdirAll(dir); err != nil {
		return nil, fmt.Errorf("semlsm: mkdir %s: %w", dir, err)
	}
	lockH, err := opts.FS.Lock(filepath.Join(dir, "LOCK"))
	if err != nil {
		return nil, fmt.Errorf("semlsm: lock %s: %w", dir, err)
	}

	db := &DB{
		dir:       dir,
		opts:      opts,
		lockH:     lockH,
		commitQ:   make(chan *commitReq, 1024),
		closeCh:   make(chan struct{}),
		flushWake: make(chan struct{}, 1),
		snaps:     map[*Snapshot]struct{}{},
		logger:    opts.Logger,
	}
	db.stallCond = sync.NewCond(&db.mu)
	db.busy = map[uint64]bool{}
	db.semJobs = map[*semJobHandle]struct{}{}
	db.nextTry = map[uint64]time.Time{}
	db.semFail = map[string]uint64{}
	db.semJobsOut = map[string]uint64{}
	if err := db.openRecover(); err != nil {
		// Recovery may have opened writers before a later WAL or I/O error.
		if db.wal != nil {
			_ = db.wal.Close()
		}
		if db.manifest != nil {
			_ = db.manifest.Close()
		}
		if db.rs != nil {
			db.rs.release()
		}
		_ = lockH.Unlock()
		return nil, err
	}

	// Workers.
	db.obsolete = version.NewObsoleteDeleter(opts.FS, db.tableCache, 20*time.Millisecond)
	db.obsolete.SetLogger(opts.Logger)
	db.obsolete.Start()
	db.flushWG.Add(1)
	go db.flushLoop()
	db.commitWG.Add(1)
	go db.commitLoop()
	db.startCompactionWorkers()

	return db, nil
}

// allocateFileNum returns the next file number (manifestMu must be held).
func (db *DB) allocateFileNum() uint64 {
	db.nextFileNum++
	return db.nextFileNum
}

// openRecover performs all recovery-time work before workers start.
func (db *DB) openRecover() error {
	// OPTIONS.json: check on reopen, write on create.
	if st, err := db.opts.FS.Stat(filepath.Join(db.dir, "OPTIONS.json")); err == nil && st.Size > 0 {
		if err := checkOptionsFile(db.opts, db.dir); err != nil {
			return err
		}
	} else if err := writeOptionsFile(db.opts, db.dir); err != nil {
		return err
	}

	db.vs = version.NewVersionSet()
	db.mu.Lock()
	mem := memtable.New()
	db.mem = mem
	db.mu.Unlock()

	// Manifest: CURRENT or fresh. The readState is built only after the
	// manifest is loaded: building it earlier would pin the empty
	// pre-replay version, and a clean reopen (no WAL data to replay, so no
	// recovered flush) would serve NotFound for every key until the first
	// install replaced the read state.
	if err := db.loadOrCreateManifest(); err != nil {
		return err
	}
	db.mu.Lock()
	db.rs = &readState{mem: mem, version: db.vs.Acquire()}
	db.mu.Unlock()

	// Replay WALs with number >= logNum, ascending; only the last tolerates
	// a torn tail, which is truncated to valid bytes.
	replayNums, err := db.replayWalNums()
	if err != nil {
		return err
	}
	var replayMaxTs uint64
	recovered := memtable.New()
	var recoverCount uint64
	for i, num := range replayNums {
		last := i == len(replayNums)-1
		var validBytes int64
		validBytes, err = wal.Replay(db.opts.FS, filepath.Join(db.dir, walName(num)), last, func(payload []byte) error {
			_, recs, derr := wal.DecodeBatch(payload)
			if derr != nil {
				return derr
			}
			for _, r := range recs {
				recovered.Add(base.MakeInternalKey(r.Key, r.Ts, r.Kind), r.Value)
				if r.Ts > replayMaxTs {
					replayMaxTs = r.Ts
				}
				recoverCount++
			}
			return nil
		})
		if err != nil {
			return fmt.Errorf("semlsm: replay wal %d: %w", num, err)
		}
		if last && validBytes > 0 {
			if terr := db.truncateWAL(num, validBytes); terr != nil {
				return terr
			}
		}
	}

	// Create the active WAL only after replay succeeds, so a failed Open does
	// not leave an empty newer WAL that changes which tail may be torn. Recovery
	// flushes them into L0 (P5.2 "simple, avoids WAL reuse").
	// M-013 item 1d: raise the allocator above every existing file number
	// (SSTs, WALs, ckpts, manifests) BEFORE allocating anything, so the new
	// WAL can never truncate or shadow an old file.
	if err := db.scanMaxFileNum(); err != nil {
		return err
	}
	db.manifestMu.Lock()
	db.walNum = db.allocateFileNum()
	db.nextFileNumSnapshot()
	db.manifestMu.Unlock()
	w, err := wal.NewWriter(db.opts.FS, filepath.Join(db.dir, walName(db.walNum)))
	if err != nil {
		return err
	}
	db.wal = w
	if err := db.opts.FS.SyncDir(db.dir); err != nil {
		return fmt.Errorf("semlsm: syncdir after wal create: %w", err)
	}

	// Flush recovered data to L0 and advance log_num past every replayed WAL.
	if recoverCount > 0 {
		if err := db.flushRecovered(recovered, replayMaxTs); err != nil {
			return err
		}
	} else {
		if err := db.advanceLogNum(); err != nil {
			return err
		}
	}

	// Delete obsolete files: replayed WALs, orphans, temp files.
	if err := db.cleanupOrphans(replayNums); err != nil {
		return err
	}

	// lastTs = max(manifest, replayed, clock.Now()) per I2.
	now := uint64(db.opts.Clock.Now().UnixNano())
	db.mu.Lock()
	db.lastTs = max3(db.vs.LastTs, replayMaxTs, now)
	db.visibleTs = db.lastTs
	db.mu.Unlock()

	db.blockCache = sstable.NewBlockCache(db.opts.BlockCacheBytes)
	db.tableCache = sstable.NewTableCache(db.opts.MaxOpenTables, func(num uint64) (*sstable.Reader, error) {
		r, err := sstable.OpenReader(db.opts.FS, filepath.Join(db.dir, sstName(num)))
		if err != nil {
			return nil, err
		}
		r.UseCache(num, db.blockCache)
		return r, nil
	})
	db.ckptCache = checkpoint.NewCkptCache(db.opts.MaxOpenTables, func(num uint64) (*checkpoint.Reader, error) {
		return checkpoint.OpenReader(db.opts.FS, filepath.Join(db.dir, ckptName(num)))
	})
	return nil
}

// nextFileNumSnapshot syncs vs.NextFileNum after allocations done outside
// manifest edits.
func (db *DB) nextFileNumSnapshot() {
	if db.nextFileNum > db.vs.NextFileNum {
		db.vs.NextFileNum = db.nextFileNum
	}
}

func (db *DB) replayWalNums() ([]uint64, error) {
	entries, err := db.opts.FS.List(db.dir)
	if err != nil {
		return nil, fmt.Errorf("semlsm: list %s: %w", db.dir, err)
	}
	logNum := db.vs.LogNum
	var nums []uint64
	for _, e := range entries {
		if e.IsDir {
			continue
		}
		num, ok, suffix := parseFileNum(e.Name)
		if !ok || suffix != ".wal" {
			continue
		}
		if num >= logNum && (db.walNum == 0 || num != db.walNum) {
			nums = append(nums, num)
		}
	}
	sort.Slice(nums, func(i, j int) bool { return nums[i] < nums[j] })
	return nums, nil
}

func (db *DB) truncateWAL(num uint64, validBytes int64) error {
	f, err := db.opts.FS.OpenReadWrite(filepath.Join(db.dir, walName(num)))
	if err != nil {
		return fmt.Errorf("semlsm: truncate wal %d: %w", num, err)
	}
	if err := f.Truncate(validBytes); err != nil {
		f.Close()
		return fmt.Errorf("semlsm: truncate wal %d: %w", num, err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("semlsm: truncate wal %d: %w", num, err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("semlsm: truncate wal %d: %w", num, err)
	}
	return nil
}

// loadOrCreateManifest loads the current manifest (rewriting it fresh per
// M-011 item 7) or creates one for a new DB.
func (db *DB) loadOrCreateManifest() error {
	db.vs = version.NewVersionSet()
	currentName, err := manifest.ReadCurrent(db.opts.FS, db.dir)
	if err == nil {
		mf, merr := db.opts.FS.Open(filepath.Join(db.dir, currentName))
		if merr != nil {
			// CURRENT names a manifest that cannot be opened: corrupted
			// pointer (I12) — never a wrong answer, report corruption.
			return fmt.Errorf("semlsm: manifest %s: %w: %v", currentName, ErrCorruption, merr)
		}
		mf.Close()
		edits, rerr := manifest.ReadAll(db.opts.FS, filepath.Join(db.dir, currentName))
		if rerr != nil {
			return fmt.Errorf("semlsm: manifest %s: %w", currentName, rerr)
		}
		for i := range edits {
			if _, aerr := db.vs.Apply(&edits[i]); aerr != nil {
				return fmt.Errorf("semlsm: manifest edit %d: %w", i, aerr)
			}
		}
		// Rewrite a fresh manifest with a full snapshot edit (M-011 item 7):
		// never append to a recovered manifest.
		if werr := db.rewriteManifest(); werr != nil {
			return werr
		}
		_ = currentName
		return nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("semlsm: read current: %w", err)
	}
	// New DB.
	return db.rewriteManifest()
}

// rewriteManifest writes a fresh MANIFEST containing one full snapshot edit
// of the current VersionSet state and points CURRENT at it.
func (db *DB) rewriteManifest() error {
	db.manifestMu.Lock()
	defer db.manifestMu.Unlock()
	return db.rewriteManifestLocked()
}

// rewriteManifestLocked does the manifest rewrite with manifestMu already
// held (M-013 item 3): the whole edit is atomic with respect to other
// manifest writers.
func (db *DB) rewriteManifestLocked() error {
	newNum := db.vs.NextFileNum + 1
	if db.manifestNum > 0 {
		newNum = db.manifestNum + 1
	}
	if newNum <= db.vs.NextFileNum {
		newNum = db.vs.NextFileNum + 1
	}
	name := manifest.ManifestName(newNum)
	w, err := manifest.NewWriter(db.opts.FS, filepath.Join(db.dir, name))
	if err != nil {
		return err
	}
	snap := &manifest.Edit{
		NextFileNum: newNum + 1,
		LastTs:      db.vs.CurrentLastTs(),
		LogNum:      db.vs.CurrentLogNum(),
	}
	cur := db.vs.Acquire()
	defer cur.Unref()
	for l, files := range cur.Levels {
		for _, fm := range files {
			snap.AddFiles = append(snap.AddFiles, manifest.FileMeta{
				Level: l, Num: fm.Num, Size: fm.Size,
				Smallest: append([]byte(nil), fm.Smallest...),
				Largest:  append([]byte(nil), fm.Largest...),
				MinTs:    fm.MinTs, MaxTs: fm.MaxTs, Count: fm.Count,
			})
		}
	}
	for num, cf := range cur.Ckpts.ByNum {
		snap.AddCkpts = append(snap.AddCkpts, *cf)
		_ = num
	}
	if err := w.Append(snap); err != nil {
		w.Close()
		return fmt.Errorf("semlsm: snapshot edit: %w", err)
	}
	if err := w.Sync(); err != nil {
		w.Close()
		return err
	}
	if err := manifest.SetCurrent(db.opts.FS, db.dir, name); err != nil {
		w.Close()
		return err
	}
	// The writer stays open: it becomes db.manifest for future edits.
	oldNum := db.manifestNum
	oldWriter := db.manifest
	db.manifest = w
	db.manifestNum = newNum
	if newNum > db.nextFileNum {
		db.nextFileNum = newNum
	}
	// close and remove the previous manifest (best effort)
	if oldWriter != nil {
		_ = oldWriter.Close()
	}
	if oldNum > 0 {
		_ = db.opts.FS.Remove(filepath.Join(db.dir, manifest.ManifestName(oldNum)))
	}
	return nil
}

// appendManifestLocked appends one edit with the allocator state stamped in
// (M-013 item 1a), syncs it, and applies it to the VersionSet. manifestMu
// must be held; lastTs is the edit's commit-timestamp context (a flushed
// memtable's max ts, or the VersionSet's for non-flush edits).
// The edit is validated BEFORE it becomes durable (M-015 item 4): a rejected
// edit must leave the manifest bytes untouched, or the next Open replays it,
// fails, and the DB is bricked.
func (db *DB) appendManifestLocked(e *manifest.Edit, lastTs uint64) error {
	e.NextFileNum = db.nextFileNum
	if lastTs > db.vs.CurrentLastTs() {
		e.LastTs = lastTs
	} else {
		e.LastTs = db.vs.CurrentLastTs()
	}
	if err := db.vs.Validate(e); err != nil {
		return fmt.Errorf("semlsm: validate edit: %w", err)
	}
	if err := db.manifest.Append(e); err != nil {
		return err
	}
	if err := db.manifest.Sync(); err != nil {
		return err
	}
	if _, aerr := db.vs.Apply(e); aerr != nil {
		// Unreachable: Apply's checks are identical to Validate's and the
		// set cannot change in between (manifestMu serializes all writers).
		return fmt.Errorf("semlsm: apply edit: %w", aerr)
	}
	// roll the manifest at the size threshold
	if db.manifest.Size() >= manifest.RollThreshold {
		return db.rewriteManifestLocked()
	}
	return nil
}

// flushRecovered flushes the recovery memtable into L0 via the normal flush
// path; walNum 0 means "no WAL to delete" and the edit advances log_num.
func (db *DB) flushRecovered(mt *memtable.Memtable, maxTs uint64) error {
	// all replayed WALs are fully durable in this SST; log_num may advance
	// to the active WAL
	return db.flushOne(immEntry{mem: mt, walNum: 0}, db.walNum)
}

func (db *DB) advanceLogNum() error {
	db.manifestMu.Lock()
	defer db.manifestMu.Unlock()
	e := &manifest.Edit{}
	e.LogNum = db.walNum
	return db.appendManifestLocked(e, db.vs.CurrentLastTs())
}

// cleanupOrphans removes replayed WALs, files not in the manifest, and .tmp
// files (P5.2, I6).
func (db *DB) cleanupOrphans(replayedWals []uint64) error {
	entries, err := db.opts.FS.List(db.dir)
	if err != nil {
		return fmt.Errorf("semlsm: list %s: %w", db.dir, err)
	}
	keepSST := map[uint64]bool{}
	keepCkpt := map[uint64]bool{}
	cur := db.vs.Acquire()
	defer cur.Unref()
	for _, level := range cur.Levels {
		for _, fm := range level {
			keepSST[fm.Num] = true
		}
	}
	for num := range cur.Ckpts.ByNum {
		keepCkpt[num] = true
	}
	oldWals := map[uint64]bool{}
	for _, n := range replayedWals {
		oldWals[n] = true
	}
	for _, e := range entries {
		if e.IsDir {
			// Includes lost+found: never sweep or recurse into quarantine.
			continue
		}
		num, ok, suffix := parseFileNum(e.Name)
		if !ok {
			// CURRENT, OPTIONS.json, LOCK stay; unknown junk left alone
			continue
		}
		path := filepath.Join(db.dir, e.Name)
		switch suffix {
		case ".wal":
			if oldWals[num] {
				_ = db.opts.FS.Remove(path)
			}
		case ".sst":
			if !keepSST[num] {
				_ = db.opts.FS.Remove(path)
			}
		case ".ckpt":
			if !keepCkpt[num] {
				_ = db.opts.FS.Remove(path)
			}
		case ".tmp":
			_ = db.opts.FS.Remove(path)
		}
	}

	// If even one legacy live checkpoint is unreadable, the archive union
	// is unknown. Preserve every archive on this Open.
	keepArchive, err := liveArchives(db.opts.FS, db.dir, cur)
	if err != nil {
		db.logger.Warn("semlsm: skipping archive orphan sweep: live archive set unknown", "err", err)
		return db.opts.FS.SyncDir(db.dir)
	}
	archDir := db.archiveDir()
	if ents, err := db.opts.FS.List(archDir); err == nil {
		removed := false
		for _, e := range ents {
			if e.IsDir {
				continue
			}
			if num, ok, suffix := parseFileNum(e.Name); ok && suffix == ".sst" && !keepArchive[num] {
				_ = db.opts.FS.Remove(filepath.Join(archDir, e.Name))
				removed = true
			}
		}
		if removed {
			_ = db.opts.FS.SyncDir(archDir)
		}
	}
	return db.opts.FS.SyncDir(db.dir)
}

func max3(a, b, c uint64) uint64 {
	m := a
	if b > m {
		m = b
	}
	if c > m {
		m = c
	}
	return m
}

// Close shuts down the DB (P5.6): waits for workers, syncs the WAL, releases
// LOCK. Leaked snapshots are forcibly released with a warning.
func (db *DB) Close() error {
	var firstErr error
	db.closeOnce.Do(func() {
		db.mu.Lock()
		db.closed.Store(true)
		// forcibly release leaked snapshots
		db.snapsMu.Lock()
		leaked := len(db.snaps)
		for s := range db.snaps {
			s.releaseLocked()
		}
		db.snaps = map[*Snapshot]struct{}{}
		db.snapsMu.Unlock()
		db.stallCond.Broadcast()
		db.mu.Unlock()
		if leaked > 0 {
			db.logger.Warn("semlsm: forcibly released leaked snapshots at close", "count", leaked)
		}

		// stop the commit goroutine; it completes queued requests first
		close(db.closeCh)
		db.commitWG.Wait()

		// wake and wait for the flush worker
		select {
		case db.flushWake <- struct{}{}:
		default:
		}
		db.flushWG.Wait()

		// wait for the compaction workers (flush and semantic jobs): they
		// exit on closeCh; a job mid-install must finish before the caches
		// and the manifest are torn down. Semantic jobs blocked in an LLM
		// call are cancelled first, or Close would wait out the job timeout.
		db.mu.Lock()
		for h := range db.semJobs {
			h.cancel()
		}
		db.mu.Unlock()
		db.compactWG.Wait()
		db.ckptMergeWG.Wait()
		// A persistent flush error leaves immutable WALs queued. Their data
		// is already synced by rotate, but their handles still belong to DB.
		if db.rs != nil {
			for _, im := range db.rs.imms {
				if im.wal != nil {
					if err := im.wal.Close(); err != nil && firstErr == nil {
						firstErr = err
					}
				}
			}
		}

		// sync + close WAL
		if db.wal != nil {
			if err := db.wal.Sync(); err != nil {
				firstErr = err
			}
			if err := db.wal.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
		// close manifest
		if db.manifest != nil {
			_ = db.manifest.Close()
		}
		// close cached readers
		if db.tableCache != nil {
			db.tableCache.CloseAll()
		}
		if db.ckptCache != nil {
			db.ckptCache.CloseAll()
		}
		// release the read state, then give the deleter the files that
		// freeing it makes obsolete, and only then stop the deleter
		// (M-032 note 2: files freed by the last release reach the disk).
		db.mu.Lock()
		if db.rs != nil {
			db.rs.release()
			db.rs = nil
		}
		db.mu.Unlock()
		db.drainObsolete()
		if db.obsolete != nil {
			db.obsolete.Stop()
		}

		if err := db.lockH.Unlock(); err != nil && firstErr == nil {
			firstErr = err
		}
	})
	return firstErr
}

// Stats returns a snapshot of engine statistics.
func (db *DB) Stats() Stats {
	db.mu.Lock()
	defer db.mu.Unlock()
	var s Stats
	if db.rs != nil {
		s.MemtableBytes = db.rs.mem.ApproxBytes()
		s.ImmutableMemtables = len(db.rs.imms)
		for l, files := range db.rs.version.Levels {
			s.LevelFiles[l] = len(files)
			for _, fm := range files {
				s.LevelBytes[l] += fm.Size
			}
		}
	}
	s.VisibleTs = db.visibleTs
	s.LastTs = db.lastTs
	s.StallTime = time.Duration(db.stallNanos.Load())
	s.SnapshotsOpen = int(db.snapshotsOpen.Load())
	s.CompactionErrors = db.compactErrs.Load()
	s.CheckpointMerges = db.ckptMerges.Load()
	s.CheckpointFilesMerged = db.ckptFilesMerged.Load()

	// Tier B accounting (P10.3). LLM health comes from the stack's live
	// state; the capability check keeps non-stack Summarizers working.
	s.SemanticTuplesCreated = db.semTuples.Load()
	s.SemanticRecordsSummarized = db.semRecs.Load()
	s.SemanticRawBytesRemoved = db.semRawBytes.Load()
	s.SemanticSummaryBytes = db.semSumBytes.Load()
	if s.SemanticSummaryBytes > 0 {
		s.SemanticCompressionRatio = float64(s.SemanticRawBytesRemoved) / float64(s.SemanticSummaryBytes)
	}
	s.LLMCalls = db.semCalls.Load()
	s.LLMInputTokens = db.semInTokens.Load()
	s.LLMOutputTokens = db.semOutTokens.Load()
	s.SemanticPreemptions = db.semPreemptions.Load()
	db.semStatsMu.Lock()
	s.LLMFailures = make(map[string]uint64, len(db.semFail))
	for k, v := range db.semFail {
		s.LLMFailures[k] = v
	}
	s.SemanticJobs = make(map[string]uint64, len(db.semJobsOut))
	for k, v := range db.semJobsOut {
		s.SemanticJobs[k] = v
	}
	db.semStatsMu.Unlock()
	// LLM health and cache hits come from the stack's live state via
	// capability checks, so non-stack Summarizers keep working.
	if h, ok := db.opts.Summarizer.(interface{ CacheHits() int64 }); ok {
		s.LLMCacheHits = uint64(h.CacheHits())
	}
	if h, ok := db.opts.Summarizer.(interface {
		BreakerState() (string, time.Duration)
	}); ok {
		s.LLMBreakerState, _ = h.BreakerState()
	}
	return s
}

// scanMaxFileNum raises the allocator above every file number that exists in
// the directory (M-013 item 1d): SSTs, WALs, ckpts and manifests.
func (db *DB) scanMaxFileNum() error {
	entries, err := db.opts.FS.List(db.dir)
	if err != nil {
		return fmt.Errorf("semlsm: scan file nums: %w", err)
	}
	max := db.vs.NextFileNum
	if db.nextFileNum > max {
		max = db.nextFileNum
	}
	for _, e := range entries {
		if e.IsDir {
			continue
		}
		if n, ok := manifest.ParseManifestNum(e.Name); ok {
			if n > max {
				max = n
			}
			continue
		}
		if n, ok, _ := parseFileNum(e.Name); ok {
			if n > max {
				max = n
			}
		}
	}
	db.manifestMu.Lock()
	db.nextFileNum = max
	db.manifestMu.Unlock()
	return nil
}
