package semlsm

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"time"

	"semlsm/internal/base"
	"semlsm/internal/memtable"
	"semlsm/internal/wal"
)

// maxCommitGroup is the largest number of requests drained per group commit.
const maxCommitGroup = 256

// errDBFailed is the sticky error marker for the commit path.
var errDBFailed = errors.New("semlsm: write failed; database is in a failed state")

// Batch is an order-preserving set of writes; the last write to a key wins
// at build time, so a key appears at most once per batch (I2).
type Batch struct {
	recs []base.Record
}

// NewBatch returns an empty batch.
func NewBatch() *Batch { return &Batch{} }

// Put adds or replaces the write for key (copies both slices).
func (b *Batch) Put(key, value []byte) {
	b.put(base.KindPut, key, value)
}

// Delete adds or replaces a tombstone for key.
func (b *Batch) Delete(key []byte) {
	b.put(base.KindDelete, key, nil)
}

func (b *Batch) put(kind base.Kind, key, value []byte) {
	for i := range b.recs {
		if string(b.recs[i].Key) == string(key) {
			b.recs[i].Kind = kind
			b.recs[i].Value = append([]byte(nil), value...)
			return
		}
	}
	b.recs = append(b.recs, base.Record{
		Key:   append([]byte(nil), key...),
		Kind:  kind,
		Value: append([]byte(nil), value...),
	})
}

// Len returns the number of records.
func (b *Batch) Len() int { return len(b.recs) }

// Put writes one key (a single-record batch).
func (db *DB) Put(key, value []byte) (uint64, error) {
	b := NewBatch()
	b.Put(key, value)
	return db.Apply(b)
}

// Delete writes one tombstone.
func (db *DB) Delete(key []byte) (uint64, error) {
	b := NewBatch()
	b.Delete(key)
	return db.Apply(b)
}

// Apply commits a batch atomically and returns its commit timestamp.
func (db *DB) Apply(b *Batch) (uint64, error) {
	if db.closed.Load() {
		return 0, ErrClosed
	}
	db.mu.Lock()
	failed := db.failedErr
	db.mu.Unlock()
	if failed != nil {
		return 0, fmt.Errorf("%w: %v", errDBFailed, failed)
	}
	// validate every record before entering the queue
	for i := range b.recs {
		if err := base.ValidateKV(b.recs[i].Key, b.recs[i].Value); err != nil {
			return 0, err
		}
	}
	req := &commitReq{recs: b.recs, done: make(chan error, 1)}
	select {
	case db.commitQ <- req:
	case <-db.closeCh:
		return 0, ErrClosed
	}
	if err := <-req.done; err != nil {
		return 0, err
	}
	return req.ts, nil
}

// commitLoop is the single writer goroutine (B7).
func (db *DB) commitLoop() {
	defer db.commitWG.Done()
	for {
		select {
		case req, ok := <-db.commitQ:
			if !ok {
				return
			}
			group := []*commitReq{req}
		drain:
			for len(group) < maxCommitGroup {
				select {
				case r, ok := <-db.commitQ:
					if !ok {
						break drain
					}
					group = append(group, r)
				default:
					break drain
				}
			}
			db.commitGroup(group)
		case <-db.closeCh:
			// complete anything already queued, then exit
			for {
				select {
				case req, ok := <-db.commitQ:
					if !ok {
						return
					}
					db.commitGroup([]*commitReq{req})
				default:
					return
				}
			}
		}
	}
}

// stallWait blocks while the flush pipeline is over capacity, recording the
// stall time (P5.3). Uses a cond broadcast by flush installs; Wait releases
// db.mu while blocked so readers proceed.
func (db *DB) stallWait() {
	start := time.Now()
	db.mu.Lock()
	for db.stalledLocked() {
		db.stallCond.Wait()
	}
	db.mu.Unlock()
	db.stallNanos.Add(int64(time.Since(start)))
}

func (db *DB) stalledLocked() bool {
	return db.rs != nil &&
		(len(db.rs.imms) >= db.opts.MaxImmutableMemtables ||
			len(db.rs.version.Levels[0]) >= db.opts.L0StallTrigger)
}

// commitGroup performs one group commit: WAL frames, sync per policy,
// memtable apply, visibleTs publish, then wake writers (B7).
func (db *DB) commitGroup(group []*commitReq) {
	db.stallWait()

	db.mu.Lock()
	closed := db.closed.Load()
	failed := db.failedErr
	db.mu.Unlock()
	if closed {
		db.completeGroup(group, ErrClosed)
		return
	}
	if failed != nil {
		db.completeGroup(group, fmt.Errorf("%w: %v", errDBFailed, failed))
		return
	}

	// Assign timestamps: every batch gets its own strictly increasing ts
	// (I2: records within one batch share it). An empty batch still consumes
	// a timestamp; gaps are allowed.
	now := uint64(db.opts.Clock.Now().UnixNano())

	if len(group) == 1 && len(group[0].recs) == 0 {
		ts := db.lastTs + 1
		if now > ts {
			ts = now
		}
		db.lastTs = ts
		group[0].ts = ts
		db.mu.Lock()
		db.visibleTs = ts
		db.mu.Unlock()
		if group[0].flush {
			// Flush: rotate so the active memtable reaches L0
			if err := db.rotate(); err != nil {
				db.setFailed(err)
				group[0].done <- err
				return
			}
		}
		group[0].done <- nil
		return
	}

	for _, req := range group {
		ts := db.lastTs + 1
		if now > ts {
			ts = now
		}
		db.lastTs = ts
		for i := range req.recs {
			req.recs[i].Ts = ts
		}
		req.ts = ts
	}

	// WAL append.
	for _, req := range group {
		payload, err := wal.EncodeBatch(req.ts, req.recs)
		if err != nil {
			db.setFailed(err)
			db.completeGroup(group, err)
			return
		}
		frame, err := wal.Frame(payload)
		if err != nil {
			db.setFailed(err)
			db.completeGroup(group, err)
			return
		}
		if err := db.wal.AppendFrame(frame); err != nil {
			db.setFailed(err)
			db.completeGroup(group, err)
			return
		}
	}

	// Sync per policy.
	switch db.opts.SyncMode {
	case SyncAlways:
		if err := db.wal.Sync(); err != nil {
			db.setFailed(err)
			db.completeGroup(group, err)
			return
		}
	case SyncEveryN:
		db.syncCnt += uint64(len(group))
		if db.syncCnt >= uint64(db.opts.SyncEveryN) {
			db.syncCnt = 0
			if err := db.wal.Sync(); err != nil {
				db.setFailed(err)
				db.completeGroup(group, err)
				return
			}
		}
	case SyncNone:
	}

	// Apply to the active memtable (commit goroutine is the single writer).
	for _, req := range group {
		for i := range req.recs {
			db.mem.Add(base.MakeInternalKey(req.recs[i].Key, req.recs[i].Ts, req.recs[i].Kind), req.recs[i].Value)
		}
	}

	// Rotate if the memtable exceeds its budget.
	if db.mem.ApproxBytes() >= db.opts.MemtableBytes {
		if err := db.rotate(); err != nil {
			db.setFailed(err)
			db.completeGroup(group, err)
			return
		}
	}

	// Explicit flush requests: rotate the now-current memtable so its data
	// reaches L0 (Flush).
	anyFlush := false
	for _, req := range group {
		if req.flush {
			anyFlush = true
		}
	}
	if anyFlush {
		if err := db.rotate(); err != nil {
			db.setFailed(err)
			db.completeGroup(group, err)
			return
		}
	}

	// Publish: all batches in the group become visible atomically.
	db.mu.Lock()
	db.visibleTs = group[len(group)-1].ts
	db.mu.Unlock()
	db.completeGroup(group, nil)
}

// rotate swaps in a fresh memtable and WAL (commit goroutine only, B7).
func (db *DB) rotate() error {
	db.manifestMu.Lock()
	newNum := db.allocateFileNum()
	db.nextFileNumSnapshot()
	db.manifestMu.Unlock()
	w, err := wal.NewWriter(db.opts.FS, walPath(db.dir, newNum))
	if err != nil {
		return fmt.Errorf("semlsm: rotate wal: %w", err)
	}
	if err := db.opts.FS.SyncDir(db.dir); err != nil {
		return fmt.Errorf("semlsm: rotate wal syncdir: %w", err)
	}
	// make the old WAL durable before handing its memtable to the flusher
	if err := db.wal.Sync(); err != nil {
		return fmt.Errorf("semlsm: rotate wal sync: %w", err)
	}
	oldNum := db.walNum
	oldMem := db.mem
	oldWal := db.wal

	db.mu.Lock()
	db.wal = w
	db.walNum = newNum
	db.mem = memtable.New()
	if db.rs != nil {
		// prepend: newest immutable first (B6 raw lookup order)
		// B7: a readState is immutable once built - any change creates a new
		// one. Prepend the newest immutable (B6 raw lookup order). The
		// version reference transfers to the new readState.
		newImms := make([]immEntry, 0, len(db.rs.imms)+1)
		newImms = append(newImms, immEntry{mem: oldMem, walNum: oldNum, wal: oldWal})
		newImms = append(newImms, db.rs.imms...)
		db.rs = &readState{mem: db.mem, imms: newImms, version: db.rs.version}
	}
	db.mu.Unlock()
	db.wakeFlush()
	return nil
}

func (db *DB) setFailed(err error) {
	db.mu.Lock()
	if db.failedErr == nil {
		db.failedErr = err
	}
	db.mu.Unlock()
	db.logger.Error("semlsm: commit path failed", "err", err)
}

func (db *DB) completeGroup(group []*commitReq, err error) {
	for _, req := range group {
		req.done <- err
	}
}

func walPath(dir string, num uint64) string {
	return filepath.Join(dir, walName(num))
}

// wakeFlush signals the flush worker.
func (db *DB) wakeFlush() {
	select {
	case db.flushWake <- struct{}{}:
	default:
	}
}

// Flush rotates the active memtable and waits for it to reach L0 (B5).
func (db *DB) Flush() error {
	if db.closed.Load() {
		return ErrClosed
	}
	req := &commitReq{done: make(chan error, 1), flush: true}
	select {
	case db.commitQ <- req:
	case <-db.closeCh:
		return ErrClosed
	}
	if err := <-req.done; err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return db.WaitIdle(ctx)
}
