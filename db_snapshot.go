package semlsm

import (
	"sync/atomic"

	"semlsm/internal/base"
)

// Summary is defined fully in P8; the empty type keeps the API shape.
type Summary struct {
	SchemaVersion int
	Stats         StatsBlock
	Narrative     Narrative
}

// StatsBlock and Narrative are placeholders until P8 defines them.
type StatsBlock struct {
	RecordCount  uint64
	PutCount     uint64
	DeleteCount  uint64
	DistinctKeys uint64
	FirstTs      uint64
	LastTs       uint64
	ValueBytes   uint64
}

type Narrative struct {
	Summary    string
	KeyFacts   []Fact
	Anomalies  []Fact
	FinalState string
}

type Fact struct {
	Text string
	Refs []string
}

// Snapshot is a stable read point (I9): it pins its version and memtables so
// every read returns identical results for its whole life.
type Snapshot struct {
	db       *DB
	ts       uint64
	rs       *readState
	released atomic.Bool
}

// NewSnapshot returns a snapshot at the current visible timestamp.
func (db *DB) NewSnapshot() *Snapshot {
	rs, visible := db.acquireReadState()
	s := &Snapshot{db: db, ts: visible, rs: rs}
	db.mu.Lock()
	db.snaps[s] = struct{}{}
	db.mu.Unlock()
	db.snapshotsOpen.Add(1)
	return s
}

// OpenSnapshotAt returns a snapshot at the given timestamp; a timestamp
// newer than the last visible commit returns ErrFutureTimestamp.
func (db *DB) OpenSnapshotAt(ts uint64) (*Snapshot, error) {
	rs, visible := db.acquireReadState()
	if rs == nil {
		return nil, ErrClosed
	}
	if ts > visible {
		releaseReadState(rs)
		return nil, ErrFutureTimestamp
	}
	s := &Snapshot{db: db, ts: ts, rs: rs}
	db.mu.Lock()
	db.snaps[s] = struct{}{}
	db.mu.Unlock()
	db.snapshotsOpen.Add(1)
	return s, nil
}

// Ts returns the snapshot timestamp.
func (s *Snapshot) Ts() uint64 { return s.ts }

// Get reads key at the snapshot timestamp.
func (s *Snapshot) Get(key []byte) (Result, error) {
	if s.released.Load() {
		return Result{}, ErrReleased
	}
	if err := validateKeyForRead(key); err != nil {
		return Result{}, err
	}
	return s.db.getAtState(key, s.ts, s.rs)
}

// NewIterator returns an iterator over the snapshot.
func (s *Snapshot) NewIterator(o IterOptions) *Iterator {
	if s.released.Load() {
		it := &Iterator{err: ErrReleased}
		return it
	}
	// The iterator owns its own reference: take one on the snapshot's
	// pinned version, because Iterator.Close releases exactly the readState
	// it is given. Sharing s.rs directly would let every iterator Close
	// consume one of the snapshot's references and eventually drive the
	// version's refcount below zero (found by the P11.4 model test).
	s.rs.version.Ref()
	return s.db.newIteratorAt(o, s.ts,
		&readState{mem: s.rs.mem, imms: s.rs.imms, version: s.rs.version}, s)
}

// Release unpins the snapshot. Idempotent.
func (s *Snapshot) Release() {
	if s.released.Swap(true) {
		return
	}
	s.db.mu.Lock()
	delete(s.db.snaps, s)
	if s.db.rs != nil {
		releaseReadState(s.rs)
	}
	s.db.mu.Unlock()
	s.db.snapshotsOpen.Add(-1)
}

// releaseLocked is used by Close on leaked snapshots (mu held).
func (s *Snapshot) releaseLocked() {
	if s.released.Swap(true) {
		return
	}
	if s.db.rs != nil && s.rs != nil {
		releaseReadState(s.rs)
	}
	s.db.snapshotsOpen.Add(-1)
}

func validateKeyForRead(key []byte) error {
	return base.ValidateKV(key, nil)
}
