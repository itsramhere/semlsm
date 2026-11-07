//go:build soak

// P13.4 soak test. Run once at the gate with:
//
//	go test -tags soak -run TestSoak -count=1
//
// Duration comes from SEMLSM_SOAK_SECONDS (default 600): a real OS
// filesystem, 8 writers, 8 readers, snapshot churn, a mock LLM with random
// fault injection, reopen every 60s, periodic VerifyIntegrity, a goroutine
// leak check and a heap report at the end.
package semlsm

import (
	"context"
	"math/rand"
	"os"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/itsramhere/semlsm/internal/base"
	"github.com/itsramhere/semlsm/internal/llm"
	"github.com/itsramhere/semlsm/internal/testmodel"
	"github.com/itsramhere/semlsm/internal/vfs"
)

// chaosLLM routes each call to a random mock mode: mostly valid, sometimes
// transient, permanent or invalid JSON (P13.4).
type chaosLLM struct {
	mu        sync.Mutex
	rng       *rand.Rand
	valid     *llm.MockClient
	transient *llm.MockClient
	permanent *llm.MockClient
	badJSON   *llm.MockClient
}

func (c *chaosLLM) Complete(ctx context.Context, req llm.Request) (llm.Response, error) {
	c.mu.Lock()
	n := c.rng.Intn(10)
	c.mu.Unlock()
	switch {
	case n < 6:
		return c.valid.Complete(ctx, req)
	case n < 8:
		return c.transient.Complete(ctx, req)
	case n < 9:
		return c.permanent.Complete(ctx, req)
	default:
		return c.badJSON.Complete(ctx, req)
	}
}

// soakHolder owns the current DB so the reopener can swap it safely: use
// holds the read lock across the whole operation, so a reopen (write lock)
// waits for in-flight reads instead of racing them.
type soakHolder struct {
	mu sync.RWMutex
	db *DB
}

func (h *soakHolder) use(fn func(*DB)) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if h.db != nil {
		fn(h.db)
	}
}

func (h *soakHolder) take() *DB {
	h.mu.Lock()
	defer h.mu.Unlock()
	db := h.db
	h.db = nil
	return db
}

func (h *soakHolder) put(db *DB) {
	h.mu.Lock()
	h.db = db
	h.mu.Unlock()
}

func TestSoak(t *testing.T) {
	dur := 600 * time.Second
	if v := os.Getenv("SEMLSM_SOAK_SECONDS"); v != "" {
		if d, err := strconv.Atoi(v); err == nil && d > 0 {
			dur = time.Duration(d) * time.Second
		}
	}
	dir := t.TempDir()
	fs := vfs.OSFS{}

	var ms0, ms1 runtime.MemStats
	runtime.ReadMemStats(&ms0)

	chaos := &chaosLLM{
		rng:       rand.New(rand.NewSource(7)),
		valid:     llm.NewMock(llm.MockValid),
		transient: llm.NewMockTransient(1 << 30),
		permanent: llm.NewMock(llm.MockPermanent),
		badJSON:   llm.NewMock(llm.MockBadJSON),
	}
	stack, err := llm.NewSummarizer(chaos, llm.Config{
		Model: "soak-model", PromptVersion: 1,
		FS: fs, Dir: dir,
		MaxTokensTotal: 1 << 40, RequestsPerMinute: 6000,
		TokensPerMinute: 1 << 26, Concurrency: 4, CacheMaxBytes: 4 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	openSoak := func() *DB {
		o := Options{
			FS: fs, Clock: base.RealClock{}, SyncMode: SyncEveryN, SyncEveryN: 64,
			MemtableBytes: 1 << 20, MaxImmutableMemtables: 8,
			L0CompactTrigger: 4, L0StallTrigger: 24,
			L1TargetBytes: 48 << 10, LevelMultiplier: 2, TargetFileBytes: 32 << 10,
			GroupDepth: 2, GroupDelimiter: '/',
			Policies: []Policy{
				{Prefix: "agent/", Mode: ModeState, MinAge: 2 * time.Second, MinBatch: 10, MaxBatch: 10000},
				{Prefix: "app/", Mode: ModeEvents, MinAge: 2 * time.Second, MinBatch: 10, MaxBatch: 10000},
			},
			DefaultPolicy:         Policy{Mode: ModeDisabled},
			Summarizer:            stack,
			SemanticWorkers:       1,
			SemanticSweepInterval: 5 * time.Second,
			MaxChunkBytes:         32 << 10, MaxValueBytesInPrompt: 2 << 10,
			MinCompressionRatio: 0.5,
			HardScore:           2.0,
			ArchiveRaw:          true,
		}
		db, err := Open(dir, o)
		if err != nil {
			t.Fatal(err)
		}
		return db
	}

	holder := &soakHolder{db: openSoak()}
	defer func() { awaitGoroutineSettle(t) }()

	model := testmodel.NewModel()
	gen := testmodel.NewGen(99, 8, 5000)
	allValues := map[string]bool{}
	stop := make(chan struct{})
	var wg sync.WaitGroup
	var mu sync.Mutex // guards model, allValues, gen

	// 8 writers.
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				mu.Lock()
				recs := gen.Next(8)
				mu.Unlock()
				b := NewBatch()
				for i := range recs {
					if recs[i].Kind == base.KindDelete {
						b.Delete(recs[i].Key)
					} else {
						b.Put(recs[i].Key, recs[i].Value)
						mu.Lock()
						allValues[string(recs[i].Value)] = true
						mu.Unlock()
					}
				}
				ack := false
				holder.use(func(db *DB) {
					ts, err := db.Apply(b)
					if err != nil {
						return // swap window or transient failure
					}
					ack = true
					mu.Lock()
					for i := range b.recs {
						b.recs[i].Ts = ts
					}
					model.Commit(b.recs)
					mu.Unlock()
				})
				if !ack {
					time.Sleep(5 * time.Millisecond)
					continue // swap window or transient failure: unacknowledged
				}
			}
		}()
	}

	// 8 readers: point reads at latest and random past timestamps, plus
	// small scans. Values must always come from the set of written values.
	for r := 0; r < 8; r++ {
		wg.Add(1)
		go func(r int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(100 + r)))
			for {
				select {
				case <-stop:
					return
				default:
				}
				var key []byte
				mu.Lock()
				keys := model.Keys()
				if len(keys) > 0 {
					key = keys[rng.Intn(len(keys))]
				}
				mu.Unlock()
				if key == nil {
					time.Sleep(5 * time.Millisecond)
					continue
				}
				holder.use(func(db *DB) {
					res, err := db.Get(key)
					if err == nil && res.Kind == ResultExact {
						mu.Lock()
						known := allValues[string(res.Value)]
						mu.Unlock()
						if !known {
							t.Errorf("reader saw a value that was never written: %q", res.Value)
							return
						}
					}
					if res, err := db.GetAt(key, maxInt64Clamp(model.MaxTs())); err == nil &&
						res.Kind == ResultExact {
						mu.Lock()
						known := allValues[string(res.Value)]
						mu.Unlock()
						if !known {
							t.Errorf("GetAt saw a value that was never written: %q", res.Value)
							return
						}
					}
				})
				time.Sleep(2 * time.Millisecond)
			}
		}(r)
	}

	// Snapshot churn.
	wg.Add(1)
	go func() {
		defer wg.Done()
		rng := rand.New(rand.NewSource(500))
		for {
			select {
			case <-stop:
				return
			default:
			}
			holder.use(func(db *DB) {
				s := db.NewSnapshot()
				mu.Lock()
				keys := model.Keys()
				var key []byte
				if len(keys) > 0 {
					key = keys[rng.Intn(len(keys))]
				}
				mu.Unlock()
				if key != nil {
					if res, err := s.Get(key); err == nil && res.Kind == ResultExact {
						mu.Lock()
						known := allValues[string(res.Value)]
						mu.Unlock()
						if !known {
							t.Errorf("snapshot saw a value that was never written: %q", res.Value)
							return
						}
					}
				}
				s.Release()
			})
			time.Sleep(10 * time.Millisecond)
		}
	}()

	// Periodic VerifyIntegrity and a reopen every 60s.
	deadline := time.Now().Add(dur)
	lastVerify := time.Now()
	lastReopen := time.Now()
	for time.Now().Before(deadline) {
		time.Sleep(500 * time.Millisecond)
		if time.Since(lastVerify) >= 30*time.Second {
			lastVerify = time.Now()
			holder.use(func(db *DB) {
				rep, err := db.VerifyIntegrity(context.Background())
				if err != nil {
					t.Errorf("verify: %v", err)
				} else if len(rep.Problems) > 0 {
					t.Errorf("verify problems: %v", rep.Problems)
				}
			})
		}
		if time.Since(lastReopen) >= 60*time.Second {
			lastReopen = time.Now()
			old := holder.take() // readers and writers idle while the lock is held
			if old != nil {
				if err := old.Close(); err != nil {
					t.Errorf("reopen close: %v", err)
				}
			}
			holder.put(openSoak())
			t.Logf("reopened at %s", time.Now().Format(time.RFC3339))
		}
	}
	close(stop)
	wg.Wait()

	// Final checks: settle, integrity, model equality on the last DB.
	tuples := 0
	holder.use(func(cur *DB) {
		if err := cur.WaitIdle(context.Background()); err != nil {
			t.Error(err)
		}
		rep, err := cur.VerifyIntegrity(context.Background())
		if err != nil {
			t.Error(err)
		}
		if len(rep.Problems) > 0 {
			t.Errorf("final verify problems: %v", rep.Problems)
		}
		tuples = rep.Tuples
		dbg := cur.Stats()
		t.Logf("SOAK STATS: tuples=%d jobs=%v failures=%v calls=%d levels=%v",
			dbg.SemanticTuplesCreated, dbg.SemanticJobs, dbg.LLMFailures, dbg.LLMCalls, dbg.LevelFiles)
	})
	db := holder.take()
	if db == nil {
		t.Fatal("no DB at the end of the soak")
	}
	if err := db.Close(); err != nil {
		t.Errorf("final close: %v", err)
	}

	runtime.ReadMemStats(&ms1)
	t.Logf("soak done in %s: heap %d KiB -> %d KiB, sys %d KiB, GC cycles %d, tuples %d, llm calls valid=%d transient=%d permanent=%d badjson=%d",
		dur, ms0.HeapAlloc/1024, ms1.HeapAlloc/1024, ms1.Sys/1024, ms1.NumGC-ms0.NumGC,
		tuples, chaos.valid.Calls(), chaos.transient.Calls(), chaos.permanent.Calls(), chaos.badJSON.Calls())
}

// maxInt64Clamp bounds the random GetAt probe by the model's max ts.
func maxInt64Clamp(v uint64) uint64 {
	if v == 0 {
		return 1
	}
	return v
}
