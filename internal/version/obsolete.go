package version

import (
	"errors"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/itsramhere/semlsm/internal/sstable"
	"github.com/itsramhere/semlsm/internal/vfs"
)

// ObsoleteFile is one file scheduled for deletion.
type ObsoleteFile struct {
	Path   string
	Num    uint64
	IsCkpt bool
}

// ObsoleteDeleter deletes obsolete files in the background: it evicts the
// table cache entry first, then removes the file with up to 5 retries at
// growing delays. Failures are left for startup cleanup (I6).
type ObsoleteDeleter struct {
	fs         vfs.FS
	tc         *sstable.TableCache // optional; may be nil
	retryDelay time.Duration       // base delay; doubles per retry
	logger     *slog.Logger

	mu       sync.Mutex
	queue    []ObsoleteFile
	failed   []ObsoleteFile
	wake     chan struct{}
	stopOnce sync.Once
	done     chan struct{}
	wg       sync.WaitGroup
}

// NewObsoleteDeleter builds a deleter. retryDelay <= 0 defaults to 100ms;
// tests inject small values.
func NewObsoleteDeleter(fs vfs.FS, tc *sstable.TableCache, retryDelay time.Duration) *ObsoleteDeleter {
	if retryDelay <= 0 {
		retryDelay = 100 * time.Millisecond
	}
	logger := slog.New(slog.DiscardHandler)
	return &ObsoleteDeleter{
		fs:         fs,
		tc:         tc,
		retryDelay: retryDelay,
		logger:     logger,
		wake:       make(chan struct{}, 1),
		done:       make(chan struct{}),
	}
}

// SetLogger attaches a logger for failure reports.
func (od *ObsoleteDeleter) SetLogger(l *slog.Logger) {
	if l != nil {
		od.logger = l
	}
}

// Enqueue schedules a file for deletion.
func (od *ObsoleteDeleter) Enqueue(f ObsoleteFile) {
	od.mu.Lock()
	od.queue = append(od.queue, f)
	od.mu.Unlock()
	select {
	case od.wake <- struct{}{}:
	default:
	}
}

// Start launches the worker goroutine.
func (od *ObsoleteDeleter) Start() {
	od.wg.Add(1)
	go od.loop()
}

// Stop signals the worker to exit after its current file and waits for it.
func (od *ObsoleteDeleter) Stop() {
	od.stopOnce.Do(func() {
		close(od.done)
	})
	od.wg.Wait()
}

// Failed returns files that could not be deleted (startup cleanup list).
func (od *ObsoleteDeleter) Failed() []ObsoleteFile {
	od.mu.Lock()
	defer od.mu.Unlock()
	return append([]ObsoleteFile(nil), od.failed...)
}

func (od *ObsoleteDeleter) pop() (ObsoleteFile, bool) {
	od.mu.Lock()
	defer od.mu.Unlock()
	if len(od.queue) == 0 {
		return ObsoleteFile{}, false
	}
	f := od.queue[0]
	od.queue = od.queue[1:]
	return f, true
}

func (od *ObsoleteDeleter) loop() {
	defer od.wg.Done()
	for {
		select {
		case <-od.done:
			// drain what is already queued before exiting
			for {
				f, ok := od.pop()
				if !ok {
					return
				}
				od.deleteOne(f)
			}
		case <-od.wake:
			for {
				f, ok := od.pop()
				if !ok {
					break
				}
				od.deleteOne(f)
				select {
				case <-od.done:
					// switch to drain mode
					for {
						g, ok := od.pop()
						if !ok {
							return
						}
						od.deleteOne(g)
					}
				default:
				}
			}
		}
	}
}

// deleteOne evicts the cache entry and removes the file with retries.
// A missing file counts as success (M-011 item 6): replaying add-then-delete
// edits at recovery must not burn retries on already-removed files.
func (od *ObsoleteDeleter) deleteOne(f ObsoleteFile) {
	if od.tc != nil && !f.IsCkpt {
		od.tc.Evict(f.Num)
	}
	delay := od.retryDelay
	var lastErr error
	for attempt := 0; attempt < 5; attempt++ {
		err := od.fs.Remove(f.Path)
		if err == nil || errors.Is(err, os.ErrNotExist) {
			return
		}
		lastErr = err
		if attempt < 4 {
			time.Sleep(delay)
			delay *= 2
		}
	}
	od.mu.Lock()
	od.failed = append(od.failed, f)
	od.mu.Unlock()
	if lastErr != nil {
		od.logger.Warn("obsolete file deletion failed after retries",
			"path", f.Path, "num", f.Num, "err", lastErr)
	}
}
