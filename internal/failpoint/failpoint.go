// Package failpoint provides named failure hooks used by crash tests.
//
// Hook names are registered up front with Define (typically as package-level
// vars, e.g. var fpAfterSST = failpoint.Define("flush.after_sst_sync")).
// List reports all defined names, so P13.1's crash matrix can enumerate every
// hook in the codebase even when none is currently armed.
//
// Hit costs one atomic load when no hook is armed (the normal case).
package failpoint

import (
	"sort"
	"sync"
	"sync/atomic"
)

var (
	mu      sync.RWMutex
	hooks   = map[string]func() error{}
	defined = map[string]bool{}
	active  atomic.Int64 // number of armed hooks; 0 => Hit is a no-op
)

// Define registers a hook name permanently and returns it, so callers can
// write var fpName = failpoint.Define("pkg.name") and Hit(fpName).
func Define(name string) string {
	mu.Lock()
	defer mu.Unlock()
	defined[name] = true
	return name
}

// Hit runs the hook registered for name, returning its error.
// With no hook armed anywhere it returns nil immediately (atomic check).
func Hit(name string) error {
	if active.Load() == 0 {
		return nil
	}
	mu.RLock()
	fn, ok := hooks[name]
	mu.RUnlock()
	if !ok {
		return nil
	}
	return fn()
}

// Set arms fn as the hook for name. fn may return nil.
// A nil fn disarms the hook.
func Set(name string, fn func() error) {
	mu.Lock()
	defer mu.Unlock()
	if fn == nil {
		delete(hooks, name)
	} else {
		hooks[name] = fn
	}
	active.Store(int64(len(hooks)))
}

// SetError arms a hook that always returns err.
func SetError(name string, err error) {
	Set(name, func() error { return err })
}

// Reset disarms all hooks but keeps the definitions from Define.
func Reset() {
	mu.Lock()
	defer mu.Unlock()
	hooks = map[string]func() error{}
	active.Store(0)
}

// List returns all names passed to Define, sorted.
func List() []string {
	mu.RLock()
	defer mu.RUnlock()
	out := make([]string, 0, len(defined))
	for name := range defined {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}
