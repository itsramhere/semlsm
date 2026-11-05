// Package failpoint provides named failure hooks used by crash tests.
// Hooks are compiled always; Hit costs one atomic map lookup behind an
// atomic load and returns nil immediately when no hook is set.
package failpoint

import "sync"

var (
	mu    sync.RWMutex
	hooks = map[string]func() error{}
)

// Hit runs the hook registered for name, returning its error.
// With no hook set (the normal case) it is near zero cost.
func Hit(name string) error {
	mu.RLock()
	fn, ok := hooks[name]
	mu.RUnlock()
	if !ok {
		return nil
	}
	return fn()
}

// Set registers fn as the hook for name. fn may return nil.
// A nil fn removes the hook.
func Set(name string, fn func() error) {
	mu.Lock()
	defer mu.Unlock()
	if fn == nil {
		delete(hooks, name)
		return
	}
	hooks[name] = fn
}

// SetError registers a hook that always returns err.
func SetError(name string, err error) {
	Set(name, func() error { return err })
}

// Reset removes all hooks. Tests must defer Reset to avoid cross-test bleed.
func Reset() {
	mu.Lock()
	defer mu.Unlock()
	hooks = map[string]func() error{}
}

// List returns the names of all currently registered hooks.
func List() []string {
	mu.RLock()
	defer mu.RUnlock()
	out := make([]string, 0, len(hooks))
	for name := range hooks {
		out = append(out, name)
	}
	return out
}
