package llm

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"path/filepath"
	"sort"
	"sync"

	"semlsm/internal/vfs"
)

// cacheEntry is the on-disk JSON of one cached response under
// <dbDir>/llm/cache/<hex>.json (P9.4).
type cacheEntry struct {
	Model         string `json:"model"`
	PromptVersion int    `json:"prompt_version"`
	Text          string `json:"text"`
	InputTokens   int    `json:"input_tokens"`
	OutputTokens  int    `json:"output_tokens"`
	CreatedUnix   int64  `json:"created_unix"`
}

// Cache is an on-disk response cache (P9.4). The key is the hex SHA-256 of
// model|promptVersion|system|user. Entries are written atomically; when the
// directory holds more than maxBytes, the oldest entries (by creation time
// on this clock) are evicted first. The caller stores only validated
// responses (the P9.6 stack enforces that with its Accept hook), so a crash
// or preemption never re-spends tokens on the same prompt.
type Cache struct {
	mu    sync.Mutex
	fs    vfs.FS
	dir   string
	max   int64
	clock Clock
}

// OpenCache opens (or initializes) the cache directory. maxBytes must be > 0.
func OpenCache(fs vfs.FS, dbDir string, maxBytes int64, clock Clock) (*Cache, error) {
	if maxBytes <= 0 {
		return nil, fmt.Errorf("llm: cache size cap must be > 0, got %d", maxBytes)
	}
	if clock == nil {
		clock = RealClock{}
	}
	dir := filepath.Join(dbDir, "llm", "cache")
	if err := fs.MkdirAll(dir); err != nil {
		return nil, fmt.Errorf("llm: create %s: %w", dir, err)
	}
	return &Cache{fs: fs, dir: dir, max: maxBytes, clock: clock}, nil
}

// CacheKey is the hex SHA-256 of model|promptVersion|system|user (P9.4).
func CacheKey(model string, promptVersion int, r Request) string {
	h := sha256.Sum256([]byte(fmt.Sprintf("%s|%d|%s|%s", model, promptVersion, r.System, r.User)))
	return hex.EncodeToString(h[:])
}

// Get returns the cached response for the request, or ok=false. A corrupt or
// mismatched entry is removed and treated as a miss.
func (c *Cache) Get(model string, promptVersion int, r Request) (Response, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	name := c.path(model, promptVersion, r)
	data, err := readFileAll(c.fs, name)
	if err != nil {
		return Response{}, false
	}
	var e cacheEntry
	if err := json.Unmarshal(data, &e); err != nil ||
		e.Model != model || e.PromptVersion != promptVersion {
		c.fs.Remove(name) // corrupt or stale shape: drop it
		return Response{}, false
	}
	return Response{
		Text:         e.Text,
		InputTokens:  e.InputTokens,
		OutputTokens: e.OutputTokens,
		Model:        e.Model,
	}, true
}

// Put stores a response under the request's key, then evicts the oldest
// entries while the directory exceeds the size cap. An entry larger than the
// whole cap is not stored. Errors are swallowed: the cache is best-effort and
// a failed write must never fail the call it serves.
func (c *Cache) Put(model string, promptVersion int, r Request, resp Response) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e := cacheEntry{
		Model:         model,
		PromptVersion: promptVersion,
		Text:          resp.Text,
		InputTokens:   resp.InputTokens,
		OutputTokens:  resp.OutputTokens,
		CreatedUnix:   c.clock.Now().Unix(),
	}
	data, err := json.Marshal(e)
	if err != nil {
		return
	}
	name := c.path(model, promptVersion, r)
	if int64(len(data)) > c.max {
		return
	}
	if err := writeFileAtomic(c.fs, name, data); err != nil {
		return
	}
	c.evictLocked()
}

func (c *Cache) path(model string, promptVersion int, r Request) string {
	return filepath.Join(c.dir, CacheKey(model, promptVersion, r)+".json")
}

type cacheFile struct {
	name    string
	size    int64
	created int64
}

// evictLocked removes oldest-first until the total size fits the cap
// (must hold mu). The listing includes the entry just written.
func (c *Cache) evictLocked() {
	entries, err := c.fs.List(c.dir)
	if err != nil {
		return
	}
	var files []cacheFile
	total := int64(0)
	for _, en := range entries {
		if en.IsDir || filepath.Ext(en.Name) != ".json" || len(en.Name) != 64+5 { // <hex>.json
			continue
		}
		total += en.Size
		files = append(files, cacheFile{name: en.Name, size: en.Size, created: c.createdOf(en.Name)})
	}
	if total <= c.max {
		return
	}
	sort.Slice(files, func(i, j int) bool { return files[i].created < files[j].created })
	for _, f := range files {
		if total <= c.max {
			return
		}
		if c.fs.Remove(filepath.Join(c.dir, f.name)) == nil {
			total -= f.size
		}
	}
}

// createdOf reads one entry's creation time; unreadable entries sort oldest
// so they are evicted first.
func (c *Cache) createdOf(name string) int64 {
	data, err := readFileAll(c.fs, filepath.Join(c.dir, name))
	if err != nil {
		return math.MinInt64
	}
	var e cacheEntry
	if json.Unmarshal(data, &e) != nil {
		return math.MinInt64
	}
	return e.CreatedUnix
}

// Entries returns the number of cached files (tests).
func (c *Cache) Entries() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	entries, err := c.fs.List(c.dir)
	if err != nil {
		return 0
	}
	n := 0
	for _, e := range entries {
		if !e.IsDir && filepath.Ext(e.Name) == ".json" {
			n++
		}
	}
	return n
}
