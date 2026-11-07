package checkpoint

import "sync"

// CkptCache is a ref-counted LRU of open .ckpt Readers keyed by file number,
// mirroring the sstable TableCache semantics (P7.1).
type CkptCache struct {
	mu       sync.Mutex
	capacity int
	m        map[uint64]*cacheEntry
	order    []uint64 // LRU order, oldest last
	open     func(num uint64) (*Reader, error)
}

type cacheEntry struct {
	r       *Reader
	refs    int
	removed bool
}

// NewCkptCache builds a cache with the given capacity and opener.
func NewCkptCache(capacity int, open func(num uint64) (*Reader, error)) *CkptCache {
	if capacity <= 0 {
		capacity = 64
	}
	return &CkptCache{capacity: capacity, m: map[uint64]*cacheEntry{}, open: open}
}

// Get returns the reader for num with one reference held.
func (c *CkptCache) Get(num uint64) (*Reader, func(), error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if e, ok := c.m[num]; ok {
		e.refs++
		c.touch(num)
		return e.r, c.release(e), nil
	}
	r, err := c.open(num)
	if err != nil {
		return nil, nil, err
	}
	e := &cacheEntry{r: r, refs: 1}
	c.m[num] = e
	c.order = append([]uint64{num}, c.order...)
	c.evict()
	return r, c.release(e), nil
}

func (c *CkptCache) touch(num uint64) {
	for i, n := range c.order {
		if n == num {
			c.order = append(c.order[:i], c.order[i+1:]...)
			break
		}
	}
	c.order = append([]uint64{num}, c.order...)
}

func (c *CkptCache) release(e *cacheEntry) func() {
	once := false
	return func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		if once {
			return
		}
		once = true
		e.refs--
		if e.refs == 0 && e.removed {
			_ = e.r.Close()
		}
	}
}

// Evict removes a dead checkpoint from the cache. Outstanding borrowers keep
// its reader until their last release, just like the table cache.
func (c *CkptCache) Evict(num uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e := c.m[num]
	if e == nil {
		return
	}
	delete(c.m, num)
	for i, n := range c.order {
		if n == num {
			c.order = append(c.order[:i], c.order[i+1:]...)
			break
		}
	}
	e.removed = true
	if e.refs == 0 {
		_ = e.r.Close()
	}
}

func (c *CkptCache) evict() {
	for len(c.m) > c.capacity {
		var victim uint64
		found := false
		for i := len(c.order) - 1; i >= 0; i-- {
			if e := c.m[c.order[i]]; e != nil && e.refs == 0 {
				victim = c.order[i]
				found = true
				break
			}
		}
		if !found {
			return
		}
		if e := c.m[victim]; e != nil {
			_ = e.r.Close()
			delete(c.m, victim)
		}
		for i, n := range c.order {
			if n == victim {
				c.order = append(c.order[:i], c.order[i+1:]...)
				break
			}
		}
	}
}

// CloseAll closes all readers (DB shutdown).
func (c *CkptCache) CloseAll() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, e := range c.m {
		_ = e.r.Close()
	}
	c.m = map[uint64]*cacheEntry{}
	c.order = nil
}
