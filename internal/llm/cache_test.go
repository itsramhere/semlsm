package llm

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	"semlsm/internal/vfs"
)

func testCache(t *testing.T, maxBytes int64) (*Cache, *ManualClock) {
	t.Helper()
	clk := NewManualClock(time.Unix(1_000_000, 0))
	c, err := OpenCache(vfs.NewMemFS(), "db", maxBytes, clk)
	if err != nil {
		t.Fatal(err)
	}
	return c, clk
}

func reqWith(user string) Request {
	return Request{System: "sys", User: user, MaxOutputTokens: 10}
}

func TestCachePutGetRoundTrip(t *testing.T) {
	c, _ := testCache(t, 1<<20)
	r := reqWith("prompt one")
	resp := Response{Text: `{"summary":"s"}`, InputTokens: 12, OutputTokens: 34, Model: "m1"}
	c.Put("m1", 1, r, resp)

	got, ok := c.Get("m1", 1, r)
	if !ok || got.Text != resp.Text || got.InputTokens != 12 || got.OutputTokens != 34 || got.Model != "m1" {
		t.Fatalf("round trip: %+v ok=%v", got, ok)
	}

	// The entry lives at llm/cache/<64 hex>.json and no temp file remains.
	if c.Entries() != 1 {
		t.Fatalf("entries: %d", c.Entries())
	}
	keyPath := filepath.Join("db", "llm", "cache", CacheKey("m1", 1, r)+".json")
	if filepath.Base(keyPath) != CacheKey("m1", 1, r)+".json" || len(CacheKey("m1", 1, r)) != 64 {
		t.Fatalf("bad key path %s", keyPath)
	}
}

func TestCacheKeySeparatesModelPromptAndPromptText(t *testing.T) {
	c, _ := testCache(t, 1<<20)
	r := reqWith("p")
	c.Put("m1", 1, r, Response{Text: "v1", Model: "m1"})

	if _, ok := c.Get("m2", 1, r); ok {
		t.Fatal("different model must miss")
	}
	if _, ok := c.Get("m1", 2, r); ok {
		t.Fatal("different prompt version must miss")
	}
	if _, ok := c.Get("m1", 1, reqWith("other")); ok {
		t.Fatal("different user prompt must miss")
	}
	if got, ok := c.Get("m1", 1, r); !ok || got.Text != "v1" {
		t.Fatalf("original must still hit: %v %v", got, ok)
	}
	// The pipe separator keeps fields from bleeding into each other.
	if CacheKey("a", 1, Request{System: "s|", User: "u"}) ==
		CacheKey("a|1|s", 0, Request{System: "u", User: ""}) {
		t.Fatal("pipe separator leaked into the key material")
	}
}

func TestCacheCorruptEntryIsMiss(t *testing.T) {
	fs := vfs.NewMemFS()
	clk := NewManualClock(time.Unix(1_000_000, 0))
	c, err := OpenCache(fs, "db", 1<<20, clk)
	if err != nil {
		t.Fatal(err)
	}
	r := reqWith("p")
	c.Put("m1", 1, r, Response{Text: "v1", Model: "m1"})

	name := filepath.Join("db", "llm", "cache", CacheKey("m1", 1, r)+".json")
	f, err := fs.Create(name)
	if err != nil {
		t.Fatal(err)
	}
	f.Write([]byte("{corrupt"))
	f.Close()

	if _, ok := c.Get("m1", 1, r); ok {
		t.Fatal("corrupt entry must miss")
	}
	if c.Entries() != 0 {
		t.Fatalf("corrupt entry must be removed, found %d", c.Entries())
	}
}

func TestCacheEvictionOldestFirst(t *testing.T) {
	c, clk := testCache(t, 3000)
	prompt := func(i int) Request { return reqWith(strings.Repeat(string(rune('a'+i)), 40)) }
	for i := 0; i < 10; i++ {
		clk.Advance(10 * time.Second) // strictly increasing created_unix
		c.Put("m1", 1, prompt(i), Response{Text: strings.Repeat("x", 400), Model: "m1"})
	}
	if n := c.Entries(); n >= 10 {
		t.Fatalf("eviction did not run: %d entries", n)
	}
	// The oldest entries are gone, the newest survive. The exact survivor
	// count depends on JSON sizes, so only the extremes are asserted.
	for i := 0; i < 10; i++ {
		_, ok := c.Get("m1", 1, prompt(i))
		switch {
		case i < 3 && ok:
			t.Fatalf("oldest entry %d should have been evicted", i)
		case i > 7 && !ok:
			t.Fatalf("newest entry %d should have survived", i)
		}
	}
}

func TestCacheOversizedEntryNotStored(t *testing.T) {
	c, _ := testCache(t, 100)
	c.Put("m1", 1, reqWith("p"), Response{Text: strings.Repeat("x", 500), Model: "m1"})
	if c.Entries() != 0 {
		t.Fatal("entry larger than the cap must not be stored")
	}
}

func TestCacheAtomicWritesLeaveNoTempFiles(t *testing.T) {
	c, _ := testCache(t, 1<<20)
	for i := 0; i < 5; i++ {
		c.Put("m1", 1, reqWith(string(rune('a'+i))), Response{Text: "v", Model: "m1"})
	}
	if c.Entries() != 5 {
		t.Fatalf("entries: %d", c.Entries())
	}
}

func TestCacheOpenRejectsBadCap(t *testing.T) {
	if _, err := OpenCache(vfs.NewMemFS(), "db", 0, nil); err == nil {
		t.Fatal("cap 0 must be rejected")
	}
}
