package semlsm_test

import (
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/itsramhere/semlsm"
	"github.com/itsramhere/semlsm/llm"
)

func ExampleOpen() {
	dir, err := os.MkdirTemp("", "semlsm-example-*")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)

	// The mock makes this example safe to run without an API key.
	client := llm.NewMock(llm.MockValid)
	stack, err := llm.NewSummarizer(client, llm.Config{
		Model: "mock", PromptVersion: 1, Dir: dir,
		MaxTokensTotal: 2_000_000, RequestsPerMinute: 60,
		TokensPerMinute: 200_000, Concurrency: 2, CacheMaxBytes: 16 << 20,
	})
	if err != nil {
		panic(err)
	}
	db, err := semlsm.Open(dir, semlsm.Options{
		Policies: []semlsm.Policy{{
			Prefix: "agent/", Mode: semlsm.ModeState,
			MinAge: time.Hour, MinBatch: 100, MaxBatch: 10_000,
		}},
		Summarizer: stack,
		ArchiveRaw: true,
	})
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			panic(err)
		}
	}()

	show := func(r semlsm.Result, err error) {
		if err != nil {
			panic(err)
		}
		switch r.Kind {
		case semlsm.ResultExact:
			fmt.Printf("Exact: %s\n", r.Value)
		case semlsm.ResultSummarized:
			fmt.Println("Summarized:", r.Checkpoint.Summary.Narrative.Summary)
		case semlsm.ResultExpired:
			fmt.Println("Expired tuple:", r.Checkpoint.ID)
		case semlsm.ResultNotFound:
			fmt.Println("NotFound")
		}
	}
	key := []byte("agent/1/state")
	ts, err := db.Put(key, []byte("hello"))
	if err != nil {
		panic(err)
	}
	snapshot := db.NewSnapshot()
	defer snapshot.Release()
	if _, err := db.Put(key, []byte("updated")); err != nil {
		panic(err)
	}
	show(db.Get(key))
	show(db.GetAt(key, ts))
	show(snapshot.Get(key))
	show(db.Get([]byte("missing")))

	items, err := db.History(key, 0, ^uint64(0))
	if err != nil {
		panic(err)
	}
	fmt.Println("History entries:", len(items))

	// After semantic compaction, a summarized run can be read exactly
	// from its archive. These two fresh versions have not been compacted.
	archived, err := db.GetArchived(key, ts)
	if errors.Is(err, semlsm.ErrNotArchived) {
		fmt.Println("No archived run yet")
	} else {
		show(archived, err)
	}
	// Output:
	// Exact: updated
	// Exact: hello
	// Exact: hello
	// NotFound
	// History entries: 2
	// No archived run yet
}

func ExampleOpen_http() {
	// Configure the environment before using this with a live endpoint.
	opts := llm.HTTPOptionsFromEnv()
	if opts.BaseURL == "" || opts.APIKey == "" {
		return
	}
	client := llm.NewHTTPClient(opts)
	stack, err := llm.NewSummarizer(client, llm.Config{
		Model: client.Model(), PromptVersion: 1, Dir: "mydb",
		MaxTokensTotal: 2_000_000, RequestsPerMinute: 60,
		TokensPerMinute: 200_000, Concurrency: 2, CacheMaxBytes: 16 << 20,
	})
	if err != nil {
		panic(err)
	}
	db, err := semlsm.Open("mydb", semlsm.Options{
		Policies: []semlsm.Policy{{
			Prefix: "agent/", Mode: semlsm.ModeState,
			MinAge: time.Hour, MinBatch: 100, MaxBatch: 10_000,
		}},
		Summarizer: stack,
		ArchiveRaw: true,
	})
	if err != nil {
		panic(err)
	}
	if err := db.Close(); err != nil {
		panic(err)
	}
}
