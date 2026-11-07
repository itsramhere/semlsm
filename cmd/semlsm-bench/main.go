// Command semlsm-bench measures write throughput, point-read and scan
// latency (p50/p99) and on-disk size, with semantic compaction off and on
// (P14.2). Results go to bench-results/bench-<timestamp>.json and a summary
// table on stdout. The LLM is the deterministic mock; no numbers are
// targets except the demo's reduction in P14.3.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"semlsm"
	"semlsm/internal/base"
	"semlsm/internal/llm"
	"semlsm/internal/vfs"
)

type latencies struct {
	P50 time.Duration `json:"p50_ns"`
	P99 time.Duration `json:"p99_ns"`
	N   int           `json:"n"`
}

type phaseResult struct {
	Semantic      bool      `json:"semantic"`
	Records       int       `json:"records"`
	Write         latencies `json:"write"`
	GetLatest     latencies `json:"get_latest"`
	GetHistorical latencies `json:"get_historical"`
	Scan          latencies `json:"scan_us"`
	BytesOnDisk   int64     `json:"bytes_on_disk"`
	WriteMBps     float64   `json:"write_mbps"`
}

func main() {
	records := flag.Int("records", 50000, "records per phase")
	outDir := flag.String("out", "bench-results", "output directory for the JSON report")
	flag.Parse()
	if err := run(*records, *outDir); err != nil {
		fmt.Fprintf(os.Stderr, "semlsm-bench: %v\n", err)
		os.Exit(1)
	}
}

func run(records int, outDir string) error {
	result := map[string]phaseResult{}
	off, err := runPhase(records, false)
	if err != nil {
		return err
	}
	result["semantic_off"] = off
	on, err := runPhase(records, true)
	if err != nil {
		return err
	}
	result["semantic_on"] = on

	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return err
	}
	name := filepath.Join(outDir, fmt.Sprintf("bench-%d.json", time.Now().UnixNano()))
	data, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(name, data, 0o644); err != nil {
		return err
	}

	fmt.Printf("records per phase: %d\n\n", records)
	fmt.Printf("%-18s %14s %14s\n", "metric", "semantic off", "semantic on")
	row := func(label string, get func(phaseResult) string) {
		fmt.Printf("%-18s %14s %14s\n", label, get(off), get(on))
	}
	row("write p50 (ns)", func(p phaseResult) string { return fmt.Sprintf("%d", p.Write.P50.Nanoseconds()) })
	row("write p99 (ns)", func(p phaseResult) string { return fmt.Sprintf("%d", p.Write.P99.Nanoseconds()) })
	row("write MB/s", func(p phaseResult) string { return fmt.Sprintf("%.2f", p.WriteMBps) })
	row("get latest p50 (ns)", func(p phaseResult) string { return fmt.Sprintf("%d", p.GetLatest.P50.Nanoseconds()) })
	row("get latest p99 (ns)", func(p phaseResult) string { return fmt.Sprintf("%d", p.GetLatest.P99.Nanoseconds()) })
	row("get hist p50 (ns)", func(p phaseResult) string { return fmt.Sprintf("%d", p.GetHistorical.P50.Nanoseconds()) })
	row("get hist p99 (ns)", func(p phaseResult) string { return fmt.Sprintf("%d", p.GetHistorical.P99.Nanoseconds()) })
	row("scan per-record p50 (ns)", func(p phaseResult) string { return fmt.Sprintf("%d", p.Scan.P50.Nanoseconds()) })
	row("scan per-record p99 (ns)", func(p phaseResult) string { return fmt.Sprintf("%d", p.Scan.P99.Nanoseconds()) })
	row("bytes on disk", func(p phaseResult) string { return fmt.Sprintf("%d", p.BytesOnDisk) })
	fmt.Printf("\nreport: %s\n", name)
	return nil
}

// runPhase builds a fresh in-memory DB, writes records, and measures.
func runPhase(records int, semantic bool) (phaseResult, error) {
	res := phaseResult{Semantic: semantic, Records: records}
	fs := vfs.NewMemFS()
	opts := semlsm.Options{
		FS:             fs,
		GroupDepth:     2,
		GroupDelimiter: '/',
		Policies: []semlsm.Policy{
			{Prefix: "agent/", Mode: semlsm.ModeState, MinAge: time.Hour, MinBatch: 100, MaxBatch: 10000},
		},
		DefaultPolicy: semlsm.Policy{Mode: semlsm.ModeDisabled},
		SyncMode:      semlsm.SyncEveryN, SyncEveryN: 128,
	}
	if semantic {
		stack, err := llm.NewSummarizer(llm.NewMock(llm.MockValid), llm.Config{
			Model: "mock", PromptVersion: 1, FS: fs, Dir: "db",
			MaxTokensTotal: 1 << 40, RequestsPerMinute: 6000,
			TokensPerMinute: 1 << 26, Concurrency: 4, CacheMaxBytes: 8 << 20,
		})
		if err != nil {
			return res, err
		}
		opts.Summarizer = stack
	}
	db, err := semlsm.Open("db", opts)
	if err != nil {
		return res, err
	}
	defer db.Close()

	// The wall clock on this platform resolves only ~1ms spans, so every
	// latency is measured as a batch span divided by its operation count
	// (honest aggregates, not fake per-op precision).
	value := []byte(`{"cpu":42.5,"mem":1024,"status":"ok","payload":"` + string(make([]byte, 120)) + `"}`)
	histTs := make([]uint64, 0, records/10)

	const batch = 1000
	write := make([]time.Duration, 0, records/batch)
	var total int64
	var writeElapsed time.Duration
	for i := 0; i < records; i += batch {
		b := semlsm.NewBatch()
		for j := 0; j < batch && i+j < records; j++ {
			b.Put([]byte(fmt.Sprintf("agent/%d/telemetry", (i+j)%100)), value)
			total += int64(len(value) + 20)
		}
		start := time.Now()
		if _, err := db.Apply(b); err != nil {
			return res, err
		}
		span := time.Since(start)
		writeElapsed += span
		write = append(write, span/time.Duration(b.Len()))
		if len(histTs) < 200 {
			res2, err := db.Get([]byte("agent/0/telemetry"))
			if err == nil && res2.Kind == semlsm.ResultExact {
				histTs = append(histTs, res2.VersionTs/2+1)
			}
		}
	}
	res.Write = pct(write)
	if writeElapsed.Seconds() > 0 {
		res.WriteMBps = float64(total) / 1e6 / writeElapsed.Seconds()
	}

	getLatest := make([]time.Duration, 0, 20)
	getHist := make([]time.Duration, 0, 20)
	const readsPerSpan = 2000
	for i := 0; i < 20; i++ {
		start := time.Now()
		for j := 0; j < readsPerSpan; j++ {
			key := []byte(fmt.Sprintf("agent/%d/telemetry", j%100))
			if _, err := db.Get(key); err != nil {
				return res, err
			}
		}
		getLatest = append(getLatest, time.Since(start)/readsPerSpan)
		if len(histTs) > 0 {
			start = time.Now()
			for j := 0; j < readsPerSpan; j++ {
				key := []byte(fmt.Sprintf("agent/%d/telemetry", j%100))
				if _, err := db.GetAt(key, histTs[j%len(histTs)]); err != nil {
					return res, err
				}
			}
			getHist = append(getHist, time.Since(start)/readsPerSpan)
		}
	}
	res.GetLatest = pct(getLatest)
	res.GetHistorical = pct(getHist)

	scans := make([]time.Duration, 0, 20)
	for i := 0; i < 20; i++ {
		start := time.Now()
		it := db.NewIterator(semlsm.IterOptions{Start: []byte("agent/")})
		n := 0
		for it.Next() {
			n++
		}
		if err := it.Err(); err != nil {
			return res, err
		}
		it.Close()
		scans = append(scans, time.Since(start)/time.Duration(max(n, 1)))
	}
	res.Scan = pct(scans)

	// Force the semantic phase's compaction so its "after" size is honest.
	if semantic {
		if err := db.Flush(); err != nil {
			return res, err
		}
		for lvl := 0; lvl < base.NumLevels; lvl++ {
			if err := db.CompactLevel(context.Background(), lvl); err != nil {
				return res, err
			}
		}
	}
	res.BytesOnDisk = diskBytes(fs, "db")
	return res, nil
}

// diskBytes sums every file's size under dir (recursively one level).
func diskBytes(fs vfs.FS, dir string) int64 {
	var total int64
	if ents, err := fs.List(dir); err == nil {
		for _, e := range ents {
			if e.IsDir {
				if sub, err := fs.List(filepath.Join(dir, e.Name)); err == nil {
					for _, se := range sub {
						total += se.Size
					}
				}
				continue
			}
			total += e.Size
		}
	}
	return total
}

func pct(ds []time.Duration) latencies {
	if len(ds) == 0 {
		return latencies{}
	}
	sorted := append([]time.Duration(nil), ds...)
	sort.Slice(sorted, func(a, b int) bool { return sorted[a] < sorted[b] })
	return latencies{
		P50: sorted[len(sorted)*50/100],
		P99: sorted[min(len(sorted)-1, len(sorted)*99/100)],
		N:   len(ds),
	}
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
