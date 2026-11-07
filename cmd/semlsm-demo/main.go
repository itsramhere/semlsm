// Command semlsm-demo generates 100k realistic agent-telemetry records with
// a few planted anomalies, compacts them with the deterministic mock LLM,
// and prints the before/after sizes, the reduction ratio, and three reads
// side by side: an Exact read, a Summarized read, and a pinned-snapshot read
// (P14.3). The summarized range must shrink at least 10x or the demo exits 1.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"semlsm"
	"semlsm/internal/base"
	"semlsm/internal/llm"
	"semlsm/internal/vfs"
)

// telemetry is one realistic record value.
type telemetry struct {
	Agent  string  `json:"agent"`
	CPU    float64 `json:"cpu"`
	MemMB  float64 `json:"mem_mb"`
	Status string  `json:"status"`
	Uptime int     `json:"uptime_s"`
}

func main() {
	agents := flag.Int("agents", 100, "number of telemetry agents")
	perAgent := flag.Int("per-agent", 1000, "telemetry versions per agent")
	dir := flag.String("dir", "", "demo database directory (empty = in memory)")
	flag.Parse()
	if err := run(*agents, *perAgent, *dir); err != nil {
		fmt.Fprintf(os.Stderr, "semlsm-demo: %v\n", err)
		os.Exit(1)
	}
}

func run(agents, perAgent int, dir string) error {
	var fs vfs.FS = vfs.NewMemFS()
	if dir != "" {
		fs = vfs.OSFS{}
		if err := fs.MkdirAll(dir); err != nil {
			return err
		}
	}
	dbDir := dir
	if dbDir == "" {
		dbDir = "demo-db"
	}

	stack, err := llm.NewSummarizer(llm.NewMock(llm.MockValid), llm.Config{
		Model: "mock", PromptVersion: 1, FS: fs, Dir: dbDir,
		MaxTokensTotal: 1 << 40, RequestsPerMinute: 6000,
		TokensPerMinute: 1 << 26, Concurrency: 4, CacheMaxBytes: 8 << 20,
	})
	if err != nil {
		return err
	}
	// A manual clock: writes happen at T0, then the clock jumps 2h so every
	// record is older than the 1h MinAge horizon when compaction runs.
	clk := base.NewManualClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	opts := semlsm.Options{
		FS: fs, Clock: clk, SyncMode: semlsm.SyncEveryN, SyncEveryN: 256,
		MemtableBytes: 4 << 20, MaxImmutableMemtables: 8,
		L0CompactTrigger: 4, L0StallTrigger: 24,
		L1TargetBytes: 256 << 10, LevelMultiplier: 2, TargetFileBytes: 1 << 20,
		GroupDepth: 2, GroupDelimiter: '/',
		Policies: []semlsm.Policy{
			{Prefix: "agent/", Mode: semlsm.ModeState, MinAge: time.Hour, MinBatch: 100, MaxBatch: 10000},
		},
		DefaultPolicy:       semlsm.Policy{Mode: semlsm.ModeDisabled},
		Summarizer:          stack,
		MinCompressionRatio: 5.0,
		ArchiveRaw:          true,
	}
	db, err := semlsm.Open(dbDir, opts)
	if err != nil {
		return err
	}

	// Generate the workload: agents tick every 10s from a fixed start time;
	// five planted anomalies spike the CPU and flip the status.
	anomalyAt := map[int]bool{perAgent / 5: true, perAgent / 3: true, perAgent / 2: true, 2 * perAgent / 3: true, perAgent - 2: true}
	var rawBytes int64
	var firstTs uint64
	// One batch per version tick: every agent's key appears exactly once,
	// so the batch's last-write-wins dedup never collapses versions.
	for i := 0; i < perAgent; i++ {
		b := semlsm.NewBatch()
		for a := 0; a < agents; a++ {
			key := fmt.Sprintf("agent/%d/telemetry", a)
			t := telemetry{
				Agent:  fmt.Sprintf("agent-%03d", a),
				CPU:    10 + float64((a*7+i*13)%60),
				MemMB:  512 + float64((a*3+i)%256),
				Status: "ok",
				Uptime: i * 10,
			}
			if anomalyAt[i] && a == 0 {
				t.CPU = 999.9
				t.Status = "anomaly:cpu-spike"
			}
			v, err := json.Marshal(t)
			if err != nil {
				return err
			}
			b.Put([]byte(key), v)
			rawBytes += int64(len(key) + len(v) + 9)
		}
		ts, err := db.Apply(b)
		if err != nil {
			return err
		}
		if i == 0 {
			firstTs = ts
		}
	}
	if err := db.Flush(); err != nil {
		return err
	}
	before := diskBytes(fs, dbDir)

	// Age the data past the MinAge horizon.
	clk.Set(clk.Now().Add(2 * time.Hour))

	// Pin two snapshots before compaction: one at the latest version and one
	// at a mid-range ts inside the future dropped run — the I9 point is that
	// the pinned read keeps returning the exact original value while the
	// live read starts answering Summarized.
	snap := db.NewSnapshot()
	snapKey := []byte("agent/0/telemetry")
	midTs := (firstTs + db.Stats().LastTs) / 2
	midSnap, err := db.OpenSnapshotAt(midTs)
	if err != nil {
		return err
	}
	midPinned, err := midSnap.Get(snapKey)
	if err != nil {
		return err
	}

	// Compact everything down, looping each level until no job or plan makes
	// progress (each pick takes one file, and the whole aged range should be
	// summarized, not just one file's worth).
	jobsTotal := func() int {
		st := db.Stats()
		return int(st.SemanticJobs["committed"] + st.SemanticJobs["raw-only"])
	}
	for lvl := 0; lvl < base.NumLevels; lvl++ {
		structural := lvl < 2
		for round := 0; round < 8*agents; round++ {
			beforeJobs := jobsTotal()
			if err := db.CompactLevel(context.Background(), lvl); err != nil {
				return err
			}
			progressed := jobsTotal() > beforeJobs
			if !progressed {
				if structural {
					// structural levels create no jobs: stop when the level
					// is empty
					if db.Stats().LevelFiles[lvl] == 0 {
						break
					}
				} else {
					break // no eligible file: this level is drained
				}
			}
			if structural && db.Stats().LevelFiles[lvl] == 0 {
				break
			}
		}
	}

	// The three reads, side by side: an exact latest read, the SAME mid-range
	// ts through the pinned snapshot (Exact original) and live (Summarized).
	latest, err := db.Get(snapKey)
	if err != nil {
		return err
	}
	mid, err := db.GetAt(snapKey, midTs)
	if err != nil {
		return err
	}

	// Release the pinned snapshots FIRST: they hold the pre-compaction
	// version's files on disk (I9), so measuring before their release would
	// count the raw pre-compaction store.
	midSnap.Release()
	snap.Release()
	if err := db.WaitIdle(context.Background()); err != nil {
		return err
	}
	prevLive := int64(-1)
	live := diskBytes(fs, dbDir)
	for dl := time.Now().Add(3 * time.Second); time.Now().Before(dl); {
		time.Sleep(20 * time.Millisecond)
		live = diskBytes(fs, dbDir)
		if live == prevLive {
			break
		}
		prevLive = live
	}
	arch := diskBytes(fs, filepath.Join(dbDir, "archive"))
	after := live - arch // the live store; archives hold the raw records on purpose (P12)
	// Break the live store down so the numbers explain themselves.
	var sstB, ckptB, cacheB, otherB int64
	var sstN, ckptN int
	if ents, err := fs.List(dbDir); err == nil {
		for _, e := range ents {
			switch {
			case e.IsDir:
				if e.Name == "llm" {
					if sub, err := fs.List(filepath.Join(dbDir, "llm", "cache")); err == nil {
						for _, se := range sub {
							cacheB += se.Size
						}
					}
				}
			case strings.HasSuffix(e.Name, ".sst"):
				sstN++
				sstB += e.Size
			case strings.HasSuffix(e.Name, ".ckpt"):
				ckptN++
				ckptB += e.Size
			default:
				otherB += e.Size
			}
		}
	}
	st := db.Stats()
	ratio := 0.0
	if st.SemanticSummaryBytes > 0 {
		ratio = float64(st.SemanticRawBytesRemoved) / float64(st.SemanticSummaryBytes)
	}

	fmt.Printf("SemLSM demo: %d agents x %d telemetry versions = %d records\n\n", agents, perAgent, agents*perAgent)
	fmt.Printf("planted anomalies: 5 records with cpu=999.9 per sampled agent\n\n")
	fmt.Printf("raw record bytes:            %12d\n", rawBytes)
	fmt.Printf("on-disk before (raw):        %12d\n", before)
	fmt.Printf("on-disk after, live store:   %12d   (%d sst %d B + %d ckpt %d B + other %d B; llm cache %d B separate)\n",
		after, sstN, sstB, ckptN, ckptB, otherB, cacheB)
	fmt.Printf("on-disk after, archive tier: %12d   (the dropped raw records, kept recoverable)\n", arch)
	fmt.Printf("summarized raw bytes:        %12d\n", st.SemanticRawBytesRemoved)
	fmt.Printf("summary bytes:               %12d\n", st.SemanticSummaryBytes)
	fmt.Printf("summarized reduction:        %11.1fx  (records summarized: %d)\n", ratio, st.SemanticRecordsSummarized)
	fmt.Printf("NOTE: measured with the deterministic mock (fixed-size summaries);\n")
	fmt.Printf("a real LLM writes larger summaries, so the live-LLM ratio will be lower\n")
	fmt.Printf("and stays unmeasured until the optional P15 run.\n\n")

	fmt.Printf("%-22s %s\n", "read kind", "result")
	fmt.Printf("%-22s Exact @%d: %.60s...\n", "latest (raw head)", latest.VersionTs, latest.Value)
	if mid.Kind == semlsm.ResultSummarized {
		fmt.Printf("%-22s Summarized @%d: tuple %d covers %d..%d (%d versions)\n",
			"mid-range (live)", midTs, mid.Checkpoint.ID, mid.Checkpoint.KeyMinTs,
			mid.Checkpoint.KeyMaxTs, mid.Checkpoint.KeyCount)
		fmt.Printf("%-22s summary text is the mock's canned output: %q\n",
			"", mid.Checkpoint.Summary.Narrative.Summary)
	} else {
		fmt.Printf("%-22s kind=%d @%d\n", "mid-range (live)", mid.Kind, mid.VersionTs)
	}
	if midPinned.Kind == semlsm.ResultExact {
		fmt.Printf("%-22s Exact @%d: %.60s...  (the pinned snapshot still sees the original value: I9)\n",
			"mid-range (pinned)", midPinned.VersionTs, midPinned.Value)
	} else {
		fmt.Printf("%-22s kind=%d @%d\n", "mid-range (pinned)", midPinned.Kind, midPinned.VersionTs)
	}

	// The planted anomalies are recoverable exactly from the archive tier:
	// sample three anomaly ticks of agent 0 (ticks at perAgent/5, /3, /2).
	found := 0
	for _, tick := range []int{perAgent / 5, perAgent / 3, perAgent / 2} {
		at := firstTs + uint64(tick)
		res, err := db.GetArchived([]byte("agent/0/telemetry"), at)
		if err == nil && res.Kind == semlsm.ResultExact && containsField(res.Value, "anomaly:cpu-spike") {
			found++
		}
	}
	fmt.Printf("\nplanted anomalies recovered from the archive: %d/3 sampled\n", found)

	if ratio < 10.0 {
		db.Close()
		snap.Release()
		return fmt.Errorf("summarized reduction %.1fx is below the required 10x", ratio)
	}
	if err := db.Close(); err != nil {
		return err
	}
	snap.Release()
	fmt.Printf("\nreduction target met (>= 10x)\n")
	return nil
}

// containsField reports whether the JSON value carries the given status.
func containsField(value []byte, status string) bool {
	var t telemetry
	if err := json.Unmarshal(value, &t); err != nil {
		return false
	}
	return t.Status == status
}

// diskBytes sums the sizes of every file under dir (one level down).
func diskBytes(fs vfs.FS, dir string) int64 {
	var total int64
	ents, err := fs.List(dir)
	if err != nil {
		return 0
	}
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
	return total
}
