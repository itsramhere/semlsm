package semlsm

import (
	"context"
	"fmt"
	"testing"
	"time"

	"semlsm/internal/base"
	"semlsm/internal/llm"
	"semlsm/internal/testmodel"
	"semlsm/internal/vfs"
)

// TestCorruptionNeverSilent: every byte flip (at the matrix's sampled
// offsets) must be detected somewhere - Open fails, a read errors, or
// VerifyIntegrity returns an error or reports a problem. A flip that is
// accepted silently by all three means a byte not covered by a CRC (I12).
func TestCorruptionNeverSilent(t *testing.T) {
	ctx := context.Background()
	fs := newTestMemFS(t)
	clk := base.NewManualClock(semStart)
	stack := newSemStack(t, fs, "db", llm.NewMock(llm.MockValid), 0)
	o := semOptions(fs, clk, stack)
	o.ArchiveRaw = true
	db, err := Open("db", o)
	if err != nil {
		t.Fatal(err)
	}
	model := testmodel.NewModel()
	for i := 0; i < 40; i++ {
		hardeningCommit(t, db, model, "agent/1/s", i)
		hardeningCommit(t, db, model, "app/1/e", i)
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	for lvl := 0; lvl < 4; lvl++ {
		if err := db.CompactLevel(ctx, lvl); err != nil {
			t.Fatal(err)
		}
	}
	clk.Set(semStart.Add(3 * time.Hour))
	if err := db.CompactLevel(ctx, base.NumLevels-1); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	type fileRef struct{ dir, name string }
	var files []fileRef
	for _, dir := range []string{"db", "db/archive"} {
		ents, err := fs.List(dir)
		if err != nil {
			continue
		}
		for _, e := range ents {
			if !e.IsDir && e.Size > 0 {
				files = append(files, fileRef{dir, e.Name})
			}
		}
	}
	var silent []string
	for _, fr := range files {
		data := hardeningReadFile(t, fs, fr.dir, fr.name)
		for _, off := range []int{0, 1, len(data) / 2, len(data) - 2, len(data) - 1} {
			if off < 0 || off >= len(data) {
				continue
			}
			bad := trackTestMemFS(t, fs.CrashClone(vfs.DropUnsynced, nil))
			hardeningFlip(t, bad, fr.dir, fr.name, off)
			stack2 := newSemStack(t, bad, "db", llm.NewMock(llm.MockValid), 0)
			o2 := semOptions(bad, clk, stack2)
			o2.ArchiveRaw = true
			db2, err := Open("db", o2)
			if err != nil {
				continue // detected at open
			}
			detected := false
			for _, mk := range model.Keys() {
				if _, err := db2.Get(mk); err != nil {
					detected = true
					break
				}
			}
			if !detected {
				rep, err := db2.VerifyIntegrity(ctx)
				if err != nil || len(rep.Problems) > 0 {
					detected = true
				}
			}
			db2.Close()
			if !detected {
				silent = append(silent, fmt.Sprintf("%s/%s@%d(len %d)", fr.dir, fr.name, off, len(data)))
			}
		}
	}
	if len(silent) > 0 {
		t.Errorf("%d flips went undetected by open, reads and VerifyIntegrity: %v", len(silent), silent)
	}
}
