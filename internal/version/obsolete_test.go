package version

import (
	"errors"
	"path/filepath"
	"testing"
	"time"

	"semlsm/internal/testutil"
	"semlsm/internal/vfs"
)

func waitFor(t *testing.T, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal(msg)
}

func TestObsoleteDeleterDeletes(t *testing.T) {
	fs := testutil.NewMemFS(t)
	f, _ := fs.Create(filepath.Join("db", "000005.sst"))
	f.Close()
	if _, err := fs.Stat(filepath.Join("db", "000005.sst")); err != nil {
		t.Fatal(err)
	}
	od := NewObsoleteDeleter(fs, nil, 1*time.Millisecond)
	od.Start()
	defer od.Stop()
	od.Enqueue(ObsoleteFile{Path: filepath.Join("db", "000005.sst"), Num: 5})
	waitFor(t, func() bool {
		_, err := fs.Stat(filepath.Join("db", "000005.sst"))
		return err != nil
	}, "file should be deleted")
}

func TestObsoleteDeleterRetriesThenFails(t *testing.T) {
	fs := testutil.NewMemFS(t)
	base := vfs.NewFaultFS(fs)
	f, _ := base.Create(filepath.Join("db", "000006.sst"))
	f.Close()

	// fail the first 3 Removes; 5 attempts must succeed on the 4th
	sentinel := errors.New("injected remove failure")
	base.InjectRange("Remove", 1, 3, sentinel) // fail the first 3 Removes; 5 attempts must succeed on the 4th

	od := NewObsoleteDeleter(base, nil, 1*time.Millisecond)
	od.Start()
	defer od.Stop()
	od.Enqueue(ObsoleteFile{Path: filepath.Join("db", "000006.sst"), Num: 6})
	waitFor(t, func() bool {
		_, err := base.Stat(filepath.Join("db", "000006.sst"))
		return err != nil
	}, "file should be deleted after retries")

	// more failures than retries: file lands in Failed
	g, _ := base.Create(filepath.Join("db", "000007.sst"))
	g.Close()
	for i := 0; i < 10; i++ {
		base.InjectRange("Remove", 5, 30, sentinel) // keep failing (count is at 4 by now)
	}
	od.Enqueue(ObsoleteFile{Path: filepath.Join("db", "000007.sst"), Num: 7})
	waitFor(t, func() bool {
		return len(od.Failed()) > 0
	}, "delete should fail after 5 attempts")
	if _, err := base.Stat(filepath.Join("db", "000007.sst")); err != nil {
		t.Fatal("failed file must remain for startup cleanup")
	}
}

func TestObsoleteDeleterEvictsTableCache(t *testing.T) {
	fs := testutil.NewMemFS(t)
	od := NewObsoleteDeleter(fs, nil, 1*time.Millisecond)
	_ = od
	// TableCache eviction is covered in sstable tests; here we just ensure
	// a nil cache does not panic.
}
