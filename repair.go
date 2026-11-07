package semlsm

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"semlsm/internal/manifest"
	"semlsm/internal/version"
	"semlsm/internal/wal"
)

// TruncatedFile describes bytes explicitly discarded from a damaged log.
type TruncatedFile struct {
	Name         string
	BytesDropped int64
}

// RepairReport records completed changes. QuarantinedFiles contains paths
// relative to dir under lost+found; existing quarantine entries are preserved.
type RepairReport struct {
	TruncatedFiles   []TruncatedFile
	QuarantinedFiles []string
}

// Repair explicitly rolls back complete final frames with bad CRCs in CURRENT's
// manifest and the newest WAL Open would replay. Other log corruption is an
// error. Unreferenced data files are quarantined before Open can sweep them.
// Repair requires exclusive access and never deletes data files. As with Open,
// opts must describe the database's persisted group settings and archive dir.
func Repair(dir string, opts Options) (report RepairReport, err error) {
	opts, err = opts.withDefaults()
	if err != nil {
		return report, err
	}
	lock, err := opts.FS.Lock(filepath.Join(dir, "LOCK"))
	if err != nil {
		return report, fmt.Errorf("semlsm: lock %s: %w", dir, err)
	}
	defer func() { err = errors.Join(err, lock.Unlock()) }()
	if err := checkOptionsFile(opts, dir); err != nil {
		return report, err
	}
	name, err := manifest.ReadCurrent(opts.FS, dir)
	if err != nil {
		return report, fmt.Errorf("semlsm: repair current: %w", err)
	}
	type truncation struct {
		name  string
		valid int64
	}
	var pending []truncation
	vs := version.NewVersionSet()
	valid, err := wal.ReplayStrict(opts.FS, filepath.Join(dir, name), func(payload []byte) error {
		var edit manifest.Edit
		if err := json.Unmarshal(payload, &edit); err != nil {
			return fmt.Errorf("semlsm: repair manifest JSON: %w", ErrCorruption)
		}
		_, err := vs.Apply(&edit)
		return err
	})
	if errors.Is(err, wal.ErrFinalFrameCRC) {
		pending = append(pending, truncation{name, valid})
	} else if err != nil {
		return report, fmt.Errorf("semlsm: repair manifest: %w", err)
	}
	// Reuse Open's WAL enumeration, with no newly-created active WAL to omit.
	db := &DB{dir: dir, opts: opts, vs: vs}
	nums, err := db.replayWalNums()
	if err != nil {
		return report, err
	}
	for i, num := range nums {
		name := walName(num)
		decode := func(payload []byte) error {
			_, _, err := wal.DecodeBatch(payload)
			return err
		}
		var valid int64
		var err error
		if i == len(nums)-1 {
			// Refuse a damaged length hiding valid frames, just as for the
			// manifest. Repair never approves a rollback over later frames.
			valid, err = wal.ReplayStrict(opts.FS, filepath.Join(dir, name), decode)
		} else {
			valid, err = wal.Replay(opts.FS, filepath.Join(dir, name), false, decode)
		}
		if i == len(nums)-1 && errors.Is(err, wal.ErrFinalFrameCRC) {
			pending = append(pending, truncation{name, valid})
		} else if err != nil {
			return report, fmt.Errorf("semlsm: repair wal %d: %w", num, err)
		}
	}
	cur := vs.Acquire()
	defer cur.Unref()
	keepArchive, err := liveArchives(opts.FS, dir, cur)
	if err != nil {
		return report, fmt.Errorf("semlsm: repair cannot determine live archives: %w", err)
	}
	keep := map[string]bool{}
	for _, level := range cur.Levels {
		for _, fm := range level {
			keep[sstName(fm.Num)] = true
		}
	}
	for num := range cur.Ckpts.ByNum {
		keep[ckptName(num)] = true
	}
	type move struct{ source, dest string }
	var moves []move
	for _, sourceDir := range []string{dir, db.archiveDir()} {
		entries, err := opts.FS.List(sourceDir)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return report, fmt.Errorf("semlsm: repair list: %w", err)
		}
		for _, entry := range entries {
			if entry.IsDir {
				continue
			} // includes lost+found, which Open also ignores
			// Quarantine all data files, including ones with non-numeric names.
			suffix := filepath.Ext(entry.Name)
			if sourceDir == dir {
				if (suffix != ".sst" && suffix != ".ckpt") || keep[entry.Name] {
					continue
				}
			} else {
				num, ok, _ := parseFileNum(entry.Name)
				if suffix != ".sst" || (ok && keepArchive[num]) {
					continue
				}
			}
			dest := filepath.Join("lost+found", entry.Name)
			if sourceDir != dir {
				dest = filepath.Join("lost+found", "archive", entry.Name)
			}
			moves = append(moves, move{filepath.Join(sourceDir, entry.Name), dest})
		}
	}
	sort.Slice(moves, func(i, j int) bool { return moves[i].dest < moves[j].dest })
	for _, mv := range moves {
		dest := filepath.Join(dir, mv.dest)
		for i := 1; ; i++ {
			_, err := opts.FS.Stat(dest)
			if errors.Is(err, os.ErrNotExist) {
				break
			}
			if err != nil {
				return report, fmt.Errorf("semlsm: repair quarantine stat: %w", err)
			}
			dest = fmt.Sprintf("%s.%d", filepath.Join(dir, mv.dest), i)
		}
		for _, d := range []string{filepath.Join(dir, "lost+found"), filepath.Dir(dest)} {
			if err := opts.FS.MkdirAll(d); err != nil {
				return report, fmt.Errorf("semlsm: repair mkdir: %w", err)
			}
			if err := opts.FS.SyncDir(filepath.Dir(d)); err != nil {
				return report, fmt.Errorf("semlsm: repair syncdir: %w", err)
			}
		}
		if err := opts.FS.Rename(mv.source, dest); err != nil {
			return report, fmt.Errorf("semlsm: repair quarantine: %w", err)
		}
		rel, _ := filepath.Rel(dir, dest)
		report.QuarantinedFiles = append(report.QuarantinedFiles, rel)
		for _, d := range []string{filepath.Dir(dest), filepath.Dir(mv.source)} {
			if err := opts.FS.SyncDir(d); err != nil {
				return report, fmt.Errorf("semlsm: repair syncdir: %w", err)
			}
		}
	}
	// Publish the rollback only after quarantine is durable. If a move fails,
	// the damaged log still prevents Open from sweeping the remaining files.
	for _, tr := range pending {
		path := filepath.Join(dir, tr.name)
		st, err := opts.FS.Stat(path)
		if err != nil {
			return report, fmt.Errorf("semlsm: repair stat: %w", err)
		}
		f, err := opts.FS.OpenReadWrite(path)
		if err != nil {
			return report, fmt.Errorf("semlsm: repair open: %w", err)
		}
		err = f.Truncate(tr.valid)
		if err == nil {
			err = f.Sync()
		}
		err = errors.Join(err, f.Close())
		if err != nil {
			return report, fmt.Errorf("semlsm: repair truncate %s: %w", tr.name, err)
		}
		report.TruncatedFiles = append(report.TruncatedFiles, TruncatedFile{tr.name, st.Size - tr.valid})
	}
	return report, nil
}
