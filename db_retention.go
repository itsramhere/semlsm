package semlsm

import (
	"context"
	"time"

	"semlsm/internal/checkpoint"
	"semlsm/internal/failpoint"
	"semlsm/internal/manifest"
)

var (
	fpRetentionAfterWrite    = failpoint.Define("retention.after_write")
	fpRetentionAfterManifest = failpoint.Define("retention.after_manifest")
)

// ApplyRetention rewrites due checkpoint tuples now, independently of the
// automatic interval. Already-retired tuples are never changed again.
func (db *DB) ApplyRetention(ctx context.Context) error {
	_, err := db.rewriteCheckpoints(ctx, false, true)
	return err
}

func retentionDue(ts uint64, now time.Time, age time.Duration) bool {
	if age <= 0 {
		return false
	}
	cutoff := now.Add(-age).UnixNano()
	return cutoff > 0 && ts < uint64(cutoff)
}

func (db *DB) retentionCandidate(cf *manifest.CkptFile, now time.Time) bool {
	for _, ref := range cf.Tuples {
		if retentionDue(ref.TsTo, now, db.opts.ArchiveRetention) || retentionDue(ref.TsTo, now, db.opts.SummaryRetention) {
			return true
		}
	}
	return false
}

func (db *DB) retainedMeta(meta checkpoint.TupleMeta, now time.Time) (checkpoint.TupleMeta, bool) {
	if meta.RetiredUnixNano != 0 {
		return meta, false
	}
	stub := retentionDue(meta.TsTo, now, db.opts.SummaryRetention)
	archive := meta.ArchiveFileNum != 0 && (stub || retentionDue(meta.TsTo, now, db.opts.ArchiveRetention))
	if !stub && !archive {
		return meta, false
	}
	if archive {
		meta.ArchiveFileNum = 0
		meta.ArchiveRetiredUnixNano = now.UnixNano()
	}
	if stub {
		meta.RetiredUnixNano = now.UnixNano()
	}
	return meta, true
}
