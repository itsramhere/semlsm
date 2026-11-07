package semlsm

import (
	"path/filepath"

	"semlsm/internal/checkpoint"
	"semlsm/internal/manifest"
	"semlsm/internal/version"
	"semlsm/internal/vfs"
)

// checkpointArchives uses manifest metadata whenever available. Legacy
// checkpoints must be read with errors checked for every referenced tuple:
// Reader.Tuples deliberately omits tuples it cannot decode.
func checkpointArchives(fs vfs.FS, dir string, cf *manifest.CkptFile) ([]uint64, error) {
	if cf.ArchiveNums != nil || len(cf.Tuples) == 0 {
		return cf.ArchiveNums, nil
	}
	r, err := checkpoint.OpenReader(fs, filepath.Join(dir, ckptName(cf.FileNum)))
	if err != nil {
		return nil, err
	}
	defer r.Close()
	var nums []uint64
	for _, ref := range cf.Tuples {
		meta, err := r.ReadMeta(ref.ID)
		if err != nil {
			return nil, err
		}
		if meta.ArchiveFileNum != 0 {
			nums = append(nums, meta.ArchiveFileNum)
		}
	}
	return nums, nil
}

func liveArchives(fs vfs.FS, dir string, v *version.Version) (map[uint64]bool, error) {
	files := make([]manifest.CkptFile, 0, len(v.Ckpts.ByNum))
	for _, cf := range v.Ckpts.ByNum {
		files = append(files, *cf)
	}
	return checkpointArchiveSet(fs, dir, files)
}

func checkpointArchiveSet(fs vfs.FS, dir string, files []manifest.CkptFile) (map[uint64]bool, error) {
	keep := map[uint64]bool{}
	for i := range files {
		nums, err := checkpointArchives(fs, dir, &files[i])
		if err != nil {
			return nil, err
		}
		for _, num := range nums {
			keep[num] = true
		}
	}
	return keep, nil
}
