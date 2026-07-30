//go:build !windows

package transfer

import (
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"syscall"
)

// SystemStagingVolumeStat exists for tests and development tooling. Production
// private storage remains Windows-only through localapp.PreparePrivateRoot.
func SystemStagingVolumeStat(ctx context.Context, path string) (StagingVolumeStat, error) {
	if ctx == nil || path == "" {
		return StagingVolumeStat{}, ErrStageFileVolume
	}
	select {
	case <-ctx.Done():
		return StagingVolumeStat{}, ctx.Err()
	default:
	}
	absolute, err := filepath.Abs(path)
	if err != nil || filepath.Clean(absolute) != absolute {
		return StagingVolumeStat{}, ErrStageFileVolume
	}
	info, err := os.Stat(absolute)
	if err != nil || !info.IsDir() {
		return StagingVolumeStat{}, ErrStageFileVolume
	}
	metadata, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return StagingVolumeStat{}, ErrStageFileVolume
	}
	var stat syscall.Statfs_t
	if err := syscall.Statfs(absolute, &stat); err != nil || stat.Blocks == 0 || stat.Bsize <= 0 {
		return StagingVolumeStat{}, ErrStageFileVolume
	}
	capacity := uint64(stat.Blocks) * uint64(stat.Bsize)
	free := uint64(stat.Bavail) * uint64(stat.Bsize)
	if capacity > math.MaxInt64 || free > math.MaxInt64 || free > capacity {
		return StagingVolumeStat{}, ErrStageFileVolume
	}
	return StagingVolumeStat{
		ID: fmt.Sprintf("dev:%d", metadata.Dev), FreeBytes: int64(free), CapacityBytes: int64(capacity),
	}, nil
}
