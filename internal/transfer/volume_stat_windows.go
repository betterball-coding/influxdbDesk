//go:build windows

package transfer

import (
	"context"
	"math"
	"path/filepath"
	"strings"

	"golang.org/x/sys/windows"
)

// SystemStagingVolumeStat returns a stable Windows Volume GUID identity and
// fresh capacity values. Any identity/capacity ambiguity fails closed.
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
	pathPointer, err := windows.UTF16PtrFromString(absolute)
	if err != nil {
		return StagingVolumeStat{}, ErrStageFileVolume
	}
	mount := make([]uint16, windows.MAX_PATH+1)
	if err := windows.GetVolumePathName(pathPointer, &mount[0], uint32(len(mount))); err != nil {
		return StagingVolumeStat{}, ErrStageFileVolume
	}
	mountPath := windows.UTF16ToString(mount)
	mountPointer, err := windows.UTF16PtrFromString(mountPath)
	if err != nil {
		return StagingVolumeStat{}, ErrStageFileVolume
	}
	volume := make([]uint16, windows.MAX_PATH+1)
	if err := windows.GetVolumeNameForVolumeMountPoint(mountPointer, &volume[0], uint32(len(volume))); err != nil {
		return StagingVolumeStat{}, ErrStageFileVolume
	}
	volumeID := windows.UTF16ToString(volume)
	if !strings.HasPrefix(volumeID, `\\?\Volume{`) || !strings.HasSuffix(volumeID, `}\`) {
		return StagingVolumeStat{}, ErrStageFileVolume
	}
	var freeToCaller, capacity, totalFree uint64
	if err := windows.GetDiskFreeSpaceEx(pathPointer, &freeToCaller, &capacity, &totalFree); err != nil {
		return StagingVolumeStat{}, ErrStageFileVolume
	}
	if capacity == 0 || capacity > math.MaxInt64 || freeToCaller > math.MaxInt64 || totalFree > capacity {
		return StagingVolumeStat{}, ErrStageFileVolume
	}
	select {
	case <-ctx.Done():
		return StagingVolumeStat{}, ctx.Err()
	default:
	}
	return StagingVolumeStat{
		ID: volumeID, FreeBytes: int64(freeToCaller), CapacityBytes: int64(capacity),
	}, nil
}
