//go:build !windows

package transfer

import (
	"context"
	"errors"
	"testing"
)

func TestSystemStagingVolumeStatReturnsStableIdentityAndCapacity(t *testing.T) {
	directory := t.TempDir()
	first, err := SystemStagingVolumeStat(context.Background(), directory)
	if err != nil {
		t.Fatal(err)
	}
	second, err := SystemStagingVolumeStat(context.Background(), directory)
	if err != nil {
		t.Fatal(err)
	}
	if first.ID == "" || first.ID != second.ID || first.CapacityBytes <= 0 ||
		first.FreeBytes < 0 || first.FreeBytes > first.CapacityBytes {
		t.Fatalf("invalid volume stats: first=%+v second=%+v", first, second)
	}
}

func TestSystemStagingVolumeStatRejectsCanceledOrMissingPath(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := SystemStagingVolumeStat(ctx, t.TempDir()); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled stat error=%v", err)
	}
	if _, err := SystemStagingVolumeStat(context.Background(), t.TempDir()+"/missing"); !errors.Is(err, ErrStageFileVolume) {
		t.Fatalf("missing stat error=%v", err)
	}
}
