package transfer

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/influxdesk/influxdesk/internal/store"
	"github.com/influxdesk/influxdesk/internal/tasks"
)

func newStagingFileTest(
	t *testing.T,
	jobID string,
	stat StagingVolumeStatFunc,
) (*StagingFileCoordinator, *store.Store, ImportJob, string) {
	t.Helper()
	root := t.TempDir()
	database, err := store.Open(context.Background(), filepath.Join(root, "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	repository := NewRepository(
		database,
		tasks.NewRepository(database, func() time.Time { return now }),
		nil,
		func() time.Time { return now },
	)
	job, _, err := repository.CreateImport(context.Background(), CreateImportRequest{
		JobID: jobID, ProfileID: "profile-stage", ClientScope: "preflight/profile-stage/" + jobID,
		RequestDigest: "digest-" + jobID, LedgerExpiresAt: now.Add(90 * 24 * time.Hour),
		InitialCheckpoint: Checkpoint{
			Sequence: "0", LogicalOffset: "0", AdaptiveMaxPoints: "5000", AdaptiveMaxBytes: "5242880",
			SourceSHA256: "source", StagingSHA256: "staging", NormalizationVersion: "lp-v1",
			SpecDigest: "spec", TargetDigest: "target",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	directory := filepath.Join(root, "staging")
	if err := os.Mkdir(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	return NewStagingFileCoordinator(repository.quota, stat), database, job, directory
}

func healthyStagingVolume(context.Context, string) (StagingVolumeStat, error) {
	return StagingVolumeStat{
		ID: "volume-guid-private", FreeBytes: 80 * 1024 * 1024 * 1024,
		CapacityBytes: 100 * 1024 * 1024 * 1024,
	}, nil
}

func TestStagingFileCoordinatorCommitsAndSettlesActualLength(t *testing.T) {
	coordinator, database, job, directory := newStagingFileTest(t, "job-stage-success", healthyStagingVolume)
	result, err := coordinator.Stage(context.Background(), StageImportFileRequest{
		Job: job, StagingDirectory: directory, Format: ImportStageLP,
		Source: strings.NewReader("cpu,host=b value=1i 1700000000123456789\n"),
	})
	if err != nil {
		t.Fatal(err)
	}
	wantPath := filepath.Join(directory, "import-job-stage-success.canonical.lp")
	if result.StagingPath != wantPath || len(result.Cleanup.ReservationIDs) != 1 {
		t.Fatalf("result=%+v", result)
	}
	if _, err := os.Stat(wantPath + ".part"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("part still exists: %v", err)
	}
	payload, err := os.ReadFile(wantPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(payload) != "cpu,host=b value=1i 1700000000123456789\n" {
		t.Fatalf("payload=%q", payload)
	}
	logical, err := strconv.ParseInt(result.Stage.LogicalBytes, 10, 64)
	if err != nil || logical != int64(len(payload)) {
		t.Fatalf("logical=%q size=%d", result.Stage.LogicalBytes, len(payload))
	}
	var reserved, consumed string
	if err := database.DB().QueryRowContext(context.Background(), `SELECT reserved_bytes,consumed_bytes
		FROM transfer_reservations WHERE reservation_id=?`, result.Cleanup.ReservationIDs[0]).Scan(
		&reserved, &consumed,
	); err != nil {
		t.Fatal(err)
	}
	if reserved != result.Stage.LogicalBytes || consumed != result.Stage.LogicalBytes {
		t.Fatalf("reservation reserved=%s consumed=%s logical=%s", reserved, consumed, result.Stage.LogicalBytes)
	}
	if err := coordinator.Cleanup(context.Background(), result.Cleanup); err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Cleanup(context.Background(), result.Cleanup); err != nil {
		t.Fatalf("cleanup replay: %v", err)
	}
	if _, err := os.Stat(wantPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("final remains after cleanup: %v", err)
	}
	if got := stagingReservationCount(t, database, job.Task.ID); got != 0 {
		t.Fatalf("reservations after cleanup=%d", got)
	}
}

func TestStagingFileCoordinatorQuotaFailureIsNotReady(t *testing.T) {
	capacity := int64(100 * 1024 * 1024 * 1024)
	stat := func(context.Context, string) (StagingVolumeStat, error) {
		return StagingVolumeStat{
			ID:            "volume-guid-private",
			FreeBytes:     VolumeReserveFloor(capacity) + ReservationExtentBytes - 1,
			CapacityBytes: capacity,
		}, nil
	}
	coordinator, database, job, directory := newStagingFileTest(t, "job-stage-quota", stat)
	result, err := coordinator.Stage(context.Background(), StageImportFileRequest{
		Job: job, StagingDirectory: directory, Format: ImportStageLP,
		Source: strings.NewReader("cpu value=1i 1\n"),
	})
	if !errors.Is(err, ErrPrivateQuota) || result.StagingPath != "" {
		t.Fatalf("result=%+v error=%v", result, err)
	}
	_, partPath, finalPath, pathErr := stagingFilePaths(directory, job.Task.ID)
	if pathErr != nil {
		t.Fatal(pathErr)
	}
	if info, statErr := os.Stat(partPath); statErr != nil || info.Size() != 0 {
		t.Fatalf("exclusive part missing after fail-closed error: info=%v error=%v", info, statErr)
	}
	if _, statErr := os.Stat(finalPath); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("final exists after quota rejection: %v", statErr)
	}
	if got := stagingReservationCount(t, database, job.Task.ID); got != 0 {
		t.Fatalf("quota rejection created reservations=%d", got)
	}
	if err := coordinator.Cleanup(context.Background(), result.Cleanup); err != nil {
		t.Fatal(err)
	}
}

func TestStagingFileCoordinatorFailuresKeepConservativeReservationUntilCleanup(t *testing.T) {
	tests := []struct {
		name      string
		configure func(*StagingFileCoordinator)
		want      error
	}{
		{
			name: "before sync",
			configure: func(coordinator *StagingFileCoordinator) {
				coordinator.hooks.beforeSync = func(context.Context, string) error { return errors.New("injected") }
			},
			want: ErrStageFileSync,
		},
		{
			name: "before rename",
			configure: func(coordinator *StagingFileCoordinator) {
				coordinator.hooks.beforeRename = func(context.Context, string, string) error { return errors.New("injected") }
			},
			want: ErrStageFileCommit,
		},
		{
			name: "rename",
			configure: func(coordinator *StagingFileCoordinator) {
				coordinator.hooks.rename = func(string, string) error { return errors.New("injected") }
			},
			want: ErrStageFileCommit,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			jobID := "job-stage-" + strings.ReplaceAll(test.name, " ", "-")
			coordinator, database, job, directory := newStagingFileTest(t, jobID, healthyStagingVolume)
			test.configure(coordinator)
			result, err := coordinator.Stage(context.Background(), StageImportFileRequest{
				Job: job, StagingDirectory: directory, Format: ImportStageLP,
				Source: strings.NewReader("cpu value=1i 1\n"),
			})
			if !errors.Is(err, test.want) || result.StagingPath != "" || len(result.Cleanup.ReservationIDs) != 1 {
				t.Fatalf("result=%+v error=%v", result, err)
			}
			_, partPath, finalPath, pathErr := stagingFilePaths(directory, job.Task.ID)
			if pathErr != nil {
				t.Fatal(pathErr)
			}
			if _, statErr := os.Stat(partPath); statErr != nil {
				t.Fatalf("part missing before explicit cleanup: %v", statErr)
			}
			if _, statErr := os.Stat(finalPath); !errors.Is(statErr, os.ErrNotExist) {
				t.Fatalf("final exists on failed commit: %v", statErr)
			}
			var reserved, consumed string
			if err := database.DB().QueryRowContext(context.Background(), `SELECT reserved_bytes,consumed_bytes
				FROM transfer_reservations WHERE reservation_id=?`, result.Cleanup.ReservationIDs[0]).Scan(
				&reserved, &consumed,
			); err != nil {
				t.Fatal(err)
			}
			if reserved != strconv.FormatInt(ReservationExtentBytes, 10) || consumed != "0" {
				t.Fatalf("reservation prematurely settled: reserved=%s consumed=%s", reserved, consumed)
			}
			if err := coordinator.Cleanup(context.Background(), result.Cleanup); err != nil {
				t.Fatal(err)
			}
			if got := stagingReservationCount(t, database, job.Task.ID); got != 0 {
				t.Fatalf("reservations after cleanup=%d", got)
			}
		})
	}
}

func TestStagingFileCoordinatorRejectsPathEscapeAndSymlink(t *testing.T) {
	coordinator, _, job, directory := newStagingFileTest(t, "job-stage-path", healthyStagingVolume)

	t.Run("job traversal", func(t *testing.T) {
		forged := job
		forged.Task.ID = "../outside"
		_, err := coordinator.Stage(context.Background(), StageImportFileRequest{
			Job: forged, StagingDirectory: directory, Format: ImportStageLP,
			Source: strings.NewReader("cpu value=1i 1\n"),
		})
		if !errors.Is(err, ErrStageFilePathInvalid) {
			t.Fatalf("error=%v", err)
		}
	})

	t.Run("unclean directory", func(t *testing.T) {
		outside := filepath.Join(filepath.Dir(directory), "outside")
		if err := os.Mkdir(outside, 0o700); err != nil {
			t.Fatal(err)
		}
		unclean := directory + string(os.PathSeparator) + ".." + string(os.PathSeparator) + filepath.Base(outside)
		_, err := coordinator.Stage(context.Background(), StageImportFileRequest{
			Job: job, StagingDirectory: unclean, Format: ImportStageLP,
			Source: strings.NewReader("cpu value=1i 1\n"),
		})
		if !errors.Is(err, ErrStageFilePathInvalid) {
			t.Fatalf("error=%v", err)
		}
	})

	t.Run("symlink directory", func(t *testing.T) {
		link := filepath.Join(filepath.Dir(directory), "staging-link")
		if err := os.Symlink(directory, link); err != nil {
			t.Skipf("symlink unavailable: %v", err)
		}
		_, err := coordinator.Stage(context.Background(), StageImportFileRequest{
			Job: job, StagingDirectory: link, Format: ImportStageLP,
			Source: strings.NewReader("cpu value=1i 1\n"),
		})
		if !errors.Is(err, ErrStageFilePathInvalid) {
			t.Fatalf("error=%v", err)
		}
	})
}

func stagingReservationCount(t *testing.T, database *store.Store, jobID string) int {
	t.Helper()
	var count int
	if err := database.DB().QueryRowContext(context.Background(),
		"SELECT COUNT(*) FROM transfer_reservations WHERE job_id=?", jobID,
	).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}
