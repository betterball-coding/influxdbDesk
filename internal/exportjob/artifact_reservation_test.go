package exportjob

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/influxdesk/influxdesk/internal/store"
	"github.com/influxdesk/influxdesk/internal/tasks"
	"github.com/influxdesk/influxdesk/internal/transfer"
)

func TestArtifactReservationReusesInitialExtentAndSettlesExactBytes(t *testing.T) {
	repo, database, _ := newExportRepository(t)
	job, fragment := startExportFragment(t, repo, "0")
	attemptID := uuid.NewString()
	first := reserveArtifactExtent(t, repo, job, fragment, attemptID, "0", false)
	if len(first.Extents) != 1 || job.TargetReservationID == nil ||
		first.Extents[0].ReservationID != *job.TargetReservationID ||
		first.ReservedBytes != transfer.ReservationExtentBytes {
		t.Fatalf("initial reservation was not reused: job=%+v ledger=%+v", job, first)
	}
	second := reserveArtifactExtent(t, repo, job, fragment, attemptID, "1", false)
	if len(second.Extents) != 2 || second.ReservedBytes != 2*transfer.ReservationExtentBytes {
		t.Fatalf("second extent ledger=%+v", second)
	}

	actual := transfer.ReservationExtentBytes + 123
	completeFragmentWithSize(t, repo, fragment, actual)
	settled, err := repo.SettleArtifactReservation(context.Background(), job.Task.ID, fragment.FragmentID, actual)
	if err != nil {
		t.Fatal(err)
	}
	if settled.State != ArtifactReservationSettled || settled.SettledBytes != actual || len(settled.Extents) != 2 {
		t.Fatalf("settled ledger=%+v", settled)
	}
	if settled.Extents[0].ReservedBytes != transfer.ReservationExtentBytes ||
		settled.Extents[0].ConsumedBytes != transfer.ReservationExtentBytes ||
		settled.Extents[1].ReservedBytes != transfer.ReservationExtentBytes ||
		settled.Extents[1].ConsumedBytes != 123 {
		t.Fatalf("settled extents=%+v", settled.Extents)
	}
	assertVolumeOutstanding(t, database, job.TargetVolumeID, 0)

	replayed, err := repo.SettleArtifactReservation(context.Background(), job.Task.ID, fragment.FragmentID, actual)
	if err != nil || replayed.SettledBytes != actual {
		t.Fatalf("settle replay=%+v error=%v", replayed, err)
	}
	if _, err := repo.SettleArtifactReservation(context.Background(), job.Task.ID, fragment.FragmentID, actual-1); !errors.Is(err, ErrArtifactReservation) {
		t.Fatalf("different settlement error=%v", err)
	}
}

func TestArtifactReservationConcurrentSameExtentCreatesOneMapping(t *testing.T) {
	repo, database, _ := newExportRepository(t)
	job, fragment := startExportFragment(t, repo, "0")
	attemptID := uuid.NewString()
	const callers = 16
	errorsSeen := make(chan error, callers)
	var wait sync.WaitGroup
	for range callers {
		wait.Add(1)
		go func() {
			defer wait.Done()
			_, err := repo.ReserveArtifactExtent(context.Background(), artifactExtentRequest(
				job, fragment, attemptID, "0", false,
			))
			errorsSeen <- err
		}()
	}
	wait.Wait()
	close(errorsSeen)
	for err := range errorsSeen {
		if err != nil {
			t.Fatalf("concurrent ReserveArtifactExtent() error=%v", err)
		}
	}
	ledger, err := repo.GetArtifactReservation(context.Background(), job.Task.ID, fragment.FragmentID)
	if err != nil || len(ledger.Extents) != 1 {
		t.Fatalf("ledger=%+v error=%v", ledger, err)
	}
	assertTableCount(t, database, "export_artifact_reservation_extents", 1)
	assertTableCount(t, database, "transfer_reservations", 1)
}

func TestArtifactReservationRestartTakeoverReusesOutstandingExtent(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "restart.db")
	database, err := store.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	repo := NewRepository(database, tasks.NewRepository(database, func() time.Time { return exportTestNow }),
		func() time.Time { return exportTestNow })
	job, fragment := startExportFragment(t, repo, "0")
	firstAttempt := uuid.NewString()
	before := reserveArtifactExtent(t, repo, job, fragment, firstAttempt, "0", false)
	reservationID := before.Extents[0].ReservationID
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}

	database, err = store.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	repo = NewRepository(database, tasks.NewRepository(database, func() time.Time { return exportTestNow }),
		func() time.Time { return exportTestNow })
	recovered, err := repo.GetArtifactReservation(ctx, job.Task.ID, fragment.FragmentID)
	if err != nil || len(recovered.Extents) != 1 || recovered.Extents[0].ReservationID != reservationID {
		t.Fatalf("recovered ledger=%+v error=%v", recovered, err)
	}
	if changed, err := repo.Recover(ctx); err != nil || changed != 1 {
		t.Fatalf("Recover() changed=%d error=%v", changed, err)
	}
	recoveredJob, err := repo.Get(ctx, job.Task.ID)
	if err != nil {
		t.Fatal(err)
	}
	restarted, _, err := repo.Restart(ctx, job.Task.ID, CommandEnvelope{
		CommandRequestID: uuid.NewString(), ExpectedStateRevision: recoveredJob.Task.StateRevision,
	})
	if err != nil {
		t.Fatal(err)
	}
	running, err := repo.BeginRun(ctx, WorkerTransition{
		JobID: job.Task.ID, ExpectedStateRevision: restarted.Task.StateRevision,
	})
	if err != nil || running.Task.State != StateRunning {
		t.Fatalf("BeginRun() job=%+v error=%v", running, err)
	}
	fragment, err = repo.RetryCorruptFragment(ctx, fragment.FragmentID, *fragment.PartPath)
	if err != nil {
		t.Fatal(err)
	}
	secondAttempt := uuid.NewString()
	takenOver := reserveArtifactExtent(t, repo, job, fragment, secondAttempt, "0", true)
	if len(takenOver.Extents) != 1 || takenOver.Extents[0].ReservationID != reservationID ||
		takenOver.ActiveAttempt != secondAttempt {
		t.Fatalf("takeover ledger=%+v", takenOver)
	}
	old := artifactExtentRequest(job, fragment, firstAttempt, "1", false)
	if _, err := repo.ReserveArtifactExtent(ctx, old); !errors.Is(err, ErrArtifactReservation) {
		t.Fatalf("old attempt error=%v", err)
	}
	assertTableCount(t, database, "transfer_reservations", 1)
}

func TestArtifactReservationChecksLiveSpaceBeforeReuse(t *testing.T) {
	repo, database, _ := newExportRepository(t)
	job, fragment := startExportFragment(t, repo, "0")
	request := artifactExtentRequest(job, fragment, uuid.NewString(), "0", false)
	request.VolumeFreeBytes = transfer.VolumeReserveFloor(request.VolumeCapacity) + transfer.ReservationExtentBytes - 1
	if _, err := repo.ReserveArtifactExtent(context.Background(), request); !errors.Is(err, transfer.ErrTargetLowSpace) {
		t.Fatalf("low-space ReserveArtifactExtent() error=%v", err)
	}
	assertTableCount(t, database, "export_artifact_reservations", 0)
	assertTableCount(t, database, "transfer_reservations", 1)

	request.VolumeFreeBytes = 90 * 1024 * 1024 * 1024
	if _, err := repo.ReserveArtifactExtent(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	request.ExtentOrdinal = "1"
	request.VolumeFreeBytes = transfer.VolumeReserveFloor(request.VolumeCapacity) +
		2*transfer.ReservationExtentBytes - 1
	if _, err := repo.ReserveArtifactExtent(context.Background(), request); !errors.Is(err, transfer.ErrTargetLowSpace) {
		t.Fatalf("new-extent low-space error=%v", err)
	}
	assertTableCount(t, database, "export_artifact_reservation_extents", 1)
	assertTableCount(t, database, "transfer_reservations", 1)
}

func TestAbortArtifactReservationRequiresDeletedPartAndIsArtifactScoped(t *testing.T) {
	repo, database, _ := newExportRepository(t)
	job, first := startExportFragment(t, repo, "0")
	second := beginExportFragment(t, repo, job.Task.ID, "1")
	reserveArtifactExtent(t, repo, job, first, uuid.NewString(), "0", false)
	secondLedger := reserveArtifactExtent(t, repo, job, second, uuid.NewString(), "0", false)
	secondReservationID := secondLedger.Extents[0].ReservationID

	if err := os.WriteFile(*first.PartPath, []byte("partial"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.MarkFragmentCorrupt(context.Background(), first.FragmentID); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.AbortArtifactReservation(context.Background(), job.Task.ID, first.FragmentID); !errors.Is(err, ErrArtifactPartPresent) {
		t.Fatalf("part-present abort error=%v", err)
	}
	if err := os.Remove(*first.PartPath); err != nil {
		t.Fatal(err)
	}
	aborted, err := repo.AbortArtifactReservation(context.Background(), job.Task.ID, first.FragmentID)
	if err != nil || aborted.State != ArtifactReservationAborted || len(aborted.Extents) != 0 {
		t.Fatalf("aborted ledger=%+v error=%v", aborted, err)
	}
	if _, err := repo.AbortArtifactReservation(context.Background(), job.Task.ID, first.FragmentID); err != nil {
		t.Fatalf("abort replay error=%v", err)
	}
	var secondCount int
	if err := database.DB().QueryRow(`SELECT COUNT(*) FROM transfer_reservations WHERE reservation_id=?`,
		secondReservationID).Scan(&secondCount); err != nil || secondCount != 1 {
		t.Fatalf("other artifact reservation count=%d error=%v", secondCount, err)
	}

	if _, err := repo.RetryCorruptFragment(context.Background(), first.FragmentID, *first.PartPath); err != nil {
		t.Fatal(err)
	}
	first, _ = repo.GetFragment(context.Background(), first.FragmentID)
	reopened := reserveArtifactExtent(t, repo, job, first, uuid.NewString(), "0", false)
	if reopened.State != ArtifactReservationActive || len(reopened.Extents) != 1 ||
		reopened.Extents[0].ReservationID == secondReservationID {
		t.Fatalf("reopened ledger=%+v", reopened)
	}
}

func startExportFragment(t *testing.T, repo *Repository, ordinal string) (Job, Fragment) {
	t.Helper()
	job := mustStart(t, repo, testStartRequest("artifact-profile"))
	if _, err := repo.BeginRun(context.Background(), WorkerTransition{
		JobID: job.Task.ID, ExpectedStateRevision: job.Task.StateRevision,
	}); err != nil {
		t.Fatal(err)
	}
	return job, beginExportFragment(t, repo, job.Task.ID, ordinal)
}

func beginExportFragment(t *testing.T, repo *Repository, jobID, ordinal string) Fragment {
	t.Helper()
	start, end := "0", "10"
	partPath := filepath.Join(t.TempDir(), "artifact-"+ordinal+".part")
	fragment, err := repo.BeginFragment(context.Background(), BeginFragmentRequest{
		FragmentID: uuid.NewString(), JobID: jobID, Ordinal: ordinal, Kind: FragmentData,
		StartNS: &start, EndNS: &end, PartPath: partPath,
	})
	if err != nil {
		t.Fatal(err)
	}
	return fragment
}

func artifactExtentRequest(
	job Job,
	fragment Fragment,
	attemptID, ordinal string,
	takeover bool,
) ReserveArtifactExtentRequest {
	return ReserveArtifactExtentRequest{
		JobID: job.Task.ID, ArtifactID: fragment.FragmentID, AttemptID: attemptID,
		ExtentOrdinal: ordinal, VolumeID: job.TargetVolumeID,
		RequestedBytes:  transfer.ReservationExtentBytes,
		VolumeFreeBytes: 90 * 1024 * 1024 * 1024,
		VolumeCapacity:  100 * 1024 * 1024 * 1024,
		Takeover:        takeover,
	}
}

func reserveArtifactExtent(
	t *testing.T,
	repo *Repository,
	job Job,
	fragment Fragment,
	attemptID, ordinal string,
	takeover bool,
) ArtifactReservation {
	t.Helper()
	reservation, err := repo.ReserveArtifactExtent(context.Background(),
		artifactExtentRequest(job, fragment, attemptID, ordinal, takeover))
	if err != nil {
		t.Fatal(err)
	}
	return reservation
}

func completeFragmentWithSize(t *testing.T, repo *Repository, fragment Fragment, compressed int64) {
	t.Helper()
	finalPath := filepath.Join(filepath.Dir(*fragment.PartPath), "artifact.lp.gz")
	if _, err := repo.MarkFragmentFinalizing(context.Background(), FinalizeFragmentRequest{
		FragmentID: fragment.FragmentID, PartPath: *fragment.PartPath, FinalPath: finalPath,
		CompressedSize: strconv.FormatInt(compressed, 10), UncompressedSize: strconv.FormatInt(compressed*2, 10),
		ChecksumSHA256: digestText("artifact-reservation"),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.CompleteFragment(context.Background(), fragment.FragmentID); err != nil {
		t.Fatal(err)
	}
}

func assertVolumeOutstanding(t *testing.T, database *store.Store, volumeID string, want int64) {
	t.Helper()
	rows, err := database.DB().Query(`SELECT reserved_bytes,consumed_bytes FROM transfer_reservations
		WHERE scope_kind='VOLUME' AND scope_id=?`, volumeID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var total int64
	for rows.Next() {
		var reservedText, consumedText string
		if err := rows.Scan(&reservedText, &consumedText); err != nil {
			t.Fatal(err)
		}
		reserved, _ := strconv.ParseInt(reservedText, 10, 64)
		consumed, _ := strconv.ParseInt(consumedText, 10, 64)
		total += reserved - consumed
	}
	if total != want {
		t.Fatalf("volume outstanding=%d, want %d", total, want)
	}
}

func assertTableCount(t *testing.T, database *store.Store, table string, want int) {
	t.Helper()
	var count int
	if err := database.DB().QueryRow("SELECT COUNT(*) FROM " + table).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != want {
		t.Fatalf("%s count=%d, want %d", table, count, want)
	}
}
