package transfer

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"reflect"
	"testing"

	"github.com/google/uuid"
	"github.com/influxdesk/influxdesk/internal/tasks"
)

func createIncidentForResolution(t *testing.T, repository *Repository, kind string) (ImportJob, Permit) {
	t.Helper()
	job, segmentID, permit := createRunningImport(t, repository)
	attemptID := uuid.NewString()
	if err := beginAttemptForTest(repository, context.Background(), AttemptRequest{
		JobID: job.Task.ID, BatchID: "batch-1", AttemptID: attemptID,
		RunSegmentID: segmentID, ParentCheckpointDigest: permit.CurrentCheckpointDigest,
		StartOffset: "0", EndOffset: "120", PayloadDigest: "payload-exact",
	}, job.ProfileID, permit); err != nil {
		t.Fatal(err)
	}
	incident, err := repository.RecordIncident(context.Background(), attemptID, uuid.NewString(), kind)
	if err != nil {
		t.Fatal(err)
	}
	if incident.OpenIncident == nil {
		t.Fatal("incident was not persisted")
	}
	return incident, permit
}

func resolutionPermit(parent Permit, segmentID, checkpoint string) Permit {
	parent.RunSegmentID = segmentID
	parent.CurrentCheckpointDigest = checkpoint
	return parent
}

func grantHash(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func TestAcceptPartialPauseCommitsChildIncidentAndLedgerAtomically(t *testing.T) {
	t.Parallel()
	repository, database := newTransferRepository(t)
	defer database.Close()
	job, _ := createIncidentForResolution(t, repository, "PARTIAL")
	parent := job.CheckpointDigest
	request := IncidentResolutionCommit{
		JobID: job.Task.ID, IncidentID: job.OpenIncident.IncidentID,
		ParentCheckpointDigest: parent, ExpectedStateRevision: job.Task.StateRevision,
		CommandRequestID: uuid.NewString(), Decision: DecisionAcceptPartial,
		AfterResolution: ResolutionPause, RunSegmentID: uuid.NewString(),
	}
	resolved, replayed, err := repository.ResolveIncident(context.Background(), request)
	if err != nil || replayed {
		t.Fatalf("resolved=%+v replayed=%v err=%v", resolved, replayed, err)
	}
	if resolved.Task.State != ImportPausedSafe || resolved.CheckpointDigest == parent ||
		resolved.Checkpoint.ParentDigest != parent || resolved.Checkpoint.LogicalOffset != "120" ||
		resolved.Checkpoint.LastDisposition != "ACCEPTED_PARTIAL" || !resolved.Checkpoint.Lossy ||
		resolved.OpenIncident != nil || resolved.ActiveRunSegmentID != nil {
		t.Fatalf("unexpected resolved job: %+v", resolved)
	}

	var incidentStatus, incidentResolution string
	if err := database.DB().QueryRow(`SELECT status,resolution FROM import_incidents
		WHERE incident_id=?`, request.IncidentID).Scan(&incidentStatus, &incidentResolution); err != nil {
		t.Fatal(err)
	}
	if incidentStatus != "RESOLVED" || incidentResolution != string(DecisionAcceptPartial) {
		t.Fatalf("incident status=%s resolution=%s", incidentStatus, incidentResolution)
	}
	var segmentKind, segmentState, cause string
	if err := database.DB().QueryRow(`SELECT kind,state FROM import_run_segments
		WHERE job_id=? AND run_segment_id=?`, request.JobID, request.RunSegmentID).Scan(&segmentKind, &segmentState); err != nil {
		t.Fatal(err)
	}
	if err := database.DB().QueryRow(`SELECT cause FROM import_checkpoint_transitions
		WHERE job_id=?`, request.JobID).Scan(&cause); err != nil {
		t.Fatal(err)
	}
	if segmentKind != "RESOLUTION_ONLY" || segmentState != "CLOSED" || cause != string(DecisionAcceptPartial) {
		t.Fatalf("segment=%s/%s transition=%s", segmentKind, segmentState, cause)
	}

	retry, replayed, err := repository.ResolveIncident(context.Background(), request)
	if err != nil || !replayed || !reflect.DeepEqual(resolved, retry) {
		t.Fatalf("retry=%+v replayed=%v err=%v", retry, replayed, err)
	}
	conflict := request
	conflict.Decision = DecisionAssumeCommitted
	if _, _, err := repository.ResolveIncident(context.Background(), conflict); !errors.Is(err, tasks.ErrCommandIdempotencyConflict) {
		t.Fatalf("different decision with same command id error=%v", err)
	}
}

func TestAssumeCommittedContinueInitializesPermitFromCommittedChild(t *testing.T) {
	t.Parallel()
	repository, database := newTransferRepository(t)
	defer database.Close()
	job, oldPermit := createIncidentForResolution(t, repository, "UNKNOWN")
	parent := job.CheckpointDigest
	segmentID := uuid.NewString()
	permit := resolutionPermit(oldPermit, segmentID, parent)
	request := IncidentResolutionCommit{
		JobID: job.Task.ID, IncidentID: job.OpenIncident.IncidentID,
		ParentCheckpointDigest: parent, ExpectedStateRevision: job.Task.StateRevision,
		CommandRequestID: uuid.NewString(), Decision: DecisionAssumeCommitted,
		AfterResolution: ResolutionContinue, GrantTokenHash: grantHash("grant"),
		RunSegmentID: segmentID, Permit: &permit,
	}
	resolved, replayed, err := repository.ResolveIncident(context.Background(), request)
	if err != nil || replayed {
		t.Fatalf("resolved=%+v replayed=%v err=%v", resolved, replayed, err)
	}
	if resolved.Task.State != ImportRunning || resolved.CheckpointDigest == parent ||
		resolved.Checkpoint.LastDisposition != "ASSUMED_COMMITTED" ||
		resolved.ActiveRunSegmentID == nil || *resolved.ActiveRunSegmentID != segmentID {
		t.Fatalf("unexpected continued job: %+v", resolved)
	}
	active, found := repository.permits.Get(job.Task.ID)
	if !found || active.RunSegmentID != segmentID || active.CurrentCheckpointDigest != resolved.CheckpointDigest {
		t.Fatalf("permit did not start at committed child: %+v found=%v", active, found)
	}
	if err := beginAttemptForTest(repository, context.Background(), AttemptRequest{
		JobID: job.Task.ID, BatchID: "batch-2", AttemptID: uuid.NewString(),
		RunSegmentID: segmentID, ParentCheckpointDigest: resolved.CheckpointDigest,
		StartOffset: "120", EndOffset: "240", PayloadDigest: "next-payload",
	}, job.ProfileID, active); err != nil {
		t.Fatalf("child permit could not dispatch direct successor: %v", err)
	}
}

func TestReplayExactEnforcesIncidentBytesAndPausesAfterACK(t *testing.T) {
	t.Parallel()
	repository, database := newTransferRepository(t)
	defer database.Close()
	job, oldPermit := createIncidentForResolution(t, repository, "UNKNOWN")
	parent := job.CheckpointDigest
	segmentID := uuid.NewString()
	permit := resolutionPermit(oldPermit, segmentID, parent)
	request := IncidentResolutionCommit{
		JobID: job.Task.ID, IncidentID: job.OpenIncident.IncidentID,
		ParentCheckpointDigest: parent, ExpectedStateRevision: job.Task.StateRevision,
		CommandRequestID: uuid.NewString(), Decision: DecisionReplayExact,
		AfterResolution: ResolutionPause, GrantTokenHash: grantHash("replay-grant"),
		RunSegmentID: segmentID, Permit: &permit,
	}
	replaying, replayed, err := repository.ResolveIncident(context.Background(), request)
	if err != nil || replayed || replaying.Task.State != ImportRunning || replaying.OpenIncident == nil ||
		replaying.CheckpointDigest != parent {
		t.Fatalf("replaying=%+v replayed=%v err=%v", replaying, replayed, err)
	}
	active, found := repository.permits.Get(job.Task.ID)
	if !found {
		t.Fatal("replay permit was not activated")
	}
	wrong := AttemptRequest{
		JobID: job.Task.ID, BatchID: "replay-wrong", AttemptID: uuid.NewString(),
		RunSegmentID: segmentID, ParentCheckpointDigest: parent,
		StartOffset: "0", EndOffset: "120", PayloadDigest: "different-payload",
	}
	if err := beginAttemptForTest(repository, context.Background(), wrong, job.ProfileID, active); !errors.Is(err, ErrCheckpointConflict) {
		t.Fatalf("mismatched replay payload error=%v", err)
	}
	attemptID := uuid.NewString()
	if err := beginAttemptForTest(repository, context.Background(), AttemptRequest{
		JobID: job.Task.ID, BatchID: job.OpenIncident.BatchID, AttemptID: attemptID,
		RunSegmentID: segmentID, ParentCheckpointDigest: parent,
		StartOffset: job.OpenIncident.StartOffset, EndOffset: job.OpenIncident.EndOffset,
		PayloadDigest: job.OpenIncident.PayloadDigest,
	}, job.ProfileID, active); err != nil {
		t.Fatal(err)
	}
	settled, err := repository.SettleACK(context.Background(), attemptID)
	if err != nil {
		t.Fatal(err)
	}
	if settled.Task.State != ImportPausedSafe || settled.CheckpointDigest == parent ||
		settled.Checkpoint.LastDisposition != "REPLAY_ACKED" || settled.OpenIncident != nil ||
		settled.ActiveRunSegmentID != nil {
		t.Fatalf("unexpected replay settlement: %+v", settled)
	}
	if _, found := repository.permits.Get(job.Task.ID); found {
		t.Fatal("PAUSE replay retained a permit")
	}
	var cause, incidentStatus, incidentResolution, resolutionCommandID string
	if err := database.DB().QueryRow(`SELECT cause FROM import_checkpoint_transitions
		WHERE job_id=?`, job.Task.ID).Scan(&cause); err != nil {
		t.Fatal(err)
	}
	if err := database.DB().QueryRow(`SELECT status,resolution,resolution_command_request_id FROM import_incidents
		WHERE incident_id=?`, job.OpenIncident.IncidentID).Scan(&incidentStatus, &incidentResolution,
		&resolutionCommandID); err != nil {
		t.Fatal(err)
	}
	if cause != "REPLAY_ACK" || incidentStatus != "RESOLVED" || incidentResolution != string(DecisionReplayExact) {
		t.Fatalf("cause=%s incident=%s/%s", cause, incidentStatus, incidentResolution)
	}
	if resolutionCommandID != request.CommandRequestID {
		t.Fatalf("resolution command id=%s, want %s", resolutionCommandID, request.CommandRequestID)
	}
}

func TestNormalTooLargeCommitsLimitChildWithoutAdvancingOffset(t *testing.T) {
	t.Parallel()
	repository, database := newTransferRepository(t)
	defer database.Close()
	job, segmentID, permit := createRunningImport(t, repository)
	parent := job.CheckpointDigest
	attemptID := uuid.NewString()
	if err := beginAttemptForTest(repository, context.Background(), AttemptRequest{
		JobID: job.Task.ID, BatchID: "batch-413", AttemptID: attemptID,
		RunSegmentID: segmentID, ParentCheckpointDigest: parent,
		StartOffset: "0", EndOffset: "120", PayloadDigest: "too-large",
	}, job.ProfileID, permit); err != nil {
		t.Fatal(err)
	}
	settled, err := repository.SettleTooLarge(context.Background(), attemptID)
	if err != nil {
		t.Fatal(err)
	}
	if settled.Task.State != ImportRunning || settled.CheckpointDigest == parent ||
		settled.Checkpoint.ParentDigest != parent || settled.Checkpoint.LogicalOffset != "0" ||
		settled.Checkpoint.LastDisposition != "REJECTED_TOO_LARGE" ||
		settled.Checkpoint.AdaptiveMaxPoints != "2500" || settled.Checkpoint.AdaptiveMaxBytes != "2621440" {
		t.Fatalf("unexpected 413 settlement: %+v", settled)
	}
	active, found := repository.permits.Get(job.Task.ID)
	if !found || active.CurrentCheckpointDigest != settled.CheckpointDigest {
		t.Fatalf("413 child was not reflected in permit: %+v found=%v", active, found)
	}
	var cause string
	if err := database.DB().QueryRow(`SELECT cause FROM import_checkpoint_transitions
		WHERE job_id=?`, job.Task.ID).Scan(&cause); err != nil {
		t.Fatal(err)
	}
	if cause != "BATCH_LIMIT_REDUCED" {
		t.Fatalf("transition cause=%s", cause)
	}
}

func TestReplayTooLargePreservesCheckpointAndOpenIncident(t *testing.T) {
	t.Parallel()
	repository, database := newTransferRepository(t)
	defer database.Close()
	job, oldPermit := createIncidentForResolution(t, repository, "PARTIAL")
	parent := job.CheckpointDigest
	limits := job.Checkpoint.AdaptiveMaxPoints + "/" + job.Checkpoint.AdaptiveMaxBytes
	segmentID := uuid.NewString()
	permit := resolutionPermit(oldPermit, segmentID, parent)
	request := IncidentResolutionCommit{
		JobID: job.Task.ID, IncidentID: job.OpenIncident.IncidentID,
		ParentCheckpointDigest: parent, ExpectedStateRevision: job.Task.StateRevision,
		CommandRequestID: uuid.NewString(), Decision: DecisionReplayExact,
		AfterResolution: ResolutionContinue, GrantTokenHash: grantHash("replay-413-grant"),
		RunSegmentID: segmentID, Permit: &permit,
	}
	if _, _, err := repository.ResolveIncident(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	active, found := repository.permits.Get(job.Task.ID)
	if !found {
		t.Fatal("replay permit missing")
	}
	attemptID := uuid.NewString()
	if err := beginAttemptForTest(repository, context.Background(), AttemptRequest{
		JobID: job.Task.ID, BatchID: job.OpenIncident.BatchID, AttemptID: attemptID,
		RunSegmentID: segmentID, ParentCheckpointDigest: parent,
		StartOffset: job.OpenIncident.StartOffset, EndOffset: job.OpenIncident.EndOffset,
		PayloadDigest: job.OpenIncident.PayloadDigest,
	}, job.ProfileID, active); err != nil {
		t.Fatal(err)
	}
	settled, err := repository.SettleTooLarge(context.Background(), attemptID)
	if err != nil {
		t.Fatal(err)
	}
	if settled.Task.State != ImportNeedsPartialDecision || settled.CheckpointDigest != parent ||
		settled.OpenIncident == nil || settled.OpenIncident.IncidentID != job.OpenIncident.IncidentID ||
		settled.Checkpoint.AdaptiveMaxPoints+"/"+settled.Checkpoint.AdaptiveMaxBytes != limits ||
		settled.ActiveRunSegmentID != nil {
		t.Fatalf("replay 413 changed protected lineage: %+v", settled)
	}
	if _, found := repository.permits.Get(job.Task.ID); found {
		t.Fatal("replay 413 retained permit")
	}
	var transitions int
	if err := database.DB().QueryRow(`SELECT COUNT(*) FROM import_checkpoint_transitions
		WHERE job_id=?`, job.Task.ID).Scan(&transitions); err != nil {
		t.Fatal(err)
	}
	if transitions != 0 {
		t.Fatalf("replay 413 created %d child transitions", transitions)
	}
}
