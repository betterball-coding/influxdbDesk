package transfer

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestImportRuntimeSourceUsesPersistedPrivatePathOnly(t *testing.T) {
	repository, database := newTransferRepository(t)
	defer database.Close()
	checkpoint := initialCheckpoint()
	targetDigest, err := ImportTargetDigest(ImportTarget{Database: "metrics", RetentionPolicy: "autogen"})
	if err != nil {
		t.Fatal(err)
	}
	checkpoint.TargetDigest = targetDigest
	job, _, err := repository.CreateImport(context.Background(), CreateImportRequest{
		JobID: uuid.NewString(), ProfileID: "profile", ClientScope: "scope", RequestDigest: "digest",
		LedgerExpiresAt: time.Now().Add(time.Hour), InitialCheckpoint: checkpoint,
	})
	if err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	_, _, stagingPath, err := stagingFilePaths(directory, job.Task.ID)
	if err != nil {
		t.Fatal(err)
	}
	contents := []byte("m f=1i 1\n")
	if err := os.WriteFile(stagingPath, contents, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := database.DB().Exec(`INSERT INTO import_preflight_details(
		job_id,profile_revision,connection_id,connection_generation,source_identity_digest,
		expected_source_sha256,expected_source_size,import_format,normalization_version,
		spec_digest,target_digest,target_database,target_retention_policy,staging_file_name,
		source_sha256,staging_sha256,logical_bytes,point_count,updated_at
	) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, job.Task.ID, "1", "connection", "1", "identity",
		"source", "10", "LP", "canonical-lp-v1", job.Checkpoint.SpecDigest,
		job.Checkpoint.TargetDigest, "metrics", "autogen", filepath.Base(stagingPath), job.Checkpoint.SourceSHA256,
		job.Checkpoint.StagingSHA256, strconv.Itoa(len(contents)), "1", time.Now().Format(time.RFC3339Nano)); err != nil {
		t.Fatal(err)
	}
	// Runtime source is available only after the authorized run has started.
	if _, err := repository.GetImportRuntimeSource(context.Background(), job.Task.ID, directory); err != ErrImportState {
		t.Fatalf("READY runtime source error=%v", err)
	}
	segmentID := uuid.NewString()
	permit := Permit{
		RunSegmentID: segmentID, CurrentCheckpointDigest: job.CheckpointDigest,
		ProfileID: job.ProfileID, ProfileRevision: "1", ConnectionID: "connection",
		ConnectionGeneration: "1", ProtectionRevision: "2", LeaseID: "lease",
	}
	running, _, err := repository.StartRun(context.Background(), StartRunRequest{
		JobID: job.Task.ID, ExpectedProfileID: job.ProfileID, RunSegmentID: segmentID, Kind: "NORMAL",
		CommandScope: "IMPORT/runtime/START/command", CommandDigest: "runtime-command-digest",
		ExpectedStateRevision: job.Task.StateRevision, GrantExpiresAt: repositoryTestNow.Add(time.Minute),
		CommandExpiresAt: repositoryTestNow.Add(90 * 24 * time.Hour), Permit: permit,
	})
	if err != nil {
		t.Fatal(err)
	}
	source, err := repository.GetImportRuntimeSource(context.Background(), running.Task.ID, directory)
	if err != nil {
		t.Fatal(err)
	}
	if source.StagingPath != stagingPath || source.TargetDigest != job.Checkpoint.TargetDigest ||
		source.Database != "metrics" || source.RetentionPolicy != "autogen" ||
		source.LogicalBytes != strconv.Itoa(len(contents)) {
		t.Fatalf("runtime source=%+v", source)
	}
	if _, err := repository.GetImportRuntimeSource(context.Background(), running.Task.ID, filepath.Dir(directory)); err == nil {
		t.Fatal("runtime source accepted a different staging root")
	}
	if _, err := database.DB().Exec(`UPDATE import_preflight_details SET target_database='other'
		WHERE job_id=?`, running.Task.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := repository.GetImportRuntimeSource(context.Background(), running.Task.ID, directory); !errors.Is(err, ErrCheckpointConflict) {
		t.Fatalf("tampered target error=%v", err)
	}
}
