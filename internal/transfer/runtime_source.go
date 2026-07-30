package transfer

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"strconv"
)

type ImportRuntimeSource struct {
	StagingPath     string
	TargetDigest    string
	Database        string
	RetentionPolicy string
	LogicalBytes    string
}

// ImportConnectionBinding is the persisted generation that admitted an Import
// job. Control commands use it to serialize with that generation's dispatch
// gate instead of trusting a caller-selected profile or the latest connection.
type ImportConnectionBinding struct {
	ProfileID            string
	ProfileRevision      string
	ConnectionID         string
	ConnectionGeneration string
}

func (r *Repository) GetImportConnectionBinding(
	ctx context.Context,
	jobID string,
) (ImportConnectionBinding, error) {
	if r == nil || r.store == nil || jobID == "" {
		return ImportConnectionBinding{}, ErrImportState
	}
	var binding ImportConnectionBinding
	err := r.store.DB().QueryRowContext(ctx, `SELECT t.profile_id,p.profile_revision,
		p.connection_id,p.connection_generation
		FROM transfer_jobs t JOIN import_preflight_details p ON p.job_id=t.job_id
		WHERE t.job_id=? AND t.kind='IMPORT'`, jobID).Scan(&binding.ProfileID,
		&binding.ProfileRevision, &binding.ConnectionID, &binding.ConnectionGeneration)
	if errors.Is(err, sql.ErrNoRows) {
		return ImportConnectionBinding{}, ErrImportState
	}
	if err != nil {
		return ImportConnectionBinding{}, err
	}
	if binding.ProfileID == "" || binding.ProfileRevision == "" || binding.ConnectionID == "" {
		return ImportConnectionBinding{}, ErrImportState
	}
	if _, err := canonicalDecimal(binding.ConnectionGeneration); err != nil {
		return ImportConnectionBinding{}, ErrImportState
	}
	return binding, nil
}

// GetImportRuntimeSource derives the immutable staging path from persisted
// metadata and the protected backend root. No caller-provided path participates.
func (r *Repository) GetImportRuntimeSource(
	ctx context.Context,
	jobID, stagingDirectory string,
) (ImportRuntimeSource, error) {
	job, err := r.GetImport(ctx, jobID)
	if err != nil {
		return ImportRuntimeSource{}, err
	}
	if job.Task.State != ImportRunning || job.CheckpointDigest == "" {
		return ImportRuntimeSource{}, ErrImportState
	}
	var fileName, stagingSHA, targetDigest, database, retentionPolicy, logicalBytes string
	err = r.store.DB().QueryRowContext(ctx, `SELECT staging_file_name,staging_sha256,
		target_digest,target_database,target_retention_policy,logical_bytes
		FROM import_preflight_details WHERE job_id=?`, jobID).Scan(
		&fileName, &stagingSHA, &targetDigest, &database, &retentionPolicy, &logicalBytes)
	if errors.Is(err, sql.ErrNoRows) {
		return ImportRuntimeSource{}, ErrImportState
	}
	if err != nil {
		return ImportRuntimeSource{}, err
	}
	canonicalTargetDigest, targetErr := ImportTargetDigest(ImportTarget{
		Database: database, RetentionPolicy: retentionPolicy,
	})
	_, _, expectedPath, err := stagingFilePaths(stagingDirectory, jobID)
	if err != nil || filepath.Base(expectedPath) != fileName || stagingSHA != job.Checkpoint.StagingSHA256 ||
		targetErr != nil || canonicalTargetDigest != targetDigest || targetDigest != job.Checkpoint.TargetDigest {
		return ImportRuntimeSource{}, ErrCheckpointConflict
	}
	logicalSize, err := strconv.ParseInt(logicalBytes, 10, 64)
	if err != nil || logicalSize <= 0 {
		return ImportRuntimeSource{}, ErrCheckpointConflict
	}
	info, err := os.Lstat(expectedPath)
	if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() || info.Size() != logicalSize {
		return ImportRuntimeSource{}, ErrCheckpointConflict
	}
	return ImportRuntimeSource{
		StagingPath: expectedPath, TargetDigest: targetDigest, Database: database,
		RetentionPolicy: retentionPolicy, LogicalBytes: logicalBytes,
	}, nil
}
