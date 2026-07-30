package transfer

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/influxdesk/influxdesk/internal/store"
	"github.com/influxdesk/influxdesk/internal/tasks"
)

const (
	CanonicalLPNormalizationV1 = "canonical-lp-v1"
	importLedgerRetention      = 90 * 24 * time.Hour
	initialImportMaxPoints     = "5000"
	initialImportMaxBytes      = "5242880"
)

type ImportSourceIdentity struct {
	SHA256    string `json:"sha256"`
	SizeBytes string `json:"sizeBytes"`
}

type ImportTarget struct {
	Database        string `json:"database"`
	RetentionPolicy string `json:"retentionPolicy,omitempty"`
}

type NumericTextFieldMapping struct {
	Measurement string          `json:"measurement"`
	Field       string          `json:"field"`
	Kind        NumericTextKind `json:"kind"`
}

// PreflightImportRequest is safe for Wails binding. SourcePath is used only to
// open the user-selected file after durable idempotency and retained-job
// admission; it is never persisted or included in public task data.
type PreflightImportRequest struct {
	ClientRequestID      string                    `json:"clientRequestId"`
	ProfileID            string                    `json:"profileId"`
	ProfileRevision      string                    `json:"profileRevision"`
	ConnectionID         string                    `json:"connectionId"`
	ConnectionGeneration string                    `json:"connectionGeneration"`
	SourcePath           string                    `json:"sourcePath"`
	Source               ImportSourceIdentity      `json:"source"`
	Format               ImportStageFormat         `json:"format"`
	Target               ImportTarget              `json:"target"`
	CSVMapping           *CSVMapping               `json:"csvMapping,omitempty"`
	NumericTextMappings  []NumericTextFieldMapping `json:"numericTextMappings,omitempty"`
}

type PreflightImportResponse struct {
	Job      ImportJob `json:"job"`
	Replayed bool      `json:"replayed"`
	Ready    bool      `json:"ready"`
}

type importPreflightHooks struct {
	afterCreate func() error
	beforeReady func() error
}

type ImportPreflightService struct {
	repository       *Repository
	staging          *StagingFileCoordinator
	stagingDirectory string
	hooks            importPreflightHooks
}

func NewImportPreflightService(
	repository *Repository,
	staging *StagingFileCoordinator,
	stagingDirectory string,
) *ImportPreflightService {
	return &ImportPreflightService{
		repository: repository, staging: staging, stagingDirectory: stagingDirectory,
	}
}

// NewImportPreflightService wires the repository-owned quota manager without
// exposing it to the Wails facade package.
func (r *Repository) NewImportPreflightService(
	stagingDirectory string,
	volumeStat StagingVolumeStatFunc,
) *ImportPreflightService {
	if r == nil {
		return nil
	}
	return NewImportPreflightService(
		r,
		NewStagingFileCoordinator(r.quota, volumeStat),
		stagingDirectory,
	)
}

// CleanupFileFunc binds the private staging root owned by this service. The
// resulting callback is backend-only; no Wails request can select a path.
func (s *ImportPreflightService) CleanupFileFunc() ImportCleanupFileFunc {
	if s == nil || s.staging == nil || s.stagingDirectory == "" {
		return nil
	}
	return s.staging.CleanupFileFunc(s.stagingDirectory)
}

// PreflightImport persists admission before touching the source. Once a task
// ID exists, staging failures are represented by the task snapshot rather than
// leaking a source path or parser input through the IPC error channel.
func (s *ImportPreflightService) PreflightImport(
	ctx context.Context,
	request PreflightImportRequest,
) (PreflightImportResponse, error) {
	if ctx == nil || s == nil || s.repository == nil || s.staging == nil {
		return PreflightImportResponse{}, ErrPreflightInvalid
	}
	canonical, err := canonicalizePreflight(request)
	if err != nil {
		return PreflightImportResponse{}, err
	}

	if replay, found, err := s.repository.lookupPreflightReplay(ctx, canonical.scope, canonical.requestDigest); err != nil {
		return PreflightImportResponse{}, err
	} else if found {
		return preflightResponse(replay, true), nil
	}
	if err := validateSourcePath(request.SourcePath); err != nil {
		return PreflightImportResponse{}, err
	}

	job, replayed, err := s.repository.createPreflight(ctx, canonical)
	if err != nil {
		return PreflightImportResponse{}, err
	}
	if replayed {
		return preflightResponse(job, true), nil
	}
	if s.hooks.afterCreate != nil {
		if err := s.hooks.afterCreate(); err != nil {
			return PreflightImportResponse{}, err
		}
	}

	job, err = s.repository.transitionPreflightToStaging(ctx, job.Task.ID)
	if err != nil {
		return PreflightImportResponse{}, err
	}
	file, err := os.Open(request.SourcePath)
	if err != nil {
		return s.failPreflight(job.Task.ID, ErrPreflightSource)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() != canonical.sourceSize {
		return s.failPreflight(job.Task.ID, ErrPreflightSourceChanged)
	}

	result, stageErr := s.staging.Stage(ctx, StageImportFileRequest{
		Job: job, StagingDirectory: s.stagingDirectory, Source: file,
		Format: request.Format, CSVMapping: request.CSVMapping,
		NumericTextResolver: numericTextResolver(canonical.numericMappings),
	})
	if stageErr != nil {
		return s.failPreflight(job.Task.ID, stageErr)
	}
	if result.Stage.SourceSHA256 != canonical.sourceSHA256 {
		return s.failPreflight(job.Task.ID, ErrPreflightSourceChanged)
	}
	if s.hooks.beforeReady != nil {
		if err := s.hooks.beforeReady(); err != nil {
			return PreflightImportResponse{}, err
		}
	}

	// The task was already accepted. Complete its durable state even if the
	// originating IPC context was canceled after the staging file committed.
	job, err = s.repository.completePreflight(context.WithoutCancel(ctx), canonical, result)
	if err != nil {
		return PreflightImportResponse{}, err
	}
	return preflightResponse(job, false), nil
}

func (s *ImportPreflightService) failPreflight(jobID string, cause error) (PreflightImportResponse, error) {
	code, message := safePreflightFailure(cause)
	job, err := s.repository.failPreflight(context.Background(), jobID, code, message)
	if err != nil {
		return PreflightImportResponse{}, err
	}
	return preflightResponse(job, false), nil
}

func preflightResponse(job ImportJob, replayed bool) PreflightImportResponse {
	return PreflightImportResponse{
		Job: job, Replayed: replayed,
		Ready: job.Task.State == ImportReady && job.CheckpointDigest != "",
	}
}

type canonicalPreflight struct {
	request              PreflightImportRequest
	scope                string
	requestDigest        string
	sourceIdentityDigest string
	sourceSHA256         string
	sourceSize           int64
	specDigest           string
	targetDigest         string
	numericMappings      []NumericTextFieldMapping
}

func canonicalizePreflight(request PreflightImportRequest) (canonicalPreflight, error) {
	if _, err := uuid.Parse(request.ClientRequestID); err != nil ||
		!validPreflightIdentifier(request.ProfileID) || !validPreflightIdentifier(request.ConnectionID) {
		return canonicalPreflight{}, ErrPreflightInvalid
	}
	profileRevision, err := canonicalDecimal(request.ProfileRevision)
	if err != nil {
		return canonicalPreflight{}, err
	}
	generation, err := canonicalDecimal(request.ConnectionGeneration)
	if err != nil {
		return canonicalPreflight{}, err
	}
	sourceSizeText, err := canonicalDecimal(request.Source.SizeBytes)
	if err != nil {
		return canonicalPreflight{}, err
	}
	sourceSize, err := strconv.ParseInt(sourceSizeText, 10, 64)
	if err != nil || sourceSize < 0 {
		return canonicalPreflight{}, ErrPreflightInvalid
	}
	sourceSHA := strings.ToLower(request.Source.SHA256)
	if !validSHA256(sourceSHA) || !validPreflightText(request.Target.Database) ||
		(request.Target.RetentionPolicy != "" && !validPreflightText(request.Target.RetentionPolicy)) {
		return canonicalPreflight{}, ErrPreflightInvalid
	}
	if !validImportStageFormat(request.Format) ||
		(request.Format == ImportStageCSV) != (request.CSVMapping != nil) {
		return canonicalPreflight{}, ErrPreflightInvalid
	}
	if request.Format != ImportStageTypedJSONL && len(request.NumericTextMappings) != 0 {
		return canonicalPreflight{}, ErrPreflightInvalid
	}

	numericMappings, err := canonicalNumericMappings(request.NumericTextMappings)
	if err != nil {
		return canonicalPreflight{}, err
	}
	csvSpec, err := canonicalCSVSpec(request.CSVMapping)
	if err != nil {
		return canonicalPreflight{}, err
	}
	sourceIdentityDigest, err := digestJSON(struct {
		SchemaVersion int    `json:"schemaVersion"`
		SHA256        string `json:"sha256"`
		SizeBytes     string `json:"sizeBytes"`
	}{1, sourceSHA, sourceSizeText})
	if err != nil {
		return canonicalPreflight{}, err
	}
	targetDigest, err := ImportTargetDigest(request.Target)
	if err != nil {
		return canonicalPreflight{}, err
	}
	specDigest, err := digestJSON(struct {
		SchemaVersion        int                       `json:"schemaVersion"`
		Format               ImportStageFormat         `json:"format"`
		NormalizationVersion string                    `json:"normalizationVersion"`
		AdaptiveMaxPoints    string                    `json:"adaptiveMaxPoints"`
		AdaptiveMaxBytes     string                    `json:"adaptiveMaxBytes"`
		CSV                  *canonicalCSVMapping      `json:"csv,omitempty"`
		NumericMappings      []NumericTextFieldMapping `json:"numericMappings,omitempty"`
	}{1, request.Format, CanonicalLPNormalizationV1, initialImportMaxPoints,
		initialImportMaxBytes, csvSpec, numericMappings})
	if err != nil {
		return canonicalPreflight{}, err
	}
	requestDigest, err := digestJSON(struct {
		SchemaVersion        int    `json:"schemaVersion"`
		ClientRequestID      string `json:"clientRequestId"`
		ProfileID            string `json:"profileId"`
		ProfileRevision      string `json:"profileRevision"`
		ConnectionID         string `json:"connectionId"`
		ConnectionGeneration string `json:"connectionGeneration"`
		SourceIdentityDigest string `json:"sourceIdentityDigest"`
		SpecDigest           string `json:"specDigest"`
		TargetDigest         string `json:"targetDigest"`
	}{1, request.ClientRequestID, request.ProfileID, profileRevision, request.ConnectionID,
		generation, sourceIdentityDigest, specDigest, targetDigest})
	if err != nil {
		return canonicalPreflight{}, err
	}
	request.ProfileRevision = profileRevision
	request.ConnectionGeneration = generation
	request.Source.SHA256 = sourceSHA
	request.Source.SizeBytes = sourceSizeText
	return canonicalPreflight{
		request:       request,
		scope:         "PreflightImport\x1f" + request.ProfileID + "\x1f" + request.ConnectionID + "\x1f" + request.ClientRequestID,
		requestDigest: requestDigest, sourceIdentityDigest: sourceIdentityDigest,
		sourceSHA256: sourceSHA, sourceSize: sourceSize, specDigest: specDigest,
		targetDigest: targetDigest, numericMappings: numericMappings,
	}, nil
}

type canonicalCSVMapping struct {
	StaticMeasurement string              `json:"staticMeasurement,omitempty"`
	MeasurementColumn string              `json:"measurementColumn,omitempty"`
	TimestampColumn   string              `json:"timestampColumn"`
	Tags              []canonicalCSVTag   `json:"tags,omitempty"`
	Fields            []preflightCSVField `json:"fields"`
}

type canonicalCSVTag struct {
	Source string `json:"source"`
	Target string `json:"target"`
}

type preflightCSVField struct {
	Source string       `json:"source"`
	Target string       `json:"target"`
	Kind   CSVFieldKind `json:"kind"`
}

func canonicalCSVSpec(mapping *CSVMapping) (*canonicalCSVMapping, error) {
	if mapping == nil {
		return nil, nil
	}
	if (mapping.StaticMeasurement == "") == (mapping.MeasurementColumn == "") ||
		mapping.TimestampColumn == "" || len(mapping.FieldColumns) == 0 {
		return nil, ErrStageCSVMappingInvalid
	}
	out := &canonicalCSVMapping{
		StaticMeasurement: mapping.StaticMeasurement,
		MeasurementColumn: mapping.MeasurementColumn,
		TimestampColumn:   mapping.TimestampColumn,
	}
	if !validOptionalPreflightText(out.StaticMeasurement) ||
		!validOptionalPreflightText(out.MeasurementColumn) || !validPreflightText(out.TimestampColumn) {
		return nil, ErrStageCSVMappingInvalid
	}
	for source, target := range mapping.TagColumns {
		if !validPreflightText(source) || !validPreflightText(target) {
			return nil, ErrStageCSVMappingInvalid
		}
		out.Tags = append(out.Tags, canonicalCSVTag{Source: source, Target: target})
	}
	for source, field := range mapping.FieldColumns {
		if !validPreflightText(source) || !validPreflightText(field.Target) || !validCSVFieldKind(field.Kind) {
			return nil, ErrStageCSVMappingInvalid
		}
		out.Fields = append(out.Fields, preflightCSVField{Source: source, Target: field.Target, Kind: field.Kind})
	}
	sort.Slice(out.Tags, func(i, j int) bool { return out.Tags[i].Source < out.Tags[j].Source })
	sort.Slice(out.Fields, func(i, j int) bool { return out.Fields[i].Source < out.Fields[j].Source })
	return out, nil
}

func canonicalNumericMappings(mappings []NumericTextFieldMapping) ([]NumericTextFieldMapping, error) {
	out := append([]NumericTextFieldMapping(nil), mappings...)
	sort.Slice(out, func(i, j int) bool {
		if out[i].Measurement == out[j].Measurement {
			return out[i].Field < out[j].Field
		}
		return out[i].Measurement < out[j].Measurement
	})
	for index, mapping := range out {
		if !validPreflightText(mapping.Measurement) || !validPreflightText(mapping.Field) ||
			(mapping.Kind != NumericTextAsInt64 && mapping.Kind != NumericTextAsUint64 && mapping.Kind != NumericTextAsFloat64) {
			return nil, ErrPreflightInvalid
		}
		if index > 0 && mapping.Measurement == out[index-1].Measurement && mapping.Field == out[index-1].Field {
			return nil, ErrPreflightInvalid
		}
	}
	return out, nil
}

func numericTextResolver(mappings []NumericTextFieldMapping) NumericTextResolver {
	if len(mappings) == 0 {
		return nil
	}
	type key struct{ measurement, field string }
	lookup := make(map[key]NumericTextKind, len(mappings))
	for _, mapping := range mappings {
		lookup[key{mapping.Measurement, mapping.Field}] = mapping.Kind
	}
	return func(measurement, field string) (NumericTextKind, bool) {
		kind, found := lookup[key{measurement, field}]
		return kind, found
	}
}

func (r *Repository) lookupPreflightReplay(
	ctx context.Context,
	scope, digest string,
) (ImportJob, bool, error) {
	entry, found, err := tasks.LookupIdempotencyInTx(ctx, r.store.DB(), scope)
	if err != nil || !found {
		return ImportJob{}, found, err
	}
	if entry.RequestDigest != digest || entry.ResourceKind != "IMPORT" {
		return ImportJob{}, true, tasks.ErrIdempotencyConflict
	}
	job, err := r.getImportInTx(ctx, r.store.DB(), entry.ResourceID)
	return job, true, err
}

func (r *Repository) createPreflight(
	ctx context.Context,
	canonical canonicalPreflight,
) (ImportJob, bool, error) {
	jobID := uuid.NewString()
	var out ImportJob
	replayed := false
	err := r.store.WithImmediate(ctx, func(tx store.Executor) error {
		entry, found, err := tasks.LookupIdempotencyInTx(ctx, tx, canonical.scope)
		if err != nil {
			return err
		}
		if found {
			if entry.RequestDigest != canonical.requestDigest || entry.ResourceKind != "IMPORT" {
				return tasks.ErrIdempotencyConflict
			}
			out, err = r.getImportInTx(ctx, tx, entry.ResourceID)
			replayed = true
			return err
		}
		if err := r.quota.AdmitInTx(ctx, tx, jobID, canonical.request.ProfileID, "IMPORT", ImportPreflighting); err != nil {
			return err
		}
		meta, err := r.tasks.CreateInTx(ctx, tx, jobID, "IMPORT", ImportPreflighting, false)
		if err != nil {
			return err
		}
		now := r.now().UTC()
		nowText := now.Format(time.RFC3339Nano)
		if _, err := tx.ExecContext(ctx, `INSERT INTO import_jobs(
			job_id,state,state_revision,snapshot_revision,checkpoint_digest,updated_at
		) VALUES(?,?,?,?,?,?)`, jobID, ImportPreflighting, meta.StateRevision,
			meta.SnapshotRevision, "", nowText); err != nil {
			return fmt.Errorf("insert preflight import: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO import_preflight_details(
			job_id,profile_revision,connection_id,connection_generation,source_identity_digest,
			expected_source_sha256,expected_source_size,import_format,normalization_version,
			spec_digest,target_digest,target_database,target_retention_policy,updated_at
		) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, jobID, canonical.request.ProfileRevision,
			canonical.request.ConnectionID, canonical.request.ConnectionGeneration,
			canonical.sourceIdentityDigest, canonical.sourceSHA256,
			canonical.request.Source.SizeBytes, string(canonical.request.Format),
			CanonicalLPNormalizationV1, canonical.specDigest, canonical.targetDigest,
			canonical.request.Target.Database, canonical.request.Target.RetentionPolicy, nowText); err != nil {
			return fmt.Errorf("insert import preflight details: %w", err)
		}
		if err := tasks.InsertIdempotencyInTx(ctx, tx, tasks.IdempotencyEntry{
			Scope: canonical.scope, RequestDigest: canonical.requestDigest, ResourceKind: "IMPORT",
			ResourceID: jobID, CreatedAt: now, ExpiresAt: now.Add(importLedgerRetention),
		}); err != nil {
			return err
		}
		out = ImportJob{Task: meta, ProfileID: canonical.request.ProfileID}
		return nil
	})
	return out, replayed, err
}

func (r *Repository) transitionPreflightToStaging(ctx context.Context, jobID string) (ImportJob, error) {
	lock := r.jobLock(jobID)
	lock.Lock()
	defer lock.Unlock()
	var out ImportJob
	err := r.store.WithImmediate(ctx, func(tx store.Executor) error {
		job, err := r.getImportInTx(ctx, tx, jobID)
		if err != nil {
			return err
		}
		if job.Task.State != ImportPreflighting || job.CheckpointDigest != "" {
			return ErrImportState
		}
		updated, err := r.tasks.ApplyInTx(ctx, tx, job.Task, tasks.Change{
			State: ImportStaging, StateChanged: true, ChangeType: "STATE",
		})
		if err != nil {
			return err
		}
		now := r.now().UTC().Format(time.RFC3339Nano)
		result, err := tx.ExecContext(ctx, `UPDATE import_jobs SET state=?,state_revision=?,
			snapshot_revision=?,updated_at=? WHERE job_id=? AND state=?`, ImportStaging,
			updated.StateRevision, updated.SnapshotRevision, now, jobID, ImportPreflighting)
		if err != nil {
			return err
		}
		if err := requireOneRow(result, ErrImportState); err != nil {
			return err
		}
		result, err = tx.ExecContext(ctx, `UPDATE transfer_jobs SET state=?,updated_at=? WHERE job_id=?`,
			ImportStaging, now, jobID)
		if err != nil {
			return err
		}
		if err := requireOneRow(result, ErrImportState); err != nil {
			return err
		}
		job.Task = updated
		out = job
		return nil
	})
	return out, err
}

func (r *Repository) completePreflight(
	ctx context.Context,
	canonical canonicalPreflight,
	result StageImportFileResult,
) (ImportJob, error) {
	jobID := result.Cleanup.JobID
	lock := r.jobLock(jobID)
	lock.Lock()
	defer lock.Unlock()
	checkpoint := Checkpoint{
		Sequence: "0", LogicalOffset: "0", AdaptiveMaxPoints: initialImportMaxPoints,
		AdaptiveMaxBytes: initialImportMaxBytes, SourceSHA256: result.Stage.SourceSHA256,
		StagingSHA256:        result.Stage.StagingSHA256,
		NormalizationVersion: CanonicalLPNormalizationV1,
		SpecDigest:           canonical.specDigest, TargetDigest: canonical.targetDigest,
	}
	digest, err := CheckpointDigest(checkpoint)
	if err != nil {
		return ImportJob{}, err
	}
	var out ImportJob
	err = r.store.WithImmediate(ctx, func(tx store.Executor) error {
		job, err := r.getImportInTx(ctx, tx, jobID)
		if err != nil {
			return err
		}
		if job.Task.State != ImportStaging || job.CheckpointDigest != "" {
			return ErrImportState
		}
		var profileRevision, connectionID, generation, sourceIdentity, sourceSHA,
			sourceSize, format, normalization, specDigest, targetDigest, targetDatabase,
			targetRetentionPolicy string
		if err := tx.QueryRowContext(ctx, `SELECT profile_revision,connection_id,
			connection_generation,source_identity_digest,expected_source_sha256,
			expected_source_size,import_format,normalization_version,spec_digest,target_digest,
			target_database,target_retention_policy
			FROM import_preflight_details WHERE job_id=?`, jobID).Scan(&profileRevision,
			&connectionID, &generation, &sourceIdentity, &sourceSHA, &sourceSize, &format,
			&normalization, &specDigest, &targetDigest, &targetDatabase, &targetRetentionPolicy); err != nil {
			return err
		}
		if profileRevision != canonical.request.ProfileRevision || connectionID != canonical.request.ConnectionID ||
			generation != canonical.request.ConnectionGeneration || sourceIdentity != canonical.sourceIdentityDigest ||
			sourceSHA != canonical.sourceSHA256 || sourceSize != canonical.request.Source.SizeBytes ||
			format != string(canonical.request.Format) || normalization != CanonicalLPNormalizationV1 ||
			specDigest != canonical.specDigest || targetDigest != canonical.targetDigest ||
			targetDatabase != canonical.request.Target.Database ||
			targetRetentionPolicy != canonical.request.Target.RetentionPolicy {
			return ErrPreflightSourceChanged
		}
		now := r.now().UTC()
		if err := insertCheckpoint(ctx, tx, jobID, digest, checkpoint, now); err != nil {
			return err
		}
		updated, err := r.tasks.ApplyInTx(ctx, tx, job.Task, tasks.Change{
			State: ImportReady, StateChanged: true, ChangeType: "STATE",
		})
		if err != nil {
			return err
		}
		nowText := now.Format(time.RFC3339Nano)
		resultUpdate, err := tx.ExecContext(ctx, `UPDATE import_jobs SET state=?,state_revision=?,
			snapshot_revision=?,checkpoint_digest=?,updated_at=? WHERE job_id=? AND state=?
			AND checkpoint_digest=''`, ImportReady, updated.StateRevision, updated.SnapshotRevision,
			digest, nowText, jobID, ImportStaging)
		if err != nil {
			return err
		}
		if err := requireOneRow(resultUpdate, ErrImportState); err != nil {
			return err
		}
		resultUpdate, err = tx.ExecContext(ctx, `UPDATE transfer_jobs SET state=?,updated_at=? WHERE job_id=?`,
			ImportReady, nowText, jobID)
		if err != nil {
			return err
		}
		if err := requireOneRow(resultUpdate, ErrImportState); err != nil {
			return err
		}
		resultUpdate, err = tx.ExecContext(ctx, `UPDATE import_preflight_details SET
			staging_file_name=?,source_sha256=?,staging_sha256=?,logical_bytes=?,point_count=?,updated_at=?
			WHERE job_id=?`, filepath.Base(result.StagingPath), result.Stage.SourceSHA256,
			result.Stage.StagingSHA256, result.Stage.LogicalBytes, result.Stage.PointCount,
			nowText, jobID)
		if err != nil {
			return err
		}
		if err := requireOneRow(resultUpdate, ErrImportState); err != nil {
			return err
		}
		job.Task = updated
		job.CheckpointDigest = digest
		job.Checkpoint = checkpoint
		out = job
		return nil
	})
	return out, err
}

func (r *Repository) failPreflight(
	ctx context.Context,
	jobID, code, message string,
) (ImportJob, error) {
	lock := r.jobLock(jobID)
	lock.Lock()
	defer lock.Unlock()
	var out ImportJob
	err := r.store.WithImmediate(ctx, func(tx store.Executor) error {
		job, err := r.getImportInTx(ctx, tx, jobID)
		if err != nil {
			return err
		}
		if job.Task.State == ImportFailed && job.Task.Terminal {
			out = job
			return nil
		}
		if job.Task.State != ImportPreflighting && job.Task.State != ImportStaging {
			return ErrImportState
		}
		updated, err := r.tasks.ApplyInTx(ctx, tx, job.Task, tasks.Change{
			State: ImportFailed, Terminal: true, StateChanged: true, ChangeType: "STATE",
			PublicErrorCode: &code, PublicSafeMessage: &message,
		})
		if err != nil {
			return err
		}
		now := r.now().UTC().Format(time.RFC3339Nano)
		result, err := tx.ExecContext(ctx, `UPDATE import_jobs SET state=?,state_revision=?,
			snapshot_revision=?,updated_at=? WHERE job_id=?`, ImportFailed,
			updated.StateRevision, updated.SnapshotRevision, now, jobID)
		if err != nil {
			return err
		}
		if err := requireOneRow(result, ErrImportState); err != nil {
			return err
		}
		result, err = tx.ExecContext(ctx, `UPDATE transfer_jobs SET state=?,updated_at=? WHERE job_id=?`,
			ImportFailed, now, jobID)
		if err != nil {
			return err
		}
		if err := requireOneRow(result, ErrImportState); err != nil {
			return err
		}
		job.Task = updated
		out = job
		return nil
	})
	return out, err
}

func safePreflightFailure(err error) (string, string) {
	switch {
	case errors.Is(err, ErrPrivateQuota), errors.Is(err, ErrStageReservation):
		return "PRIVATE_TRANSFER_QUOTA_EXCEEDED", "Private transfer storage is unavailable."
	case errors.Is(err, ErrPreflightSourceChanged):
		return "IMPORT_SOURCE_CHANGED", "The selected source no longer matches its verified identity."
	case errors.Is(err, ErrPreflightSource), errors.Is(err, ErrStageRead):
		return "IMPORT_SOURCE_UNAVAILABLE", "The selected import source could not be read."
	case errors.Is(err, ErrStageInvalidGzip), errors.Is(err, ErrStageGzipLimit):
		return "IMPORT_GZIP_INVALID", "The compressed import source failed validation."
	case errors.Is(err, ErrStagePointTooLarge):
		return "IMPORT_POINT_TOO_LARGE", "A canonical point exceeds the import size limit."
	case errors.Is(err, ErrStageMissingTimestamp):
		return "IMPORT_TIMESTAMP_REQUIRED", "Every imported point must include a nanosecond timestamp."
	case errors.Is(err, ErrStageCSVMappingRequired), errors.Is(err, ErrStageCSVMappingInvalid),
		errors.Is(err, ErrStageAmbiguousNumeric), errors.Is(err, ErrStageInvalidInput),
		errors.Is(err, ErrStageEmpty):
		return "IMPORT_SOURCE_INVALID", "The import source does not match the selected explicit mapping."
	default:
		return "IMPORT_STAGING_FAILED", "The import source could not be staged safely."
	}
}

func digestJSON(value any) (string, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}

func validSHA256(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func validImportStageFormat(format ImportStageFormat) bool {
	switch format {
	case ImportStageLP, ImportStageTXT, ImportStageLPGzip, ImportStageTypedJSONL, ImportStageCSV:
		return true
	default:
		return false
	}
}

func validateSourcePath(path string) error {
	if path == "" || strings.IndexByte(path, 0) >= 0 || !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return ErrPreflightInvalid
	}
	return nil
}

func validPreflightIdentifier(value string) bool {
	return value != "" && len(value) <= 256 && !strings.ContainsAny(value, "\x00\r\n\x1f")
}

func validPreflightText(value string) bool {
	if value == "" || len(value) > 4096 || !utf8.ValidString(value) || strings.IndexByte(value, 0) >= 0 {
		return false
	}
	for _, current := range value {
		if current == '\r' || current == '\n' {
			return false
		}
	}
	return true
}

func validOptionalPreflightText(value string) bool {
	return value == "" || validPreflightText(value)
}
