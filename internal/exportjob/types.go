package exportjob

import (
	"context"
	"errors"

	"github.com/influxdesk/influxdesk/internal/exportlane"
	"github.com/influxdesk/influxdesk/internal/tasks"
)

const (
	StateQueued            = "QUEUED"
	StateRunning           = "RUNNING"
	StateFinalizing        = "FINALIZING"
	StatePausedRestartable = "PAUSED_RESTARTABLE"
	StateCleaning          = "CLEANING"
	StateSucceeded         = "SUCCEEDED"
	StateCanceled          = "CANCELED"
	StateFailed            = "FAILED"
)

const (
	FragmentWriting    = "WRITING"
	FragmentFinalizing = "FINALIZING"
	FragmentComplete   = "COMPLETE"
	FragmentCorrupt    = "CORRUPT"

	FragmentData     = "DATA"
	FragmentManifest = "MANIFEST"
)

var (
	ErrInvalidRequest             = errors.New("INVALID_EXPORT_REQUEST")
	ErrStateConflict              = errors.New("EXPORT_STATE_CONFLICT")
	ErrFragmentNotFound           = errors.New("EXPORT_FRAGMENT_NOT_FOUND")
	ErrFragmentState              = errors.New("EXPORT_FRAGMENT_STATE_CONFLICT")
	ErrFragmentCorrupt            = errors.New("EXPORT_FRAGMENT_CORRUPT")
	ErrFragmentValidationRequired = errors.New("EXPORT_FRAGMENT_VALIDATION_REQUIRED")
	ErrConnectionJobLimit         = errors.New("EXPORT_CONNECTION_JOB_LIMIT")
	ErrArtifactReservation        = errors.New("EXPORT_ARTIFACT_RESERVATION_CONFLICT")
	ErrArtifactPartPresent        = errors.New("EXPORT_ARTIFACT_PART_PRESENT")
)

const (
	ArtifactReservationActive  = "ACTIVE"
	ArtifactReservationSettled = "SETTLED"
	ArtifactReservationAborted = "ABORTED"
)

type ReserveArtifactExtentRequest struct {
	JobID           string
	ArtifactID      string
	AttemptID       string
	ExtentOrdinal   string
	VolumeID        string
	RequestedBytes  int64
	VolumeFreeBytes int64
	VolumeCapacity  int64
	Takeover        bool
}

type ArtifactReservation struct {
	JobID         string
	ArtifactID    string
	State         string
	ActiveAttempt string
	ReservedBytes int64
	SettledBytes  int64
	Extents       []ArtifactReservationExtent
}

type ArtifactReservationExtent struct {
	Ordinal       string
	ReservationID string
	ReservedBytes int64
	ConsumedBytes int64
	AttemptID     string
}

type StartRequest struct {
	JobID                  string
	ProfileID              string
	ProfileRevision        string
	ConnectionID           string
	ConnectionGeneration   string
	ClientRequestID        string
	RequestDigest          string
	SpecDigest             string
	TargetDigest           string
	TargetVolumeID         string
	TargetReservationBytes int64
	TargetVolumeFreeBytes  int64
	TargetVolumeCapacity   int64
	Plan                   PlanDetail
}

// PlanDetail is the minimum durable input needed to deterministically rebuild
// an exportworker.Plan after a process restart. Generated InfluxQL is rebuilt
// from these normalized fields and is never persisted in an idempotency ledger.
type PlanDetail struct {
	SchemaVersion   int    `json:"schemaVersion"`
	Database        string `json:"database"`
	RetentionPolicy string `json:"retentionPolicy"`
	Measurement     string `json:"measurement"`
	StartNS         string `json:"startNs"`
	EndNS           string `json:"endNs"`
	SliceWidthNS    string `json:"sliceWidthNs"`
	OutputDirectory string `json:"outputDirectory"`
	TypePreserving  bool   `json:"typePreserving"`
	Lossy           bool   `json:"lossy"`
}

type CommandEnvelope struct {
	CommandRequestID      string `json:"commandRequestId"`
	ExpectedStateRevision string `json:"expectedStateRevision"`
}

type Job struct {
	Task                 tasks.Meta `json:"task"`
	ProfileID            string     `json:"profileId"`
	ProfileRevision      string     `json:"profileRevision"`
	ConnectionID         string     `json:"connectionId"`
	ConnectionGeneration string     `json:"connectionGeneration"`
	SpecDigest           string     `json:"specDigest"`
	TargetDigest         string     `json:"targetDigest"`
	TargetVolumeID       string     `json:"targetVolumeId"`
	TargetReservationID  *string    `json:"targetReservationId,omitempty"`
	CancelRequested      bool       `json:"cancelRequested"`
	Retained             bool       `json:"retained"`
	CleanupFinalState    *string    `json:"cleanupFinalState,omitempty"`
	Plan                 PlanDetail `json:"plan"`
	Fragments            []Fragment `json:"fragments,omitempty"`
}

type ListFilter struct {
	ProfileID string `json:"profileId"`
	State     string `json:"state"`
	Limit     int    `json:"limit"`
}

type Fragment struct {
	FragmentID       string  `json:"fragmentId"`
	JobID            string  `json:"jobId"`
	Ordinal          string  `json:"ordinal"`
	Kind             string  `json:"kind"`
	State            string  `json:"state"`
	StartNS          *string `json:"startNs,omitempty"`
	EndNS            *string `json:"endNs,omitempty"`
	PartPath         *string `json:"partPath,omitempty"`
	FinalPath        *string `json:"finalPath,omitempty"`
	CompressedSize   *string `json:"compressedSize,omitempty"`
	UncompressedSize *string `json:"uncompressedSize,omitempty"`
	ChecksumSHA256   *string `json:"checksumSha256,omitempty"`
	Reusable         bool    `json:"reusable"`
	CreatedAt        string  `json:"createdAt"`
	UpdatedAt        string  `json:"updatedAt"`
}

type BeginFragmentRequest struct {
	FragmentID string
	JobID      string
	Ordinal    string
	Kind       string
	StartNS    *string
	EndNS      *string
	PartPath   string
}

type FinalizeFragmentRequest struct {
	FragmentID       string
	PartPath         string
	FinalPath        string
	CompressedSize   string
	UncompressedSize string
	ChecksumSHA256   string
}

type ArtifactValidation struct {
	CompressedSize   string
	UncompressedSize string
	ChecksumSHA256   string
}

type FragmentValidator interface {
	ValidateFragment(context.Context, Fragment) (ArtifactValidation, error)
}

type FragmentValidatorFunc func(context.Context, Fragment) (ArtifactValidation, error)

func (f FragmentValidatorFunc) ValidateFragment(ctx context.Context, fragment Fragment) (ArtifactValidation, error) {
	return f(ctx, fragment)
}

type CleanupCompletion struct {
	JobID                 string
	ExpectedStateRevision string
}

type WorkerTransition struct {
	JobID                 string
	ExpectedStateRevision string
	PublicErrorCode       string
}

func (j Job) LaneRequest(quanta []exportlane.Quantum) (exportlane.JobRequest, error) {
	if j.Task.State != StateQueued || j.Task.Terminal || j.CancelRequested || len(quanta) == 0 {
		return exportlane.JobRequest{}, ErrStateConflict
	}
	return exportlane.JobRequest{
		ID: j.Task.ID,
		Generation: exportlane.GenerationKey{
			ConnectionID: j.ConnectionID,
			Generation:   j.ConnectionGeneration,
		},
		Quanta: quanta,
	}, nil
}
