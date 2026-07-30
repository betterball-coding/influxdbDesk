package transfer

import (
	"errors"
	"math/big"
	"strings"
	"time"

	"github.com/influxdesk/influxdesk/internal/tasks"
)

const (
	RetainedTransferJobsGlobal           = 64
	RetainedTransferJobsPerProfile       = 16
	PrivateTransferQuotaBytes      int64 = 53_687_091_200
	ReservationExtentBytes         int64 = 67_108_864
)

const (
	ImportPreflighting         = "PREFLIGHTING"
	ImportStaging              = "STAGING"
	ImportReady                = "READY"
	ImportRunning              = "RUNNING"
	ImportPausedSafe           = "PAUSED_SAFE"
	ImportPausedRestage        = "PAUSED_RESTAGE"
	ImportNeedsPartialDecision = "NEEDS_PARTIAL_DECISION"
	ImportNeedsUnknownDecision = "NEEDS_UNKNOWN_DECISION"
	ImportCleaning             = "CLEANING"
	ImportSucceeded            = "SUCCEEDED"
	ImportCanceled             = "CANCELED"
	ImportAborted              = "ABORTED"
	ImportFailed               = "FAILED"
)

var (
	ErrTransferGlobalLimit    = errors.New("TRANSFER_JOB_LIMIT_GLOBAL")
	ErrTransferProfileLimit   = errors.New("TRANSFER_JOB_LIMIT_PROFILE")
	ErrPrivateQuota           = errors.New("PRIVATE_TRANSFER_QUOTA_EXCEEDED")
	ErrTargetLowSpace         = errors.New("TARGET_VOLUME_LOW_SPACE")
	ErrImportState            = errors.New("IMPORT_STATE_CONFLICT")
	ErrImportProfileMismatch  = errors.New("IMPORT_PROFILE_MISMATCH")
	ErrImportOffsetConflict   = errors.New("IMPORT_OFFSET_CONFLICT")
	ErrCheckpointConflict     = errors.New("IMPORT_CHECKPOINT_CONFLICT")
	ErrPermitMismatch         = errors.New("IMPORT_RUN_PERMIT_MISMATCH")
	ErrRoundTripBarrier       = errors.New("IMPORT_ROUND_TRIP_BARRIER_REQUIRED")
	ErrOpenIncident           = errors.New("IMPORT_OPEN_INCIDENT")
	ErrInvalidGzip            = errors.New("INVALID_GZIP_ARTIFACT")
	ErrPreflightInvalid       = errors.New("INVALID_IMPORT_PREFLIGHT")
	ErrPreflightSource        = errors.New("IMPORT_SOURCE_UNAVAILABLE")
	ErrPreflightSourceChanged = errors.New("IMPORT_SOURCE_CHANGED")
)

type Checkpoint struct {
	Sequence             string `json:"sequence"`
	ParentDigest         string `json:"parentDigest,omitempty"`
	LogicalOffset        string `json:"logicalOffset"`
	LastSettledBatchID   string `json:"lastSettledBatchId,omitempty"`
	LastDisposition      string `json:"lastDisposition,omitempty"`
	AdaptiveMaxPoints    string `json:"adaptiveMaxPoints"`
	AdaptiveMaxBytes     string `json:"adaptiveMaxBytes"`
	SourceSHA256         string `json:"sourceSha256"`
	StagingSHA256        string `json:"stagingSha256"`
	NormalizationVersion string `json:"normalizationVersion"`
	SpecDigest           string `json:"specDigest"`
	TargetDigest         string `json:"targetDigest"`
	Lossy                bool   `json:"lossy"`
}

type Transition struct {
	JobID                  string `json:"jobId"`
	Sequence               string `json:"sequence"`
	OwnerRunSegmentID      string `json:"ownerRunSegmentId"`
	Cause                  string `json:"cause"`
	ParentCheckpointDigest string `json:"parentCheckpointDigest"`
	ChildCheckpointDigest  string `json:"childCheckpointDigest"`
	BatchID                string `json:"batchId,omitempty"`
	AttemptID              string `json:"attemptId,omitempty"`
	IncidentID             string `json:"incidentId,omitempty"`
	CommandRequestID       string `json:"commandRequestId,omitempty"`
	CommittedAt            string `json:"committedAt"`
}

type Incident struct {
	IncidentID                 string  `json:"incidentId"`
	Kind                       string  `json:"kind"`
	Status                     string  `json:"status"`
	ParentCheckpointDigest     string  `json:"parentCheckpointDigest"`
	RunSegmentID               string  `json:"runSegmentId"`
	BatchID                    string  `json:"batchId"`
	AttemptID                  string  `json:"attemptId"`
	StartOffset                string  `json:"startOffset"`
	EndOffset                  string  `json:"endOffset"`
	PayloadDigest              string  `json:"payloadDigest"`
	ReplayParentIncidentID     *string `json:"replayParentIncidentId,omitempty"`
	Resolution                 *string `json:"resolution,omitempty"`
	ResolvedAt                 *string `json:"resolvedAt,omitempty"`
	ResolutionCommandRequestID *string `json:"resolutionCommandRequestId,omitempty"`
}

type RunSegment struct {
	RunSegmentID              string  `json:"runSegmentId"`
	JobID                     string  `json:"jobId"`
	Kind                      string  `json:"kind"`
	State                     string  `json:"state"`
	InitialCheckpointDigest   string  `json:"initialCheckpointDigest"`
	CommittedCheckpointDigest string  `json:"committedCheckpointDigest"`
	ActiveAttemptID           *string `json:"activeAttemptId,omitempty"`
	StopReason                *string `json:"stopReason,omitempty"`
}

type Permit struct {
	RunSegmentID            string `json:"runSegmentId"`
	CurrentCheckpointDigest string `json:"currentCheckpointDigest"`
	ProfileID               string `json:"profileId"`
	ProfileRevision         string `json:"profileRevision"`
	ConnectionID            string `json:"connectionId"`
	ConnectionGeneration    string `json:"connectionGeneration"`
	ProtectionRevision      string `json:"protectionRevision"`
	LeaseID                 string `json:"leaseId"`
}

type PermitInvalidationReason string

const (
	PermitInvalidatedProtectionLocked PermitInvalidationReason = "PROTECTION_LOCKED"
	PermitInvalidatedBindingChanged   PermitInvalidationReason = "PROTECTION_BINDING_CHANGED"
	PermitInvalidatedConnectionClosed PermitInvalidationReason = "CONNECTION_CLOSED"
	PermitInvalidatedLeaseExpired     PermitInvalidationReason = "LEASE_EXPIRED"
	PermitInvalidatedSessionLocked    PermitInvalidationReason = "WINDOWS_SESSION_LOCKED"
	PermitInvalidatedRunnerStopped    PermitInvalidationReason = "RUNNER_STOPPED"
)

type ImportJob struct {
	Task               tasks.Meta `json:"task"`
	ProfileID          string     `json:"profileId"`
	CheckpointDigest   string     `json:"checkpointDigest"`
	Checkpoint         Checkpoint `json:"checkpoint"`
	ActiveRunSegmentID *string    `json:"activeRunSegmentId,omitempty"`
	PauseRequested     bool       `json:"pauseRequested"`
	OpenIncident       *Incident  `json:"openIncident,omitempty"`
}

type CreateImportRequest struct {
	JobID             string
	ProfileID         string
	ClientScope       string
	RequestDigest     string
	LedgerExpiresAt   time.Time
	InitialCheckpoint Checkpoint
}

type StartRunRequest struct {
	JobID                 string
	ExpectedProfileID     string
	RunSegmentID          string
	Kind                  string
	CommandScope          string
	CommandDigest         string
	ExpectedStateRevision string
	GrantExpiresAt        time.Time
	CommandExpiresAt      time.Time
	Permit                Permit
}

type AttemptRequest struct {
	JobID                  string
	BatchID                string
	AttemptID              string
	RunSegmentID           string
	ParentCheckpointDigest string
	StartOffset            string
	EndOffset              string
	PayloadDigest          string
}

type AbortRequest struct {
	JobID                  string
	IncidentID             string
	ParentCheckpointDigest string
	ExpectedStateRevision  string
	CommandRequestID       string
}

func canonicalDecimal(value string) (string, error) {
	if value == "" || strings.HasPrefix(value, "+") || (len(value) > 1 && value[0] == '0') {
		return "", tasks.ErrInvalidDecimal
	}
	n := new(big.Int)
	if _, ok := n.SetString(value, 10); !ok || n.Sign() < 0 {
		return "", tasks.ErrInvalidDecimal
	}
	return n.String(), nil
}

func incrementDecimal(value string) (string, error) {
	value, err := canonicalDecimal(value)
	if err != nil {
		return "", err
	}
	n := new(big.Int)
	n.SetString(value, 10)
	n.Add(n, big.NewInt(1))
	return n.String(), nil
}

func compareCanonicalDecimals(left, right string) (int, error) {
	left, err := canonicalDecimal(left)
	if err != nil {
		return 0, err
	}
	right, err = canonicalDecimal(right)
	if err != nil {
		return 0, err
	}
	leftNumber := new(big.Int)
	rightNumber := new(big.Int)
	leftNumber.SetString(left, 10)
	rightNumber.SetString(right, 10)
	return leftNumber.Cmp(rightNumber), nil
}

func boolInt(v bool) int {
	if v {
		return 1
	}
	return 0
}
