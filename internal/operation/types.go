package operation

import (
	"errors"
	"math/big"
	"strings"
	"time"

	"github.com/influxdesk/influxdesk/internal/tasks"
)

const (
	StateQueued                = "QUEUED"
	StateDispatching           = "DISPATCHING"
	StateSucceeded             = "SUCCEEDED"
	StateRejected              = "REJECTED"
	StateRejectedNotSent       = "REJECTED_NOT_SENT"
	StateCanceledNotSent       = "CANCELED_NOT_SENT"
	StateOutcomeUnknown        = "OUTCOME_UNKNOWN"
	StateFailedInternalNotSent = "FAILED_INTERNAL_NOT_SENT"
	StateInterruptedNotSent    = "INTERRUPTED_NOT_SENT"
)

var (
	ErrPreviewUnavailable   = errors.New("MUTATION_PREVIEW_UNAVAILABLE")
	ErrPreviewExpired       = errors.New("MUTATION_PREVIEW_EXPIRED")
	ErrConfirmationMismatch = errors.New("MUTATION_CONFIRMATION_MISMATCH")
	ErrNotFound             = errors.New("OPERATION_NOT_FOUND")
	ErrStateConflict        = errors.New("OPERATION_STATE_CONFLICT")
)

type PreviewRequest struct {
	Database        string `json:"database"`
	RetentionPolicy string `json:"retentionPolicy,omitempty"`
	Query           string `json:"query"`
}

type Preview struct {
	CanonicalQuery       string  `json:"canonicalQuery"`
	OperationKind        string  `json:"operationKind"`
	Target               string  `json:"target,omitempty"`
	ConfirmationRequired bool    `json:"confirmationRequired"`
	Executable           bool    `json:"executable"`
	Token                *string `json:"token,omitempty"`
	ExpiresAt            *string `json:"expiresAt,omitempty"`
}

type ExecuteRequest struct {
	ClientRequestID string `json:"clientRequestId"`
	PreviewToken    string `json:"previewToken"`
	Confirmation    string `json:"confirmation,omitempty"`
}

type CommandEnvelope struct {
	CommandRequestID      string `json:"commandRequestId"`
	ExpectedStateRevision string `json:"expectedStateRevision"`
}

type Operation struct {
	Task                 tasks.Meta `json:"task"`
	ProfileID            string     `json:"profileId"`
	ProfileRevision      string     `json:"profileRevision"`
	ConnectionID         string     `json:"connectionId"`
	ConnectionGeneration string     `json:"connectionGeneration"`
	ProtectionRevision   string     `json:"protectionRevision"`
	ActionDigest         string     `json:"actionDigest"`
	OperationKind        string     `json:"operationKind"`
	DispatchAttempted    bool       `json:"dispatchAttempted"`
	CancelRequested      bool       `json:"cancelRequested"`
	Replayed             bool       `json:"replayed,omitempty"`
}

type Options struct {
	PreviewTTL      time.Duration
	MutationTimeout time.Duration
	Now             func() time.Time
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
