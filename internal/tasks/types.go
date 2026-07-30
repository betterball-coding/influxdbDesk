package tasks

import (
	"errors"
	"fmt"
	"math/big"
	"strings"
	"time"
)

var (
	ErrNotFound                   = errors.New("TASK_NOT_FOUND")
	ErrIdempotencyConflict        = errors.New("IDEMPOTENCY_CONFLICT")
	ErrCommandIdempotencyConflict = errors.New("COMMAND_IDEMPOTENCY_CONFLICT")
	ErrRevisionConflict           = errors.New("REVISION_CONFLICT")
	ErrInvalidDecimal             = errors.New("INVALID_EXACT_NUMBER")
)

type Meta struct {
	ID                string  `json:"id"`
	Kind              string  `json:"kind"`
	State             string  `json:"state"`
	Terminal          bool    `json:"terminal"`
	SnapshotRevision  string  `json:"snapshotRevision"`
	StateRevision     string  `json:"stateRevision"`
	LastEventSeq      string  `json:"lastEventSeq"`
	CreatedAt         string  `json:"createdAt"`
	UpdatedAt         string  `json:"updatedAt"`
	TerminalAt        *string `json:"terminalAt,omitempty"`
	PublicErrorCode   *string `json:"publicErrorCode,omitempty"`
	PublicSafeMessage *string `json:"publicSafeMessage,omitempty"`
	Archived          bool    `json:"archived"`
	ResultAvailable   bool    `json:"resultAvailable"`
}

// TaskMeta keeps the public contract name while Meta remains concise inside
// repository code.
type TaskMeta = Meta

type Event struct {
	SchemaVersion    int    `json:"schemaVersion"`
	Seq              string `json:"seq"`
	Kind             string `json:"kind"`
	ID               string `json:"id"`
	ResourceRevision string `json:"resourceRevision"`
	State            string `json:"state"`
	ChangeType       string `json:"changeType"`
	OccurredAt       string `json:"occurredAt"`
}

type CreateRequest struct {
	ID              string
	Kind            string
	InitialState    string
	ClientScope     string
	RequestDigest   string
	LedgerExpiresAt time.Time
	ResultAvailable bool
}

type Change struct {
	State             string
	Terminal          bool
	StateChanged      bool
	ChangeType        string
	PublicErrorCode   *string
	PublicSafeMessage *string
	Archived          *bool
	ResultAvailable   *bool
}

func canonicalDecimal(value string) (string, error) {
	if value == "" || strings.HasPrefix(value, "+") || (len(value) > 1 && value[0] == '0') {
		return "", ErrInvalidDecimal
	}
	n := new(big.Int)
	if _, ok := n.SetString(value, 10); !ok || n.Sign() < 0 {
		return "", ErrInvalidDecimal
	}
	return n.String(), nil
}

func incrementDecimal(value string) (string, error) {
	canonical, err := canonicalDecimal(value)
	if err != nil {
		return "", err
	}
	n := new(big.Int)
	n.SetString(canonical, 10)
	n.Add(n, big.NewInt(1))
	return n.String(), nil
}

func validatePublicText(code, message *string) error {
	if code != nil && (len(*code) > 96 || strings.ContainsAny(*code, "\r\n")) {
		return fmt.Errorf("invalid public error code")
	}
	if message != nil && (len(*message) > 512 || strings.ContainsAny(*message, "\r\n")) {
		return fmt.Errorf("invalid public safe message")
	}
	return nil
}
