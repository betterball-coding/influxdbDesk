package protection

import (
	"errors"
	"fmt"
)

type Mode string

const (
	PermanentReadOnly Mode = "PermanentReadOnly"
	ProtectedLocked   Mode = "ProtectedLocked"
	ProtectedUnlocked Mode = "ProtectedUnlocked"
)

const CommandRetryWindowHours = 24

var (
	ErrNotFound           = errors.New("CONNECTION_NOT_FOUND")
	ErrConnectionClosed   = errors.New("CONNECTION_CLOSED")
	ErrGenerationConflict = errors.New("CONNECTION_GENERATION_CONFLICT")
	ErrRevisionConflict   = errors.New("PROTECTION_REVISION_CONFLICT")
	ErrCommandConflict    = errors.New("COMMAND_IDEMPOTENCY_CONFLICT")
	ErrLockPending        = errors.New("PROTECTION_LOCK_PENDING")
	ErrLocked             = errors.New("PROTECTION_LOCKED")
	ErrLeaseExpired       = errors.New("PROTECTION_LEASE_EXPIRED")
	ErrBindingMismatch    = errors.New("PROTECTION_BINDING_MISMATCH")
)

type Snapshot struct {
	ConnectionID         string  `json:"connectionId"`
	ConnectionGeneration string  `json:"connectionGeneration"`
	ProtectionRevision   string  `json:"protectionRevision"`
	Mode                 Mode    `json:"mode"`
	LeaseID              *string `json:"leaseId,omitempty"`
	UnlockedUntil        *string `json:"unlockedUntil,omitempty"`
}

type RegisterRequest struct {
	ConnectionID         string
	ConnectionGeneration string
	ProfileID            string
	ProfileRevision      string
	Mode                 Mode
}

type UnlockRequest struct {
	ConnectionID                 string `json:"connectionId"`
	CommandRequestID             string `json:"commandRequestId"`
	ExpectedConnectionGeneration string `json:"expectedConnectionGeneration"`
	ExpectedProtectionRevision   string `json:"expectedProtectionRevision"`
}

type LockRequest struct {
	ConnectionID                 string `json:"connectionId"`
	CommandRequestID             string `json:"commandRequestId"`
	ExpectedConnectionGeneration string `json:"expectedConnectionGeneration"`
}

type CommandResult struct {
	Snapshot                  Snapshot `json:"snapshot"`
	AppliedProtectionRevision string   `json:"appliedProtectionRevision,omitempty"`
	Replayed                  bool     `json:"replayed"`
}

type DispatchBinding struct {
	ConnectionID         string
	ConnectionGeneration string
	ProfileRevision      string
	ProtectionRevision   string
	LeaseID              string
}

type CommandError struct {
	Code string
}

func (e *CommandError) Error() string { return e.Code }

func (e *CommandError) Is(target error) bool {
	return target != nil && target.Error() == e.Code
}

func commandError(code string) error {
	if code == "" || code == "OK" {
		return nil
	}
	return &CommandError{Code: code}
}

func validateMode(mode Mode) error {
	switch mode {
	case PermanentReadOnly, ProtectedLocked, ProtectedUnlocked:
		return nil
	default:
		return fmt.Errorf("invalid protection mode %q", mode)
	}
}
