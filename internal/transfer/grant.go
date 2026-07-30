package transfer

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/influxdesk/influxdesk/internal/protection"
)

var (
	ErrGrantExpired  = errors.New("IMPORT_RUN_GRANT_EXPIRED")
	ErrGrantMismatch = errors.New("IMPORT_RUN_GRANT_MISMATCH")
	ErrGrantReserved = errors.New("IMPORT_RUN_GRANT_RESERVED")
)

type GrantAction string

const (
	GrantStart   GrantAction = "START"
	GrantResume  GrantAction = "RESUME"
	GrantResolve GrantAction = "RESOLVE"
)

type GrantBinding struct {
	JobID                 string
	CommandRequestID      string
	Action                GrantAction
	Decision              string
	AfterResolution       string
	ExpectedStateRevision string
	CheckpointDigest      string
	IncidentDigest        string
	SourceSHA256          string
	StagingSHA256         string
	NormalizationVersion  string
	SpecDigest            string
	TargetDigest          string
	ProfileID             string
	ProfileRevision       string
	ConnectionID          string
	ConnectionGeneration  string
	ProtectionRevision    string
	LeaseID               string
}

type ImportRunGrant struct {
	Token     string `json:"token"`
	ExpiresAt string `json:"expiresAt"`
}

type grantRecord struct {
	binding       GrantBinding
	digest        string
	token         string
	expires       time.Time
	reservationID string
	consumed      bool
}

type GrantRegistry struct {
	mu      sync.Mutex
	now     func() time.Time
	random  io.Reader
	records map[string]*grantRecord
	scopes  map[string]string
}

type GrantReservation struct {
	registry      *GrantRegistry
	token         string
	reservationID string
	binding       GrantBinding
	expires       time.Time
}

func NewGrantRegistry(now func() time.Time, random io.Reader) *GrantRegistry {
	if now == nil {
		now = time.Now
	}
	if random == nil {
		random = rand.Reader
	}
	return &GrantRegistry{now: now, random: random, records: make(map[string]*grantRecord), scopes: make(map[string]string)}
}

// Issue is idempotent for the same Preview command while its grant remains live.
func (r *GrantRegistry) Issue(binding GrantBinding, expires time.Time) (ImportRunGrant, error) {
	if err := validateGrantBinding(binding); err != nil {
		return ImportRunGrant{}, err
	}
	now := r.now().UTC()
	if !now.Before(expires) || expires.Sub(now) > 120*time.Second {
		return ImportRunGrant{}, ErrGrantMismatch
	}
	digest := grantBindingDigest(binding)
	scope := binding.JobID + "\x1f" + binding.CommandRequestID
	r.mu.Lock()
	defer r.mu.Unlock()
	r.purgeLocked(now)
	if token := r.scopes[scope]; token != "" {
		existing := r.records[token]
		if existing == nil || existing.digest != digest || existing.consumed || existing.reservationID != "" {
			return ImportRunGrant{}, ErrGrantMismatch
		}
		return ImportRunGrant{Token: token, ExpiresAt: existing.expires.Format(time.RFC3339Nano)}, nil
	}
	var token string
	for attempts := 0; attempts < 4; attempts++ {
		bytes := make([]byte, 32)
		if _, err := io.ReadFull(r.random, bytes); err != nil {
			return ImportRunGrant{}, err
		}
		token = hex.EncodeToString(bytes)
		if r.records[token] == nil {
			break
		}
		token = ""
	}
	if token == "" {
		return ImportRunGrant{}, errors.New("cannot allocate unique import grant")
	}
	r.records[token] = &grantRecord{binding: binding, digest: digest, token: token, expires: expires.UTC()}
	r.scopes[scope] = token
	return ImportRunGrant{Token: token, ExpiresAt: expires.UTC().Format(time.RFC3339Nano)}, nil
}

func (r *GrantRegistry) Reserve(token string, expected GrantBinding) (*GrantReservation, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.purgeLocked(r.now().UTC())
	record := r.records[token]
	if record == nil || record.consumed {
		return nil, ErrGrantExpired
	}
	if record.digest != grantBindingDigest(expected) {
		return nil, ErrGrantMismatch
	}
	if record.reservationID != "" {
		return nil, ErrGrantReserved
	}
	reservationID := uuid.NewString()
	record.reservationID = reservationID
	return &GrantReservation{
		registry: r, token: token, reservationID: reservationID,
		binding: record.binding, expires: record.expires,
	}, nil
}

func (r *GrantReservation) Binding() GrantBinding { return r.binding }

func (r *GrantReservation) ExpiresAt() time.Time { return r.expires }

// ValidateLive is called while dispatchGate is held, immediately before the
// durable run-segment transaction. Reserving just before expiry must not allow
// a caller to wait past the grant TTL and then start a run.
func (r *GrantReservation) ValidateLive() error {
	if r == nil || r.registry == nil {
		return ErrGrantMismatch
	}
	r.registry.mu.Lock()
	defer r.registry.mu.Unlock()
	record := r.registry.records[r.token]
	if record == nil || record.reservationID != r.reservationID || record.consumed {
		return ErrGrantMismatch
	}
	if !r.registry.now().UTC().Before(record.expires) {
		return ErrGrantExpired
	}
	return nil
}

func (r *GrantReservation) Commit() error {
	if r == nil || r.registry == nil {
		return ErrGrantMismatch
	}
	r.registry.mu.Lock()
	defer r.registry.mu.Unlock()
	record := r.registry.records[r.token]
	if record == nil || record.reservationID != r.reservationID || record.consumed {
		return ErrGrantMismatch
	}
	record.consumed = true
	record.reservationID = ""
	delete(r.registry.scopes, record.binding.JobID+"\x1f"+record.binding.CommandRequestID)
	return nil
}

func (r *GrantReservation) Rollback() {
	if r == nil || r.registry == nil {
		return
	}
	r.registry.mu.Lock()
	if record := r.registry.records[r.token]; record != nil && record.reservationID == r.reservationID && !record.consumed {
		record.reservationID = ""
	}
	r.registry.mu.Unlock()
}

func (r *GrantRegistry) Reset() {
	r.mu.Lock()
	r.records = make(map[string]*grantRecord)
	r.scopes = make(map[string]string)
	r.mu.Unlock()
}

func (r *GrantRegistry) purgeLocked(now time.Time) {
	for token, record := range r.records {
		if !now.Before(record.expires) && record.reservationID == "" {
			delete(r.records, token)
			delete(r.scopes, record.binding.JobID+"\x1f"+record.binding.CommandRequestID)
		}
	}
}

func validateGrantBinding(binding GrantBinding) error {
	if binding.JobID == "" || binding.CheckpointDigest == "" || binding.SourceSHA256 == "" ||
		binding.StagingSHA256 == "" || binding.NormalizationVersion == "" || binding.SpecDigest == "" ||
		binding.TargetDigest == "" || binding.ProfileID == "" || binding.ConnectionID == "" ||
		binding.LeaseID == "" {
		return ErrGrantMismatch
	}
	if _, err := uuid.Parse(binding.CommandRequestID); err != nil {
		return ErrGrantMismatch
	}
	for _, value := range []string{
		binding.ExpectedStateRevision, binding.ProfileRevision, binding.ConnectionGeneration,
		binding.ProtectionRevision,
	} {
		if _, err := canonicalDecimal(value); err != nil {
			return ErrGrantMismatch
		}
	}
	switch binding.Action {
	case GrantStart, GrantResume, GrantResolve:
	default:
		return ErrGrantMismatch
	}
	return nil
}

func grantBindingDigest(binding GrantBinding) string {
	values := []string{
		"ImportRunGrantV1", binding.JobID, binding.CommandRequestID, string(binding.Action),
		binding.Decision, binding.AfterResolution, binding.ExpectedStateRevision,
		binding.CheckpointDigest, binding.IncidentDigest, binding.SourceSHA256,
		binding.StagingSHA256, binding.NormalizationVersion, binding.SpecDigest,
		binding.TargetDigest, binding.ProfileID, binding.ProfileRevision, binding.ConnectionID,
		binding.ConnectionGeneration, binding.ProtectionRevision, binding.LeaseID,
	}
	hash := sha256.New()
	for _, value := range values {
		_, _ = hash.Write([]byte(strconv.Itoa(len(value))))
		_, _ = hash.Write([]byte{0})
		_, _ = hash.Write([]byte(value))
	}
	return hex.EncodeToString(hash.Sum(nil))
}

func ProtectionBinding(binding GrantBinding) protection.DispatchBinding {
	return protection.DispatchBinding{
		ConnectionID: binding.ConnectionID, ConnectionGeneration: binding.ConnectionGeneration,
		ProfileRevision: binding.ProfileRevision, ProtectionRevision: binding.ProtectionRevision,
		LeaseID: binding.LeaseID,
	}
}
