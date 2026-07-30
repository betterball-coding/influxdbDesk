package protection

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math/big"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/influxdesk/influxdesk/internal/store"
)

const unlockDuration = 10 * time.Minute

type lease struct {
	id      string
	expires time.Time
}

type connectionState struct {
	gate chan struct{}
	mu   sync.Mutex

	lockWaiters  int
	locksDrained chan struct{}
	snapshot     Snapshot
	profileID    string
	profileRev   string
	lease        *lease
	closed       bool
}

func newConnectionState(snapshot Snapshot, profileID, profileRevision string) *connectionState {
	gate := make(chan struct{}, 1)
	gate <- struct{}{}
	drained := make(chan struct{})
	close(drained)
	return &connectionState{
		gate: gate, locksDrained: drained, snapshot: snapshot,
		profileID: profileID, profileRev: profileRevision,
	}
}

func (s *connectionState) acquire(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.gate:
		return nil
	}
}

func (s *connectionState) acquireAcceptedLock() { <-s.gate }

func (s *connectionState) release() { s.gate <- struct{}{} }

func (s *connectionState) markLockPending() {
	s.mu.Lock()
	if s.lockWaiters == 0 {
		s.locksDrained = make(chan struct{})
	}
	s.lockWaiters++
	s.mu.Unlock()
}

func (s *connectionState) finishLockPending() {
	s.mu.Lock()
	s.lockWaiters--
	if s.lockWaiters == 0 {
		close(s.locksDrained)
	}
	s.mu.Unlock()
}

func (s *connectionState) acquireAfterLocks(ctx context.Context) error {
	for {
		s.mu.Lock()
		pending := s.lockWaiters != 0
		drained := s.locksDrained
		s.mu.Unlock()
		if pending {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-drained:
			}
			continue
		}
		if err := s.acquire(ctx); err != nil {
			return err
		}
		s.mu.Lock()
		pending = s.lockWaiters != 0
		s.mu.Unlock()
		if !pending {
			return nil
		}
		s.release()
	}
}

type Manager struct {
	store *store.Store
	now   func() time.Time
	rand  io.Reader

	mu          sync.RWMutex
	connections map[string]*connectionState
}

func NewManager(s *store.Store, now func() time.Time, random io.Reader) *Manager {
	if now == nil {
		now = time.Now
	}
	if random == nil {
		random = rand.Reader
	}
	return &Manager{store: s, now: now, rand: random, connections: make(map[string]*connectionState)}
}

func generationKey(connectionID, generation string) string { return connectionID + "\x00" + generation }

func canonicalDecimal(value string) (string, error) {
	if value == "" || strings.HasPrefix(value, "+") || (len(value) > 1 && value[0] == '0') {
		return "", ErrBindingMismatch
	}
	n := new(big.Int)
	if _, ok := n.SetString(value, 10); !ok || n.Sign() < 0 {
		return "", ErrBindingMismatch
	}
	return n.String(), nil
}

func increment(value string) (string, error) {
	normalized, err := canonicalDecimal(value)
	if err != nil {
		return "", err
	}
	n := new(big.Int)
	n.SetString(normalized, 10)
	n.Add(n, big.NewInt(1))
	return n.String(), nil
}

func (m *Manager) Register(ctx context.Context, req RegisterRequest) (Snapshot, error) {
	if req.ConnectionID == "" || req.ProfileID == "" {
		return Snapshot{}, errors.New("connection and profile are required")
	}
	generation, err := canonicalDecimal(req.ConnectionGeneration)
	if err != nil {
		return Snapshot{}, err
	}
	profileRevision, err := canonicalDecimal(req.ProfileRevision)
	if err != nil {
		return Snapshot{}, err
	}
	if err := validateMode(req.Mode); err != nil {
		return Snapshot{}, err
	}
	if req.Mode == ProtectedUnlocked {
		return Snapshot{}, errors.New("a new generation cannot start unlocked")
	}

	snapshot := Snapshot{
		ConnectionID: req.ConnectionID, ConnectionGeneration: generation,
		ProtectionRevision: "1", Mode: req.Mode,
	}
	now := m.now().UTC().Format(time.RFC3339Nano)
	err = m.store.WithImmediate(ctx, func(tx store.Executor) error {
		_, err := tx.ExecContext(ctx, `INSERT INTO protection_snapshots(
			connection_id,connection_generation,profile_id,profile_revision,
			protection_revision,mode,updated_at
		) VALUES(?,?,?,?,?,?,?)`, req.ConnectionID, generation, req.ProfileID,
			profileRevision, "1", string(req.Mode), now)
		return err
	})
	if err != nil {
		return Snapshot{}, fmt.Errorf("register protection generation: %w", err)
	}

	m.mu.Lock()
	m.connections[generationKey(req.ConnectionID, generation)] = newConnectionState(snapshot, req.ProfileID, profileRevision)
	m.mu.Unlock()
	return snapshot, nil
}

// Recover must run before any network entry point is opened. It rebuilds
// active generation gates and turns a persisted unlocked mode back into a
// locked snapshot because leases are intentionally process-local.
func (m *Manager) Recover(ctx context.Context) error {
	type row struct {
		snapshot              Snapshot
		profileID, profileRev string
	}
	rows, err := m.store.DB().QueryContext(ctx, `SELECT connection_id,connection_generation,
		profile_id,profile_revision,protection_revision,mode
		FROM protection_snapshots WHERE closed_at IS NULL`)
	if err != nil {
		return err
	}
	var active []row
	for rows.Next() {
		var item row
		var mode string
		if err := rows.Scan(&item.snapshot.ConnectionID, &item.snapshot.ConnectionGeneration,
			&item.profileID, &item.profileRev, &item.snapshot.ProtectionRevision, &mode); err != nil {
			rows.Close()
			return err
		}
		item.snapshot.Mode = Mode(mode)
		active = append(active, item)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if err := rows.Err(); err != nil {
		return err
	}

	for i := range active {
		if active[i].snapshot.Mode != ProtectedUnlocked {
			continue
		}
		newRevision, err := increment(active[i].snapshot.ProtectionRevision)
		if err != nil {
			return err
		}
		now := m.now().UTC()
		if err := m.store.WithImmediate(ctx, func(tx store.Executor) error {
			active[i].snapshot.ProtectionRevision = newRevision
			active[i].snapshot.Mode = ProtectedLocked
			if err := writeSnapshotTx(ctx, tx, active[i].snapshot, now); err != nil {
				return err
			}
			return insertProtectionEvent(ctx, tx, active[i].snapshot, "LOCKED_ON_RESTART", now)
		}); err != nil {
			return err
		}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, item := range active {
		m.connections[generationKey(item.snapshot.ConnectionID, item.snapshot.ConnectionGeneration)] =
			newConnectionState(item.snapshot, item.profileID, item.profileRev)
	}
	return nil
}

func (m *Manager) state(connectionID, generation string) (*connectionState, error) {
	m.mu.RLock()
	state := m.connections[generationKey(connectionID, generation)]
	m.mu.RUnlock()
	if state == nil {
		return nil, ErrNotFound
	}
	return state, nil
}

func (m *Manager) Get(connectionID, generation string) (Snapshot, error) {
	state, err := m.state(connectionID, generation)
	if err != nil {
		return Snapshot{}, err
	}
	state.mu.Lock()
	snapshot := cloneSnapshot(state.snapshot)
	expired := leaseExpiredAt(snapshot.Mode, state.lease, m.now().UTC())
	state.mu.Unlock()
	if !expired {
		return snapshot, nil
	}
	if err := state.acquire(context.Background()); err != nil {
		return Snapshot{}, err
	}
	defer state.release()
	snapshot, _, err = m.reconcileExpiredLeaseUnderGate(context.Background(), state)
	return snapshot, err
}

// WithGrantBinding validates an execution binding and keeps the generation gate
// held until issue returns. Callers must create and publish the authorization
// artifact inside issue so a later Lock cannot return before issuance finishes.
func (m *Manager) WithGrantBinding(
	ctx context.Context,
	connectionID, generation string,
	issue func(DispatchBinding, Snapshot) error,
) (Snapshot, error) {
	if issue == nil {
		return Snapshot{}, errors.New("grant binding callback is required")
	}
	state, err := m.state(connectionID, generation)
	if err != nil {
		return Snapshot{}, err
	}
	state.mu.Lock()
	if state.lockWaiters != 0 {
		snapshot := cloneSnapshot(state.snapshot)
		state.mu.Unlock()
		return snapshot, ErrLockPending
	}
	state.mu.Unlock()
	if err := state.acquire(ctx); err != nil {
		return Snapshot{}, err
	}
	defer state.release()
	for {
		snapshot, expired, err := m.reconcileExpiredLeaseUnderGate(ctx, state)
		if err != nil {
			return Snapshot{}, err
		}
		if expired {
			return snapshot, ErrLeaseExpired
		}

		state.mu.Lock()
		snapshot = cloneSnapshot(state.snapshot)
		switch {
		case state.lockWaiters != 0:
			state.mu.Unlock()
			return snapshot, ErrLockPending
		case state.closed:
			state.mu.Unlock()
			return snapshot, ErrConnectionClosed
		case state.snapshot.Mode != ProtectedUnlocked || state.lease == nil:
			state.mu.Unlock()
			return snapshot, ErrLocked
		case leaseExpiredAt(state.snapshot.Mode, state.lease, m.now().UTC()):
			state.mu.Unlock()
			continue
		}
		binding := DispatchBinding{
			ConnectionID: connectionID, ConnectionGeneration: generation,
			ProfileRevision: state.profileRev, ProtectionRevision: state.snapshot.ProtectionRevision,
			LeaseID: state.lease.id,
		}
		state.mu.Unlock()
		if err := issue(binding, snapshot); err != nil {
			return snapshot, err
		}
		return snapshot, nil
	}
}

// GrantBinding is the read-only compatibility form of WithGrantBinding.
func (m *Manager) GrantBinding(ctx context.Context, connectionID, generation string) (DispatchBinding, Snapshot, error) {
	var binding DispatchBinding
	snapshot, err := m.WithGrantBinding(ctx, connectionID, generation, func(value DispatchBinding, _ Snapshot) error {
		binding = value
		return nil
	})
	return binding, snapshot, err
}

func cloneSnapshot(snapshot Snapshot) Snapshot {
	out := snapshot
	if snapshot.LeaseID != nil {
		v := *snapshot.LeaseID
		out.LeaseID = &v
	}
	if snapshot.UnlockedUntil != nil {
		v := *snapshot.UnlockedUntil
		out.UnlockedUntil = &v
	}
	return out
}

func leaseExpiredAt(mode Mode, current *lease, now time.Time) bool {
	return mode == ProtectedUnlocked && current != nil && !now.Before(current.expires)
}

// reconcileExpiredLeaseUnderGate converts an expired process-local lease into
// a durable locked snapshot. The caller must hold the generation gate.
func (m *Manager) reconcileExpiredLeaseUnderGate(ctx context.Context, state *connectionState) (Snapshot, bool, error) {
	now := m.now().UTC()
	state.mu.Lock()
	expired := leaseExpiredAt(state.snapshot.Mode, state.lease, now)
	snapshot := cloneSnapshot(state.snapshot)
	state.mu.Unlock()
	if !expired {
		return snapshot, false, nil
	}

	closed := false
	err := m.store.WithImmediate(ctx, func(tx store.Executor) error {
		current, _, _, isClosed, err := readSnapshotTx(ctx, tx, snapshot.ConnectionID, snapshot.ConnectionGeneration)
		if err != nil {
			if errors.Is(err, ErrNotFound) {
				return ErrGenerationConflict
			}
			return err
		}
		closed = isClosed
		current.LeaseID = nil
		current.UnlockedUntil = nil
		if !isClosed && current.Mode == ProtectedUnlocked {
			revision, err := increment(current.ProtectionRevision)
			if err != nil {
				return err
			}
			current.ProtectionRevision = revision
			current.Mode = ProtectedLocked
			if err := writeSnapshotTx(ctx, tx, current, now); err != nil {
				return err
			}
			if err := insertProtectionEvent(ctx, tx, current, "LEASE_EXPIRED", now); err != nil {
				return err
			}
		}
		snapshot = current
		return nil
	})
	if err != nil {
		return Snapshot{}, false, err
	}

	state.mu.Lock()
	state.snapshot = cloneSnapshot(snapshot)
	state.lease = nil
	state.closed = state.closed || closed
	state.mu.Unlock()
	return cloneSnapshot(snapshot), true, nil
}

func snapshotFromStateIfCurrent(state *connectionState, fallback Snapshot) Snapshot {
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.snapshot.ConnectionID == fallback.ConnectionID &&
		state.snapshot.ConnectionGeneration == fallback.ConnectionGeneration &&
		state.snapshot.ProtectionRevision == fallback.ProtectionRevision {
		return cloneSnapshot(state.snapshot)
	}
	return cloneSnapshot(fallback)
}

func (m *Manager) Unlock(ctx context.Context, req UnlockRequest) (CommandResult, error) {
	if _, err := uuid.Parse(req.CommandRequestID); err != nil {
		return CommandResult{}, errors.New("invalid commandRequestId")
	}
	generation, err := canonicalDecimal(req.ExpectedConnectionGeneration)
	if err != nil {
		return CommandResult{}, err
	}
	revision, err := canonicalDecimal(req.ExpectedProtectionRevision)
	if err != nil {
		return CommandResult{}, err
	}
	digest := commandDigest("UNLOCK", req.ConnectionID, generation, revision)
	scope := commandScope(req.ConnectionID, generation, "UNLOCK", req.CommandRequestID)

	if replay, found, err := m.lookupReplay(ctx, scope, digest); err != nil || found {
		return replay, err
	}
	state, err := m.state(req.ConnectionID, generation)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return m.rejectUnavailableGeneration(ctx, commandRecord{
				scope: scope, connectionID: req.ConnectionID, generation: generation,
				kind: "UNLOCK", requestID: req.CommandRequestID, digest: digest,
			})
		}
		return CommandResult{}, err
	}
	state.mu.Lock()
	if state.lockWaiters != 0 {
		state.mu.Unlock()
		return m.persistCommandRejection(ctx, commandRecord{
			scope: scope, connectionID: req.ConnectionID, generation: generation,
			kind: "UNLOCK", requestID: req.CommandRequestID, digest: digest,
		}, ErrLockPending)
	}
	state.mu.Unlock()
	if err := state.acquire(ctx); err != nil {
		return CommandResult{}, err
	}
	defer state.release()

	state.mu.Lock()
	if state.lockWaiters != 0 {
		state.mu.Unlock()
		return m.persistCommandRejection(ctx, commandRecord{
			scope: scope, connectionID: req.ConnectionID, generation: generation,
			kind: "UNLOCK", requestID: req.CommandRequestID, digest: digest,
		}, ErrLockPending)
	}
	state.mu.Unlock()
	if _, _, err := m.reconcileExpiredLeaseUnderGate(ctx, state); err != nil {
		return CommandResult{}, err
	}

	leaseBytes := make([]byte, 32)
	if _, err := io.ReadFull(m.rand, leaseBytes); err != nil {
		return CommandResult{}, fmt.Errorf("create protection lease: %w", err)
	}
	leaseID := hex.EncodeToString(leaseBytes)
	expires := m.now().UTC().Add(unlockDuration)
	var result CommandResult
	var committedResultCode string
	err = m.store.WithImmediate(ctx, func(tx store.Executor) error {
		entry, found, err := lookupLedger(ctx, tx, scope)
		if err != nil {
			return err
		}
		if found {
			if entry.digest != digest {
				return ErrCommandConflict
			}
			result, err = replayFromTx(ctx, tx, entry)
			result.Replayed = true
			if err != nil {
				committedResultCode = entry.resultCode
				return nil
			}
			return nil
		}

		current, profileID, profileRev, closed, err := readSnapshotTx(ctx, tx, req.ConnectionID, generation)
		if err != nil {
			if errors.Is(err, ErrNotFound) {
				return ErrGenerationConflict
			}
			return err
		}
		_ = profileID
		_ = profileRev
		if closed {
			committedResultCode = ErrConnectionClosed.Error()
			result = CommandResult{Snapshot: current}
			return insertLedger(ctx, tx, ledgerEntry{
				scope: scope, connectionID: req.ConnectionID, generation: generation,
				kind: "UNLOCK", requestID: req.CommandRequestID, digest: digest,
				resultCode: committedResultCode, committedAt: m.now().UTC(),
			})
		}
		if current.ProtectionRevision != revision {
			committedResultCode = ErrRevisionConflict.Error()
			result = CommandResult{Snapshot: current}
			return insertLedger(ctx, tx, ledgerEntry{
				scope: scope, connectionID: req.ConnectionID, generation: generation,
				kind: "UNLOCK", requestID: req.CommandRequestID, digest: digest,
				resultCode: committedResultCode, committedAt: m.now().UTC(),
			})
		}
		if current.Mode == PermanentReadOnly {
			committedResultCode = ErrLocked.Error()
			result = CommandResult{Snapshot: current}
			return insertLedger(ctx, tx, ledgerEntry{
				scope: scope, connectionID: req.ConnectionID, generation: generation,
				kind: "UNLOCK", requestID: req.CommandRequestID, digest: digest,
				resultCode: committedResultCode, committedAt: m.now().UTC(),
			})
		}
		newRevision, err := increment(current.ProtectionRevision)
		if err != nil {
			return err
		}
		now := m.now().UTC()
		current.ProtectionRevision = newRevision
		current.Mode = ProtectedUnlocked
		current.LeaseID = &leaseID
		expiresText := expires.Format(time.RFC3339Nano)
		current.UnlockedUntil = &expiresText
		if err := writeSnapshotTx(ctx, tx, current, now); err != nil {
			return err
		}
		if err := insertProtectionEvent(ctx, tx, current, "UNLOCKED", now); err != nil {
			return err
		}
		if err := insertLedger(ctx, tx, ledgerEntry{
			scope: scope, connectionID: req.ConnectionID, generation: generation,
			kind: "UNLOCK", requestID: req.CommandRequestID, digest: digest,
			resultCode: "OK", appliedRevision: newRevision, committedAt: now,
		}); err != nil {
			return err
		}
		result = CommandResult{Snapshot: current, AppliedProtectionRevision: newRevision}
		return nil
	})
	if err != nil {
		return CommandResult{}, err
	}
	if result.Replayed || committedResultCode != "" {
		result.Snapshot = snapshotFromStateIfCurrent(state, result.Snapshot)
	}
	if committedResultCode != "" {
		return result, commandError(committedResultCode)
	}
	if result.Replayed {
		return result, commandErrorForResult(result)
	}
	state.mu.Lock()
	state.snapshot = cloneSnapshot(result.Snapshot)
	state.lease = &lease{id: leaseID, expires: expires}
	state.mu.Unlock()
	return result, nil
}

func (m *Manager) Lock(ctx context.Context, req LockRequest) (CommandResult, error) {
	if _, err := uuid.Parse(req.CommandRequestID); err != nil {
		return CommandResult{}, errors.New("invalid commandRequestId")
	}
	generation, err := canonicalDecimal(req.ExpectedConnectionGeneration)
	if err != nil {
		return CommandResult{}, err
	}
	digest := commandDigest("LOCK", req.ConnectionID, generation, "ABSENT")
	scope := commandScope(req.ConnectionID, generation, "LOCK", req.CommandRequestID)
	if replay, found, err := m.lookupReplay(ctx, scope, digest); err != nil || found {
		return replay, err
	}
	state, err := m.state(req.ConnectionID, generation)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return m.rejectUnavailableGeneration(ctx, commandRecord{
				scope: scope, connectionID: req.ConnectionID, generation: generation,
				kind: "LOCK", requestID: req.CommandRequestID, digest: digest,
			})
		}
		return CommandResult{}, err
	}

	// This is the accepted boundary. From here the command ignores IPC
	// cancellation and completes after all earlier start barriers leave.
	state.markLockPending()
	state.acquireAcceptedLock()
	defer state.release()
	defer state.finishLockPending()

	var result CommandResult
	var committedResultCode string
	err = m.store.WithImmediate(context.Background(), func(tx store.Executor) error {
		entry, found, err := lookupLedger(context.Background(), tx, scope)
		if err != nil {
			return err
		}
		if found {
			if entry.digest != digest {
				return ErrCommandConflict
			}
			result, err = replayFromTx(context.Background(), tx, entry)
			result.Replayed = true
			if err != nil {
				committedResultCode = entry.resultCode
				return nil
			}
			return nil
		}
		current, _, _, closed, err := readSnapshotTx(context.Background(), tx, req.ConnectionID, generation)
		if err != nil {
			if errors.Is(err, ErrNotFound) {
				return ErrGenerationConflict
			}
			return err
		}
		if closed {
			committedResultCode = ErrConnectionClosed.Error()
			result = CommandResult{Snapshot: current}
			return insertLedger(context.Background(), tx, ledgerEntry{
				scope: scope, connectionID: req.ConnectionID, generation: generation,
				kind: "LOCK", requestID: req.CommandRequestID, digest: digest,
				resultCode: committedResultCode, committedAt: m.now().UTC(),
			})
		}
		newRevision, err := increment(current.ProtectionRevision)
		if err != nil {
			return err
		}
		current.ProtectionRevision = newRevision
		if current.Mode != PermanentReadOnly {
			current.Mode = ProtectedLocked
		}
		current.LeaseID = nil
		current.UnlockedUntil = nil
		now := m.now().UTC()
		if err := writeSnapshotTx(context.Background(), tx, current, now); err != nil {
			return err
		}
		if err := insertProtectionEvent(context.Background(), tx, current, "LOCKED", now); err != nil {
			return err
		}
		if err := insertLedger(context.Background(), tx, ledgerEntry{
			scope: scope, connectionID: req.ConnectionID, generation: generation,
			kind: "LOCK", requestID: req.CommandRequestID, digest: digest,
			resultCode: "OK", appliedRevision: newRevision, committedAt: now,
		}); err != nil {
			return err
		}
		result = CommandResult{Snapshot: current, AppliedProtectionRevision: newRevision}
		return nil
	})
	if err != nil {
		return CommandResult{}, err
	}
	if result.Replayed || committedResultCode != "" {
		result.Snapshot = snapshotFromStateIfCurrent(state, result.Snapshot)
	}
	if committedResultCode != "" {
		return result, commandError(committedResultCode)
	}
	state.mu.Lock()
	state.snapshot = cloneSnapshot(result.Snapshot)
	state.lease = nil
	state.mu.Unlock()
	return result, commandErrorForResult(result)
}

// BeginMutationDispatch holds the generation gate while it validates the
// protection binding, durably persists the dispatch boundary, and invokes
// beginRoundTrip. beginRoundTrip must return only after the one underlying
// RoundTrip call has actually been entered.
func (m *Manager) BeginMutationDispatch(
	ctx context.Context,
	binding DispatchBinding,
	persistDispatch func(context.Context) error,
	beginRoundTrip func(context.Context) error,
) error {
	state, err := m.state(binding.ConnectionID, binding.ConnectionGeneration)
	if err != nil {
		return err
	}
	state.mu.Lock()
	if state.lockWaiters != 0 {
		state.mu.Unlock()
		return ErrLockPending
	}
	state.mu.Unlock()
	if err := state.acquire(ctx); err != nil {
		return err
	}
	defer state.release()
	if _, expired, err := m.reconcileExpiredLeaseUnderGate(ctx, state); err != nil {
		return err
	} else if expired {
		return ErrLeaseExpired
	}

	state.mu.Lock()
	err = m.validateDispatchLocked(state, binding)
	state.mu.Unlock()
	if err != nil {
		return err
	}
	if err := persistDispatch(ctx); err != nil {
		return err
	}
	return beginRoundTrip(ctx)
}

// WithAuthorizedGate revalidates a lease binding and runs a durable, non-network
// authorization commit while holding dispatchGate. It is used to consume an
// ImportRunGrant and create its run segment without racing a pending Lock.
func (m *Manager) WithAuthorizedGate(ctx context.Context, binding DispatchBinding, commit func(context.Context) error) error {
	state, err := m.state(binding.ConnectionID, binding.ConnectionGeneration)
	if err != nil {
		return err
	}
	state.mu.Lock()
	if state.lockWaiters != 0 {
		state.mu.Unlock()
		return ErrLockPending
	}
	state.mu.Unlock()
	if err := state.acquire(ctx); err != nil {
		return err
	}
	defer state.release()
	if _, expired, err := m.reconcileExpiredLeaseUnderGate(ctx, state); err != nil {
		return err
	} else if expired {
		return ErrLeaseExpired
	}
	state.mu.Lock()
	err = m.validateDispatchLocked(state, binding)
	state.mu.Unlock()
	if err != nil {
		return err
	}
	return commit(ctx)
}

// WithGenerationGate serializes local control state with dispatch and Lock. It
// performs no authorization and must never invoke a network operation itself.
func (m *Manager) WithGenerationGate(
	ctx context.Context,
	connectionID, generation string,
	control func(context.Context) error,
) error {
	if control == nil {
		return errors.New("generation gate callback is required")
	}
	state, err := m.state(connectionID, generation)
	if err != nil {
		return err
	}
	if err := state.acquireAfterLocks(ctx); err != nil {
		return err
	}
	defer state.release()
	return control(ctx)
}

func (m *Manager) validateDispatchLocked(state *connectionState, binding DispatchBinding) error {
	if state.lockWaiters != 0 {
		return ErrLockPending
	}
	if state.closed {
		return ErrConnectionClosed
	}
	if state.snapshot.ConnectionGeneration != binding.ConnectionGeneration || state.profileRev != binding.ProfileRevision ||
		state.snapshot.ProtectionRevision != binding.ProtectionRevision {
		return ErrBindingMismatch
	}
	if state.snapshot.Mode != ProtectedUnlocked || state.lease == nil || state.lease.id != binding.LeaseID {
		return ErrLocked
	}
	if !m.now().UTC().Before(state.lease.expires) {
		return ErrLeaseExpired
	}
	return nil
}

func (m *Manager) CloseGeneration(ctx context.Context, connectionID, generation string) error {
	state, err := m.state(connectionID, generation)
	if err != nil {
		return err
	}
	state.markLockPending()
	state.acquireAcceptedLock()
	defer state.release()
	defer state.finishLockPending()

	now := m.now().UTC()
	purge := now.Add(CommandRetryWindowHours * time.Hour)
	err = m.store.WithImmediate(context.Background(), func(tx store.Executor) error {
		current, _, _, _, err := readSnapshotTx(context.Background(), tx, connectionID, generation)
		if err != nil {
			return err
		}
		if current.Mode != PermanentReadOnly {
			current.Mode = ProtectedLocked
		}
		_, err = tx.ExecContext(context.Background(), `UPDATE protection_snapshots SET
			mode=?,closed_at=?,purge_after=?,updated_at=?
			WHERE connection_id=? AND connection_generation=?`, string(current.Mode),
			now.Format(time.RFC3339Nano), purge.Format(time.RFC3339Nano), now.Format(time.RFC3339Nano),
			connectionID, generation)
		if err != nil {
			return err
		}
		return insertProtectionEvent(context.Background(), tx, current, "GENERATION_CLOSED", now)
	})
	if err != nil {
		return err
	}
	state.mu.Lock()
	state.closed = true
	state.lease = nil
	state.snapshot.LeaseID = nil
	state.snapshot.UnlockedUntil = nil
	if state.snapshot.Mode != PermanentReadOnly {
		state.snapshot.Mode = ProtectedLocked
	}
	state.mu.Unlock()
	return nil
}

func (m *Manager) PurgeClosed(ctx context.Context) (int64, error) {
	now := m.now().UTC().Format(time.RFC3339Nano)
	var deleted int64
	err := m.store.WithImmediate(ctx, func(tx store.Executor) error {
		rows, err := tx.QueryContext(ctx, `SELECT connection_id,connection_generation
			FROM protection_snapshots WHERE purge_after IS NOT NULL AND purge_after<=?`, now)
		if err != nil {
			return err
		}
		defer rows.Close()
		type key struct{ id, generation string }
		var keys []key
		for rows.Next() {
			var k key
			if err := rows.Scan(&k.id, &k.generation); err != nil {
				return err
			}
			keys = append(keys, k)
		}
		for _, k := range keys {
			if _, err := tx.ExecContext(ctx, `DELETE FROM protection_command_ledger
				WHERE connection_id=? AND connection_generation=?`, k.id, k.generation); err != nil {
				return err
			}
			result, err := tx.ExecContext(ctx, `DELETE FROM protection_snapshots
				WHERE connection_id=? AND connection_generation=?`, k.id, k.generation)
			if err != nil {
				return err
			}
			n, _ := result.RowsAffected()
			deleted += n
		}
		return nil
	})
	return deleted, err
}

func commandScope(connectionID, generation, kind, requestID string) string {
	return connectionID + "\x1f" + generation + "\x1f" + kind + "\x1f" + requestID
}

func commandDigest(kind, connectionID, generation, expectedRevision string) string {
	canonical := "ProtectionCommandCanonicalV1\n" + kind + "\n" + connectionID + "\n" + generation + "\n" + expectedRevision + "\n"
	sum := sha256.Sum256([]byte(canonical))
	return hex.EncodeToString(sum[:])
}

type commandRecord struct {
	scope, connectionID, generation, kind, requestID, digest string
}

func (m *Manager) persistCommandRejection(ctx context.Context, record commandRecord, rejection error) (CommandResult, error) {
	var result CommandResult
	resultCode := rejection.Error()
	err := m.store.WithImmediate(ctx, func(tx store.Executor) error {
		entry, found, err := lookupLedger(ctx, tx, record.scope)
		if err != nil {
			return err
		}
		if found {
			if entry.digest != record.digest {
				return ErrCommandConflict
			}
			snapshot, _, _, _, err := readSnapshotTx(ctx, tx, entry.connectionID, entry.generation)
			if err != nil {
				return err
			}
			result = CommandResult{
				Snapshot: snapshot, AppliedProtectionRevision: entry.appliedRevision, Replayed: true,
			}
			resultCode = entry.resultCode
			return nil
		}

		snapshot, _, _, _, err := readSnapshotTx(ctx, tx, record.connectionID, record.generation)
		if err != nil {
			if errors.Is(err, ErrNotFound) {
				return ErrGenerationConflict
			}
			return err
		}
		result = CommandResult{Snapshot: snapshot}
		return insertLedger(ctx, tx, ledgerEntry{
			scope: record.scope, connectionID: record.connectionID, generation: record.generation,
			kind: record.kind, requestID: record.requestID, digest: record.digest,
			resultCode: resultCode, committedAt: m.now().UTC(),
		})
	})
	if err != nil {
		return CommandResult{}, err
	}
	if state, stateErr := m.state(record.connectionID, record.generation); stateErr == nil {
		result.Snapshot = snapshotFromStateIfCurrent(state, result.Snapshot)
	}
	return result, commandError(resultCode)
}

func (m *Manager) rejectUnavailableGeneration(ctx context.Context, record commandRecord) (CommandResult, error) {
	var result CommandResult
	resultCode := ""
	err := m.store.WithImmediate(ctx, func(tx store.Executor) error {
		entry, found, err := lookupLedger(ctx, tx, record.scope)
		if err != nil {
			return err
		}
		if found {
			if entry.digest != record.digest {
				return ErrCommandConflict
			}
			snapshot, _, _, _, err := readSnapshotTx(ctx, tx, entry.connectionID, entry.generation)
			if err != nil {
				return err
			}
			result = CommandResult{
				Snapshot: snapshot, AppliedProtectionRevision: entry.appliedRevision, Replayed: true,
			}
			resultCode = entry.resultCode
			return nil
		}

		snapshot, _, _, closed, err := readSnapshotTx(ctx, tx, record.connectionID, record.generation)
		if err == nil {
			result.Snapshot = snapshot
			resultCode = ErrGenerationConflict.Error()
			if closed {
				resultCode = ErrConnectionClosed.Error()
			}
			return insertLedger(ctx, tx, ledgerEntry{
				scope: record.scope, connectionID: record.connectionID, generation: record.generation,
				kind: record.kind, requestID: record.requestID, digest: record.digest,
				resultCode: resultCode, committedAt: m.now().UTC(),
			})
		}
		if !errors.Is(err, ErrNotFound) {
			return err
		}
		resultCode, err = unavailableGenerationCode(ctx, tx, record.connectionID, record.generation)
		return err
	})
	if err != nil {
		return CommandResult{}, err
	}
	return result, commandError(resultCode)
}

func unavailableGenerationCode(ctx context.Context, tx store.Executor, connectionID, generation string) (string, error) {
	var count int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM protection_snapshots
		WHERE connection_id=?`, connectionID).Scan(&count); err != nil {
		return "", err
	}
	if count != 0 {
		return ErrGenerationConflict.Error(), nil
	}
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM protection_events
		WHERE connection_id=? AND connection_generation=?`, connectionID, generation).Scan(&count); err != nil {
		return "", err
	}
	if count != 0 {
		return ErrGenerationConflict.Error(), nil
	}
	var lastGeneration string
	err := tx.QueryRowContext(ctx, `SELECT last_generation FROM connection_generation_counters
		WHERE profile_id=?`, connectionID).Scan(&lastGeneration)
	if err == nil {
		return ErrGenerationConflict.Error(), nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return "", err
	}
	return ErrNotFound.Error(), nil
}

func commandErrorForResult(result CommandResult) error { return nil }

func (m *Manager) lookupReplay(ctx context.Context, scope, digest string) (CommandResult, bool, error) {
	entry, found, err := lookupLedger(ctx, m.store.DB(), scope)
	if err != nil || !found {
		return CommandResult{}, found, err
	}
	if entry.digest != digest {
		return CommandResult{}, true, ErrCommandConflict
	}
	if state, stateErr := m.state(entry.connectionID, entry.generation); stateErr == nil {
		if err := state.acquire(ctx); err != nil {
			return CommandResult{}, true, err
		}
		defer state.release()
		if _, _, err := m.reconcileExpiredLeaseUnderGate(ctx, state); err != nil {
			return CommandResult{}, true, err
		}
		result, replayErr := replayFromTx(ctx, m.store.DB(), entry)
		result.Replayed = true
		result.Snapshot = snapshotFromStateIfCurrent(state, result.Snapshot)
		return result, true, replayErr
	}
	result, err := replayFromTx(ctx, m.store.DB(), entry)
	result.Replayed = true
	return result, true, err
}

type ledgerEntry struct {
	scope, connectionID, generation, kind, requestID, digest string
	resultCode, appliedRevision                              string
	committedAt                                              time.Time
}

func lookupLedger(ctx context.Context, tx store.Executor, scope string) (ledgerEntry, bool, error) {
	var e ledgerEntry
	var applied sql.NullString
	var committed string
	err := tx.QueryRowContext(ctx, `SELECT scope,connection_id,connection_generation,
		command_kind,command_request_id,request_digest,result_code,
		applied_protection_revision,committed_at
		FROM protection_command_ledger WHERE scope=?`, scope).Scan(
		&e.scope, &e.connectionID, &e.generation, &e.kind, &e.requestID, &e.digest,
		&e.resultCode, &applied, &committed,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return ledgerEntry{}, false, nil
	}
	if err != nil {
		return ledgerEntry{}, false, err
	}
	if applied.Valid {
		e.appliedRevision = applied.String
	}
	e.committedAt, err = time.Parse(time.RFC3339Nano, committed)
	return e, true, err
}

func insertLedger(ctx context.Context, tx store.Executor, e ledgerEntry) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO protection_command_ledger(
		scope,connection_id,connection_generation,command_kind,command_request_id,
		request_digest,result_code,applied_protection_revision,committed_at
	) VALUES(?,?,?,?,?,?,?,?,?)`, e.scope, e.connectionID, e.generation, e.kind,
		e.requestID, e.digest, e.resultCode, nullable(e.appliedRevision),
		e.committedAt.Format(time.RFC3339Nano))
	return err
}

func replayFromTx(ctx context.Context, tx store.Executor, e ledgerEntry) (CommandResult, error) {
	snapshot, _, _, _, err := readSnapshotTx(ctx, tx, e.connectionID, e.generation)
	if err != nil {
		return CommandResult{}, err
	}
	return CommandResult{Snapshot: snapshot, AppliedProtectionRevision: e.appliedRevision, Replayed: true}, commandError(e.resultCode)
}

func readSnapshotTx(ctx context.Context, tx store.Executor, connectionID, generation string) (Snapshot, string, string, bool, error) {
	var snapshot Snapshot
	var profileID, profileRev, mode string
	var closed sql.NullString
	err := tx.QueryRowContext(ctx, `SELECT connection_id,connection_generation,profile_id,
		profile_revision,protection_revision,mode,closed_at
		FROM protection_snapshots WHERE connection_id=? AND connection_generation=?`,
		connectionID, generation).Scan(&snapshot.ConnectionID, &snapshot.ConnectionGeneration,
		&profileID, &profileRev, &snapshot.ProtectionRevision, &mode, &closed)
	if errors.Is(err, sql.ErrNoRows) {
		return Snapshot{}, "", "", false, ErrNotFound
	}
	if err != nil {
		return Snapshot{}, "", "", false, err
	}
	snapshot.Mode = Mode(mode)
	return snapshot, profileID, profileRev, closed.Valid, nil
}

func writeSnapshotTx(ctx context.Context, tx store.Executor, snapshot Snapshot, now time.Time) error {
	_, err := tx.ExecContext(ctx, `UPDATE protection_snapshots SET
		protection_revision=?,mode=?,updated_at=?
		WHERE connection_id=? AND connection_generation=?`, snapshot.ProtectionRevision,
		string(snapshot.Mode), now.Format(time.RFC3339Nano), snapshot.ConnectionID,
		snapshot.ConnectionGeneration)
	return err
}

func insertProtectionEvent(ctx context.Context, tx store.Executor, snapshot Snapshot, changeType string, now time.Time) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO protection_events(
		connection_id,connection_generation,protection_revision,mode,change_type,occurred_at
	) VALUES(?,?,?,?,?,?)`, snapshot.ConnectionID, snapshot.ConnectionGeneration,
		snapshot.ProtectionRevision, string(snapshot.Mode), changeType, now.Format(time.RFC3339Nano))
	return err
}

func nullable(value string) any {
	if value == "" {
		return nil
	}
	return value
}
