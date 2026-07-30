package protection

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/influxdesk/influxdesk/internal/store"
)

func newManager(t *testing.T) (*Manager, *store.Store, *time.Time) {
	t.Helper()
	s, err := store.Open(context.Background(), filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 7, 23, 9, 0, 0, 0, time.UTC)
	m := NewManager(s, func() time.Time { return now }, strings.NewReader(strings.Repeat("x", 4096)))
	if _, err := m.Register(context.Background(), RegisterRequest{
		ConnectionID: "c1", ConnectionGeneration: "1", ProfileID: "p1",
		ProfileRevision: "3", Mode: ProtectedLocked,
	}); err != nil {
		s.Close()
		t.Fatal(err)
	}
	return m, s, &now
}

func TestUnlockReplayDoesNotIssueAnotherLease(t *testing.T) {
	t.Parallel()
	m, s, _ := newManager(t)
	defer s.Close()
	req := UnlockRequest{
		ConnectionID: "c1", CommandRequestID: uuid.NewString(),
		ExpectedConnectionGeneration: "1", ExpectedProtectionRevision: "1",
	}
	first, err := m.Unlock(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	second, err := m.Unlock(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if !second.Replayed || first.AppliedProtectionRevision != "2" || second.AppliedProtectionRevision != "2" {
		t.Fatalf("first=%+v second=%+v", first, second)
	}
	if second.Snapshot.ProtectionRevision != "2" {
		t.Fatalf("replay revision=%s", second.Snapshot.ProtectionRevision)
	}
}

func TestConcurrentUnlockReplayUsesOneLeaseAndRevision(t *testing.T) {
	t.Parallel()
	m, s, _ := newManager(t)
	defer s.Close()
	req := UnlockRequest{
		ConnectionID: "c1", CommandRequestID: uuid.NewString(),
		ExpectedConnectionGeneration: "1", ExpectedProtectionRevision: "1",
	}
	const callers = 32
	results := make(chan CommandResult, callers)
	errorsSeen := make(chan error, callers)
	var ready sync.WaitGroup
	ready.Add(callers)
	start := make(chan struct{})
	for range callers {
		go func() {
			ready.Done()
			<-start
			result, err := m.Unlock(context.Background(), req)
			results <- result
			errorsSeen <- err
		}()
	}
	ready.Wait()
	close(start)

	leaseID := ""
	for range callers {
		if err := <-errorsSeen; err != nil {
			t.Fatal(err)
		}
		result := <-results
		if result.Snapshot.ProtectionRevision != "2" || result.AppliedProtectionRevision != "2" || result.Snapshot.LeaseID == nil {
			t.Fatalf("result=%+v", result)
		}
		if leaseID == "" {
			leaseID = *result.Snapshot.LeaseID
		} else if *result.Snapshot.LeaseID != leaseID {
			t.Fatalf("multiple leases returned: %q and %q", leaseID, *result.Snapshot.LeaseID)
		}
	}
	var ledgerCount, eventCount int
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM protection_command_ledger WHERE command_request_id=?`, req.CommandRequestID).Scan(&ledgerCount); err != nil {
		t.Fatal(err)
	}
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM protection_events WHERE change_type='UNLOCKED'`).Scan(&eventCount); err != nil {
		t.Fatal(err)
	}
	if ledgerCount != 1 || eventCount != 1 {
		t.Fatalf("ledger=%d events=%d", ledgerCount, eventCount)
	}
}

func TestExpiredLeaseConvergesToLockedSnapshot(t *testing.T) {
	t.Parallel()
	m, s, now := newManager(t)
	defer s.Close()
	req := UnlockRequest{
		ConnectionID: "c1", CommandRequestID: uuid.NewString(),
		ExpectedConnectionGeneration: "1", ExpectedProtectionRevision: "1",
	}
	if _, err := m.Unlock(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	*now = now.Add(unlockDuration)

	snapshot, err := m.Get("c1", "1")
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Mode != ProtectedLocked || snapshot.ProtectionRevision != "3" || snapshot.LeaseID != nil || snapshot.UnlockedUntil != nil {
		t.Fatalf("expired snapshot=%+v", snapshot)
	}
	if _, grantSnapshot, err := m.GrantBinding(context.Background(), "c1", "1"); !errors.Is(err, ErrLocked) || grantSnapshot != snapshot {
		t.Fatalf("grant snapshot=%+v err=%v", grantSnapshot, err)
	}
	replayed, err := m.Unlock(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if !replayed.Replayed || replayed.AppliedProtectionRevision != "2" || replayed.Snapshot != snapshot {
		t.Fatalf("replay=%+v", replayed)
	}
	var count int
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM protection_events WHERE change_type='LEASE_EXPIRED'`).Scan(&count); err != nil || count != 1 {
		t.Fatalf("expiry events=%d err=%v", count, err)
	}
}

func TestGrantBindingPersistsLeaseExpiry(t *testing.T) {
	t.Parallel()
	m, s, now := newManager(t)
	defer s.Close()
	if _, err := m.Unlock(context.Background(), UnlockRequest{
		ConnectionID: "c1", CommandRequestID: uuid.NewString(),
		ExpectedConnectionGeneration: "1", ExpectedProtectionRevision: "1",
	}); err != nil {
		t.Fatal(err)
	}
	*now = now.Add(unlockDuration + time.Nanosecond)
	_, snapshot, err := m.GrantBinding(context.Background(), "c1", "1")
	if !errors.Is(err, ErrLeaseExpired) {
		t.Fatalf("grant error=%v", err)
	}
	if snapshot.Mode != ProtectedLocked || snapshot.ProtectionRevision != "3" || snapshot.LeaseID != nil {
		t.Fatalf("grant snapshot=%+v", snapshot)
	}
}

func TestLockDominatesNewDispatch(t *testing.T) {
	t.Parallel()
	m, s, _ := newManager(t)
	defer s.Close()
	unlocked, err := m.Unlock(context.Background(), UnlockRequest{
		ConnectionID: "c1", CommandRequestID: uuid.NewString(),
		ExpectedConnectionGeneration: "1", ExpectedProtectionRevision: "1",
	})
	if err != nil {
		t.Fatal(err)
	}
	binding := DispatchBinding{
		ConnectionID: "c1", ConnectionGeneration: "1", ProfileRevision: "3",
		ProtectionRevision: "2", LeaseID: *unlocked.Snapshot.LeaseID,
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	firstDone := make(chan error, 1)
	go func() {
		firstDone <- m.BeginMutationDispatch(context.Background(), binding,
			func(context.Context) error { return nil },
			func(context.Context) error { close(entered); <-release; return nil })
	}()
	<-entered

	lockDone := make(chan error, 1)
	go func() {
		_, err := m.Lock(context.Background(), LockRequest{
			ConnectionID: "c1", CommandRequestID: uuid.NewString(), ExpectedConnectionGeneration: "1",
		})
		lockDone <- err
	}()
	deadline := time.Now().Add(time.Second)
	for {
		state, _ := m.state("c1", "1")
		state.mu.Lock()
		pending := state.lockWaiters
		state.mu.Unlock()
		if pending > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("lock did not become pending")
		}
		time.Sleep(time.Millisecond)
	}
	var starts atomic.Int64
	err = m.BeginMutationDispatch(context.Background(), binding,
		func(context.Context) error { return nil },
		func(context.Context) error { starts.Add(1); return nil })
	if !errors.Is(err, ErrLockPending) {
		t.Fatalf("new dispatch error=%v", err)
	}
	close(release)
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
	if err := <-lockDone; err != nil {
		t.Fatal(err)
	}
	if starts.Load() != 0 {
		t.Fatalf("late starts=%d", starts.Load())
	}
	if err := m.BeginMutationDispatch(context.Background(), binding,
		func(context.Context) error { return nil }, func(context.Context) error { return nil }); !errors.Is(err, ErrBindingMismatch) && !errors.Is(err, ErrLocked) {
		t.Fatalf("old binding error=%v", err)
	}
}

func TestGenerationGateWaitsForAllAcceptedLocks(t *testing.T) {
	t.Parallel()
	m, s, _ := newManager(t)
	defer s.Close()
	if _, err := m.Unlock(context.Background(), UnlockRequest{
		ConnectionID: "c1", CommandRequestID: uuid.NewString(),
		ExpectedConnectionGeneration: "1", ExpectedProtectionRevision: "1",
	}); err != nil {
		t.Fatal(err)
	}

	gateHeld := make(chan struct{})
	releaseGate := make(chan struct{})
	firstControlDone := make(chan error, 1)
	go func() {
		firstControlDone <- m.WithGenerationGate(context.Background(), "c1", "1", func(context.Context) error {
			close(gateHeld)
			<-releaseGate
			return nil
		})
	}()
	<-gateHeld

	lockDone := make(chan error, 2)
	for range 2 {
		go func() {
			_, err := m.Lock(context.Background(), LockRequest{
				ConnectionID: "c1", CommandRequestID: uuid.NewString(), ExpectedConnectionGeneration: "1",
			})
			lockDone <- err
		}()
	}
	waitForLockWaiters(t, m, 2)

	secondControlEntered := make(chan Snapshot, 1)
	secondControlDone := make(chan error, 1)
	go func() {
		secondControlDone <- m.WithGenerationGate(context.Background(), "c1", "1", func(context.Context) error {
			snapshot, err := m.Get("c1", "1")
			if err != nil {
				return err
			}
			secondControlEntered <- snapshot
			return nil
		})
	}()
	select {
	case snapshot := <-secondControlEntered:
		t.Fatalf("later control entered while accepted Locks were pending: %+v", snapshot)
	default:
	}

	close(releaseGate)
	if err := <-firstControlDone; err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := <-lockDone; err != nil {
			t.Fatal(err)
		}
	}
	if err := <-secondControlDone; err != nil {
		t.Fatal(err)
	}
	snapshot := <-secondControlEntered
	if snapshot.Mode != ProtectedLocked || snapshot.ProtectionRevision != "4" {
		t.Fatalf("control observed snapshot before both Locks committed: %+v", snapshot)
	}
}

func TestWithGrantBindingSerializesIssuanceBeforeLock(t *testing.T) {
	t.Parallel()
	m, s, _ := newManager(t)
	defer s.Close()
	if _, err := m.Unlock(context.Background(), UnlockRequest{
		ConnectionID: "c1", CommandRequestID: uuid.NewString(),
		ExpectedConnectionGeneration: "1", ExpectedProtectionRevision: "1",
	}); err != nil {
		t.Fatal(err)
	}
	beforeStore := make(chan struct{})
	releaseStore := make(chan struct{})
	grantDone := make(chan error, 1)
	var stores atomic.Int64
	go func() {
		_, err := m.WithGrantBinding(context.Background(), "c1", "1", func(binding DispatchBinding, _ Snapshot) error {
			if binding.ProtectionRevision != "2" {
				return errors.New("unexpected grant revision")
			}
			close(beforeStore)
			<-releaseStore
			stores.Add(1)
			return nil
		})
		grantDone <- err
	}()
	<-beforeStore
	lockDone := make(chan error, 1)
	go func() {
		_, err := m.Lock(context.Background(), LockRequest{
			ConnectionID: "c1", CommandRequestID: uuid.NewString(), ExpectedConnectionGeneration: "1",
		})
		lockDone <- err
	}()
	waitForLockPending(t, m)
	select {
	case err := <-lockDone:
		t.Fatalf("Lock returned before grant store completed: %v", err)
	default:
	}
	close(releaseStore)
	if err := <-grantDone; err != nil {
		t.Fatal(err)
	}
	if err := <-lockDone; err != nil {
		t.Fatal(err)
	}
	if stores.Load() != 1 {
		t.Fatalf("stores=%d", stores.Load())
	}
	if _, err := m.WithGrantBinding(context.Background(), "c1", "1", func(DispatchBinding, Snapshot) error {
		stores.Add(1)
		return nil
	}); !errors.Is(err, ErrLocked) {
		t.Fatalf("post-lock grant error=%v", err)
	}
	if stores.Load() != 1 {
		t.Fatalf("old revision store count increased after Lock: %d", stores.Load())
	}
}

func TestLockPendingUnlockRejectionIsDurablyReplayed(t *testing.T) {
	t.Parallel()
	m, s, _ := newManager(t)
	defer s.Close()
	unlocked, err := m.Unlock(context.Background(), UnlockRequest{
		ConnectionID: "c1", CommandRequestID: uuid.NewString(),
		ExpectedConnectionGeneration: "1", ExpectedProtectionRevision: "1",
	})
	if err != nil {
		t.Fatal(err)
	}
	binding := DispatchBinding{
		ConnectionID: "c1", ConnectionGeneration: "1", ProfileRevision: "3",
		ProtectionRevision: "2", LeaseID: *unlocked.Snapshot.LeaseID,
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	dispatchDone := make(chan error, 1)
	go func() {
		dispatchDone <- m.BeginMutationDispatch(context.Background(), binding,
			func(context.Context) error { return nil },
			func(context.Context) error { close(entered); <-release; return nil })
	}()
	<-entered
	lockDone := make(chan error, 1)
	go func() {
		_, err := m.Lock(context.Background(), LockRequest{
			ConnectionID: "c1", CommandRequestID: uuid.NewString(), ExpectedConnectionGeneration: "1",
		})
		lockDone <- err
	}()
	waitForLockPending(t, m)

	req := UnlockRequest{
		ConnectionID: "c1", CommandRequestID: uuid.NewString(),
		ExpectedConnectionGeneration: "1", ExpectedProtectionRevision: "2",
	}
	first, err := m.Unlock(context.Background(), req)
	if !errors.Is(err, ErrLockPending) || first.Replayed {
		t.Fatalf("first=%+v err=%v", first, err)
	}
	close(release)
	if err := <-dispatchDone; err != nil {
		t.Fatal(err)
	}
	if err := <-lockDone; err != nil {
		t.Fatal(err)
	}
	second, err := m.Unlock(context.Background(), req)
	if !errors.Is(err, ErrLockPending) || !second.Replayed {
		t.Fatalf("replay=%+v err=%v", second, err)
	}
	if second.Snapshot.Mode != ProtectedLocked || second.Snapshot.ProtectionRevision != "3" {
		t.Fatalf("replay snapshot=%+v", second.Snapshot)
	}
	var count int
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM protection_command_ledger WHERE command_request_id=?`, req.CommandRequestID).Scan(&count); err != nil || count != 1 {
		t.Fatalf("ledger=%d err=%v", count, err)
	}
}

func TestClosedGenerationLedgerMissIsPersistedAfterRecovery(t *testing.T) {
	t.Parallel()
	m, s, now := newManager(t)
	defer s.Close()
	if err := m.CloseGeneration(context.Background(), "c1", "1"); err != nil {
		t.Fatal(err)
	}
	restarted := NewManager(s, func() time.Time { return *now }, strings.NewReader(strings.Repeat("z", 4096)))
	req := UnlockRequest{
		ConnectionID: "c1", CommandRequestID: uuid.NewString(),
		ExpectedConnectionGeneration: "1", ExpectedProtectionRevision: "1",
	}
	first, err := restarted.Unlock(context.Background(), req)
	if !errors.Is(err, ErrConnectionClosed) || first.Replayed {
		t.Fatalf("first=%+v err=%v", first, err)
	}
	second, err := restarted.Unlock(context.Background(), req)
	if !errors.Is(err, ErrConnectionClosed) || !second.Replayed {
		t.Fatalf("replay=%+v err=%v", second, err)
	}
	if second.Snapshot.Mode != ProtectedLocked || second.Snapshot.ConnectionGeneration != "1" {
		t.Fatalf("snapshot=%+v", second.Snapshot)
	}
	var count int
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM protection_command_ledger WHERE command_request_id=?`, req.CommandRequestID).Scan(&count); err != nil || count != 1 {
		t.Fatalf("ledger=%d err=%v", count, err)
	}
	*now = now.Add(CommandRetryWindowHours * time.Hour)
	if deleted, err := m.PurgeClosed(context.Background()); err != nil || deleted != 1 {
		t.Fatalf("purge deleted=%d err=%v", deleted, err)
	}
	afterPurge := NewManager(s, func() time.Time { return *now }, strings.NewReader(strings.Repeat("q", 4096)))
	if result, err := afterPurge.Unlock(context.Background(), req); !errors.Is(err, ErrGenerationConflict) || result.Replayed {
		t.Fatalf("after purge=%+v err=%v", result, err)
	}
}

func waitForLockPending(t *testing.T, m *Manager) {
	waitForLockWaiters(t, m, 1)
}

func waitForLockWaiters(t *testing.T, m *Manager, want int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		state, err := m.state("c1", "1")
		if err != nil {
			t.Fatal(err)
		}
		state.mu.Lock()
		pending := state.lockWaiters >= want
		state.mu.Unlock()
		if pending {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("lock waiters did not reach %d", want)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestDelayedUnlockCannotReopenAfterLock(t *testing.T) {
	t.Parallel()
	m, s, _ := newManager(t)
	defer s.Close()
	if _, err := m.Unlock(context.Background(), UnlockRequest{
		ConnectionID: "c1", CommandRequestID: uuid.NewString(),
		ExpectedConnectionGeneration: "1", ExpectedProtectionRevision: "1",
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Lock(context.Background(), LockRequest{
		ConnectionID: "c1", CommandRequestID: uuid.NewString(), ExpectedConnectionGeneration: "1",
	}); err != nil {
		t.Fatal(err)
	}
	_, err := m.Unlock(context.Background(), UnlockRequest{
		ConnectionID: "c1", CommandRequestID: uuid.NewString(),
		ExpectedConnectionGeneration: "1", ExpectedProtectionRevision: "2",
	})
	if !errors.Is(err, ErrRevisionConflict) {
		t.Fatalf("delayed unlock error=%v", err)
	}
}

func TestRejectedUnlockIsDurablyReplayedAfterStateChanges(t *testing.T) {
	t.Parallel()
	m, s, _ := newManager(t)
	defer s.Close()
	request := UnlockRequest{
		ConnectionID: "c1", CommandRequestID: uuid.NewString(),
		ExpectedConnectionGeneration: "1", ExpectedProtectionRevision: "9",
	}
	first, err := m.Unlock(context.Background(), request)
	if !errors.Is(err, ErrRevisionConflict) || first.Replayed {
		t.Fatalf("first rejection=%+v err=%v", first, err)
	}
	if _, err := m.Lock(context.Background(), LockRequest{
		ConnectionID: "c1", CommandRequestID: uuid.NewString(), ExpectedConnectionGeneration: "1",
	}); err != nil {
		t.Fatal(err)
	}
	second, err := m.Unlock(context.Background(), request)
	if !errors.Is(err, ErrRevisionConflict) || !second.Replayed {
		t.Fatalf("replayed rejection=%+v err=%v", second, err)
	}
	var count int
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM protection_command_ledger WHERE command_request_id=?`, request.CommandRequestID).Scan(&count); err != nil || count != 1 {
		t.Fatalf("ledger count=%d err=%v", count, err)
	}
}

func TestRecoverLocksPersistedUnlockedGeneration(t *testing.T) {
	t.Parallel()
	m, s, now := newManager(t)
	defer s.Close()
	if _, err := m.Unlock(context.Background(), UnlockRequest{
		ConnectionID: "c1", CommandRequestID: uuid.NewString(),
		ExpectedConnectionGeneration: "1", ExpectedProtectionRevision: "1",
	}); err != nil {
		t.Fatal(err)
	}
	recovered := NewManager(s, func() time.Time { return *now }, strings.NewReader(strings.Repeat("y", 4096)))
	if err := recovered.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	snapshot, err := recovered.Get("c1", "1")
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Mode != ProtectedLocked || snapshot.ProtectionRevision != "3" || snapshot.LeaseID != nil {
		t.Fatalf("recovered snapshot=%+v", snapshot)
	}
}
