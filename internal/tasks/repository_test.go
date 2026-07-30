package tasks

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/influxdesk/influxdesk/internal/store"
)

func newTestRepository(t *testing.T) (*Repository, *store.Store) {
	t.Helper()
	s, err := store.Open(context.Background(), filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 7, 23, 8, 0, 0, 0, time.UTC)
	return NewRepository(s, func() time.Time { return now }), s
}

func TestCreateIsIdempotentAndDigestBound(t *testing.T) {
	t.Parallel()
	r, s := newTestRepository(t)
	defer s.Close()
	ctx := context.Background()
	req := CreateRequest{
		ID: "query-1", Kind: "QUERY", InitialState: "QUEUED",
		ClientScope: "StartReadQuery/profile-1/request-1", RequestDigest: "digest-a",
		LedgerExpiresAt: time.Now().Add(90 * 24 * time.Hour), ResultAvailable: true,
	}
	first, replayed, err := r.Create(ctx, req)
	if err != nil || replayed {
		t.Fatalf("first create replayed=%v err=%v", replayed, err)
	}
	second, replayed, err := r.Create(ctx, req)
	if err != nil || !replayed {
		t.Fatalf("replay replayed=%v err=%v", replayed, err)
	}
	if first.ID != second.ID || first.LastEventSeq != second.LastEventSeq {
		t.Fatalf("replay changed resource: first=%+v second=%+v", first, second)
	}
	req.RequestDigest = "digest-b"
	if _, _, err := r.Create(ctx, req); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("conflicting digest error=%v", err)
	}
}

func TestDualRevisionAndEventSequence(t *testing.T) {
	t.Parallel()
	r, s := newTestRepository(t)
	defer s.Close()
	ctx := context.Background()
	created, _, err := r.Create(ctx, CreateRequest{
		ID: "query-1", Kind: "QUERY", InitialState: "RUNNING",
		ClientScope: "query/scope", RequestDigest: "digest",
		LedgerExpiresAt: time.Now().Add(time.Hour), ResultAvailable: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	progress, err := r.Apply(ctx, created.ID, "1", Change{
		State: "RUNNING", ChangeType: "PROGRESS", StateChanged: false,
	})
	if err != nil {
		t.Fatal(err)
	}
	if progress.SnapshotRevision != "2" || progress.StateRevision != "1" {
		t.Fatalf("progress revisions=%s/%s", progress.SnapshotRevision, progress.StateRevision)
	}
	finished, err := r.Apply(ctx, created.ID, "1", Change{
		State: "SUCCEEDED", Terminal: true, ChangeType: "STATE", StateChanged: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if finished.SnapshotRevision != "3" || finished.StateRevision != "2" {
		t.Fatalf("terminal revisions=%s/%s", finished.SnapshotRevision, finished.StateRevision)
	}
	events, err := r.Changes(ctx, "0", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 3 || events[0].Seq != "1" || events[2].Seq != "3" {
		t.Fatalf("events=%+v", events)
	}
	if _, err := r.Apply(ctx, created.ID, "1", Change{State: "FAILED", StateChanged: true, ChangeType: "STATE"}); !errors.Is(err, ErrRevisionConflict) {
		t.Fatalf("stale revision error=%v", err)
	}
}

func TestEventSequenceContinuesBeyondInt64AsCanonicalDecimal(t *testing.T) {
	t.Parallel()
	repository, database := newTestRepository(t)
	defer database.Close()
	ctx := context.Background()
	const head = "18446744073709551615"
	if _, err := database.DB().ExecContext(ctx, `UPDATE task_event_head SET seq=? WHERE singleton=1`, head); err != nil {
		t.Fatal(err)
	}
	created, _, err := repository.Create(ctx, CreateRequest{
		ID: "large-event", Kind: "QUERY", InitialState: "QUEUED",
		ClientScope: "large-event-scope", RequestDigest: "large-event-digest",
		LedgerExpiresAt: time.Now().Add(time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}
	if created.LastEventSeq != "18446744073709551616" {
		t.Fatalf("created event seq=%s", created.LastEventSeq)
	}
	updated, err := repository.Apply(ctx, created.ID, created.StateRevision, Change{
		State: "RUNNING", StateChanged: true, ChangeType: "STATE",
	})
	if err != nil {
		t.Fatal(err)
	}
	if updated.LastEventSeq != "18446744073709551617" {
		t.Fatalf("updated event seq=%s", updated.LastEventSeq)
	}
	events, err := repository.Changes(ctx, head, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 || events[0].Seq != created.LastEventSeq || events[1].Seq != updated.LastEventSeq {
		t.Fatalf("large event changes=%+v", events)
	}
	currentHead, err := repository.EventHead(ctx)
	if err != nil || currentHead != updated.LastEventSeq {
		t.Fatalf("head=%s err=%v", currentHead, err)
	}
	var storageType string
	if err := database.DB().QueryRowContext(ctx, `SELECT typeof(seq) FROM task_events WHERE seq=?`,
		created.LastEventSeq).Scan(&storageType); err != nil {
		t.Fatal(err)
	}
	if storageType != "text" {
		t.Fatalf("event seq sqlite type=%s", storageType)
	}
}

func TestCommandLedgerDetectsConflict(t *testing.T) {
	t.Parallel()
	r, s := newTestRepository(t)
	_ = r
	defer s.Close()
	ctx := context.Background()
	err := s.WithImmediate(ctx, func(tx store.Executor) error {
		return InsertCommandInTx(ctx, tx, CommandEntry{
			Scope: "IMPORT/job/ABORT/cmd", RequestDigest: "a", ResourceKind: "IMPORT",
			ResourceID: "job", ResultState: "ABORTED", ResultStateRevision: "2",
			CommittedAt: time.Now(), ExpiresAt: time.Now().Add(time.Hour),
		})
	})
	if err != nil {
		t.Fatal(err)
	}
	err = s.WithImmediate(ctx, func(tx store.Executor) error {
		_, _, err := LookupCommandInTx(ctx, tx, "IMPORT/job/ABORT/cmd", "b")
		return err
	})
	if !errors.Is(err, ErrCommandIdempotencyConflict) {
		t.Fatalf("conflict error=%v", err)
	}
}

func TestRecoverCoreAppliesRestartStates(t *testing.T) {
	t.Parallel()
	r, s := newTestRepository(t)
	defer s.Close()
	ctx := context.Background()
	for _, req := range []CreateRequest{
		{ID: "query", Kind: "QUERY", InitialState: "RUNNING", ClientScope: "q", RequestDigest: "q", LedgerExpiresAt: time.Now().Add(time.Hour), ResultAvailable: true},
		{ID: "queued-op", Kind: "OPERATION", InitialState: "QUEUED", ClientScope: "o1", RequestDigest: "o1", LedgerExpiresAt: time.Now().Add(time.Hour)},
		{ID: "sent-op", Kind: "OPERATION", InitialState: "DISPATCHING", ClientScope: "o2", RequestDigest: "o2", LedgerExpiresAt: time.Now().Add(time.Hour)},
		{ID: "export", Kind: "EXPORT", InitialState: "RUNNING", ClientScope: "e", RequestDigest: "e", LedgerExpiresAt: time.Now().Add(time.Hour)},
	} {
		if _, _, err := r.Create(ctx, req); err != nil {
			t.Fatal(err)
		}
	}
	queryMeta, err := r.Get(ctx, "query")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.DB().ExecContext(ctx, `INSERT INTO query_sessions(
		session_id,profile_id,profile_revision,connection_id,connection_generation,
		state,terminal,snapshot_revision,state_revision,last_event_seq,
		statement_count,series_count,row_count,result_available,created_at,updated_at
	) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, "query", "profile", "1", "connection", "1",
		queryMeta.State, 0, queryMeta.SnapshotRevision, queryMeta.StateRevision, queryMeta.LastEventSeq,
		"1", "0", "0", 1, queryMeta.CreatedAt, queryMeta.UpdatedAt); err != nil {
		t.Fatal(err)
	}
	changed, err := r.RecoverCore(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if changed != 4 {
		t.Fatalf("changed=%d", changed)
	}
	wants := map[string]string{
		"query": "INTERRUPTED", "queued-op": "INTERRUPTED_NOT_SENT",
		"sent-op": "OUTCOME_UNKNOWN", "export": "PAUSED_RESTARTABLE",
	}
	for id, want := range wants {
		meta, err := r.Get(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		if meta.State != want {
			t.Fatalf("%s state=%s want=%s", id, meta.State, want)
		}
	}
	query, _ := r.Get(ctx, "query")
	if query.ResultAvailable || !query.Terminal {
		t.Fatalf("recovered query=%+v", query)
	}
}
