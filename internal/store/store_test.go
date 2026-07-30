package store

import (
	"context"
	"path/filepath"
	"testing"
)

func TestOpenEnforcesDurabilityAndMigrates(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	var journal string
	if err := s.DB().QueryRowContext(ctx, "PRAGMA journal_mode").Scan(&journal); err != nil {
		t.Fatal(err)
	}
	if journal != "wal" {
		t.Fatalf("journal_mode=%q, want wal", journal)
	}
	var synchronous int
	if err := s.DB().QueryRowContext(ctx, "PRAGMA synchronous").Scan(&synchronous); err != nil {
		t.Fatal(err)
	}
	if synchronous != 2 {
		t.Fatalf("synchronous=%d, want 2 (FULL)", synchronous)
	}
	var version int
	if err := s.DB().QueryRowContext(ctx, "SELECT MAX(version) FROM schema_migrations").Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version != 13 {
		t.Fatalf("migration version=%d, want 13", version)
	}
	if _, err := s.DB().ExecContext(ctx, `INSERT INTO profiles(
		id,revision,name,base_url,environment,auth_mode,protection_mode,created_at,updated_at
	) VALUES('00000000-0000-0000-0000-000000000001','1','legacy','http://127.0.0.1:8086',
		'development','NONE','ProtectedLocked','now','now')`); err != nil {
		t.Fatal(err)
	}
	var defaultDatabase string
	if err := s.DB().QueryRowContext(ctx, `SELECT default_database FROM profiles
		WHERE id='00000000-0000-0000-0000-000000000001'`).Scan(&defaultDatabase); err != nil {
		t.Fatal(err)
	}
	if defaultDatabase != "" {
		t.Fatalf("legacy profile default_database=%q, want empty", defaultDatabase)
	}
}

func TestWithImmediateRollsBack(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	wantErr := context.Canceled
	err = s.WithImmediate(ctx, func(tx Executor) error {
		if _, err := tx.ExecContext(ctx, `INSERT INTO tasks(
			id,kind,state,terminal,snapshot_revision,state_revision,last_event_seq,created_at,updated_at
		) VALUES ('q1','QUERY','QUEUED',0,'1','1','0','now','now')`); err != nil {
			return err
		}
		return wantErr
	})
	if err != wantErr {
		t.Fatalf("error=%v, want %v", err, wantErr)
	}
	var count int
	if err := s.DB().QueryRowContext(ctx, "SELECT COUNT(*) FROM tasks WHERE id='q1'").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("rolled back rows=%d, want 0", count)
	}
}
