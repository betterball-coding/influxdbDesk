package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"path/filepath"
	"strings"

	_ "modernc.org/sqlite"
)

const busyTimeoutMillis = 5000

// Executor is the subset shared by a database connection and an immediate
// transaction. Domain repositories depend on this interface so a complete
// state transition can be committed through one durable boundary.
type Executor interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

type Store struct {
	db *sql.DB
}

func Open(ctx context.Context, path string) (*Store, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("resolve sqlite path: %w", err)
	}

	uriPath := filepath.ToSlash(abs)
	if filepath.VolumeName(abs) != "" && !strings.HasPrefix(uriPath, "/") {
		// A Windows drive path must be encoded as file:///C:/..., otherwise
		// net/url emits file://C:/... and SQLite treats C: as a URI authority.
		uriPath = "/" + uriPath
	}
	u := &url.URL{Scheme: "file", Path: uriPath}
	q := u.Query()
	q.Add("_pragma", fmt.Sprintf("busy_timeout(%d)", busyTimeoutMillis))
	q.Add("_pragma", "foreign_keys(ON)")
	q.Add("_pragma", "journal_mode(WAL)")
	q.Add("_pragma", "synchronous(FULL)")
	u.RawQuery = q.Encode()

	return OpenDSN(ctx, u.String())
}

// OpenDSN exists for tests and controlled embeddings. Callers must not pass
// user-provided DSNs.
func OpenDSN(ctx context.Context, dsn string) (*Store, error) {
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	db.SetMaxOpenConns(4)
	db.SetMaxIdleConns(4)

	s := &Store{db: db}
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping sqlite: %w", err)
	}
	if err := s.verifyPragmas(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	if err := s.migrate(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) DB() *sql.DB { return s.db }

func (s *Store) verifyPragmas(ctx context.Context) error {
	var journal string
	if err := s.db.QueryRowContext(ctx, "PRAGMA journal_mode").Scan(&journal); err != nil {
		return fmt.Errorf("read sqlite journal mode: %w", err)
	}
	if !strings.EqualFold(journal, "wal") && !strings.EqualFold(journal, "memory") {
		return fmt.Errorf("sqlite journal mode is %q, want WAL", journal)
	}

	var synchronous int
	if err := s.db.QueryRowContext(ctx, "PRAGMA synchronous").Scan(&synchronous); err != nil {
		return fmt.Errorf("read sqlite synchronous mode: %w", err)
	}
	if synchronous != 2 { // SQLITE_SYNC_FULL
		return fmt.Errorf("sqlite synchronous mode is %d, want FULL", synchronous)
	}

	var foreignKeys int
	if err := s.db.QueryRowContext(ctx, "PRAGMA foreign_keys").Scan(&foreignKeys); err != nil {
		return fmt.Errorf("read sqlite foreign_keys: %w", err)
	}
	if foreignKeys != 1 {
		return errors.New("sqlite foreign keys are disabled")
	}
	return nil
}

// WithImmediate runs fn inside BEGIN IMMEDIATE. database/sql's TxOptions do
// not expose SQLite's immediate mode, so the transaction is pinned to one
// connection explicitly.
func (s *Store) WithImmediate(ctx context.Context, fn func(Executor) error) (err error) {
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("acquire sqlite connection: %w", err)
	}
	defer conn.Close()

	if _, err = conn.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		return fmt.Errorf("begin immediate: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_, _ = conn.ExecContext(context.Background(), "ROLLBACK")
		}
	}()

	if err = fn(conn); err != nil {
		return err
	}
	if _, err = conn.ExecContext(ctx, "COMMIT"); err != nil {
		return fmt.Errorf("commit immediate: %w", err)
	}
	committed = true
	return nil
}
