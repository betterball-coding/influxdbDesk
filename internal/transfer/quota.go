package transfer

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/influxdesk/influxdesk/internal/store"
)

type QuotaManager struct {
	store *store.Store
	now   func() time.Time
}

func NewQuotaManager(s *store.Store, now func() time.Time) *QuotaManager {
	if now == nil {
		now = time.Now
	}
	return &QuotaManager{store: s, now: now}
}

func VolumeReserveFloor(capacity int64) int64 {
	fivePercent := capacity / 20
	const twoGiB = int64(2 * 1024 * 1024 * 1024)
	const tenGiB = int64(10 * 1024 * 1024 * 1024)
	if fivePercent < twoGiB {
		return twoGiB
	}
	if fivePercent > tenGiB {
		return tenGiB
	}
	return fivePercent
}

func (q *QuotaManager) Admit(ctx context.Context, jobID, profileID, kind, state string) error {
	return q.store.WithImmediate(ctx, func(tx store.Executor) error {
		return q.AdmitInTx(ctx, tx, jobID, profileID, kind, state)
	})
}

func (q *QuotaManager) AdmitInTx(ctx context.Context, tx store.Executor, jobID, profileID, kind, state string) error {
	rows, err := tx.QueryContext(ctx, "SELECT profile_id FROM transfer_jobs WHERE retained=1")
	if err != nil {
		return err
	}
	defer rows.Close()
	global, profile := 0, 0
	for rows.Next() {
		var existingProfile string
		if err := rows.Scan(&existingProfile); err != nil {
			return err
		}
		global++
		if existingProfile == profileID {
			profile++
		}
	}
	if global >= RetainedTransferJobsGlobal {
		return ErrTransferGlobalLimit
	}
	if profile >= RetainedTransferJobsPerProfile {
		return ErrTransferProfileLimit
	}
	now := q.now().UTC().Format(time.RFC3339Nano)
	_, err = tx.ExecContext(ctx, `INSERT INTO transfer_jobs(
		job_id,profile_id,kind,state,retained,created_at,updated_at
	) VALUES(?,?,?,?,1,?,?)`, jobID, profileID, kind, state, now, now)
	return err
}

type Reservation struct {
	ReservationID string
	JobID         string
	ScopeKind     string
	ScopeID       string
	ReservedBytes int64
	ConsumedBytes int64
}

// ReserveExtent persists a reservation before a writer is allowed to grow.
// freeBytes/capacity are mandatory for volume reservations and for the private
// app-data volume. Callers must refresh them immediately before each extent.
func (q *QuotaManager) ReserveExtent(ctx context.Context, jobID, scopeKind, scopeID string, requested, freeBytes, capacity int64) (Reservation, error) {
	if requested <= 0 || requested > ReservationExtentBytes {
		return Reservation{}, errors.New("reservation must be within one extent")
	}
	if freeBytes < 0 || capacity <= 0 {
		return Reservation{}, errors.New("invalid volume capacity")
	}
	if scopeKind != "PRIVATE" && scopeKind != "VOLUME" {
		return Reservation{}, errors.New("invalid reservation scope")
	}
	var out Reservation
	err := q.store.WithImmediate(ctx, func(tx store.Executor) error {
		var retained int
		if err := tx.QueryRowContext(ctx, "SELECT retained FROM transfer_jobs WHERE job_id=?", jobID).Scan(&retained); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return errors.New("transfer job not found")
			}
			return err
		}
		if retained == 0 {
			return errors.New("transfer job is not retained")
		}
		rows, err := tx.QueryContext(ctx, `SELECT scope_kind,scope_id,reserved_bytes,consumed_bytes
			FROM transfer_reservations`)
		if err != nil {
			return err
		}
		var privateReserved, scopeOutstanding int64
		for rows.Next() {
			var existingKind, existingScope, reservedText, consumedText string
			if err := rows.Scan(&existingKind, &existingScope, &reservedText, &consumedText); err != nil {
				rows.Close()
				return err
			}
			reserved, err := strconv.ParseInt(reservedText, 10, 64)
			if err != nil {
				rows.Close()
				return err
			}
			consumed, err := strconv.ParseInt(consumedText, 10, 64)
			if err != nil {
				rows.Close()
				return err
			}
			if existingKind == "PRIVATE" {
				privateReserved += reserved
			}
			if existingKind == scopeKind && existingScope == scopeID {
				scopeOutstanding += reserved - consumed
			}
		}
		rows.Close()
		if scopeKind == "PRIVATE" && privateReserved+requested > PrivateTransferQuotaBytes {
			return ErrPrivateQuota
		}
		if freeBytes-scopeOutstanding-requested < VolumeReserveFloor(capacity) {
			if scopeKind == "PRIVATE" {
				return ErrPrivateQuota
			}
			return ErrTargetLowSpace
		}
		now := q.now().UTC().Format(time.RFC3339Nano)
		out = Reservation{
			ReservationID: uuid.NewString(), JobID: jobID, ScopeKind: scopeKind,
			ScopeID: scopeID, ReservedBytes: requested,
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO transfer_reservations(
			reservation_id,job_id,scope_kind,scope_id,reserved_bytes,consumed_bytes,created_at,updated_at
		) VALUES(?,?,?,?,?,'0',?,?)`, out.ReservationID, jobID, scopeKind, scopeID,
			strconv.FormatInt(requested, 10), now, now)
		return err
	})
	return out, err
}

func (q *QuotaManager) Consume(ctx context.Context, reservationID string, consumed int64) error {
	return q.store.WithImmediate(ctx, func(tx store.Executor) error {
		var reservedText, consumedText string
		if err := tx.QueryRowContext(ctx, `SELECT reserved_bytes,consumed_bytes
			FROM transfer_reservations WHERE reservation_id=?`, reservationID).Scan(&reservedText, &consumedText); err != nil {
			return err
		}
		reserved, err := strconv.ParseInt(reservedText, 10, 64)
		if err != nil {
			return err
		}
		current, err := strconv.ParseInt(consumedText, 10, 64)
		if err != nil {
			return err
		}
		if consumed < current || consumed > reserved {
			return errors.New("invalid reservation consumption")
		}
		_, err = tx.ExecContext(ctx, `UPDATE transfer_reservations SET consumed_bytes=?,updated_at=?
			WHERE reservation_id=?`, strconv.FormatInt(consumed, 10),
			q.now().UTC().Format(time.RFC3339Nano), reservationID)
		return err
	})
}

func (q *QuotaManager) Release(ctx context.Context, reservationID string) error {
	return q.store.WithImmediate(ctx, func(tx store.Executor) error {
		_, err := tx.ExecContext(ctx, "DELETE FROM transfer_reservations WHERE reservation_id=?", reservationID)
		return err
	})
}

func (q *QuotaManager) ReleaseRetained(ctx context.Context, jobID string) error {
	return q.store.WithImmediate(ctx, func(tx store.Executor) error {
		var reservations int
		if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM transfer_reservations WHERE job_id=?", jobID).Scan(&reservations); err != nil {
			return err
		}
		if reservations != 0 {
			return fmt.Errorf("job still has %d reservations", reservations)
		}
		_, err := tx.ExecContext(ctx, `UPDATE transfer_jobs SET retained=0,updated_at=? WHERE job_id=?`,
			q.now().UTC().Format(time.RFC3339Nano), jobID)
		return err
	})
}
