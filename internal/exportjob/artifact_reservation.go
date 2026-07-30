package exportjob

import (
	"context"
	"database/sql"
	"errors"
	"math"
	"os"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/influxdesk/influxdesk/internal/store"
	"github.com/influxdesk/influxdesk/internal/transfer"
)

// ReserveArtifactExtent binds one durable target-volume extent to a fragment.
// ExtentOrdinal is zero-based and contiguous. A restarted worker starts again
// at ordinal zero with a new AttemptID and Takeover=true, so already-durable
// extents are reused instead of double-reserved.
func (r *Repository) ReserveArtifactExtent(
	ctx context.Context,
	req ReserveArtifactExtentRequest,
) (ArtifactReservation, error) {
	ordinal, ordinalNumber, err := validateReserveArtifactExtent(req)
	if err != nil {
		return ArtifactReservation{}, err
	}
	req.ExtentOrdinal = ordinal
	var out ArtifactReservation
	err = r.store.WithImmediate(ctx, func(tx store.Executor) error {
		job, err := r.getInTx(ctx, tx, req.JobID, false)
		if err != nil {
			return err
		}
		if !job.Retained || job.TargetVolumeID != req.VolumeID {
			return ErrArtifactReservation
		}
		fragment, err := r.getFragmentInTx(ctx, tx, req.ArtifactID)
		if err != nil {
			return err
		}
		if fragment.JobID != req.JobID || fragment.State != FragmentWriting {
			return ErrArtifactReservation
		}

		ledger, found, err := r.getArtifactReservationInTx(ctx, tx, req.JobID, req.ArtifactID)
		if err != nil {
			return err
		}
		now := r.now().UTC().Format(time.RFC3339Nano)
		if !found {
			if _, err := tx.ExecContext(ctx, `INSERT INTO export_artifact_reservations(
				artifact_id,job_id,state,active_attempt_id,reserved_bytes,settled_bytes,created_at,updated_at
			) VALUES(?,?, 'ACTIVE',?,'0','0',?,?)`, req.ArtifactID, req.JobID,
				req.AttemptID, now, now); err != nil {
				return err
			}
			ledger = ArtifactReservation{
				JobID: req.JobID, ArtifactID: req.ArtifactID, State: ArtifactReservationActive,
				ActiveAttempt: req.AttemptID,
			}
		} else {
			switch ledger.State {
			case ArtifactReservationSettled:
				return ErrArtifactReservation
			case ArtifactReservationAborted:
				if len(ledger.Extents) != 0 || ordinalNumber != 0 {
					return ErrArtifactReservation
				}
				if _, err := tx.ExecContext(ctx, `UPDATE export_artifact_reservations SET
					state='ACTIVE',active_attempt_id=?,reserved_bytes='0',settled_bytes='0',updated_at=?
					WHERE artifact_id=? AND job_id=? AND state='ABORTED'`, req.AttemptID, now,
					req.ArtifactID, req.JobID); err != nil {
					return err
				}
				ledger.State, ledger.ActiveAttempt = ArtifactReservationActive, req.AttemptID
				ledger.ReservedBytes, ledger.SettledBytes = 0, 0
			case ArtifactReservationActive:
				if ledger.ActiveAttempt != req.AttemptID {
					if !req.Takeover || ordinalNumber != 0 {
						return ErrArtifactReservation
					}
					if _, err := tx.ExecContext(ctx, `UPDATE export_artifact_reservations
						SET active_attempt_id=?,updated_at=? WHERE artifact_id=? AND job_id=? AND state='ACTIVE'`,
						req.AttemptID, now, req.ArtifactID, req.JobID); err != nil {
						return err
					}
					ledger.ActiveAttempt = req.AttemptID
				}
			default:
				return ErrArtifactReservation
			}
		}
		if ledger.State == ArtifactReservationActive {
			var mapped int64
			for _, extent := range ledger.Extents {
				if extent.ReservedBytes > math.MaxInt64-mapped {
					return ErrArtifactReservation
				}
				mapped += extent.ReservedBytes
			}
			if mapped != ledger.ReservedBytes {
				return ErrArtifactReservation
			}
		}

		existing, exists := findArtifactExtent(ledger.Extents, ordinal)
		outstanding, err := targetVolumeOutstandingInTx(ctx, tx, req.VolumeID)
		if err != nil {
			return err
		}
		if exists {
			if existing.ReservedBytes != req.RequestedBytes {
				return ErrArtifactReservation
			}
			if !hasVolumeHeadroom(req.VolumeFreeBytes, req.VolumeCapacity, outstanding, 0) {
				return transfer.ErrTargetLowSpace
			}
			if _, err := tx.ExecContext(ctx, `UPDATE export_artifact_reservation_extents
				SET attempt_id=?,updated_at=? WHERE artifact_id=? AND job_id=? AND extent_ordinal=?`,
				req.AttemptID, now, req.ArtifactID, req.JobID, ordinal); err != nil {
				return err
			}
			out, _, err = r.getArtifactReservationInTx(ctx, tx, req.JobID, req.ArtifactID)
			return err
		}
		if uint64(len(ledger.Extents)) != ordinalNumber {
			return ErrArtifactReservation
		}

		reservationID, reused, err := r.claimInitialTargetReservationInTx(ctx, tx, job, req)
		if err != nil {
			return err
		}
		if reused {
			if !hasVolumeHeadroom(req.VolumeFreeBytes, req.VolumeCapacity, outstanding, 0) {
				return transfer.ErrTargetLowSpace
			}
		} else {
			if !hasVolumeHeadroom(req.VolumeFreeBytes, req.VolumeCapacity, outstanding, req.RequestedBytes) {
				return transfer.ErrTargetLowSpace
			}
			reservationID = uuid.NewString()
			if _, err := tx.ExecContext(ctx, `INSERT INTO transfer_reservations(
				reservation_id,job_id,scope_kind,scope_id,reserved_bytes,consumed_bytes,created_at,updated_at
			) VALUES(?,?,'VOLUME',?,?,'0',?,?)`, reservationID, req.JobID, req.VolumeID,
				strconv.FormatInt(req.RequestedBytes, 10), now, now); err != nil {
				return err
			}
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO export_artifact_reservation_extents(
			artifact_id,job_id,extent_ordinal,reservation_id,reserved_bytes,attempt_id,created_at,updated_at
		) VALUES(?,?,?,?,?,?,?,?)`, req.ArtifactID, req.JobID, ordinal, reservationID,
			strconv.FormatInt(req.RequestedBytes, 10), req.AttemptID, now, now); err != nil {
			return err
		}
		if ledger.ReservedBytes > math.MaxInt64-req.RequestedBytes {
			return ErrArtifactReservation
		}
		if _, err := tx.ExecContext(ctx, `UPDATE export_artifact_reservations SET
			reserved_bytes=?,updated_at=? WHERE artifact_id=? AND job_id=? AND state='ACTIVE'`,
			strconv.FormatInt(ledger.ReservedBytes+req.RequestedBytes, 10), now,
			req.ArtifactID, req.JobID); err != nil {
			return err
		}
		out, _, err = r.getArtifactReservationInTx(ctx, tx, req.JobID, req.ArtifactID)
		return err
	})
	return out, err
}

// SettleArtifactReservation atomically converts outstanding target space into
// bytes already represented by the filesystem's free-space observation. The
// fragment must already be COMPLETE and its persisted compressed size must
// exactly match actualCompressedBytes.
func (r *Repository) SettleArtifactReservation(
	ctx context.Context,
	jobID, artifactID string,
	actualCompressedBytes int64,
) (ArtifactReservation, error) {
	if uuid.Validate(jobID) != nil || uuid.Validate(artifactID) != nil || actualCompressedBytes < 0 {
		return ArtifactReservation{}, ErrInvalidRequest
	}
	var out ArtifactReservation
	err := r.store.WithImmediate(ctx, func(tx store.Executor) error {
		fragment, err := r.getFragmentInTx(ctx, tx, artifactID)
		if err != nil {
			return err
		}
		if fragment.JobID != jobID || fragment.State != FragmentComplete || fragment.CompressedSize == nil ||
			*fragment.CompressedSize != strconv.FormatInt(actualCompressedBytes, 10) {
			return ErrArtifactReservation
		}
		ledger, found, err := r.getArtifactReservationInTx(ctx, tx, jobID, artifactID)
		if err != nil {
			return err
		}
		if !found {
			return ErrArtifactReservation
		}
		if ledger.State == ArtifactReservationSettled {
			if ledger.SettledBytes != actualCompressedBytes {
				return ErrArtifactReservation
			}
			out = ledger
			return nil
		}
		if ledger.State != ArtifactReservationActive || len(ledger.Extents) == 0 ||
			actualCompressedBytes > ledger.ReservedBytes {
			return ErrArtifactReservation
		}
		remaining := actualCompressedBytes
		now := r.now().UTC().Format(time.RFC3339Nano)
		for _, extent := range ledger.Extents {
			consumed := extent.ReservedBytes
			if consumed > remaining {
				consumed = remaining
			}
			if consumed < 0 {
				return ErrArtifactReservation
			}
			result, err := tx.ExecContext(ctx, `UPDATE transfer_reservations SET
				reserved_bytes=?,consumed_bytes=?,updated_at=?
				WHERE reservation_id=? AND job_id=? AND scope_kind='VOLUME'`,
				strconv.FormatInt(consumed, 10), strconv.FormatInt(consumed, 10), now,
				extent.ReservationID, jobID)
			if err != nil {
				return err
			}
			if err := requireOne(result); err != nil {
				return err
			}
			remaining -= consumed
		}
		if remaining != 0 {
			return ErrArtifactReservation
		}
		result, err := tx.ExecContext(ctx, `UPDATE export_artifact_reservations SET
			state='SETTLED',settled_bytes=?,updated_at=?
			WHERE artifact_id=? AND job_id=? AND state='ACTIVE'`,
			strconv.FormatInt(actualCompressedBytes, 10), now, artifactID, jobID)
		if err != nil {
			return err
		}
		if err := requireOne(result); err != nil {
			return err
		}
		out, _, err = r.getArtifactReservationInTx(ctx, tx, jobID, artifactID)
		return err
	})
	return out, err
}

// AbortArtifactReservation releases ACTIVE extents only after the fragment is
// CORRUPT and its recorded .part path is confirmed absent from the filesystem.
func (r *Repository) AbortArtifactReservation(
	ctx context.Context,
	jobID, artifactID string,
) (ArtifactReservation, error) {
	if uuid.Validate(jobID) != nil || uuid.Validate(artifactID) != nil {
		return ArtifactReservation{}, ErrInvalidRequest
	}
	before, err := r.GetFragment(ctx, artifactID)
	if err != nil {
		return ArtifactReservation{}, err
	}
	if before.JobID != jobID || before.State != FragmentCorrupt || before.PartPath == nil {
		return ArtifactReservation{}, ErrArtifactReservation
	}
	if _, err := os.Lstat(*before.PartPath); err == nil {
		return ArtifactReservation{}, ErrArtifactPartPresent
	} else if !errors.Is(err, os.ErrNotExist) {
		return ArtifactReservation{}, err
	}

	var out ArtifactReservation
	err = r.store.WithImmediate(ctx, func(tx store.Executor) error {
		fragment, err := r.getFragmentInTx(ctx, tx, artifactID)
		if err != nil {
			return err
		}
		if fragment.JobID != jobID || fragment.State != FragmentCorrupt || fragment.PartPath == nil ||
			*fragment.PartPath != *before.PartPath {
			return ErrArtifactReservation
		}
		ledger, found, err := r.getArtifactReservationInTx(ctx, tx, jobID, artifactID)
		if err != nil {
			return err
		}
		now := r.now().UTC().Format(time.RFC3339Nano)
		if !found {
			if _, err := tx.ExecContext(ctx, `INSERT INTO export_artifact_reservations(
				artifact_id,job_id,state,active_attempt_id,reserved_bytes,settled_bytes,created_at,updated_at
			) VALUES(?,?,'ABORTED','','0','0',?,?)`, artifactID, jobID, now, now); err != nil {
				return err
			}
			out, _, err = r.getArtifactReservationInTx(ctx, tx, jobID, artifactID)
			return err
		}
		if ledger.State == ArtifactReservationAborted {
			out = ledger
			return nil
		}
		if ledger.State != ArtifactReservationActive {
			return ErrArtifactReservation
		}
		for _, extent := range ledger.Extents {
			if extent.ConsumedBytes != 0 {
				return ErrArtifactReservation
			}
			if _, err := tx.ExecContext(ctx, "DELETE FROM transfer_reservations WHERE reservation_id=? AND job_id=?",
				extent.ReservationID, jobID); err != nil {
				return err
			}
		}
		result, err := tx.ExecContext(ctx, `UPDATE export_artifact_reservations SET
			state='ABORTED',settled_bytes='0',updated_at=?
			WHERE artifact_id=? AND job_id=? AND state='ACTIVE'`, now, artifactID, jobID)
		if err != nil {
			return err
		}
		if err := requireOne(result); err != nil {
			return err
		}
		out, _, err = r.getArtifactReservationInTx(ctx, tx, jobID, artifactID)
		return err
	})
	return out, err
}

func (r *Repository) GetArtifactReservation(
	ctx context.Context,
	jobID, artifactID string,
) (ArtifactReservation, error) {
	if uuid.Validate(jobID) != nil || uuid.Validate(artifactID) != nil {
		return ArtifactReservation{}, ErrInvalidRequest
	}
	reservation, found, err := r.getArtifactReservationInTx(ctx, r.store.DB(), jobID, artifactID)
	if err != nil {
		return ArtifactReservation{}, err
	}
	if !found {
		return ArtifactReservation{}, ErrArtifactReservation
	}
	return reservation, nil
}

func validateReserveArtifactExtent(req ReserveArtifactExtentRequest) (string, uint64, error) {
	if uuid.Validate(req.JobID) != nil || uuid.Validate(req.ArtifactID) != nil ||
		uuid.Validate(req.AttemptID) != nil || req.VolumeID == "" ||
		req.RequestedBytes != transfer.ReservationExtentBytes || req.VolumeFreeBytes < 0 ||
		req.VolumeCapacity <= 0 {
		return "", 0, ErrInvalidRequest
	}
	ordinal, err := canonicalUnsigned(req.ExtentOrdinal)
	if err != nil {
		return "", 0, err
	}
	ordinalNumber, err := strconv.ParseUint(ordinal, 10, 32)
	if err != nil || (req.Takeover && ordinalNumber != 0) {
		return "", 0, ErrInvalidRequest
	}
	return ordinal, ordinalNumber, nil
}

func (r *Repository) claimInitialTargetReservationInTx(
	ctx context.Context,
	tx store.Executor,
	job Job,
	req ReserveArtifactExtentRequest,
) (string, bool, error) {
	if job.TargetReservationID == nil {
		return "", false, nil
	}
	var reservationID, scopeID, reservedText, consumedText string
	err := tx.QueryRowContext(ctx, `SELECT r.reservation_id,r.scope_id,r.reserved_bytes,r.consumed_bytes
		FROM transfer_reservations r
		LEFT JOIN export_artifact_reservation_extents e ON e.reservation_id=r.reservation_id
		WHERE r.reservation_id=? AND r.job_id=? AND r.scope_kind='VOLUME' AND e.reservation_id IS NULL`,
		*job.TargetReservationID, req.JobID).Scan(&reservationID, &scopeID, &reservedText, &consumedText)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	reserved, err := parseInt64Decimal(reservedText)
	if err != nil {
		return "", false, err
	}
	consumed, err := parseInt64Decimal(consumedText)
	if err != nil {
		return "", false, err
	}
	if scopeID != req.VolumeID || reserved != req.RequestedBytes || consumed != 0 {
		return "", false, ErrArtifactReservation
	}
	return reservationID, true, nil
}

func (r *Repository) getArtifactReservationInTx(
	ctx context.Context,
	tx store.Executor,
	jobID, artifactID string,
) (ArtifactReservation, bool, error) {
	var result ArtifactReservation
	var reservedText, settledText string
	err := tx.QueryRowContext(ctx, `SELECT job_id,artifact_id,state,active_attempt_id,reserved_bytes,settled_bytes
		FROM export_artifact_reservations WHERE job_id=? AND artifact_id=?`, jobID, artifactID).Scan(
		&result.JobID, &result.ArtifactID, &result.State, &result.ActiveAttempt, &reservedText, &settledText)
	if errors.Is(err, sql.ErrNoRows) {
		return ArtifactReservation{}, false, nil
	}
	if err != nil {
		return ArtifactReservation{}, false, err
	}
	result.ReservedBytes, err = parseInt64Decimal(reservedText)
	if err != nil {
		return ArtifactReservation{}, false, err
	}
	result.SettledBytes, err = parseInt64Decimal(settledText)
	if err != nil {
		return ArtifactReservation{}, false, err
	}
	rows, err := tx.QueryContext(ctx, `SELECT e.extent_ordinal,e.reservation_id,e.reserved_bytes,
		e.attempt_id,r.reserved_bytes,r.consumed_bytes
		FROM export_artifact_reservation_extents e
		JOIN transfer_reservations r ON r.reservation_id=e.reservation_id
		WHERE e.job_id=? AND e.artifact_id=?
		ORDER BY length(e.extent_ordinal),e.extent_ordinal`, jobID, artifactID)
	if err != nil {
		return ArtifactReservation{}, false, err
	}
	defer rows.Close()
	for rows.Next() {
		var extent ArtifactReservationExtent
		var extentReserved, currentReserved, consumed string
		if err := rows.Scan(&extent.Ordinal, &extent.ReservationID, &extentReserved,
			&extent.AttemptID, &currentReserved, &consumed); err != nil {
			return ArtifactReservation{}, false, err
		}
		extent.ReservedBytes, err = parseInt64Decimal(extentReserved)
		if err != nil {
			return ArtifactReservation{}, false, err
		}
		extent.ConsumedBytes, err = parseInt64Decimal(consumed)
		if err != nil {
			return ArtifactReservation{}, false, err
		}
		current, err := parseInt64Decimal(currentReserved)
		if err != nil || extent.ConsumedBytes > current ||
			(result.State == ArtifactReservationActive &&
				(current != extent.ReservedBytes || extent.ConsumedBytes != 0)) {
			return ArtifactReservation{}, false, ErrArtifactReservation
		}
		result.Extents = append(result.Extents, extent)
	}
	if err := rows.Err(); err != nil {
		return ArtifactReservation{}, false, err
	}
	return result, true, nil
}

func targetVolumeOutstandingInTx(ctx context.Context, tx store.Executor, volumeID string) (int64, error) {
	rows, err := tx.QueryContext(ctx, `SELECT reserved_bytes,consumed_bytes FROM transfer_reservations
		WHERE scope_kind='VOLUME' AND scope_id=?`, volumeID)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	var total int64
	for rows.Next() {
		var reservedText, consumedText string
		if err := rows.Scan(&reservedText, &consumedText); err != nil {
			return 0, err
		}
		reserved, err := parseInt64Decimal(reservedText)
		if err != nil {
			return 0, err
		}
		consumed, err := parseInt64Decimal(consumedText)
		if err != nil || consumed > reserved {
			return 0, ErrArtifactReservation
		}
		outstanding := reserved - consumed
		if outstanding > math.MaxInt64-total {
			return 0, ErrArtifactReservation
		}
		total += outstanding
	}
	return total, rows.Err()
}

func hasVolumeHeadroom(freeBytes, capacity, outstanding, additional int64) bool {
	if freeBytes < 0 || capacity <= 0 || outstanding < 0 || additional < 0 || outstanding > freeBytes {
		return false
	}
	remaining := freeBytes - outstanding
	if additional > remaining {
		return false
	}
	return remaining-additional >= transfer.VolumeReserveFloor(capacity)
}

func findArtifactExtent(extents []ArtifactReservationExtent, ordinal string) (ArtifactReservationExtent, bool) {
	for _, extent := range extents {
		if extent.Ordinal == ordinal {
			return extent, true
		}
	}
	return ArtifactReservationExtent{}, false
}
