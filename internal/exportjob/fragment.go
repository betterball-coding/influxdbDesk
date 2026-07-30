package exportjob

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/influxdesk/influxdesk/internal/store"
	"github.com/influxdesk/influxdesk/internal/tasks"
)

func (r *Repository) BeginFragment(ctx context.Context, req BeginFragmentRequest) (Fragment, error) {
	if err := validateBeginFragment(req); err != nil {
		return Fragment{}, err
	}
	var out Fragment
	err := r.store.WithImmediate(ctx, func(tx store.Executor) error {
		job, err := r.getInTx(ctx, tx, req.JobID, false)
		if err != nil {
			return err
		}
		if job.Task.State != StateRunning || job.CancelRequested {
			return ErrStateConflict
		}
		now := r.now().UTC().Format(time.RFC3339Nano)
		if _, err := tx.ExecContext(ctx, `INSERT INTO export_fragments(
			fragment_id,job_id,ordinal,kind,state,start_ns,end_ns,part_path,created_at,updated_at
		) VALUES(?,?,?,?,?,?,?,?,?,?)`, req.FragmentID, req.JobID, req.Ordinal, req.Kind,
			FragmentWriting, nullableString(req.StartNS), nullableString(req.EndNS), req.PartPath,
			now, now); err != nil {
			return err
		}
		if _, err := r.touchSnapshotInTx(ctx, tx, job, "FRAGMENT"); err != nil {
			return err
		}
		out, err = r.getFragmentInTx(ctx, tx, req.FragmentID)
		return err
	})
	return out, err
}

func (r *Repository) MarkFragmentFinalizing(ctx context.Context, req FinalizeFragmentRequest) (Fragment, error) {
	if err := validateFinalizeFragment(req); err != nil {
		return Fragment{}, err
	}
	var out Fragment
	err := r.store.WithImmediate(ctx, func(tx store.Executor) error {
		fragment, err := r.getFragmentInTx(ctx, tx, req.FragmentID)
		if err != nil {
			return err
		}
		if fragment.State != FragmentWriting || fragment.PartPath == nil || *fragment.PartPath != req.PartPath {
			return ErrFragmentState
		}
		job, err := r.getInTx(ctx, tx, fragment.JobID, false)
		if err != nil {
			return err
		}
		if job.Task.State != StateRunning || job.CancelRequested {
			return ErrStateConflict
		}
		now := r.now().UTC().Format(time.RFC3339Nano)
		result, err := tx.ExecContext(ctx, `UPDATE export_fragments SET state='FINALIZING',
			final_path=?,compressed_size=?,uncompressed_size=?,checksum_sha256=?,
			validation_epoch=NULL,updated_at=? WHERE fragment_id=? AND state='WRITING'`,
			req.FinalPath, req.CompressedSize, req.UncompressedSize, req.ChecksumSHA256,
			now, req.FragmentID)
		if err != nil {
			return err
		}
		if err := requireFragmentRow(result); err != nil {
			return err
		}
		if _, err := r.touchSnapshotInTx(ctx, tx, job, "FRAGMENT"); err != nil {
			return err
		}
		out, err = r.getFragmentInTx(ctx, tx, req.FragmentID)
		return err
	})
	return out, err
}

// CompleteFragment is the durable post-rename hook. The caller may invoke it
// only after the same-volume write-through rename has succeeded.
func (r *Repository) CompleteFragment(ctx context.Context, fragmentID string) (Fragment, error) {
	var out Fragment
	err := r.store.WithImmediate(ctx, func(tx store.Executor) error {
		fragment, err := r.getFragmentInTx(ctx, tx, fragmentID)
		if err != nil {
			return err
		}
		if fragment.State == FragmentComplete {
			out = fragment
			return nil
		}
		if fragment.State != FragmentFinalizing || fragment.FinalPath == nil ||
			fragment.CompressedSize == nil || fragment.UncompressedSize == nil || fragment.ChecksumSHA256 == nil {
			return ErrFragmentState
		}
		job, err := r.getInTx(ctx, tx, fragment.JobID, false)
		if err != nil {
			return err
		}
		now := r.now().UTC().Format(time.RFC3339Nano)
		result, err := tx.ExecContext(ctx, `UPDATE export_fragments SET state='COMPLETE',
			validation_epoch=NULL,updated_at=? WHERE fragment_id=? AND state='FINALIZING'`,
			now, fragmentID)
		if err != nil {
			return err
		}
		if err := requireFragmentRow(result); err != nil {
			return err
		}
		if _, err := r.touchSnapshotInTx(ctx, tx, job, "FRAGMENT"); err != nil {
			return err
		}
		out, err = r.getFragmentInTx(ctx, tx, fragmentID)
		return err
	})
	return out, err
}

func (r *Repository) MarkFragmentCorrupt(ctx context.Context, fragmentID string) (Fragment, error) {
	var out Fragment
	err := r.store.WithImmediate(ctx, func(tx store.Executor) error {
		fragment, err := r.getFragmentInTx(ctx, tx, fragmentID)
		if err != nil {
			return err
		}
		if fragment.State == FragmentCorrupt {
			out = fragment
			return nil
		}
		job, err := r.getInTx(ctx, tx, fragment.JobID, false)
		if err != nil {
			return err
		}
		if err := r.markFragmentCorruptInTx(ctx, tx, fragmentID); err != nil {
			return err
		}
		if _, err := r.touchSnapshotInTx(ctx, tx, job, "FRAGMENT"); err != nil {
			return err
		}
		out, err = r.getFragmentInTx(ctx, tx, fragmentID)
		return err
	})
	return out, err
}

func (r *Repository) RetryCorruptFragment(ctx context.Context, fragmentID, partPath string) (Fragment, error) {
	if strings.TrimSpace(partPath) == "" {
		return Fragment{}, ErrInvalidRequest
	}
	var out Fragment
	err := r.store.WithImmediate(ctx, func(tx store.Executor) error {
		fragment, err := r.getFragmentInTx(ctx, tx, fragmentID)
		if err != nil {
			return err
		}
		if fragment.State != FragmentCorrupt {
			return ErrFragmentState
		}
		job, err := r.getInTx(ctx, tx, fragment.JobID, false)
		if err != nil {
			return err
		}
		if job.Task.State != StateRunning || job.CancelRequested {
			return ErrStateConflict
		}
		now := r.now().UTC().Format(time.RFC3339Nano)
		result, err := tx.ExecContext(ctx, `UPDATE export_fragments SET state='WRITING',
			part_path=?,final_path=NULL,compressed_size=NULL,uncompressed_size=NULL,
			checksum_sha256=NULL,validation_epoch=NULL,updated_at=?
			WHERE fragment_id=? AND state='CORRUPT'`, partPath, now, fragmentID)
		if err != nil {
			return err
		}
		if err := requireFragmentRow(result); err != nil {
			return err
		}
		if _, err := r.touchSnapshotInTx(ctx, tx, job, "FRAGMENT"); err != nil {
			return err
		}
		out, err = r.getFragmentInTx(ctx, tx, fragmentID)
		return err
	})
	return out, err
}

func (r *Repository) GetFragment(ctx context.Context, fragmentID string) (Fragment, error) {
	return r.getFragmentInTx(ctx, r.store.DB(), fragmentID)
}

func (r *Repository) ListReusableFragments(ctx context.Context, jobID string) ([]Fragment, error) {
	fragments, err := r.listFragmentsInTx(ctx, r.store.DB(), jobID)
	if err != nil {
		return nil, err
	}
	reusable := make([]Fragment, 0, len(fragments))
	for _, fragment := range fragments {
		if fragment.State == FragmentComplete && fragment.Reusable {
			reusable = append(reusable, fragment)
		}
	}
	return reusable, nil
}

// ValidateFragmentForReuse invokes the filesystem validator outside SQLite,
// then conditionally commits the result only if the durable fragment metadata
// is unchanged. A failed or mismatched validation marks the fragment CORRUPT.
func (r *Repository) ValidateFragmentForReuse(
	ctx context.Context,
	fragmentID string,
	validator FragmentValidator,
) (Fragment, error) {
	if validator == nil {
		return Fragment{}, ErrInvalidRequest
	}
	before, err := r.GetFragment(ctx, fragmentID)
	if err != nil {
		return Fragment{}, err
	}
	if before.State != FragmentComplete {
		return Fragment{}, ErrFragmentState
	}
	if before.Reusable {
		return before, nil
	}
	observed, validationErr := validator.ValidateFragment(ctx, before)
	if errors.Is(validationErr, context.Canceled) || errors.Is(validationErr, context.DeadlineExceeded) {
		return Fragment{}, validationErr
	}
	if validationErr == nil {
		validationErr = validateArtifactValidation(observed)
	}
	valid := validationErr == nil && artifactMatches(before, observed)
	if validationErr == nil && !valid {
		validationErr = ErrFragmentCorrupt
	}

	var out Fragment
	err = r.store.WithImmediate(ctx, func(tx store.Executor) error {
		current, err := r.getFragmentInTx(ctx, tx, fragmentID)
		if err != nil {
			return err
		}
		if !sameFragmentVersion(before, current) || current.State != FragmentComplete {
			return tasks.ErrRevisionConflict
		}
		job, err := r.getInTx(ctx, tx, current.JobID, false)
		if err != nil {
			return err
		}
		now := r.now().UTC().Format(time.RFC3339Nano)
		if !valid {
			if err := r.markFragmentCorruptInTx(ctx, tx, fragmentID); err != nil {
				return err
			}
		} else {
			result, err := tx.ExecContext(ctx, `UPDATE export_fragments SET validation_epoch=?,
				updated_at=? WHERE fragment_id=? AND state='COMPLETE'`, r.validationEpoch, now, fragmentID)
			if err != nil {
				return err
			}
			if err := requireFragmentRow(result); err != nil {
				return err
			}
		}
		if _, err := r.touchSnapshotInTx(ctx, tx, job, "FRAGMENT_VALIDATED"); err != nil {
			return err
		}
		out, err = r.getFragmentInTx(ctx, tx, fragmentID)
		return err
	})
	if err != nil {
		return Fragment{}, err
	}
	if validationErr != nil {
		return out, fmt.Errorf("%w: %v", ErrFragmentCorrupt, validationErr)
	}
	return out, nil
}

func (r *Repository) touchSnapshotInTx(
	ctx context.Context,
	tx store.Executor,
	job Job,
	changeType string,
) (Job, error) {
	updated, err := r.tasks.ApplyInTx(ctx, tx, job.Task, tasks.Change{
		State: job.Task.State, Terminal: job.Task.Terminal, StateChanged: false,
		ChangeType: changeType, PublicErrorCode: job.Task.PublicErrorCode,
		PublicSafeMessage: job.Task.PublicSafeMessage,
	})
	if err != nil {
		return Job{}, err
	}
	result, err := tx.ExecContext(ctx, `UPDATE export_jobs SET snapshot_revision=?,updated_at=?
		WHERE job_id=? AND snapshot_revision=?`, updated.SnapshotRevision,
		r.now().UTC().Format(time.RFC3339Nano), job.Task.ID, job.Task.SnapshotRevision)
	if err != nil {
		return Job{}, err
	}
	if err := requireOne(result); err != nil {
		return Job{}, err
	}
	job.Task = updated
	return job, nil
}

func (r *Repository) markFragmentCorruptInTx(ctx context.Context, tx store.Executor, fragmentID string) error {
	result, err := tx.ExecContext(ctx, `UPDATE export_fragments SET state='CORRUPT',
		validation_epoch=NULL,updated_at=? WHERE fragment_id=? AND state<>'CORRUPT'`,
		r.now().UTC().Format(time.RFC3339Nano), fragmentID)
	if err != nil {
		return err
	}
	return requireFragmentRow(result)
}

func (r *Repository) getFragmentInTx(ctx context.Context, tx store.Executor, fragmentID string) (Fragment, error) {
	var fragment Fragment
	var startNS, endNS, partPath, finalPath sql.NullString
	var compressedSize, uncompressedSize, checksum, validationEpoch sql.NullString
	err := tx.QueryRowContext(ctx, `SELECT fragment_id,job_id,ordinal,kind,state,start_ns,end_ns,
		part_path,final_path,compressed_size,uncompressed_size,checksum_sha256,
		validation_epoch,created_at,updated_at FROM export_fragments WHERE fragment_id=?`,
		fragmentID).Scan(&fragment.FragmentID, &fragment.JobID, &fragment.Ordinal,
		&fragment.Kind, &fragment.State, &startNS, &endNS, &partPath, &finalPath,
		&compressedSize, &uncompressedSize, &checksum, &validationEpoch,
		&fragment.CreatedAt, &fragment.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return Fragment{}, ErrFragmentNotFound
	}
	if err != nil {
		return Fragment{}, err
	}
	fragment.StartNS = nullStringPointer(startNS)
	fragment.EndNS = nullStringPointer(endNS)
	fragment.PartPath = nullStringPointer(partPath)
	fragment.FinalPath = nullStringPointer(finalPath)
	fragment.CompressedSize = nullStringPointer(compressedSize)
	fragment.UncompressedSize = nullStringPointer(uncompressedSize)
	fragment.ChecksumSHA256 = nullStringPointer(checksum)
	fragment.Reusable = fragment.State == FragmentComplete && validationEpoch.Valid &&
		validationEpoch.String == r.validationEpoch
	return fragment, nil
}

func (r *Repository) listFragmentsInTx(ctx context.Context, tx store.Executor, jobID string) ([]Fragment, error) {
	rows, err := tx.QueryContext(ctx, `SELECT fragment_id FROM export_fragments WHERE job_id=?`, jobID)
	if err != nil {
		return nil, err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		ids = append(ids, id)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	fragments := make([]Fragment, 0, len(ids))
	for _, id := range ids {
		fragment, err := r.getFragmentInTx(ctx, tx, id)
		if err != nil {
			return nil, err
		}
		fragments = append(fragments, fragment)
	}
	sortFragments(fragments)
	return fragments, nil
}

func validateBeginFragment(req BeginFragmentRequest) error {
	if uuid.Validate(req.FragmentID) != nil || uuid.Validate(req.JobID) != nil ||
		strings.TrimSpace(req.PartPath) == "" || (req.Kind != FragmentData && req.Kind != FragmentManifest) {
		return ErrInvalidRequest
	}
	if _, err := canonicalUnsigned(req.Ordinal); err != nil {
		return err
	}
	if req.Kind == FragmentManifest {
		if req.StartNS != nil || req.EndNS != nil {
			return ErrInvalidRequest
		}
		return nil
	}
	if req.StartNS == nil || req.EndNS == nil {
		return ErrInvalidRequest
	}
	start, err := canonicalSigned(*req.StartNS)
	if err != nil {
		return err
	}
	end, err := canonicalSigned(*req.EndNS)
	if err != nil {
		return err
	}
	startNumber := new(big.Int)
	endNumber := new(big.Int)
	startNumber.SetString(start, 10)
	endNumber.SetString(end, 10)
	if startNumber.Cmp(endNumber) >= 0 {
		return ErrInvalidRequest
	}
	return nil
}

func validateFinalizeFragment(req FinalizeFragmentRequest) error {
	if uuid.Validate(req.FragmentID) != nil || strings.TrimSpace(req.PartPath) == "" ||
		strings.TrimSpace(req.FinalPath) == "" || !validSHA256(req.ChecksumSHA256) {
		return ErrInvalidRequest
	}
	if _, err := canonicalUnsigned(req.CompressedSize); err != nil {
		return err
	}
	if _, err := canonicalUnsigned(req.UncompressedSize); err != nil {
		return err
	}
	return nil
}

func validateArtifactValidation(validation ArtifactValidation) error {
	if !validSHA256(validation.ChecksumSHA256) {
		return ErrFragmentCorrupt
	}
	if _, err := canonicalUnsigned(validation.CompressedSize); err != nil {
		return err
	}
	if _, err := canonicalUnsigned(validation.UncompressedSize); err != nil {
		return err
	}
	return nil
}

func artifactMatches(fragment Fragment, observed ArtifactValidation) bool {
	return fragment.CompressedSize != nil && fragment.UncompressedSize != nil &&
		fragment.ChecksumSHA256 != nil && *fragment.CompressedSize == observed.CompressedSize &&
		*fragment.UncompressedSize == observed.UncompressedSize &&
		*fragment.ChecksumSHA256 == observed.ChecksumSHA256
}

func sameFragmentVersion(left, right Fragment) bool {
	return left.FragmentID == right.FragmentID && left.JobID == right.JobID &&
		left.State == right.State && left.UpdatedAt == right.UpdatedAt &&
		pointerEqual(left.CompressedSize, right.CompressedSize) &&
		pointerEqual(left.UncompressedSize, right.UncompressedSize) &&
		pointerEqual(left.ChecksumSHA256, right.ChecksumSHA256)
}

func pointerEqual(left, right *string) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}

func canonicalSigned(value string) (string, error) {
	if value == "" || strings.HasPrefix(value, "+") || value == "-0" ||
		(len(value) > 1 && value[0] == '0') || (len(value) > 2 && strings.HasPrefix(value, "-0")) {
		return "", tasks.ErrInvalidDecimal
	}
	number := new(big.Int)
	if _, ok := number.SetString(value, 10); !ok {
		return "", tasks.ErrInvalidDecimal
	}
	return number.String(), nil
}

func nullStringPointer(value sql.NullString) *string {
	if !value.Valid {
		return nil
	}
	result := value.String
	return &result
}

func requireFragmentRow(result sql.Result) error {
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows != 1 {
		return ErrFragmentState
	}
	return nil
}
