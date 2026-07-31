package transfer

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/influxdesk/influxdesk/internal/store"
)

var (
	ErrStageFilePathInvalid = errors.New("IMPORT_STAGE_PATH_INVALID")
	ErrStageFileJobInvalid  = errors.New("IMPORT_STAGE_JOB_NOT_RETAINED")
	ErrStageFileExists      = errors.New("IMPORT_STAGE_FILE_EXISTS")
	ErrStageFileCreate      = errors.New("IMPORT_STAGE_FILE_CREATE_FAILED")
	ErrStageFileVolume      = errors.New("IMPORT_STAGE_VOLUME_UNAVAILABLE")
	ErrStageFileSync        = errors.New("IMPORT_STAGE_FILE_SYNC_FAILED")
	ErrStageFileClose       = errors.New("IMPORT_STAGE_FILE_CLOSE_FAILED")
	ErrStageFileCommit      = errors.New("IMPORT_STAGE_FILE_COMMIT_FAILED")
	ErrStageFileCleanup     = errors.New("IMPORT_STAGE_FILE_CLEANUP_FAILED")
	ErrStageFileSettlement  = errors.New("IMPORT_STAGE_FILE_SETTLEMENT_FAILED")
)

// StagingVolumeStat is refreshed immediately before each private extent is
// reserved. ID must be a stable volume identity rather than a drive letter.
type StagingVolumeStat struct {
	ID            string
	FreeBytes     int64
	CapacityBytes int64
}

type StagingVolumeStatFunc func(context.Context, string) (StagingVolumeStat, error)

type StageImportFileRequest struct {
	Job              ImportJob
	StagingDirectory string
	Source           io.Reader
	Format           ImportStageFormat

	NumericTextResolver NumericTextResolver
	CSVMapping          *CSVMapping
}

// StagingFileCleanup identifies only reservations created by this staging
// attempt. It is returned on both success and failure so cleanup never needs to
// guess which quota records belong to the file.
type StagingFileCleanup struct {
	JobID            string   `json:"jobId"`
	StagingDirectory string   `json:"stagingDirectory"`
	ReservationIDs   []string `json:"reservationIds"`
}

type StageImportFileResult struct {
	StagingPath string             `json:"stagingPath"`
	Stage       StageImportResult  `json:"stage"`
	Cleanup     StagingFileCleanup `json:"cleanup"`
}

type stagingFileHooks struct {
	beforeSync   func(context.Context, string) error
	beforeRename func(context.Context, string, string) error
	rename       func(string, string) error
	remove       func(string) error
}

// StagingFileCoordinator owns the durable file boundary around StageImport.
// It does not mutate Import job state; callers may transition a job to READY
// only after Stage returns successfully.
type StagingFileCoordinator struct {
	quota      *QuotaManager
	volumeStat StagingVolumeStatFunc
	hooks      stagingFileHooks
}

func NewStagingFileCoordinator(quota *QuotaManager, volumeStat StagingVolumeStatFunc) *StagingFileCoordinator {
	return &StagingFileCoordinator{
		quota: quota, volumeStat: volumeStat,
		hooks: stagingFileHooks{rename: durableRename},
	}
}

// Stage writes a deterministic exclusive .part, syncs and closes it, performs
// a same-directory durable rename, then atomically settles its private quota
// reservations to the committed file's actual logical length.
//
// On error StagingPath is empty and the returned Cleanup remains usable. The
// coordinator deliberately keeps files and reservations conservative until
// Cleanup has confirmed deletion; callers must not mark the job READY.
func (c *StagingFileCoordinator) Stage(ctx context.Context, request StageImportFileRequest) (StageImportFileResult, error) {
	out := StageImportFileResult{}
	if c == nil || c.quota == nil || c.volumeStat == nil || ctx == nil || request.Source == nil {
		return out, ErrStageInvalidRequest
	}
	directory, partPath, finalPath, err := stagingFilePaths(request.StagingDirectory, request.Job.Task.ID)
	if err != nil {
		return out, err
	}
	out.Cleanup = StagingFileCleanup{JobID: request.Job.Task.ID, StagingDirectory: directory}
	if request.Job.Task.Kind != "IMPORT" || request.Job.ProfileID == "" ||
		!c.retainedImportMatches(ctx, request.Job.Task.ID, request.Job.ProfileID) {
		return out, ErrStageFileJobInvalid
	}
	if err := requireStagingDestinationAbsent(partPath, finalPath); err != nil {
		return out, err
	}

	file, err := os.OpenFile(partPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return out, ErrStageFileCreate
	}
	fileOpen := true
	defer func() {
		if fileOpen {
			_ = file.Close()
		}
	}()

	var reserveFailure error
	var volumeID string
	reservations := make([]Reservation, 0, 1)
	reserve := func(reserveCtx context.Context, requested int64) error {
		if requested != ReservationExtentBytes {
			reserveFailure = ErrStageFileSettlement
			return reserveFailure
		}
		stat, statErr := c.volumeStat(reserveCtx, directory)
		if statErr != nil || stat.ID == "" || stat.FreeBytes < 0 || stat.CapacityBytes <= 0 {
			reserveFailure = ErrStageFileVolume
			return reserveFailure
		}
		if volumeID == "" {
			volumeID = stat.ID
		} else if volumeID != stat.ID {
			reserveFailure = ErrStageFileVolume
			return reserveFailure
		}
		reservation, reserveErr := c.quota.ReserveExtent(
			reserveCtx, request.Job.Task.ID, "PRIVATE", stat.ID, requested,
			stat.FreeBytes, stat.CapacityBytes,
		)
		if reserveErr != nil {
			reserveFailure = reserveErr
			return reserveErr
		}
		reservations = append(reservations, reservation)
		out.Cleanup.ReservationIDs = append(out.Cleanup.ReservationIDs, reservation.ReservationID)
		return nil
	}

	settlementObserved := false
	stageResult, err := StageImport(ctx, StageImportRequest{
		Format: request.Format, Source: request.Source, Destination: file,
		Reserve: reserve,
		SettleReservation: func(_ context.Context, reserved, written int64) error {
			// The durable coordinator performs the database settlement only after
			// sync, close, and rename have committed a complete file.
			settlementObserved = reserved >= written && written > 0
			if !settlementObserved {
				return ErrStageSettlement
			}
			return nil
		},
		NumericTextResolver: request.NumericTextResolver,
		CSVMapping:          request.CSVMapping,
	})
	if err != nil {
		if reserveFailure != nil {
			return out, reserveFailure
		}
		return out, err
	}
	if !settlementObserved {
		return out, ErrStageFileSettlement
	}
	if err := stageContextError(ctx); err != nil {
		return out, err
	}
	if c.hooks.beforeSync != nil {
		if err := c.hooks.beforeSync(ctx, partPath); err != nil {
			return out, ErrStageFileSync
		}
	}
	if err := file.Sync(); err != nil {
		return out, ErrStageFileSync
	}
	if err := file.Close(); err != nil {
		fileOpen = false
		return out, ErrStageFileClose
	}
	fileOpen = false

	logicalBytes, err := strconv.ParseInt(stageResult.LogicalBytes, 10, 64)
	if err != nil || logicalBytes <= 0 {
		return out, ErrStageFileSettlement
	}
	if err := validateClosedStagingPart(directory, partPath, finalPath, logicalBytes); err != nil {
		return out, err
	}
	if err := stageContextError(ctx); err != nil {
		return out, err
	}
	if c.hooks.beforeRename != nil {
		if err := c.hooks.beforeRename(ctx, partPath, finalPath); err != nil {
			return out, ErrStageFileCommit
		}
	}
	rename := c.hooks.rename
	if rename == nil {
		rename = durableRename
	}
	if err := rename(partPath, finalPath); err != nil {
		return out, ErrStageFileCommit
	}
	if err := validateCommittedStagingFile(directory, finalPath, logicalBytes); err != nil {
		return out, err
	}
	if err := c.quota.settleStagingReservations(ctx, request.Job.Task.ID, reservations, logicalBytes); err != nil {
		return out, ErrStageFileSettlement
	}

	out.StagingPath = finalPath
	out.Stage = stageResult
	return out, nil
}

// Cleanup removes both deterministic file names before releasing this
// attempt's reservations in one FULL transaction. A deletion or verification
// failure leaves all remaining reservations charged.
func (c *StagingFileCoordinator) Cleanup(ctx context.Context, cleanup StagingFileCleanup) error {
	if c == nil || c.quota == nil || ctx == nil {
		return ErrStageInvalidRequest
	}
	_, partPath, finalPath, err := stagingFilePaths(cleanup.StagingDirectory, cleanup.JobID)
	if err != nil {
		return err
	}
	if err := c.removeRegularStagingFile(partPath); err != nil {
		return ErrStageFileCleanup
	}
	if err := c.removeRegularStagingFile(finalPath); err != nil {
		return ErrStageFileCleanup
	}
	if err := c.quota.releaseStagingReservations(ctx, cleanup.JobID, cleanup.ReservationIDs); err != nil {
		return ErrStageFileCleanup
	}
	return nil
}

// RemoveImportFiles is the protected, idempotent file half of CleanupImport.
// Reservation and retained-slot release deliberately remain with Repository so
// they can be committed together with the final task snapshot and command row.
func (c *StagingFileCoordinator) RemoveImportFiles(
	ctx context.Context,
	stagingDirectory, jobID string,
) error {
	if c == nil || ctx == nil {
		return ErrStageInvalidRequest
	}
	if err := stageContextError(ctx); err != nil {
		return ErrStageFileCleanup
	}
	_, partPath, finalPath, err := stagingFilePaths(stagingDirectory, jobID)
	if err != nil {
		return err
	}
	if err := c.removeRegularStagingFile(partPath); err != nil {
		return ErrStageFileCleanup
	}
	if err := c.removeRegularStagingFile(finalPath); err != nil {
		return ErrStageFileCleanup
	}
	return nil
}

// CleanupFileFunc binds the protected backend directory without exposing it in
// the public CleanupImport command envelope.
func (c *StagingFileCoordinator) CleanupFileFunc(stagingDirectory string) ImportCleanupFileFunc {
	return func(ctx context.Context, jobID string) error {
		return c.RemoveImportFiles(ctx, stagingDirectory, jobID)
	}
}

func (c *StagingFileCoordinator) retainedImportMatches(ctx context.Context, jobID, profileID string) bool {
	var storedProfile, kind string
	var retained int
	err := c.quota.store.DB().QueryRowContext(ctx, `SELECT profile_id,kind,retained
		FROM transfer_jobs WHERE job_id=?`, jobID).Scan(&storedProfile, &kind, &retained)
	return err == nil && storedProfile == profileID && kind == "IMPORT" && retained == 1
}

func stagingFilePaths(directory, jobID string) (string, string, string, error) {
	if !validStagingJobID(jobID) || directory == "" || !filepath.IsAbs(directory) ||
		filepath.Clean(directory) != directory {
		return "", "", "", ErrStageFilePathInvalid
	}
	evaluated, err := filepath.EvalSymlinks(directory)
	if err != nil || !sameStagingPath(evaluated, directory) {
		return "", "", "", ErrStageFilePathInvalid
	}
	info, err := os.Lstat(directory)
	if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return "", "", "", ErrStageFilePathInvalid
	}
	finalPath := filepath.Join(directory, "import-"+jobID+".canonical.lp")
	partPath := finalPath + ".part"
	if filepath.Dir(finalPath) != directory || filepath.Dir(partPath) != directory {
		return "", "", "", ErrStageFilePathInvalid
	}
	return directory, partPath, finalPath, nil
}

func validStagingJobID(jobID string) bool {
	if jobID == "" || len(jobID) > 128 {
		return false
	}
	for index := 0; index < len(jobID); index++ {
		current := jobID[index]
		if (current >= 'a' && current <= 'z') || (current >= 'A' && current <= 'Z') ||
			(current >= '0' && current <= '9') || current == '-' || current == '_' {
			continue
		}
		return false
	}
	return true
}

func requireStagingDestinationAbsent(partPath, finalPath string) error {
	for _, path := range []string{partPath, finalPath} {
		if _, err := os.Lstat(path); err == nil {
			return ErrStageFileExists
		} else if !errors.Is(err, os.ErrNotExist) {
			return ErrStageFilePathInvalid
		}
	}
	return nil
}

func validateClosedStagingPart(directory, partPath, finalPath string, expectedSize int64) error {
	evaluated, err := filepath.EvalSymlinks(directory)
	if err != nil || !sameStagingPath(evaluated, directory) {
		return ErrStageFilePathInvalid
	}
	if _, err := os.Lstat(finalPath); err == nil {
		return ErrStageFileExists
	} else if !errors.Is(err, os.ErrNotExist) {
		return ErrStageFilePathInvalid
	}
	info, err := os.Lstat(partPath)
	if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() || info.Size() != expectedSize {
		return ErrStageFileSync
	}
	return nil
}

func validateCommittedStagingFile(directory, finalPath string, expectedSize int64) error {
	evaluated, err := filepath.EvalSymlinks(directory)
	if err != nil || !sameStagingPath(evaluated, directory) {
		return ErrStageFilePathInvalid
	}
	info, err := os.Lstat(finalPath)
	if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() || info.Size() != expectedSize {
		return ErrStageFileCommit
	}
	return nil
}

func (c *StagingFileCoordinator) removeRegularStagingFile(path string) error {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return ErrStageFileCleanup
	}
	remove := os.Remove
	if c != nil && c.hooks.remove != nil {
		remove = c.hooks.remove
	}
	if err := remove(path); err != nil {
		return ErrStageFileCleanup
	}
	if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
		return ErrStageFileCleanup
	}
	return nil
}

func sameStagingPath(left, right string) bool {
	return sameStagingPathForOS(runtime.GOOS, left, right)
}

func sameStagingPathForOS(goos, left, right string) bool {
	left = filepath.Clean(left)
	right = filepath.Clean(right)
	if goos == "windows" {
		return strings.EqualFold(left, right)
	}
	// macOS exposes its trusted system temporary hierarchy through /var while
	// the canonical vnode path is /private/var. Do not generalize this exception
	// to arbitrary symlinked parents.
	if goos == "darwin" {
		return left == right || darwinPrivateVarAlias(left, right) || darwinPrivateVarAlias(right, left)
	}
	return left == right
}

func darwinPrivateVarAlias(privatePath, publicPath string) bool {
	return (publicPath == "/var" || strings.HasPrefix(publicPath, "/var/")) &&
		privatePath == "/private"+publicPath
}

func (q *QuotaManager) settleStagingReservations(
	ctx context.Context,
	jobID string,
	reservations []Reservation,
	logicalBytes int64,
) error {
	if q == nil || jobID == "" || logicalBytes <= 0 || len(reservations) == 0 {
		return ErrStageFileSettlement
	}
	return q.store.WithImmediate(ctx, func(tx store.Executor) error {
		type reservationRow struct {
			id       string
			reserved int64
			consumed int64
		}
		rows := make([]reservationRow, 0, len(reservations))
		seen := make(map[string]struct{}, len(reservations))
		var totalReserved, totalConsumed int64
		for _, reservation := range reservations {
			if reservation.ReservationID == "" {
				return ErrStageFileSettlement
			}
			if _, duplicate := seen[reservation.ReservationID]; duplicate {
				return ErrStageFileSettlement
			}
			seen[reservation.ReservationID] = struct{}{}
			var storedJob, scopeKind, reservedText, consumedText string
			if err := tx.QueryRowContext(ctx, `SELECT job_id,scope_kind,reserved_bytes,consumed_bytes
				FROM transfer_reservations WHERE reservation_id=?`, reservation.ReservationID).Scan(
				&storedJob, &scopeKind, &reservedText, &consumedText,
			); err != nil {
				return ErrStageFileSettlement
			}
			reserved, err := strconv.ParseInt(reservedText, 10, 64)
			if err != nil || reserved <= 0 || storedJob != jobID || scopeKind != "PRIVATE" {
				return ErrStageFileSettlement
			}
			consumed, err := strconv.ParseInt(consumedText, 10, 64)
			if err != nil || consumed < 0 || consumed > reserved {
				return ErrStageFileSettlement
			}
			if totalReserved > math.MaxInt64-reserved || totalConsumed > math.MaxInt64-consumed {
				return ErrStageFileSettlement
			}
			totalReserved += reserved
			totalConsumed += consumed
			rows = append(rows, reservationRow{id: reservation.ReservationID, reserved: reserved, consumed: consumed})
		}
		if totalReserved == logicalBytes && totalConsumed == logicalBytes {
			return nil
		}
		if logicalBytes > totalReserved || totalConsumed != 0 {
			return ErrStageFileSettlement
		}
		remaining := logicalBytes
		updatedAt := q.now().UTC().Format(time.RFC3339Nano)
		for _, row := range rows {
			used := min(remaining, row.reserved)
			if used == 0 {
				if _, err := tx.ExecContext(ctx, `DELETE FROM transfer_reservations
					WHERE reservation_id=? AND job_id=? AND scope_kind='PRIVATE'`, row.id, jobID); err != nil {
					return err
				}
				continue
			}
			if _, err := tx.ExecContext(ctx, `UPDATE transfer_reservations
				SET reserved_bytes=?,consumed_bytes=?,updated_at=?
				WHERE reservation_id=? AND job_id=? AND scope_kind='PRIVATE'`,
				strconv.FormatInt(used, 10), strconv.FormatInt(used, 10), updatedAt, row.id, jobID,
			); err != nil {
				return err
			}
			remaining -= used
		}
		if remaining != 0 {
			return ErrStageFileSettlement
		}
		return nil
	})
}

func (q *QuotaManager) releaseStagingReservations(ctx context.Context, jobID string, reservationIDs []string) error {
	if q == nil || jobID == "" {
		return ErrStageFileCleanup
	}
	if len(reservationIDs) == 0 {
		return nil
	}
	return q.store.WithImmediate(ctx, func(tx store.Executor) error {
		seen := make(map[string]struct{}, len(reservationIDs))
		for _, reservationID := range reservationIDs {
			if reservationID == "" {
				return ErrStageFileCleanup
			}
			if _, duplicate := seen[reservationID]; duplicate {
				return ErrStageFileCleanup
			}
			seen[reservationID] = struct{}{}
			var storedJob, scopeKind string
			err := tx.QueryRowContext(ctx, `SELECT job_id,scope_kind FROM transfer_reservations
				WHERE reservation_id=?`, reservationID).Scan(&storedJob, &scopeKind)
			if errors.Is(err, sql.ErrNoRows) {
				continue
			}
			if err != nil || storedJob != jobID || scopeKind != "PRIVATE" {
				return ErrStageFileCleanup
			}
			if _, err := tx.ExecContext(ctx, `DELETE FROM transfer_reservations
				WHERE reservation_id=? AND job_id=? AND scope_kind='PRIVATE'`, reservationID, jobID); err != nil {
				return err
			}
		}
		return nil
	})
}
