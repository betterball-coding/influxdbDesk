package transfer

import (
	"context"
)

func (r *Repository) ListRecoverableImports(ctx context.Context, profileID string, limit int) ([]ImportJob, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	query := `SELECT i.job_id FROM import_jobs i
		JOIN transfer_jobs t ON t.job_id=i.job_id
		WHERE t.retained=1 AND i.state IN (
			'PREFLIGHTING','STAGING','READY','RUNNING','PAUSED_SAFE','PAUSED_RESTAGE',
			'NEEDS_PARTIAL_DECISION','NEEDS_UNKNOWN_DECISION','CLEANING'
		)`
	args := make([]any, 0, 2)
	if profileID != "" {
		query += " AND t.profile_id=?"
		args = append(args, profileID)
	}
	query += " ORDER BY t.created_at DESC,i.job_id LIMIT ?"
	args = append(args, limit)
	rows, err := r.store.DB().QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	result := make([]ImportJob, 0, len(ids))
	for _, id := range ids {
		job, err := r.GetImport(ctx, id)
		if err != nil {
			return nil, err
		}
		result = append(result, job)
	}
	return result, nil
}
