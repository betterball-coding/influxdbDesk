package profile

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/influxdesk/influxdesk/internal/credential"
	"github.com/influxdesk/influxdesk/internal/protection"
	"github.com/influxdesk/influxdesk/internal/store"
	"github.com/influxdesk/influxdesk/internal/transport"
)

type Repository struct {
	store *store.Store
	now   func() time.Time
}

func NewRepository(s *store.Store, now func() time.Time) *Repository {
	if now == nil {
		now = time.Now
	}
	return &Repository{store: s, now: now}
}

func (r *Repository) List(ctx context.Context) ([]Profile, error) {
	rows, err := r.store.DB().QueryContext(ctx, `SELECT id,revision,name,base_url,default_database,environment,
		auth_mode,username,allow_insecure_auth,credential_kind,credential_ref,protection_mode,created_at,updated_at
		FROM profiles ORDER BY lower(name),id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	profiles := make([]Profile, 0)
	for rows.Next() {
		value, err := scanProfile(rows)
		if err != nil {
			return nil, err
		}
		profiles = append(profiles, value)
	}
	return profiles, rows.Err()
}

func (r *Repository) Get(ctx context.Context, id string) (Profile, error) {
	return getInTx(ctx, r.store.DB(), id)
}

func (r *Repository) Save(ctx context.Context, req SaveRequest) (Profile, error) {
	if err := validateSaveRequest(&req); err != nil {
		return Profile{}, err
	}
	creating := req.ExpectedRevision == ""
	if req.ID == "" {
		if !creating {
			return Profile{}, ErrInvalidProfile
		}
		req.ID = uuid.NewString()
	} else if _, err := uuid.Parse(req.ID); err != nil {
		return Profile{}, ErrInvalidProfile
	}

	var credentialKind *credential.Kind
	var credentialRef *string
	switch req.AuthMode {
	case transport.AuthBasic:
		kind := credential.KindInfluxPassword
		ref, err := credential.Key(req.ID, kind)
		if err != nil {
			return Profile{}, err
		}
		credentialKind, credentialRef = &kind, &ref
	case transport.AuthBearer:
		kind := credential.KindJWT
		ref, err := credential.Key(req.ID, kind)
		if err != nil {
			return Profile{}, err
		}
		credentialKind, credentialRef = &kind, &ref
	}

	var out Profile
	err := r.store.WithImmediate(ctx, func(tx store.Executor) error {
		now := r.now().UTC().Format(time.RFC3339Nano)
		if creating {
			out = Profile{
				ID: req.ID, Revision: "1", Name: req.Name, BaseURL: req.BaseURL,
				DefaultDatabase: req.DefaultDatabase,
				Environment:     req.Environment, AuthMode: req.AuthMode, Username: req.Username,
				AllowInsecureAuth: req.AllowInsecureAuth,
				CredentialKind:    credentialKind, CredentialRef: credentialRef,
				ProtectionMode: req.ProtectionMode, CreatedAt: now, UpdatedAt: now,
			}
			_, err := tx.ExecContext(ctx, `INSERT INTO profiles(
						id,revision,name,base_url,default_database,environment,auth_mode,username,allow_insecure_auth,
						credential_kind,credential_ref,protection_mode,created_at,updated_at
					) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, out.ID, out.Revision, out.Name, out.BaseURL,
				out.DefaultDatabase, string(out.Environment), string(out.AuthMode), nullable(out.Username), out.AllowInsecureAuth,
				nullableKind(out.CredentialKind), nullableString(out.CredentialRef), string(out.ProtectionMode), out.CreatedAt, out.UpdatedAt)
			return err
		}

		current, err := getInTx(ctx, tx, req.ID)
		if err != nil {
			return err
		}
		if current.Revision != req.ExpectedRevision {
			return ErrRevisionConflict
		}
		revision, err := incrementDecimal(current.Revision)
		if err != nil {
			return err
		}
		out = Profile{
			ID: req.ID, Revision: revision, Name: req.Name, BaseURL: req.BaseURL,
			DefaultDatabase: req.DefaultDatabase,
			Environment:     req.Environment, AuthMode: req.AuthMode, Username: req.Username,
			AllowInsecureAuth: req.AllowInsecureAuth,
			CredentialKind:    credentialKind, CredentialRef: credentialRef,
			ProtectionMode: req.ProtectionMode, CreatedAt: current.CreatedAt, UpdatedAt: now,
		}
		result, err := tx.ExecContext(ctx, `UPDATE profiles SET revision=?,name=?,base_url=?,default_database=?,
				environment=?,auth_mode=?,username=?,allow_insecure_auth=?,credential_kind=?,credential_ref=?,
				protection_mode=?,updated_at=? WHERE id=? AND revision=?`, out.Revision, out.Name,
			out.BaseURL, out.DefaultDatabase, string(out.Environment), string(out.AuthMode), nullable(out.Username),
			out.AllowInsecureAuth, nullableKind(out.CredentialKind), nullableString(out.CredentialRef), string(out.ProtectionMode),
			out.UpdatedAt, out.ID, current.Revision)
		if err != nil {
			return err
		}
		changed, err := result.RowsAffected()
		if err != nil || changed != 1 {
			return ErrRevisionConflict
		}
		return nil
	})
	return out, err
}

func (r *Repository) Delete(ctx context.Context, id, expectedRevision string) error {
	if _, err := uuid.Parse(id); err != nil {
		return ErrInvalidProfile
	}
	if _, err := canonicalDecimal(expectedRevision); err != nil {
		return err
	}
	return r.store.WithImmediate(ctx, func(tx store.Executor) error {
		current, err := getInTx(ctx, tx, id)
		if err != nil {
			return err
		}
		if current.Revision != expectedRevision {
			return ErrRevisionConflict
		}
		result, err := tx.ExecContext(ctx, "DELETE FROM profiles WHERE id=? AND revision=?", id, expectedRevision)
		if err != nil {
			return err
		}
		changed, err := result.RowsAffected()
		if err != nil || changed != 1 {
			return ErrRevisionConflict
		}
		return nil
	})
}

// NextGeneration reserves a generation durably before any network resource is opened.
// A failed open may leave a gap, but a generation is never reused.
func (r *Repository) NextGeneration(ctx context.Context, profileID string) (string, error) {
	if _, err := uuid.Parse(profileID); err != nil {
		return "", ErrInvalidProfile
	}
	var next string
	err := r.store.WithImmediate(ctx, func(tx store.Executor) error {
		var last string
		err := tx.QueryRowContext(ctx, `SELECT last_generation FROM connection_generation_counters
			WHERE profile_id=?`, profileID).Scan(&last)
		switch {
		case errors.Is(err, sql.ErrNoRows):
			next = "1"
			_, err = tx.ExecContext(ctx, `INSERT INTO connection_generation_counters(
				profile_id,last_generation,updated_at) VALUES(?,?,?)`, profileID, next,
				r.now().UTC().Format(time.RFC3339Nano))
			return err
		case err != nil:
			return err
		}
		next, err = incrementDecimal(last)
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `UPDATE connection_generation_counters
			SET last_generation=?,updated_at=? WHERE profile_id=? AND last_generation=?`, next,
			r.now().UTC().Format(time.RFC3339Nano), profileID, last)
		return err
	})
	return next, err
}

type rowScanner interface {
	Scan(...any) error
}

func getInTx(ctx context.Context, tx store.Executor, id string) (Profile, error) {
	row := tx.QueryRowContext(ctx, `SELECT id,revision,name,base_url,default_database,environment,
		auth_mode,username,allow_insecure_auth,credential_kind,credential_ref,protection_mode,created_at,updated_at
		FROM profiles WHERE id=?`, id)
	value, err := scanProfile(row)
	if errors.Is(err, sql.ErrNoRows) {
		return Profile{}, ErrNotFound
	}
	return value, err
}

func scanProfile(row rowScanner) (Profile, error) {
	var value Profile
	var environment, authMode, protectionMode string
	var username, kind, ref sql.NullString
	if err := row.Scan(&value.ID, &value.Revision, &value.Name, &value.BaseURL, &value.DefaultDatabase, &environment,
		&authMode, &username, &value.AllowInsecureAuth, &kind, &ref, &protectionMode, &value.CreatedAt, &value.UpdatedAt); err != nil {
		return Profile{}, err
	}
	value.Environment = Environment(environment)
	value.AuthMode = transport.AuthMode(authMode)
	value.ProtectionMode = protection.Mode(protectionMode)
	if username.Valid {
		value.Username = username.String
	}
	if kind.Valid {
		parsed := credential.Kind(kind.String)
		value.CredentialKind = &parsed
	}
	if ref.Valid {
		value.CredentialRef = &ref.String
	}
	return value, nil
}

func validateSaveRequest(req *SaveRequest) error {
	req.Name = strings.TrimSpace(req.Name)
	req.BaseURL = strings.TrimSpace(req.BaseURL)
	req.DefaultDatabase = strings.TrimSpace(req.DefaultDatabase)
	if req.Name == "" || len(req.Name) > 120 || req.BaseURL == "" || strings.ContainsAny(req.Name, "\r\n") {
		return ErrInvalidProfile
	}
	if len(req.DefaultDatabase) > 4096 || strings.ContainsAny(req.DefaultDatabase, "\r\n\x00") {
		return ErrInvalidProfile
	}
	switch req.Environment {
	case EnvironmentProduction, EnvironmentStaging, EnvironmentDevelopment:
	default:
		return ErrInvalidProfile
	}
	if req.AuthMode == "" {
		req.AuthMode = transport.AuthNone
	}
	switch req.AuthMode {
	case transport.AuthNone:
		req.Username = ""
	case transport.AuthBasic:
		req.Username = strings.TrimSpace(req.Username)
		if req.Username == "" || strings.ContainsAny(req.Username, "\r\n") {
			return ErrInvalidProfile
		}
	case transport.AuthBearer:
		req.Username = ""
	default:
		return ErrInvalidProfile
	}
	switch req.ProtectionMode {
	case protection.PermanentReadOnly, protection.ProtectedLocked:
	default:
		return ErrInvalidProfile
	}
	if req.ExpectedRevision != "" {
		if _, err := canonicalDecimal(req.ExpectedRevision); err != nil {
			return ErrInvalidProfile
		}
	}
	dispatcher, err := transport.NewDispatcher(transport.Config{BaseURL: req.BaseURL, Auth: transport.AuthConfig{Mode: transport.AuthNone}})
	if err != nil {
		return fmt.Errorf("%w: base URL", ErrInvalidProfile)
	}
	dispatcher.CloseIdleConnections()
	requiresConsent, err := transport.RequiresInsecureAuthConsent(req.BaseURL, req.AuthMode)
	if err != nil || requiresConsent && !req.AllowInsecureAuth {
		return fmt.Errorf("%w: insecure authenticated HTTP", ErrInvalidProfile)
	}
	if !requiresConsent {
		req.AllowInsecureAuth = false
	}
	return nil
}

func ValidateSaveRequest(req SaveRequest) error {
	return validateSaveRequest(&req)
}

func nullable(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func nullableKind(value *credential.Kind) any {
	if value == nil {
		return nil
	}
	return string(*value)
}

func nullableString(value *string) any {
	if value == nil {
		return nil
	}
	return *value
}
