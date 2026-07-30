package profile

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/influxdesk/influxdesk/internal/credential"
	"github.com/influxdesk/influxdesk/internal/transport"
)

type credentialStore interface {
	Put(string, credential.Kind, []byte) error
	Get(string, credential.Kind) ([]byte, error)
	Delete(string, credential.Kind) error
	DeleteProfile(string) error
}

type Service struct {
	repository  *Repository
	credentials credentialStore
}

type SaveCommand struct {
	Profile SaveRequest
	Secret  *string
}

func NewService(repository *Repository, credentials credentialStore) *Service {
	return &Service{repository: repository, credentials: credentials}
}

func (s *Service) List(ctx context.Context) ([]Profile, error) {
	return s.repository.List(ctx)
}

func (s *Service) Get(ctx context.Context, id string) (Profile, error) {
	return s.repository.Get(ctx, id)
}

func (s *Service) Save(ctx context.Context, command SaveCommand) (Profile, error) {
	request := command.Profile
	if request.ID == "" {
		request.ID = uuid.NewString()
	}
	if err := ValidateSaveRequest(request); err != nil {
		return Profile{}, err
	}
	var previous *Profile
	if request.ExpectedRevision != "" {
		value, err := s.repository.Get(ctx, request.ID)
		if err != nil {
			return Profile{}, err
		}
		previous = &value
	}

	newKind, needsSecret := credentialKindForAuth(request.AuthMode)
	var oldSecret []byte
	var oldKind *credential.Kind
	if previous != nil && previous.CredentialKind != nil {
		kind := *previous.CredentialKind
		oldKind = &kind
		value, err := s.credentials.Get(previous.ID, kind)
		if err != nil && !errors.Is(err, credential.ErrNotFound) {
			return Profile{}, err
		}
		oldSecret = value
		defer zeroSecret(oldSecret)
	}

	newSecretWritten := false
	if needsSecret {
		if command.Secret == nil {
			if oldKind == nil || *oldKind != newKind || len(oldSecret) == 0 {
				return Profile{}, errors.New("CREDENTIAL_REQUIRED")
			}
		} else {
			secret := []byte(*command.Secret)
			if len(secret) == 0 {
				return Profile{}, errors.New("CREDENTIAL_REQUIRED")
			}
			if err := s.credentials.Put(request.ID, newKind, secret); err != nil {
				zeroSecret(secret)
				return Profile{}, err
			}
			zeroSecret(secret)
			newSecretWritten = true
		}
	} else if command.Secret != nil {
		return Profile{}, ErrInvalidProfile
	}

	saved, err := s.repository.Save(ctx, request)
	if err != nil {
		var rollbackErr error
		if newSecretWritten {
			if oldKind != nil && *oldKind == newKind && len(oldSecret) != 0 {
				rollbackErr = s.credentials.Put(request.ID, *oldKind, oldSecret)
			} else {
				rollbackErr = s.credentials.Delete(request.ID, newKind)
			}
		}
		return Profile{}, errors.Join(err, rollbackErr)
	}
	if oldKind != nil && (!needsSecret || *oldKind != newKind) {
		if err := s.credentials.Delete(request.ID, *oldKind); err != nil {
			return Profile{}, fmt.Errorf("profile saved but old credential cleanup failed: %w", err)
		}
	}
	return saved, nil
}

func (s *Service) Delete(ctx context.Context, id, expectedRevision string) error {
	current, err := s.repository.Get(ctx, id)
	if err != nil {
		return err
	}
	if current.Revision != expectedRevision {
		return ErrRevisionConflict
	}
	backups := make(map[credential.Kind][]byte)
	for _, kind := range []credential.Kind{
		credential.KindInfluxPassword, credential.KindJWT, credential.KindSSHPassword,
		credential.KindSSHKeyPass, credential.KindMTLSKeyPass, credential.KindProxyPassword,
	} {
		value, readErr := s.credentials.Get(id, kind)
		if readErr == nil {
			backups[kind] = value
			defer zeroSecret(value)
		} else if !errors.Is(readErr, credential.ErrNotFound) {
			return readErr
		}
	}
	if err := s.credentials.DeleteProfile(id); err != nil {
		var restoreErr error
		for kind, value := range backups {
			restoreErr = errors.Join(restoreErr, s.credentials.Put(id, kind, value))
		}
		return errors.Join(err, restoreErr)
	}
	if err := s.repository.Delete(ctx, id, expectedRevision); err != nil {
		var restoreErr error
		for kind, value := range backups {
			restoreErr = errors.Join(restoreErr, s.credentials.Put(id, kind, value))
		}
		return errors.Join(err, restoreErr)
	}
	return nil
}

func credentialKindForAuth(mode transport.AuthMode) (credential.Kind, bool) {
	switch mode {
	case transport.AuthBasic:
		return credential.KindInfluxPassword, true
	case transport.AuthBearer:
		return credential.KindJWT, true
	default:
		return "", false
	}
}

func zeroSecret(value []byte) {
	for i := range value {
		value[i] = 0
	}
}
