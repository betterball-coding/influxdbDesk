package profile

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/influxdesk/influxdesk/internal/credential"
	"github.com/influxdesk/influxdesk/internal/protection"
	"github.com/influxdesk/influxdesk/internal/store"
	"github.com/influxdesk/influxdesk/internal/transport"
)

type memoryCredentials struct {
	values            map[string][]byte
	failPut           bool
	failDeleteProfile bool
}

func newMemoryCredentials() *memoryCredentials {
	return &memoryCredentials{values: make(map[string][]byte)}
}

func (m *memoryCredentials) Put(id string, kind credential.Kind, value []byte) error {
	if m.failPut {
		return errors.New("credential write failed")
	}
	m.values[id+"/"+string(kind)] = append([]byte(nil), value...)
	return nil
}

func (m *memoryCredentials) Get(id string, kind credential.Kind) ([]byte, error) {
	value := m.values[id+"/"+string(kind)]
	if value == nil {
		return nil, credential.ErrNotFound
	}
	return append([]byte(nil), value...), nil
}

func (m *memoryCredentials) Delete(id string, kind credential.Kind) error {
	delete(m.values, id+"/"+string(kind))
	return nil
}

func (m *memoryCredentials) DeleteProfile(id string) error {
	for key := range m.values {
		if len(key) > len(id) && key[:len(id)+1] == id+"/" {
			delete(m.values, key)
		}
	}
	if m.failDeleteProfile {
		return errors.New("credential delete failed")
	}
	return nil
}

func TestServiceNeverPersistsSecretAndDeletesCredential(t *testing.T) {
	ctx := context.Background()
	database, err := store.Open(ctx, filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	credentials := newMemoryCredentials()
	service := NewService(NewRepository(database, nil), credentials)
	secret := "database-password-canary"
	created, err := service.Save(ctx, SaveCommand{Profile: SaveRequest{
		Name: "production", BaseURL: "https://influx.example.test",
		Environment: EnvironmentProduction, AuthMode: transport.AuthBasic,
		Username: "operator", ProtectionMode: protection.ProtectedLocked,
	}, Secret: &secret})
	if err != nil {
		t.Fatal(err)
	}
	var matches int
	if err := database.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM profiles
		WHERE CAST(id AS TEXT) LIKE ? OR CAST(name AS TEXT) LIKE ? OR CAST(base_url AS TEXT) LIKE ?
		OR CAST(username AS TEXT) LIKE ? OR CAST(credential_ref AS TEXT) LIKE ?`,
		"%"+secret+"%", "%"+secret+"%", "%"+secret+"%", "%"+secret+"%", "%"+secret+"%").Scan(&matches); err != nil {
		t.Fatal(err)
	}
	if matches != 0 {
		t.Fatal("secret was persisted in profile storage")
	}
	if err := service.Delete(ctx, created.ID, created.Revision); err != nil {
		t.Fatal(err)
	}
	if len(credentials.values) != 0 {
		t.Fatalf("credentials survived profile deletion: %v", credentials.values)
	}
}

func TestCredentialFailureRejectsProfileCreate(t *testing.T) {
	ctx := context.Background()
	database, err := store.Open(ctx, filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	credentials := newMemoryCredentials()
	credentials.failPut = true
	service := NewService(NewRepository(database, nil), credentials)
	secret := "secret"
	_, err = service.Save(ctx, SaveCommand{Profile: SaveRequest{
		Name: "rejected", BaseURL: "https://influx.example.test",
		Environment: EnvironmentProduction, AuthMode: transport.AuthBearer,
		ProtectionMode: protection.ProtectedLocked,
	}, Secret: &secret})
	if err == nil {
		t.Fatal("expected credential failure")
	}
	profiles, listErr := service.List(ctx)
	if listErr != nil || len(profiles) != 0 {
		t.Fatalf("profile persisted after credential failure: %+v, %v", profiles, listErr)
	}
}

func TestCredentialCleanupFailureKeepsProfileAndRestoresCredential(t *testing.T) {
	ctx := context.Background()
	database, err := store.Open(ctx, filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	credentials := newMemoryCredentials()
	service := NewService(NewRepository(database, nil), credentials)
	secret := "delete-rollback-canary"
	created, err := service.Save(ctx, SaveCommand{Profile: SaveRequest{
		Name: "rollback", BaseURL: "https://influx.example.test", DefaultDatabase: "metrics",
		Environment: EnvironmentProduction, AuthMode: transport.AuthBasic,
		Username: "operator", ProtectionMode: protection.ProtectedLocked,
	}, Secret: &secret})
	if err != nil {
		t.Fatal(err)
	}
	credentials.failDeleteProfile = true
	if err := service.Delete(ctx, created.ID, created.Revision); err == nil {
		t.Fatal("credential cleanup failure was reported as success")
	}
	loaded, err := service.Get(ctx, created.ID)
	if err != nil || loaded.DefaultDatabase != "metrics" {
		t.Fatalf("profile was lost after failed delete: %+v err=%v", loaded, err)
	}
	stored := credentials.values[created.ID+"/"+string(credential.KindInfluxPassword)]
	if string(stored) != secret {
		t.Fatalf("credential was not restored after failed delete: %q", stored)
	}
}
