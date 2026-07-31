package profile

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/influxdesk/influxdesk/internal/protection"
	"github.com/influxdesk/influxdesk/internal/store"
	"github.com/influxdesk/influxdesk/internal/transport"
)

func TestProfileRevisionAndGenerationAreMonotonic(t *testing.T) {
	ctx := context.Background()
	database, err := store.Open(ctx, filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	repo := NewRepository(database, func() time.Time { return time.Unix(100, 0) })

	created, err := repo.Save(ctx, SaveRequest{
		Name: "Production", BaseURL: "https://influx.example.test/base",
		DefaultDatabase: " telemetry ",
		Environment:     EnvironmentProduction, AuthMode: transport.AuthBasic,
		Username: "operator", ProtectionMode: protection.ProtectedLocked,
	})
	if err != nil {
		t.Fatal(err)
	}
	if created.Revision != "1" || created.CredentialRef == nil || created.DefaultDatabase != "telemetry" {
		t.Fatalf("unexpected created profile: %+v", created)
	}

	updated, err := repo.Save(ctx, SaveRequest{
		ID: created.ID, ExpectedRevision: "1", Name: "Production East",
		BaseURL: created.BaseURL, DefaultDatabase: "operations", Environment: created.Environment,
		AuthMode: created.AuthMode, Username: created.Username, ProtectionMode: created.ProtectionMode,
	})
	if err != nil || updated.Revision != "2" || updated.DefaultDatabase != "operations" {
		t.Fatalf("update failed: profile=%+v err=%v", updated, err)
	}
	loaded, err := repo.Get(ctx, created.ID)
	if err != nil || loaded.DefaultDatabase != "operations" {
		t.Fatalf("loaded profile=%+v err=%v", loaded, err)
	}
	if _, err := repo.Save(ctx, SaveRequest{
		ID: created.ID, ExpectedRevision: "1", Name: "stale", BaseURL: created.BaseURL,
		Environment: created.Environment, AuthMode: created.AuthMode, Username: created.Username,
		ProtectionMode: created.ProtectionMode,
	}); !errors.Is(err, ErrRevisionConflict) {
		t.Fatalf("expected revision conflict, got %v", err)
	}

	first, err := repo.NextGeneration(ctx, created.ID)
	if err != nil {
		t.Fatal(err)
	}
	second, err := repo.NextGeneration(ctx, created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if first != "1" || second != "2" {
		t.Fatalf("unexpected generations %s, %s", first, second)
	}
	if err := repo.Delete(ctx, created.ID, "2"); err != nil {
		t.Fatal(err)
	}
	var last string
	if err := database.DB().QueryRowContext(ctx, `SELECT last_generation FROM connection_generation_counters WHERE profile_id=?`, created.ID).Scan(&last); err != nil || last != "2" {
		t.Fatalf("generation tombstone was lost: last=%s err=%v", last, err)
	}
}

func TestProfileRequiresAndPersistsAuthenticatedHTTPConsent(t *testing.T) {
	ctx := context.Background()
	database, err := store.Open(ctx, filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	repo := NewRepository(database, time.Now)
	request := SaveRequest{
		Name: "LAN", BaseURL: "http://192.0.2.10:8086", Environment: EnvironmentDevelopment,
		AuthMode: transport.AuthBasic, Username: "operator", ProtectionMode: protection.ProtectedLocked,
	}
	if _, err := repo.Save(ctx, request); !errors.Is(err, ErrInvalidProfile) {
		t.Fatalf("save without consent error = %v, want ErrInvalidProfile", err)
	}
	request.AllowInsecureAuth = true
	created, err := repo.Save(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := repo.Get(ctx, created.ID)
	if err != nil || !loaded.AllowInsecureAuth {
		t.Fatalf("loaded profile = %+v, error = %v", loaded, err)
	}
}
