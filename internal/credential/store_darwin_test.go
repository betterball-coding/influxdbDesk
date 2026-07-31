//go:build darwin && cgo

package credential

import (
	"errors"
	"os"
	"testing"

	"github.com/google/uuid"
)

func TestDarwinKeychainRoundTrip(t *testing.T) {
	if os.Getenv("INFLUXDESK_MACOS_KEYCHAIN_TEST") != "1" {
		t.Skip("set INFLUXDESK_MACOS_KEYCHAIN_TEST=1 to exercise the login keychain")
	}
	store := NewStore()
	profileID := uuid.NewString()
	defer func() {
		if err := store.DeleteProfile(profileID); err != nil {
			t.Errorf("cleanup keychain item: %v", err)
		}
	}()

	if err := store.Put(profileID, KindInfluxPassword, []byte("first-secret")); err != nil {
		t.Fatal(err)
	}
	value, err := store.Get(profileID, KindInfluxPassword)
	if err != nil {
		t.Fatal(err)
	}
	if string(value) != "first-secret" {
		t.Fatal("keychain returned a different value")
	}
	if err := store.Put(profileID, KindInfluxPassword, []byte("updated-secret")); err != nil {
		t.Fatal(err)
	}
	value, err = store.Get(profileID, KindInfluxPassword)
	if err != nil || string(value) != "updated-secret" {
		t.Fatalf("updated keychain value mismatch: %v", err)
	}
	if err := store.Delete(profileID, KindInfluxPassword); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Get(profileID, KindInfluxPassword); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}
