package secure

import (
	"bytes"
	"testing"
)

func TestHistoryEncryptionBindsRowAndProfile(t *testing.T) {
	dek, err := GenerateDEK()
	if err != nil {
		t.Fatal(err)
	}
	aad := HistoryAAD{SchemaVersion: 1, RowID: "row-1", ProfileID: "profile-1"}
	plaintext := []byte("SELECT secret FROM measurement")
	first, err := SealHistory(dek, plaintext, aad)
	if err != nil {
		t.Fatal(err)
	}
	second, err := SealHistory(dek, plaintext, aad)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(first, second) {
		t.Fatal("per-row nonce was reused")
	}
	opened, err := OpenHistory(dek, first, aad)
	if err != nil || !bytes.Equal(opened, plaintext) {
		t.Fatalf("round trip failed: %v", err)
	}
	aad.RowID = "row-2"
	if _, err := OpenHistory(dek, first, aad); err != ErrInvalidCiphertext {
		t.Fatalf("expected AAD failure, got %v", err)
	}
}

func TestHistoryEncryptionRejectsWrongKeyAndTampering(t *testing.T) {
	dek, _ := GenerateDEK()
	other, _ := GenerateDEK()
	aad := HistoryAAD{SchemaVersion: 1, RowID: "row", ProfileID: "profile"}
	ciphertext, err := SealHistory(dek, []byte("query"), aad)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := OpenHistory(other, ciphertext, aad); err != ErrInvalidCiphertext {
		t.Fatalf("expected wrong key failure, got %v", err)
	}
	ciphertext[len(ciphertext)-1] ^= 1
	if _, err := OpenHistory(dek, ciphertext, aad); err != ErrInvalidCiphertext {
		t.Fatalf("expected tamper failure, got %v", err)
	}
}
