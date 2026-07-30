package update

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"testing"
)

func TestVerifyManifestAuthenticatesRawBytesBeforeParsing(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	raw := []byte(`{"schemaVersion":1,"platform":"windows-x64","version":"1.2.0","msiUrl":"https://updates.example.test/InfluxDesk.msi","msiLength":3,"msiSha256":"ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"}`)
	signature := ed25519.Sign(privateKey, raw)

	manifest, err := VerifyManifest(raw, signature, publicKey, "windows-x64", "1.1.9")
	if err != nil {
		t.Fatal(err)
	}
	if manifest.Version != "1.2.0" {
		t.Fatalf("unexpected version %q", manifest.Version)
	}

	tampered := append([]byte(nil), raw...)
	tampered[len(tampered)-2] ^= 1
	if _, err := VerifyManifest(tampered, signature, publicKey, "windows-x64", "1.1.9"); !errors.Is(err, ErrInvalidSignature) {
		t.Fatalf("expected signature error, got %v", err)
	}
}

func TestVerifyManifestRejectsUnsafeRelease(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	cases := []string{
		`{"schemaVersion":1,"platform":"windows-x64","version":"1.0.0","msiUrl":"https://updates.example.test/a.msi","msiLength":1,"msiSha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}`,
		`{"schemaVersion":1,"platform":"windows-x64","version":"1.0.1","msiUrl":"http://updates.example.test/a.msi","msiLength":1,"msiSha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}`,
	}
	for _, rawText := range cases {
		raw := []byte(rawText)
		_, err := VerifyManifest(raw, ed25519.Sign(privateKey, raw), publicKey, "windows-x64", "1.0.0")
		if !errors.Is(err, ErrInvalidManifest) {
			t.Fatalf("expected invalid manifest for %s, got %v", rawText, err)
		}
	}
}

func TestVerifyArtifact(t *testing.T) {
	payload := []byte("abc")
	digest := sha256.Sum256(payload)
	manifest := Manifest{MSILength: int64(len(payload)), MSISHA256: hex.EncodeToString(digest[:])}
	if err := VerifyArtifact(bytes.NewReader(payload), manifest); err != nil {
		t.Fatal(err)
	}
	if err := VerifyArtifact(bytes.NewReader([]byte("abd")), manifest); !errors.Is(err, ErrInvalidArtifact) {
		t.Fatalf("expected artifact error, got %v", err)
	}
}
