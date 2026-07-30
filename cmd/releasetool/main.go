package main

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	updatepkg "github.com/influxdesk/influxdesk/internal/update"
)

func main() {
	if len(os.Args) < 2 {
		fail(errors.New("expected create or verify"))
	}
	var err error
	switch os.Args[1] {
	case "create":
		err = create(os.Args[2:])
	case "verify":
		err = verify(os.Args[2:])
	default:
		err = errors.New("expected create or verify")
	}
	if err != nil {
		fail(err)
	}
}

func create(args []string) error {
	flags := flag.NewFlagSet("create", flag.ContinueOnError)
	version := flags.String("version", "", "three-part release version")
	msiPath := flags.String("msi", "", "signed MSI path")
	msiURL := flags.String("url", "", "HTTPS MSI URL")
	outputDir := flags.String("out", "release", "output directory")
	if err := flags.Parse(args); err != nil {
		return err
	}
	privateKey, err := parsePrivateKey(os.Getenv("UPDATE_PRIVATE_KEY_BASE64"))
	if err != nil {
		return err
	}
	payload, err := os.ReadFile(*msiPath)
	if err != nil {
		return err
	}
	digest := sha256.Sum256(payload)
	manifest := updatepkg.Manifest{
		SchemaVersion: 1,
		Platform:      "windows-x64",
		Version:       *version,
		MSIURL:        *msiURL,
		MSILength:     int64(len(payload)),
		MSISHA256:     hex.EncodeToString(digest[:]),
	}
	raw, err := json.Marshal(manifest)
	if err != nil {
		return err
	}
	signature := ed25519.Sign(privateKey, raw)
	publicKey := privateKey.Public().(ed25519.PublicKey)
	if _, err := updatepkg.VerifyManifest(raw, signature, publicKey, "windows-x64", "0.0.0"); err != nil {
		return fmt.Errorf("self-verify manifest: %w", err)
	}
	if err := os.MkdirAll(*outputDir, 0o700); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(*outputDir, "manifest.json"), raw, 0o600); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(*outputDir, "manifest.sig"), signature, 0o600)
}

func verify(args []string) error {
	flags := flag.NewFlagSet("verify", flag.ContinueOnError)
	current := flags.String("current", "0.0.0", "installed version")
	manifestPath := flags.String("manifest", "", "manifest path")
	signaturePath := flags.String("signature", "", "signature path")
	msiPath := flags.String("msi", "", "MSI path")
	publicKeyText := flags.String("public-key", "", "base64 Ed25519 public key")
	if err := flags.Parse(args); err != nil {
		return err
	}
	publicKeyBytes, err := base64.StdEncoding.DecodeString(*publicKeyText)
	if err != nil || len(publicKeyBytes) != ed25519.PublicKeySize {
		return errors.New("invalid public key")
	}
	raw, err := os.ReadFile(*manifestPath)
	if err != nil {
		return err
	}
	signature, err := os.ReadFile(*signaturePath)
	if err != nil {
		return err
	}
	manifest, err := updatepkg.VerifyManifest(raw, signature, ed25519.PublicKey(publicKeyBytes), "windows-x64", *current)
	if err != nil {
		return err
	}
	artifact, err := os.Open(*msiPath)
	if err != nil {
		return err
	}
	defer artifact.Close()
	return updatepkg.VerifyArtifact(artifact, manifest)
}

func parsePrivateKey(value string) (ed25519.PrivateKey, error) {
	decoded, err := base64.StdEncoding.DecodeString(value)
	if err != nil {
		return nil, errors.New("invalid private key encoding")
	}
	switch len(decoded) {
	case ed25519.SeedSize:
		return ed25519.NewKeyFromSeed(decoded), nil
	case ed25519.PrivateKeySize:
		return ed25519.PrivateKey(decoded), nil
	default:
		return nil, errors.New("invalid private key size")
	}
}

func fail(err error) {
	_, _ = fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
