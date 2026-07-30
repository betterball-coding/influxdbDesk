package update

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"regexp"
	"strconv"
	"strings"
)

var (
	versionPattern = regexp.MustCompile(`^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$`)
	sha256Pattern  = regexp.MustCompile(`^[0-9a-f]{64}$`)
)

var (
	ErrInvalidSignature = errors.New("update manifest signature is invalid")
	ErrInvalidManifest  = errors.New("update manifest is invalid")
	ErrInvalidArtifact  = errors.New("update artifact is invalid")
)

// Manifest is parsed only after its exact source bytes pass signature verification.
type Manifest struct {
	SchemaVersion int    `json:"schemaVersion"`
	Platform      string `json:"platform"`
	Version       string `json:"version"`
	MSIURL        string `json:"msiUrl"`
	MSILength     int64  `json:"msiLength"`
	MSISHA256     string `json:"msiSha256"`
}

// VerifyManifest verifies the detached signature before parsing untrusted JSON.
func VerifyManifest(raw, signature []byte, publicKey ed25519.PublicKey, expectedPlatform, currentVersion string) (Manifest, error) {
	if len(publicKey) != ed25519.PublicKeySize || !ed25519.Verify(publicKey, raw, signature) {
		return Manifest{}, ErrInvalidSignature
	}

	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	var manifest Manifest
	if err := dec.Decode(&manifest); err != nil {
		return Manifest{}, fmt.Errorf("%w: decode", ErrInvalidManifest)
	}
	if err := requireJSONEOF(dec); err != nil {
		return Manifest{}, fmt.Errorf("%w: trailing content", ErrInvalidManifest)
	}
	if manifest.SchemaVersion != 1 || manifest.Platform != expectedPlatform {
		return Manifest{}, fmt.Errorf("%w: schema or platform", ErrInvalidManifest)
	}
	if !versionPattern.MatchString(manifest.Version) || !versionPattern.MatchString(currentVersion) {
		return Manifest{}, fmt.Errorf("%w: version", ErrInvalidManifest)
	}
	cmp, err := compareVersions(manifest.Version, currentVersion)
	if err != nil || cmp <= 0 {
		return Manifest{}, fmt.Errorf("%w: downgrade or same version", ErrInvalidManifest)
	}
	artifactURL, err := url.Parse(manifest.MSIURL)
	if err != nil || artifactURL.Scheme != "https" || artifactURL.Host == "" || artifactURL.User != nil {
		return Manifest{}, fmt.Errorf("%w: artifact URL", ErrInvalidManifest)
	}
	if manifest.MSILength <= 0 || !sha256Pattern.MatchString(manifest.MSISHA256) {
		return Manifest{}, fmt.Errorf("%w: artifact metadata", ErrInvalidManifest)
	}
	return manifest, nil
}

// VerifyArtifact streams an artifact and validates both its declared length and SHA-256.
func VerifyArtifact(reader io.Reader, manifest Manifest) error {
	hash := sha256.New()
	written, err := io.Copy(hash, reader)
	if err != nil {
		return fmt.Errorf("%w: read", ErrInvalidArtifact)
	}
	if written != manifest.MSILength {
		return fmt.Errorf("%w: length", ErrInvalidArtifact)
	}
	actual := hex.EncodeToString(hash.Sum(nil))
	if !strings.EqualFold(actual, manifest.MSISHA256) {
		return fmt.Errorf("%w: sha256", ErrInvalidArtifact)
	}
	return nil
}

func requireJSONEOF(dec *json.Decoder) error {
	var extra json.RawMessage
	err := dec.Decode(&extra)
	if errors.Is(err, io.EOF) {
		return nil
	}
	if err != nil {
		return err
	}
	return errors.New("additional JSON value")
}

func compareVersions(left, right string) (int, error) {
	lv, err := parseVersion(left)
	if err != nil {
		return 0, err
	}
	rv, err := parseVersion(right)
	if err != nil {
		return 0, err
	}
	for i := range lv {
		if lv[i] < rv[i] {
			return -1, nil
		}
		if lv[i] > rv[i] {
			return 1, nil
		}
	}
	return 0, nil
}

func parseVersion(value string) ([3]uint64, error) {
	if !versionPattern.MatchString(value) {
		return [3]uint64{}, ErrInvalidManifest
	}
	parts := strings.Split(value, ".")
	var result [3]uint64
	for i, part := range parts {
		n, err := strconv.ParseUint(part, 10, 32)
		if err != nil {
			return [3]uint64{}, err
		}
		result[i] = n
	}
	return result, nil
}
