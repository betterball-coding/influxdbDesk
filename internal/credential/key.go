package credential

import (
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
)

type Kind string

const (
	KindInfluxPassword Kind = "influx-password"
	KindJWT            Kind = "jwt"
	KindSSHPassword    Kind = "ssh-password"
	KindSSHKeyPass     Kind = "ssh-key-passphrase"
	KindMTLSKeyPass    Kind = "mtls-key-passphrase"
	KindProxyPassword  Kind = "proxy-password"
)

var (
	ErrInvalidReference = errors.New("invalid credential reference")
	ErrNotFound         = errors.New("credential not found")
	ErrUnavailable      = errors.New("Windows Credential Manager is unavailable")
)

var supportedKinds = []Kind{
	KindInfluxPassword,
	KindJWT,
	KindSSHPassword,
	KindSSHKeyPass,
	KindMTLSKeyPass,
	KindProxyPassword,
}

func Key(profileID string, kind Kind) (string, error) {
	parsed, err := uuid.Parse(profileID)
	if err != nil || parsed == uuid.Nil || !isSupportedKind(kind) {
		return "", ErrInvalidReference
	}
	return fmt.Sprintf("InfluxDesk/%s/%s", strings.ToLower(parsed.String()), kind), nil
}

func isSupportedKind(kind Kind) bool {
	for _, candidate := range supportedKinds {
		if candidate == kind {
			return true
		}
	}
	return false
}
