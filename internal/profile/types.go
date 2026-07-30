package profile

import (
	"errors"
	"math/big"
	"strings"

	"github.com/influxdesk/influxdesk/internal/credential"
	"github.com/influxdesk/influxdesk/internal/protection"
	"github.com/influxdesk/influxdesk/internal/transport"
)

type Environment string

const (
	EnvironmentProduction  Environment = "production"
	EnvironmentStaging     Environment = "staging"
	EnvironmentDevelopment Environment = "development"
)

var (
	ErrNotFound         = errors.New("PROFILE_NOT_FOUND")
	ErrRevisionConflict = errors.New("REVISION_CONFLICT")
	ErrInvalidProfile   = errors.New("INVALID_PROFILE")
	ErrProfileInUse     = errors.New("PROFILE_IN_USE")
)

type Profile struct {
	ID              string             `json:"id"`
	Revision        string             `json:"revision"`
	Name            string             `json:"name"`
	BaseURL         string             `json:"baseUrl"`
	DefaultDatabase string             `json:"defaultDatabase"`
	Environment     Environment        `json:"environment"`
	AuthMode        transport.AuthMode `json:"authMode"`
	Username        string             `json:"username,omitempty"`
	CredentialKind  *credential.Kind   `json:"-"`
	CredentialRef   *string            `json:"-"`
	ProtectionMode  protection.Mode    `json:"protectionMode"`
	CreatedAt       string             `json:"createdAt"`
	UpdatedAt       string             `json:"updatedAt"`
}

type SaveRequest struct {
	ID               string
	ExpectedRevision string
	Name             string
	BaseURL          string
	DefaultDatabase  string
	Environment      Environment
	AuthMode         transport.AuthMode
	Username         string
	ProtectionMode   protection.Mode
}

func canonicalDecimal(value string) (string, error) {
	if value == "" || strings.HasPrefix(value, "+") || (len(value) > 1 && value[0] == '0') {
		return "", ErrInvalidProfile
	}
	n := new(big.Int)
	if _, ok := n.SetString(value, 10); !ok || n.Sign() < 0 {
		return "", ErrInvalidProfile
	}
	return n.String(), nil
}

func incrementDecimal(value string) (string, error) {
	value, err := canonicalDecimal(value)
	if err != nil {
		return "", err
	}
	n := new(big.Int)
	n.SetString(value, 10)
	n.Add(n, big.NewInt(1))
	return n.String(), nil
}
