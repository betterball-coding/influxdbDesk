package update

import (
	"crypto/ed25519"
	"encoding/base64"
	"errors"
)

// EmbeddedChannelPublicKeyBase64 is injected by the signed release build.
// It is never read from the update server or manifest.
var EmbeddedChannelPublicKeyBase64 string

func EmbeddedChannelPublicKey() (ed25519.PublicKey, error) {
	decoded, err := base64.StdEncoding.DecodeString(EmbeddedChannelPublicKeyBase64)
	if err != nil || len(decoded) != ed25519.PublicKeySize {
		return nil, errors.New("embedded update channel key is unavailable")
	}
	return ed25519.PublicKey(decoded), nil
}
