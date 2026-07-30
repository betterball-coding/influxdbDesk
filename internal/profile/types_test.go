package profile

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/influxdesk/influxdesk/internal/credential"
)

func TestProfileJSONOmitsCredentialMetadata(t *testing.T) {
	kind := credential.KindJWT
	reference := "InfluxDesk/profile/JWT"
	encoded, err := json.Marshal(Profile{CredentialKind: &kind, CredentialRef: &reference})
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range [][]byte{[]byte("credentialKind"), []byte("credentialRef"), []byte(reference)} {
		if bytes.Contains(encoded, forbidden) {
			t.Fatalf("credential metadata crossed JSON boundary: %s", encoded)
		}
	}
}
