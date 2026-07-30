package query

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"time"
)

var ErrInvalidCursor = errors.New("invalid or expired result cursor")

type cursorCodec struct {
	key []byte
}

type cursorPayload struct {
	Version     int    `json:"v"`
	SessionID   string `json:"sid"`
	Generation  string `json:"g"`
	StatementID int    `json:"st"`
	SeriesID    string `json:"se"`
	NextRow     string `json:"n"`
	ExpiresUnix int64  `json:"exp"`
}

func (c cursorCodec) encode(payload cursorPayload) (string, error) {
	encoded, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	signature := c.sign(encoded)
	return base64.RawURLEncoding.EncodeToString(encoded) + "." + base64.RawURLEncoding.EncodeToString(signature), nil
}

func (c cursorCodec) decode(value string, now time.Time) (cursorPayload, error) {
	var payload cursorPayload
	parts := strings.Split(value, ".")
	if len(parts) != 2 {
		return payload, ErrInvalidCursor
	}
	encoded, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return payload, ErrInvalidCursor
	}
	signature, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil || !hmac.Equal(signature, c.sign(encoded)) {
		return payload, ErrInvalidCursor
	}
	if err := json.Unmarshal(encoded, &payload); err != nil {
		return payload, ErrInvalidCursor
	}
	if payload.Version != 1 || payload.ExpiresUnix <= now.Unix() {
		return cursorPayload{}, ErrInvalidCursor
	}
	if _, err := strconv.ParseUint(payload.NextRow, 10, 64); err != nil {
		return cursorPayload{}, ErrInvalidCursor
	}
	return payload, nil
}

func (c cursorCodec) sign(payload []byte) []byte {
	mac := hmac.New(sha256.New, c.key)
	_, _ = mac.Write(payload)
	return mac.Sum(nil)
}
