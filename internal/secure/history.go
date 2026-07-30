package secure

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

const DEKSize = 32

var ErrInvalidCiphertext = errors.New("invalid encrypted history record")

type HistoryAAD struct {
	SchemaVersion uint32
	RowID         string
	ProfileID     string
}

func GenerateDEK() ([]byte, error) {
	dek := make([]byte, DEKSize)
	if _, err := io.ReadFull(rand.Reader, dek); err != nil {
		return nil, err
	}
	return dek, nil
}

// SealHistory uses a fresh nonce for every row and binds the row identity as AAD.
func SealHistory(dek, plaintext []byte, aad HistoryAAD) ([]byte, error) {
	gcm, err := newGCM(dek)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}
	sealed := gcm.Seal(nil, nonce, plaintext, encodeAAD(aad))
	return append(nonce, sealed...), nil
}

func OpenHistory(dek, ciphertext []byte, aad HistoryAAD) ([]byte, error) {
	gcm, err := newGCM(dek)
	if err != nil {
		return nil, err
	}
	if len(ciphertext) < gcm.NonceSize()+gcm.Overhead() {
		return nil, ErrInvalidCiphertext
	}
	plaintext, err := gcm.Open(nil, ciphertext[:gcm.NonceSize()], ciphertext[gcm.NonceSize():], encodeAAD(aad))
	if err != nil {
		return nil, ErrInvalidCiphertext
	}
	return plaintext, nil
}

func newGCM(dek []byte) (cipher.AEAD, error) {
	if len(dek) != DEKSize {
		return nil, fmt.Errorf("DEK must be %d bytes", DEKSize)
	}
	block, err := aes.NewCipher(dek)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}

func encodeAAD(aad HistoryAAD) []byte {
	result := make([]byte, 4, 4+8+len(aad.RowID)+len(aad.ProfileID))
	binary.BigEndian.PutUint32(result, aad.SchemaVersion)
	result = appendLengthPrefixed(result, aad.RowID)
	result = appendLengthPrefixed(result, aad.ProfileID)
	return result
}

func appendLengthPrefixed(dst []byte, value string) []byte {
	var length [4]byte
	binary.BigEndian.PutUint32(length[:], uint32(len(value)))
	dst = append(dst, length[:]...)
	return append(dst, value...)
}
