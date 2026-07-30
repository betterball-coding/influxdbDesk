//go:build windows

package secure

import (
	"errors"
	"unsafe"

	"golang.org/x/sys/windows"
)

const cryptprotectUIForbidden = 0x1

func ProtectDEK(dek []byte) ([]byte, error) {
	if len(dek) != DEKSize {
		return nil, errors.New("invalid DEK length")
	}
	return protectData(dek)
}

func UnprotectDEK(wrapped []byte) ([]byte, error) {
	plaintext, err := unprotectData(wrapped)
	if err != nil {
		return nil, err
	}
	if len(plaintext) != DEKSize {
		zero(plaintext)
		return nil, errors.New("invalid unwrapped DEK length")
	}
	return plaintext, nil
}

func protectData(plaintext []byte) ([]byte, error) {
	in := bytesToBlob(plaintext)
	var out windows.DataBlob
	if err := windows.CryptProtectData(&in, nil, nil, 0, nil, cryptprotectUIForbidden, &out); err != nil {
		return nil, err
	}
	defer windows.LocalFree(windows.Handle(unsafe.Pointer(out.Data)))
	return append([]byte(nil), unsafe.Slice(out.Data, out.Size)...), nil
}

func unprotectData(ciphertext []byte) ([]byte, error) {
	if len(ciphertext) == 0 {
		return nil, errors.New("empty wrapped DEK")
	}
	in := bytesToBlob(ciphertext)
	var out windows.DataBlob
	if err := windows.CryptUnprotectData(&in, nil, nil, 0, nil, cryptprotectUIForbidden, &out); err != nil {
		return nil, err
	}
	defer windows.LocalFree(windows.Handle(unsafe.Pointer(out.Data)))
	return append([]byte(nil), unsafe.Slice(out.Data, out.Size)...), nil
}

func bytesToBlob(value []byte) windows.DataBlob {
	if len(value) == 0 {
		return windows.DataBlob{}
	}
	return windows.DataBlob{Size: uint32(len(value)), Data: &value[0]}
}

func zero(value []byte) {
	for i := range value {
		value[i] = 0
	}
}
