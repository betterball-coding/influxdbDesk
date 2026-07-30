//go:build windows

package credential

import (
	"errors"
	"runtime"
	"syscall"
	"unsafe"

	"golang.org/x/sys/windows"
)

const (
	credTypeGeneric         = 1
	credPersistLocalMachine = 2
	credentialBlobMaxBytes  = 2560
)

var (
	advapi32       = windows.NewLazySystemDLL("advapi32.dll")
	procCredWrite  = advapi32.NewProc("CredWriteW")
	procCredRead   = advapi32.NewProc("CredReadW")
	procCredDelete = advapi32.NewProc("CredDeleteW")
	procCredFree   = advapi32.NewProc("CredFree")
)

type credentialW struct {
	Flags              uint32
	Type               uint32
	TargetName         *uint16
	Comment            *uint16
	LastWritten        windows.Filetime
	CredentialBlobSize uint32
	CredentialBlob     *byte
	Persist            uint32
	AttributeCount     uint32
	Attributes         uintptr
	TargetAlias        *uint16
	UserName           *uint16
}

type Store struct{}

func NewStore() *Store { return &Store{} }

func (s *Store) Put(profileID string, kind Kind, secret []byte) error {
	target, err := Key(profileID, kind)
	if err != nil {
		return err
	}
	if len(secret) == 0 || len(secret) > credentialBlobMaxBytes {
		return errors.New("credential value has invalid size")
	}
	targetUTF16, err := windows.UTF16PtrFromString(target)
	if err != nil {
		return err
	}
	copyOfSecret := append([]byte(nil), secret...)
	defer zeroBytes(copyOfSecret)
	credential := credentialW{
		Type:               credTypeGeneric,
		TargetName:         targetUTF16,
		CredentialBlobSize: uint32(len(copyOfSecret)),
		CredentialBlob:     &copyOfSecret[0],
		Persist:            credPersistLocalMachine,
	}
	result, _, callErr := procCredWrite.Call(uintptr(unsafe.Pointer(&credential)), 0)
	runtime.KeepAlive(targetUTF16)
	runtime.KeepAlive(copyOfSecret)
	if result == 0 {
		return normalizeCallError(callErr)
	}
	return nil
}

func (s *Store) Get(profileID string, kind Kind) ([]byte, error) {
	target, err := Key(profileID, kind)
	if err != nil {
		return nil, err
	}
	targetUTF16, err := windows.UTF16PtrFromString(target)
	if err != nil {
		return nil, err
	}
	var credential *credentialW
	result, _, callErr := procCredRead.Call(
		uintptr(unsafe.Pointer(targetUTF16)),
		credTypeGeneric,
		0,
		uintptr(unsafe.Pointer(&credential)),
	)
	runtime.KeepAlive(targetUTF16)
	if result == 0 {
		err := normalizeCallError(callErr)
		if errors.Is(err, windows.ERROR_NOT_FOUND) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	defer procCredFree.Call(uintptr(unsafe.Pointer(credential)))
	if credential == nil || credential.CredentialBlobSize == 0 || credential.CredentialBlob == nil {
		return nil, ErrNotFound
	}
	return append([]byte(nil), unsafe.Slice(credential.CredentialBlob, credential.CredentialBlobSize)...), nil
}

func (s *Store) Delete(profileID string, kind Kind) error {
	target, err := Key(profileID, kind)
	if err != nil {
		return err
	}
	targetUTF16, err := windows.UTF16PtrFromString(target)
	if err != nil {
		return err
	}
	result, _, callErr := procCredDelete.Call(uintptr(unsafe.Pointer(targetUTF16)), credTypeGeneric, 0)
	runtime.KeepAlive(targetUTF16)
	if result == 0 {
		err := normalizeCallError(callErr)
		if errors.Is(err, windows.ERROR_NOT_FOUND) {
			return nil
		}
		return err
	}
	return nil
}

// DeleteProfile removes every credential kind before a profile row is deleted.
func (s *Store) DeleteProfile(profileID string) error {
	for _, kind := range supportedKinds {
		if err := s.Delete(profileID, kind); err != nil {
			return err
		}
	}
	return nil
}

func normalizeCallError(err error) error {
	if err == nil || errors.Is(err, syscall.Errno(0)) {
		return syscall.EINVAL
	}
	return err
}

func zeroBytes(value []byte) {
	for i := range value {
		value[i] = 0
	}
}
