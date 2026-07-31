//go:build darwin && cgo

package credential

/*
#cgo LDFLAGS: -framework CoreFoundation -framework Security
#include <CoreFoundation/CoreFoundation.h>
#include <Security/Security.h>
#include <stdlib.h>
#include <string.h>

static void influxdesk_secure_free(void *value, size_t length) {
    if (value != NULL) {
        volatile unsigned char *cursor = (volatile unsigned char *)value;
        while (length-- > 0) {
            *cursor++ = 0;
        }
        free(value);
    }
}

static OSStatus influxdesk_keychain_put(
    const char *service,
    const char *account,
    const void *secret,
    UInt32 secretLength
) {
    SecKeychainItemRef item = NULL;
    void *existing = NULL;
    UInt32 existingLength = 0;
    OSStatus status = SecKeychainFindGenericPassword(
        NULL,
        (UInt32)strlen(service), service,
        (UInt32)strlen(account), account,
        &existingLength, &existing, &item
    );
    if (status == errSecSuccess) {
        if (existing != NULL) {
            SecKeychainItemFreeContent(NULL, existing);
        }
        status = SecKeychainItemModifyAttributesAndData(item, NULL, secretLength, secret);
        if (item != NULL) {
            CFRelease(item);
        }
        return status;
    }
    if (item != NULL) {
        CFRelease(item);
    }
    if (status != errSecItemNotFound) {
        return status;
    }
    return SecKeychainAddGenericPassword(
        NULL,
        (UInt32)strlen(service), service,
        (UInt32)strlen(account), account,
        secretLength, secret,
        NULL
    );
}

static OSStatus influxdesk_keychain_get(
    const char *service,
    const char *account,
    void **secret,
    UInt32 *secretLength
) {
    return SecKeychainFindGenericPassword(
        NULL,
        (UInt32)strlen(service), service,
        (UInt32)strlen(account), account,
        secretLength, secret, NULL
    );
}

static void influxdesk_keychain_free(void *secret) {
    if (secret != NULL) {
        SecKeychainItemFreeContent(NULL, secret);
    }
}

static OSStatus influxdesk_keychain_delete(const char *service, const char *account) {
    SecKeychainItemRef item = NULL;
    OSStatus status = SecKeychainFindGenericPassword(
        NULL,
        (UInt32)strlen(service), service,
        (UInt32)strlen(account), account,
        NULL, NULL, &item
    );
    if (status != errSecSuccess) {
        if (item != NULL) {
            CFRelease(item);
        }
        return status;
    }
    status = SecKeychainItemDelete(item);
    if (item != NULL) {
        CFRelease(item);
    }
    return status;
}
*/
import "C"

import (
	"errors"
	"fmt"
	"unsafe"
)

const (
	darwinKeychainService  = "com.betterballcoding.influxdesk"
	darwinCredentialMaxLen = 2560
	darwinErrItemNotFound  = -25300
)

type Store struct{}

func NewStore() *Store { return &Store{} }

func (*Store) Put(profileID string, kind Kind, secret []byte) error {
	account, err := Key(profileID, kind)
	if err != nil {
		return err
	}
	if len(secret) == 0 || len(secret) > darwinCredentialMaxLen {
		return errors.New("credential value has invalid size")
	}
	serviceCString := C.CString(darwinKeychainService)
	accountCString := C.CString(account)
	secretBytes := C.CBytes(secret)
	defer C.free(unsafe.Pointer(serviceCString))
	defer C.free(unsafe.Pointer(accountCString))
	defer C.influxdesk_secure_free(secretBytes, C.size_t(len(secret)))
	return darwinKeychainError(C.influxdesk_keychain_put(
		serviceCString,
		accountCString,
		secretBytes,
		C.UInt32(len(secret)),
	))
}

func (*Store) Get(profileID string, kind Kind) ([]byte, error) {
	account, err := Key(profileID, kind)
	if err != nil {
		return nil, err
	}
	serviceCString := C.CString(darwinKeychainService)
	accountCString := C.CString(account)
	defer C.free(unsafe.Pointer(serviceCString))
	defer C.free(unsafe.Pointer(accountCString))

	var secret unsafe.Pointer
	var secretLength C.UInt32
	status := C.influxdesk_keychain_get(serviceCString, accountCString, &secret, &secretLength)
	if int32(status) == darwinErrItemNotFound {
		return nil, ErrNotFound
	}
	if err := darwinKeychainError(status); err != nil {
		return nil, err
	}
	defer C.influxdesk_keychain_free(secret)
	if secret == nil || secretLength == 0 || uint64(secretLength) > darwinCredentialMaxLen {
		return nil, ErrNotFound
	}
	return C.GoBytes(secret, C.int(secretLength)), nil
}

func (*Store) Delete(profileID string, kind Kind) error {
	account, err := Key(profileID, kind)
	if err != nil {
		return err
	}
	serviceCString := C.CString(darwinKeychainService)
	accountCString := C.CString(account)
	defer C.free(unsafe.Pointer(serviceCString))
	defer C.free(unsafe.Pointer(accountCString))
	status := C.influxdesk_keychain_delete(serviceCString, accountCString)
	if int32(status) == darwinErrItemNotFound {
		return nil
	}
	return darwinKeychainError(status)
}

func (s *Store) DeleteProfile(profileID string) error {
	for _, kind := range supportedKinds {
		if err := s.Delete(profileID, kind); err != nil {
			return err
		}
	}
	return nil
}

func darwinKeychainError(status C.OSStatus) error {
	if status == C.errSecSuccess {
		return nil
	}
	return fmt.Errorf("macOS Keychain operation failed (OSStatus %d)", int32(status))
}
