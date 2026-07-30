//go:build !windows

package secure

import "errors"

var ErrDPAPIUnavailable = errors.New("DPAPI CurrentUser is available only on Windows")

func ProtectDEK([]byte) ([]byte, error) {
	return nil, ErrDPAPIUnavailable
}

func UnprotectDEK([]byte) ([]byte, error) {
	return nil, ErrDPAPIUnavailable
}
