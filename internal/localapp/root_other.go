//go:build !windows && !darwin

package localapp

import "errors"

var ErrUnsupportedPlatform = errors.New("InfluxDesk production storage is supported only on Windows")

func PreparePrivateRoot() (string, error) {
	return "", ErrUnsupportedPlatform
}
