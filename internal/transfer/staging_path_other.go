//go:build !windows

package transfer

import "runtime"

func sameStagingPath(left, right string) bool {
	return sameStagingPathForOS(runtime.GOOS, left, right)
}
