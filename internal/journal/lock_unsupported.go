//go:build !darwin && !dragonfly && !freebsd && !linux && !netbsd && !openbsd && !windows

package journal

import (
	"errors"
	"fmt"
	"os"
	"runtime"
)

var errLockUnsupported = errors.New("journal process locking is unsupported on this platform")

type directoryLock struct{}

func acquireDirectoryLock(file *os.File) (*directoryLock, error) {
	_ = file.Close()
	return nil, fmt.Errorf("%w: %s", errLockUnsupported, runtime.GOOS)
}

func (*directoryLock) Close() error { return nil }
