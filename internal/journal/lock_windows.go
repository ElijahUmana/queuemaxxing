//go:build windows

package journal

import (
	"errors"
	"fmt"
	"os"
	"syscall"
	"unsafe"
)

const (
	lockfileFailImmediately = 0x00000001
	lockfileExclusiveLock   = 0x00000002
	errorLockViolation      = syscall.Errno(33)
)

var (
	kernel32         = syscall.NewLazyDLL("kernel32.dll")
	procLockFileEx   = kernel32.NewProc("LockFileEx")
	procUnlockFileEx = kernel32.NewProc("UnlockFileEx")
)

type directoryLock struct {
	file *os.File
}

func acquireDirectoryLock(file *os.File) (*directoryLock, error) {
	var overlapped syscall.Overlapped
	result, _, callErr := procLockFileEx.Call(
		file.Fd(),
		lockfileExclusiveLock|lockfileFailImmediately,
		0,
		1,
		0,
		uintptr(unsafe.Pointer(&overlapped)),
	)
	if result == 0 {
		_ = file.Close()
		if errors.Is(callErr, errorLockViolation) {
			return nil, ErrLocked
		}
		return nil, fmt.Errorf("acquire journal lock: %w", callErr)
	}
	return &directoryLock{file: file}, nil
}

func (lock *directoryLock) Close() error {
	if lock == nil || lock.file == nil {
		return nil
	}
	var overlapped syscall.Overlapped
	result, _, callErr := procUnlockFileEx.Call(lock.file.Fd(), 0, 1, 0, uintptr(unsafe.Pointer(&overlapped)))
	var err error
	if result == 0 {
		err = callErr
	}
	err = errors.Join(err, lock.file.Close())
	lock.file = nil
	return err
}
