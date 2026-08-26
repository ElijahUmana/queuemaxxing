//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd

package journal

import (
	"errors"
	"fmt"
	"os"
	"syscall"
)

type directoryLock struct {
	file *os.File
}

func acquireDirectoryLock(path string) (*directoryLock, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open journal lock: %w", err)
	}
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = file.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, ErrLocked
		}
		return nil, fmt.Errorf("acquire journal lock: %w", err)
	}
	return &directoryLock{file: file}, nil
}

func (lock *directoryLock) Close() error {
	if lock == nil || lock.file == nil {
		return nil
	}
	err := syscall.Flock(int(lock.file.Fd()), syscall.LOCK_UN)
	err = errors.Join(err, lock.file.Close())
	lock.file = nil
	return err
}
