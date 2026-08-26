//go:build !darwin && !dragonfly && !freebsd && !linux && !netbsd && !openbsd && !windows

package journal

import "os"

func replaceFile(oldPath, newPath string) error {
	return os.Rename(oldPath, newPath)
}
