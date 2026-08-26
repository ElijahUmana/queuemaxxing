//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd

package journal

import "os"

func syncDirectory(path string) error {
	// #nosec G304,G703 -- path is the operator-selected store root or one of its fixed subdirectories.
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}
