//go:build !darwin && !dragonfly && !freebsd && !linux && !netbsd && !openbsd && !windows

package journal

func syncDirectory(string) error { return nil }
