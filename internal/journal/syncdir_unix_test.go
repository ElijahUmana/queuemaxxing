//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd

package journal

import (
	"errors"
	"testing"
)

func TestDirectorySyncFaultPropagates(t *testing.T) {
	injected := errors.New("injected directory sync failure")
	journal := &FileJournal{faults: FaultHooks{BeforeSync: func(string) error {
		return injected
	}}}
	if err := journal.syncDirectoryLocked(t.TempDir()); !errors.Is(err, injected) {
		t.Fatalf("syncDirectoryLocked() error = %v, want %v", err, injected)
	}
}
