package journal

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
)

func (journal *FileJournal) rootName(path string) (string, error) {
	name, err := filepath.Rel(journal.dir, path)
	if err != nil {
		return "", err
	}
	if name == "." {
		return name, nil
	}
	if !fs.ValidPath(filepath.ToSlash(name)) || filepath.IsAbs(name) {
		return "", fmt.Errorf("journal path escapes storage root: %s", path)
	}
	return name, nil
}

func (journal *FileJournal) openRoot() error {
	root, err := os.OpenRoot(journal.dir)
	if err != nil {
		return err
	}
	journal.root = root
	return nil
}

func (journal *FileJournal) openFile(path string, flag int, perm os.FileMode) (*os.File, error) {
	name, err := journal.rootName(path)
	if err != nil {
		return nil, err
	}
	return journal.root.OpenFile(name, flag, perm)
}

func (journal *FileJournal) readFile(path string) ([]byte, error) {
	name, err := journal.rootName(path)
	if err != nil {
		return nil, err
	}
	return journal.root.ReadFile(name)
}

func (journal *FileJournal) readDir(path string) ([]os.DirEntry, error) {
	file, err := journal.openFile(path, os.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	return file.ReadDir(-1)
}

func (journal *FileJournal) lstat(path string) (os.FileInfo, error) {
	name, err := journal.rootName(path)
	if err != nil {
		return nil, err
	}
	return journal.root.Lstat(name)
}

func (journal *FileJournal) mkdir(path string, perm os.FileMode) error {
	name, err := journal.rootName(path)
	if err != nil {
		return err
	}
	return journal.root.Mkdir(name, perm)
}

func (journal *FileJournal) ensureDirectory(path string, perm os.FileMode) error {
	err := journal.mkdir(path, perm)
	if err != nil && !os.IsExist(err) {
		return err
	}
	return journal.requireDirectory(path)
}

func (journal *FileJournal) remove(path string) error {
	name, err := journal.rootName(path)
	if err != nil {
		return err
	}
	return journal.root.Remove(name)
}

func (journal *FileJournal) rename(oldPath, newPath string) error {
	oldName, err := journal.rootName(oldPath)
	if err != nil {
		return err
	}
	newName, err := journal.rootName(newPath)
	if err != nil {
		return err
	}
	return journal.root.Rename(oldName, newName)
}

func (journal *FileJournal) restrictMode(path string, mode os.FileMode, directory bool) error {
	info, err := journal.lstat(path)
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 || directory != info.IsDir() || (!directory && !info.Mode().IsRegular()) {
		return fmt.Errorf("journal path has invalid type: %s", path)
	}
	file, err := journal.openFile(path, os.O_RDONLY, 0)
	if err != nil {
		return err
	}
	defer file.Close()
	opened, err := file.Stat()
	if err != nil {
		return err
	}
	if !os.SameFile(info, opened) || directory != opened.IsDir() || (!directory && !opened.Mode().IsRegular()) {
		return fmt.Errorf("journal path changed while restricting permissions: %s", path)
	}
	if runtime.GOOS == "windows" {
		return nil
	}
	return file.Chmod(mode)
}

func (journal *FileJournal) restrictDirectoryEntries(path string) error {
	entries, err := journal.readDir(path)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		entryPath := filepath.Join(path, entry.Name())
		if entry.IsDir() {
			return fmt.Errorf("journal storage entry is a directory: %s", entryPath)
		}
		if err := journal.restrictMode(entryPath, 0o600, false); err != nil {
			return err
		}
	}
	return nil
}

func (journal *FileJournal) restrictStorageModes() error {
	if err := journal.restrictMode(journal.dir, 0o700, true); err != nil {
		return err
	}
	for _, path := range []string{journal.walDir, journal.snapshotDir} {
		if err := journal.restrictMode(path, 0o700, true); err != nil {
			return err
		}
		if err := journal.restrictDirectoryEntries(path); err != nil {
			return err
		}
	}
	quarantineDir := filepath.Join(journal.dir, "quarantine")
	if _, err := journal.lstat(quarantineDir); err == nil {
		if err := journal.restrictMode(quarantineDir, 0o700, true); err != nil {
			return err
		}
		if err := journal.restrictDirectoryEntries(quarantineDir); err != nil {
			return err
		}
	} else if !os.IsNotExist(err) {
		return err
	}
	for _, name := range []string{"LOCK", "HEAD", "HEAD.tmp"} {
		path := filepath.Join(journal.dir, name)
		if _, err := journal.lstat(path); err == nil {
			if err := journal.restrictMode(path, 0o600, false); err != nil {
				return err
			}
		} else if !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}

func (journal *FileJournal) requireDirectory(path string) error {
	info, err := journal.lstat(path)
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return fmt.Errorf("journal path is not a directory: %s", path)
	}
	return nil
}
