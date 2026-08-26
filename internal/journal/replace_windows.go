//go:build windows

package journal

import (
	"syscall"
	"unsafe"
)

const (
	movefileReplaceExisting = 0x00000001
	movefileWriteThrough     = 0x00000008
)

var procMoveFileExW = kernel32.NewProc("MoveFileExW")

func replaceFile(oldPath, newPath string) error {
	oldName, err := syscall.UTF16PtrFromString(oldPath)
	if err != nil {
		return err
	}
	newName, err := syscall.UTF16PtrFromString(newPath)
	if err != nil {
		return err
	}
	result, _, callErr := procMoveFileExW.Call(
		uintptr(unsafe.Pointer(oldName)),
		uintptr(unsafe.Pointer(newName)),
		movefileReplaceExisting|movefileWriteThrough,
	)
	if result == 0 {
		return callErr
	}
	return nil
}
