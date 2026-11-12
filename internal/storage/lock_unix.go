//go:build unix

package storage

import (
	"errors"
	"os"
	"syscall"
)

// tryLockFileExclusive attempts to take an exclusive advisory lock without
// blocking. It reports whether the lock was acquired.
func tryLockFileExclusive(file *os.File) (bool, error) {
	for {
		err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		switch {
		case err == nil:
			return true, nil
		case errors.Is(err, syscall.EWOULDBLOCK), errors.Is(err, syscall.EAGAIN):
			return false, nil
		case errors.Is(err, syscall.EINTR):
			continue
		default:
			return false, err
		}
	}
}

func unlockFile(file *os.File) error {
	return syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
}

// lockFileExclusive blocks until the exclusive advisory lock is held.
func lockFileExclusive(file *os.File) error {
	for {
		err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX)
		if errors.Is(err, syscall.EINTR) {
			continue
		}
		return err
	}
}

// processLocksSupported reports whether kernel advisory locks exist on this
// platform.
func processLocksSupported() bool {
	return true
}
