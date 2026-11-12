//go:build !unix

package storage

import "os"

// tryLockFileExclusive is unavailable on this platform. Staging reclamation
// therefore stays conservative and never deletes staging that it cannot
// attribute to a dead owner.
func tryLockFileExclusive(_ *os.File) (bool, error) {
	return false, nil
}

func lockFileExclusive(_ *os.File) error {
	return nil
}

func unlockFile(_ *os.File) error {
	return nil
}

func processLocksSupported() bool {
	return false
}
