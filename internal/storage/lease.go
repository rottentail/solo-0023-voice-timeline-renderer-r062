package storage

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const (
	leasePrefix    = "lease-"
	leaseSuffix    = ".owner"
	anonymousOwner = "anon"
	kindSeparator  = "-"

	// legacyStagingMinAge is how old ownerless (pre-lease) staging must be
	// before startup reclamation may delete it. It bounds the risk that a
	// concurrent process still running an older build is writing such staging.
	legacyStagingMinAge = time.Hour
)

// processLease is the repository's identity within one process. A staging
// directory embeds this token in its name and its creator holds an exclusive
// advisory lock on the lease file for the process lifetime. Another process
// can therefore prove the creator is gone before deleting abandoned staging.
type processLease struct {
	token string
	// file stays open for the process lifetime; closing it releases the lock
	// that proves this owner is alive.
	file    *os.File
	mu      sync.Mutex
	heldDir map[string]struct{}
}

// acquireLease lazily creates the process lease on first use.
func (repo *Repository) acquireLease() (*processLease, error) {
	repo.leaseOnce.Do(func() {
		repo.lease, repo.leaseErr = repo.createLease()
	})
	return repo.lease, repo.leaseErr
}

func (repo *Repository) createLease() (*processLease, error) {
	if !processLocksSupported() {
		// Without advisory locks the creator of staging cannot be proven
		// gone, so new staging is created anonymously and never reclaimed
		// while another process could own it.
		repo.leaseDegraded = true
		return nil, nil
	}
	for attempt := 0; attempt < 8; attempt++ {
		token, err := randomLeaseToken()
		if err != nil {
			return nil, err
		}
		path := repo.leasePathForToken(token)
		file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o640)
		if err != nil {
			if errors.Is(err, os.ErrExist) {
				continue
			}
			return nil, err
		}
		locked, err := tryLockFileExclusive(file)
		if err != nil || !locked {
			// A peer inspected or briefly locked the just-created lease.
			// Discard this token and try another; no staging references it
			// yet, so nothing can be misattributed.
			_ = file.Close()
			_ = os.Remove(path)
			continue
		}
		// Verify the published path still names the inode we locked. A peer
		// that saw the file unlocked may have removed it; if so, retry rather
		// than advertise an owner token that peers believe is dead.
		info, err := file.Stat()
		if err != nil {
			_ = unlockFile(file)
			_ = file.Close()
			_ = os.Remove(path)
			continue
		}
		pathInfo, err := os.Stat(path)
		if err != nil || !os.SameFile(info, pathInfo) {
			_ = unlockFile(file)
			_ = file.Close()
			_ = os.Remove(path)
			continue
		}
		return &processLease{
			token:   token,
			file:    file,
			heldDir: make(map[string]struct{}),
		}, nil
	}
	// Repeated contention is treated like unavailable locking: degrade to
	// anonymous staging rather than failing a legitimate request.
	repo.leaseDegraded = true
	return nil, nil
}

func randomLeaseToken() (string, error) {
	var buffer [16]byte
	if _, err := rand.Read(buffer[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(buffer[:]), nil
}

// lockPath opens and exclusively locks a fixed guard file under directory. It
// returns nil without error when advisory locks are unavailable, in which case
// the in-repository mutex still serializes local callers.
func (repo *Repository) lockPath(directory string) (*os.File, error) {
	if !processLocksSupported() {
		return nil, nil
	}
	file, err := os.OpenFile(repo.path(directory, ".lock"), os.O_CREATE|os.O_RDWR, 0o640)
	if err != nil {
		return nil, err
	}
	if err := lockFileExclusive(file); err != nil {
		_ = file.Close()
		return nil, err
	}
	return file, nil
}

func unlockPath(file *os.File) {
	if file == nil {
		return
	}
	_ = unlockFile(file)
	_ = file.Close()
}

// track records that a staging directory belongs to this live process before
// any media bytes are written into it.
func (lease *processLease) track(directory string) {
	if lease == nil {
		return
	}
	lease.mu.Lock()
	lease.heldDir[directory] = struct{}{}
	lease.mu.Unlock()
}

// release stops treating directory as live staging. It is called after the
// directory has been renamed into its published location or removed.
func (lease *processLease) release(directory string) {
	if lease == nil {
		return
	}
	lease.mu.Lock()
	delete(lease.heldDir, directory)
	lease.mu.Unlock()
}

// holds reports whether the directory is still live staging in this process.
func (lease *processLease) holds(directory string) bool {
	if lease == nil {
		return false
	}
	lease.mu.Lock()
	defer lease.mu.Unlock()
	_, ok := lease.heldDir[directory]
	return ok
}

// stagingPattern builds the MkdirTemp pattern for staging of the given kind.
// Staging created by current code always embeds an owner token.
func (repo *Repository) stagingPattern(kind string) (string, *processLease, error) {
	lease, err := repo.acquireLease()
	if err != nil {
		return "", nil, err
	}
	owner := anonymousOwner
	if lease != nil {
		owner = lease.token
	}
	return kind + kindSeparator + owner + kindSeparator, lease, nil
}

// ownerOfStaging classifies a staging directory name. It returns the owner
// token and true when the name carries an owner, and ("", false) for a name
// written before the lease scheme existed.
func ownerOfStaging(name string) (string, bool) {
	first := strings.IndexByte(name, '-')
	if first < 0 {
		return "", false
	}
	rest := name[first+1:]
	second := strings.IndexByte(rest, '-')
	if second < 0 {
		return "", false
	}
	return rest[:second], true
}

func (repo *Repository) leasePathForToken(token string) string {
	return filepath.Join(repo.path("tmp"), leasePrefix+token+leaseSuffix)
}

// ownerIsAlive reports whether the process that created the staging directory
// named `name` still holds its lease. The check is conservative: unless the
// directory's owner can be proven dead, staging is preserved.
func (repo *Repository) ownerIsAlive(name string, allowLegacy bool, modified time.Time) bool {
	token, hasOwner := ownerOfStaging(name)
	if !hasOwner {
		// Pre-lease staging has no owner token, so its creator cannot be
		// attributed. A live process running an older build may still be
		// writing it; only reclaim it at startup and only once it is old
		// enough to be certainly abandoned.
		return !allowLegacy || time.Since(modified) < legacyStagingMinAge
	}
	return repo.tokenOwnerIsAlive(token)
}

// tokenOwnerIsAlive reports whether the process holding the lease for token is
// still running. Anonymous tokens (staging created without locks) are always
// considered alive because they can never be attributed to a dead process.
func (repo *Repository) tokenOwnerIsAlive(token string) bool {
	if token == anonymousOwner {
		// Anonymous staging was created without locks, so its owner can
		// never be proven gone. Preserve it.
		return true
	}
	if !processLocksSupported() || repo.leaseDegraded {
		return true
	}
	path := repo.leasePathForToken(token)
	file, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false
		}
		return true
	}
	defer file.Close()
	locked, err := tryLockFileExclusive(file)
	if err != nil {
		return true
	}
	if locked {
		// The owning process exited, so its lease lock is no longer held.
		_ = unlockFile(file)
		_ = os.Remove(path)
		return false
	}
	return true
}

// reclaimStaging removes staging directories of the requested kind that are
// provably abandoned. Live staging owned by another process (or by this
// process) is left untouched, so concurrent registrations and render
// publications cannot delete one another's temporary media. When allowLegacy
// is set (startup), ownerless staging left by an older service run is also
// reclaimed.
func (repo *Repository) reclaimStaging(kind string, allowLegacy bool) error {
	if _, err := repo.acquireLease(); err != nil {
		return err
	}
	if err := repo.reclaimDeadLeases(); err != nil {
		return err
	}
	selector := kind + kindSeparator
	entries, err := os.ReadDir(repo.path("tmp"))
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		name := entry.Name()
		if len(name) <= len(selector) || name[:len(selector)] != selector {
			continue
		}
		directory := repo.path("tmp", name)
		if repo.lease != nil && repo.lease.holds(directory) {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return err
		}
		if repo.ownerIsAlive(name, allowLegacy, info.ModTime()) {
			// Another live process owns this staging, or its owner cannot be
			// proven gone. Neither is safe to delete.
			continue
		}
		if err := os.RemoveAll(directory); err != nil {
			return err
		}
	}
	return nil
}

// reclaimDeadLeases removes lease files whose owning process has exited.
func (repo *Repository) reclaimDeadLeases() error {
	if repo.leaseDegraded || !processLocksSupported() {
		return nil
	}
	entries, err := os.ReadDir(repo.path("tmp"))
	if err != nil {
		return err
	}
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasPrefix(name, leasePrefix) || !strings.HasSuffix(name, leaseSuffix) {
			continue
		}
		token := strings.TrimSuffix(strings.TrimPrefix(name, leasePrefix), leaseSuffix)
		if repo.lease != nil && token == repo.lease.token {
			continue
		}
		if repo.tokenOwnerIsAlive(token) {
			continue
		}
		// ownerIsAlive already removes a dead lease; tolerate disappearance.
		_ = os.Remove(repo.path("tmp", name))
	}
	return nil
}
