package storage

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type Repository struct {
	root string
	mu   sync.RWMutex

	leaseOnce sync.Once
	lease     *processLease
	leaseErr  error
	// leaseDegraded records that advisory locks are unavailable for this data
	// directory, so staging cannot be attributed to a dead owner safely.
	leaseDegraded bool
}

func NewRepository(root string) (*Repository, error) {
	absolute, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	for _, child := range []string{"sources", "timelines", "jobs", "renders", "tmp"} {
		if err := os.MkdirAll(filepath.Join(absolute, child), 0o750); err != nil {
			return nil, err
		}
	}
	return &Repository{root: absolute}, nil
}

func (repo *Repository) path(parts ...string) string {
	all := append([]string{repo.root}, parts...)
	return filepath.Join(all...)
}

// ReclaimUnpublished removes temporary media that is provably abandoned because
// its creating process no longer holds the staging lease. Staging owned by a
// live process is preserved even when several service processes share one data
// directory.
func (repo *Repository) ReclaimUnpublished() error {
	repo.mu.Lock()
	defer repo.mu.Unlock()
	if _, err := repo.acquireLease(); err != nil {
		return err
	}
	if err := repo.reclaimStaging("source", true); err != nil {
		return err
	}
	if err := repo.reclaimStaging("render", true); err != nil {
		return err
	}
	return repo.reclaimStaleWriteTemps()
}

// staleWriteTempAge bounds how long an interrupted JSON write temp file may
// linger before a later process reclaims it. Live writers finish in milliseconds
// and periodically rename their file, so an age-gated sweep cannot delete
// another process's in-flight temp file.
const staleWriteTempAge = 30 * time.Minute

func (repo *Repository) reclaimStaleWriteTemps() error {
	cutoff := time.Now().Add(-staleWriteTempAge)
	for _, directory := range []string{"sources", "timelines", "jobs", "renders"} {
		entries, err := os.ReadDir(repo.path(directory))
		if err != nil {
			return err
		}
		for _, entry := range entries {
			name := entry.Name()
			if entry.IsDir() || !strings.HasPrefix(name, ".write-") {
				continue
			}
			path := repo.path(directory, name)
			info, err := entry.Info()
			if err != nil {
				if errors.Is(err, os.ErrNotExist) {
					continue
				}
				return err
			}
			if info.ModTime().After(cutoff) {
				continue
			}
			if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
				return err
			}
		}
	}
	return nil
}
