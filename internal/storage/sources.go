package storage

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"

	"voice-timeline-renderer/internal/domain"
)

func (repo *Repository) SaveSource(asset domain.SourceAsset, wav []byte) error {
	repo.mu.Lock()
	defer repo.mu.Unlock()

	if err := repo.reclaimStaging("source", false); err != nil {
		return domain.Wrap(domain.KindInternal, "storage_error", "", "cannot reclaim source staging", err)
	}
	// Serialize publication across processes so two peers registering the
	// same identifier cannot clobber each other's immutable source. The
	// guard file is fixed, so it never accumulates.
	guard, err := repo.lockPath("sources")
	if err != nil {
		return domain.Wrap(domain.KindInternal, "storage_error", "", "cannot lock source index", err)
	}
	defer unlockPath(guard)

	final := repo.path("sources", asset.ID)
	if _, err := os.Stat(final); err == nil {
		return domain.E(domain.KindConflict, "source_conflict", "source_id", "source identifier already exists")
	} else if !errors.Is(err, os.ErrNotExist) {
		return domain.Wrap(domain.KindInternal, "storage_error", "", "cannot inspect source destination", err)
	}
	pattern, lease, err := repo.stagingPattern("source")
	if err != nil {
		return domain.Wrap(domain.KindInternal, "storage_error", "", "cannot create source staging directory", err)
	}
	temp, err := os.MkdirTemp(repo.path("tmp"), pattern)
	if err != nil {
		return domain.Wrap(domain.KindInternal, "storage_error", "", "cannot create source staging directory", err)
	}
	lease.track(temp)
	defer func() {
		lease.release(temp)
		_ = os.RemoveAll(temp)
	}()
	if err := writeBytes(filepath.Join(temp, "source.wav"), wav, 0o640); err != nil {
		return domain.Wrap(domain.KindInternal, "storage_error", "", "cannot write source media", err)
	}
	if err := writeJSONAtomic(filepath.Join(temp, "metadata.json"), asset, 0o640); err != nil {
		return domain.Wrap(domain.KindInternal, "storage_error", "", "cannot write source metadata", err)
	}
	if err := os.Rename(temp, final); err != nil {
		if _, statErr := os.Stat(final); statErr == nil {
			// A concurrent process published the same identifier first. The
			// service layer resolves idempotency by comparing stored
			// metadata; never overwrite the winner's immutable source.
			return domain.E(domain.KindConflict, "source_conflict", "source_id", "source identifier already exists")
		}
		return domain.Wrap(domain.KindInternal, "storage_error", "", "cannot commit source", err)
	}
	lease.release(temp)
	if err := syncDirectory(repo.path("sources")); err != nil {
		return domain.Wrap(domain.KindInternal, "storage_error", "", "cannot sync source index", err)
	}
	return nil
}

func (repo *Repository) GetSourceMeta(id string) (domain.SourceAsset, error) {
	repo.mu.RLock()
	defer repo.mu.RUnlock()
	var asset domain.SourceAsset
	if err := readJSON(repo.path("sources", id, "metadata.json"), &asset); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return domain.SourceAsset{}, domain.E(domain.KindNotFound, "source_not_found", "source_id", "source does not exist")
		}
		return domain.SourceAsset{}, domain.Wrap(domain.KindInternal, "storage_error", "", "cannot read source metadata", err)
	}
	return asset, nil
}

func (repo *Repository) GetSource(id string) (domain.SourceAsset, []byte, error) {
	asset, err := repo.GetSourceMeta(id)
	if err != nil {
		return domain.SourceAsset{}, nil, err
	}
	repo.mu.RLock()
	payload, readErr := os.ReadFile(repo.path("sources", id, "source.wav"))
	repo.mu.RUnlock()
	if readErr != nil {
		if errors.Is(readErr, os.ErrNotExist) {
			return domain.SourceAsset{}, nil, domain.E(domain.KindNotFound, "source_not_found", "source_id", "source media does not exist")
		}
		return domain.SourceAsset{}, nil, domain.Wrap(domain.KindInternal, "storage_error", "", "cannot read source media", readErr)
	}
	digest := sha256.Sum256(payload)
	if hex.EncodeToString(digest[:]) != asset.Digest {
		return domain.SourceAsset{}, nil, domain.E(domain.KindInternal, "source_integrity_error", "source_id", "stored source digest does not match metadata")
	}
	return asset, payload, nil
}
