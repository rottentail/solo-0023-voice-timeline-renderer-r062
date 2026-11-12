package storage

import (
	"errors"
	"os"
	"path/filepath"
	"strings"

	"voice-timeline-renderer/internal/domain"
)

func (repo *Repository) SaveJob(job domain.RenderJob) error {
	repo.mu.Lock()
	defer repo.mu.Unlock()
	path := repo.path("jobs", job.ID+".json")
	if err := writeJSONAtomic(path, job, 0o640); err != nil {
		return domain.Wrap(domain.KindInternal, "storage_error", "", "cannot write render job", err)
	}
	return nil
}

func (repo *Repository) GetJob(id string) (domain.RenderJob, error) {
	repo.mu.RLock()
	defer repo.mu.RUnlock()
	var job domain.RenderJob
	if err := readJSON(repo.path("jobs", id+".json"), &job); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return domain.RenderJob{}, domain.E(domain.KindNotFound, "render_not_found", "render_id", "render job does not exist")
		}
		return domain.RenderJob{}, domain.Wrap(domain.KindInternal, "storage_error", "", "cannot read render job", err)
	}
	return job, nil
}

func (repo *Repository) CommitRenderArtifacts(id string, wav []byte, receipt domain.RenderReceipt) error {
	repo.mu.Lock()
	defer repo.mu.Unlock()
	if err := repo.reclaimRenderStaging(); err != nil {
		return domain.Wrap(domain.KindInternal, "storage_error", "", "cannot reclaim render staging", err)
	}

	final := repo.path("renders", id)
	if _, err := os.Stat(final); err == nil {
		return domain.E(domain.KindConflict, "render_conflict", "render_id", "render artifacts already exist")
	} else if !errors.Is(err, os.ErrNotExist) {
		return domain.Wrap(domain.KindInternal, "storage_error", "", "cannot inspect render destination", err)
	}
	temp, err := os.MkdirTemp(repo.path("tmp"), "render-*")
	if err != nil {
		return domain.Wrap(domain.KindInternal, "storage_error", "", "cannot create render staging directory", err)
	}
	defer func() {
		_ = os.RemoveAll(temp)
	}()
	if err := writeBytes(filepath.Join(temp, "output.wav"), wav, 0o640); err != nil {
		return domain.Wrap(domain.KindInternal, "storage_error", "", "cannot write render output", err)
	}
	if err := writeJSONAtomic(filepath.Join(temp, "receipt.json"), receipt, 0o640); err != nil {
		return domain.Wrap(domain.KindInternal, "storage_error", "", "cannot write render receipt", err)
	}
	if err := os.Rename(temp, final); err != nil {
		return domain.Wrap(domain.KindInternal, "storage_error", "", "cannot commit render artifacts", err)
	}
	if err := syncDirectory(repo.path("renders")); err != nil {
		return domain.Wrap(domain.KindInternal, "storage_error", "", "cannot sync render index", err)
	}
	return nil
}

func (repo *Repository) GetRenderOutput(id string) ([]byte, domain.RenderReceipt, error) {
	repo.mu.RLock()
	defer repo.mu.RUnlock()
	var receipt domain.RenderReceipt
	if err := readJSON(repo.path("renders", id, "receipt.json"), &receipt); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, domain.RenderReceipt{}, domain.E(domain.KindUnready, "render_not_completed", "render_id", "render output is not available")
		}
		return nil, domain.RenderReceipt{}, domain.Wrap(domain.KindInternal, "storage_error", "", "cannot read render receipt", err)
	}
	payload, err := os.ReadFile(repo.path("renders", id, "output.wav"))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, domain.RenderReceipt{}, domain.E(domain.KindUnready, "render_not_completed", "render_id", "render output is not available")
		}
		return nil, domain.RenderReceipt{}, domain.Wrap(domain.KindInternal, "storage_error", "", "cannot read render output", err)
	}
	return payload, receipt, nil
}

func (repo *Repository) reclaimRenderStaging() error {
	entries, err := os.ReadDir(repo.path("tmp"))
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if !entry.IsDir() || !strings.HasPrefix(entry.Name(), "render-") {
			continue
		}
		directory := repo.path("tmp", entry.Name())
		info, err := os.Stat(filepath.Join(directory, "output.wav"))
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			continue
		}
		if err := os.RemoveAll(directory); err != nil {
			return err
		}
	}
	return nil
}
