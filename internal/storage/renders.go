package storage

import (
	"errors"
	"os"
	"path/filepath"

	"voice-timeline-renderer/internal/domain"
)

// TransitionJob applies fn to the current durable job record and publishes the
// result, refusing to touch a job that has already reached a terminal state.
// The read-modify-write is serialized within this repository and across
// processes by an exclusive lock on one fixed guard file, so a late or
// duplicate request in another process cannot overwrite a completed or failed
// job. A single guard file keeps the data directory bounded regardless of how
// many renders run.
func (repo *Repository) TransitionJob(id string, fn func(*domain.RenderJob) error) (domain.RenderJob, error) {
	repo.mu.Lock()
	defer repo.mu.Unlock()

	lock, err := repo.lockJobs()
	if err != nil {
		return domain.RenderJob{}, domain.Wrap(domain.KindInternal, "storage_error", "", "cannot lock render jobs", err)
	}
	if lock != nil {
		defer func() {
			_ = unlockFile(lock)
			_ = lock.Close()
		}()
	}
	path := repo.path("jobs", id+".json")
	job, err := repo.readJobLocked(id)
	if err != nil {
		return domain.RenderJob{}, err
	}
	if domain.TerminalRenderState(job.State) {
		return job, domain.E(
			domain.KindConflict,
			"render_already_terminal",
			"state",
			"render job is already in a terminal state and cannot be changed",
		)
	}
	if fn != nil {
		if err := fn(&job); err != nil {
			return domain.RenderJob{}, err
		}
	}
	if err := writeJSONAtomic(path, job, 0o640); err != nil {
		return domain.RenderJob{}, domain.Wrap(domain.KindInternal, "storage_error", "", "cannot write render job", err)
	}
	return job, nil
}

// CreateJob durably claims a new render identifier. The exclusive create is
// performed under the same cross-process job guard used by TransitionJob, so a
// peer can never observe a half-written job or race a claim.
func (repo *Repository) CreateJob(job domain.RenderJob) error {
	repo.mu.Lock()
	defer repo.mu.Unlock()
	lock, err := repo.lockJobs()
	if err != nil {
		return domain.Wrap(domain.KindInternal, "storage_error", "", "cannot lock render jobs", err)
	}
	if lock != nil {
		defer func() {
			_ = unlockFile(lock)
			_ = lock.Close()
		}()
	}
	path := repo.path("jobs", job.ID+".json")
	if _, err := os.Stat(path); err == nil {
		return domain.ErrJobExists
	} else if !errors.Is(err, os.ErrNotExist) {
		return domain.Wrap(domain.KindInternal, "storage_error", "", "cannot inspect render job", err)
	}
	if err := writeJSONExclusive(path, job, 0o640); err != nil {
		if errors.Is(err, os.ErrExist) {
			return domain.ErrJobExists
		}
		return domain.Wrap(domain.KindInternal, "storage_error", "", "cannot write render job", err)
	}
	return nil
}

// lockJobs opens and exclusively locks the single job guard file. It returns
// nil without error when the platform or filesystem does not provide advisory
// locks, in which case the in-repository mutex still serializes local callers.
func (repo *Repository) lockJobs() (*os.File, error) {
	return repo.lockPath("jobs")
}

func (repo *Repository) readJobLocked(id string) (domain.RenderJob, error) {
	var job domain.RenderJob
	if err := readJSON(repo.path("jobs", id+".json"), &job); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return domain.RenderJob{}, domain.E(domain.KindNotFound, "render_not_found", "render_id", "render job does not exist")
		}
		return domain.RenderJob{}, domain.Wrap(domain.KindInternal, "storage_error", "", "cannot read render job", err)
	}
	return job, nil
}

// ErrJobExists is returned by CreateJob when another process has already
// durably claimed the same render identifier. Callers should resolve the
// existing job instead of treating it as a failure.
var ErrJobExists = domain.ErrJobExists

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
	if err := repo.reclaimStaging("render", false); err != nil {
		return domain.Wrap(domain.KindInternal, "storage_error", "", "cannot reclaim render staging", err)
	}
	// Serialize publication across processes so a duplicate request cannot
	// replace another process's completed output folder.
	guard, err := repo.lockPath("renders")
	if err != nil {
		return domain.Wrap(domain.KindInternal, "storage_error", "", "cannot lock render index", err)
	}
	defer unlockPath(guard)

	final := repo.path("renders", id)
	if _, err := os.Stat(final); err == nil {
		return domain.E(domain.KindConflict, "render_conflict", "render_id", "render artifacts already exist")
	} else if !errors.Is(err, os.ErrNotExist) {
		return domain.Wrap(domain.KindInternal, "storage_error", "", "cannot inspect render destination", err)
	}
	pattern, lease, err := repo.stagingPattern("render")
	if err != nil {
		return domain.Wrap(domain.KindInternal, "storage_error", "", "cannot create render staging directory", err)
	}
	temp, err := os.MkdirTemp(repo.path("tmp"), pattern)
	if err != nil {
		return domain.Wrap(domain.KindInternal, "storage_error", "", "cannot create render staging directory", err)
	}
	lease.track(temp)
	defer func() {
		lease.release(temp)
		_ = os.RemoveAll(temp)
	}()
	if err := writeBytes(filepath.Join(temp, "output.wav"), wav, 0o640); err != nil {
		return domain.Wrap(domain.KindInternal, "storage_error", "", "cannot write render output", err)
	}
	if err := writeJSONAtomic(filepath.Join(temp, "receipt.json"), receipt, 0o640); err != nil {
		return domain.Wrap(domain.KindInternal, "storage_error", "", "cannot write render receipt", err)
	}
	if err := os.Rename(temp, final); err != nil {
		if _, statErr := os.Stat(final); statErr == nil {
			// Another process published this render's artifacts first. Never
			// replace an existing completed output folder.
			return domain.E(domain.KindConflict, "render_conflict", "render_id", "render artifacts already exist")
		}
		return domain.Wrap(domain.KindInternal, "storage_error", "", "cannot commit render artifacts", err)
	}
	lease.release(temp)
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
