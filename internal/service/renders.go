package service

import (
	"context"
	"errors"
	"time"

	"voice-timeline-renderer/internal/domain"
	"voice-timeline-renderer/internal/render"
)

func (s *Service) CreateRender(
	ctx context.Context,
	id string,
	request RenderWriteRequest,
) (domain.RenderJob, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return domain.RenderJob{}, false, err
	}
	if err := domain.ValidateResourceID("render_id", id); err != nil {
		return domain.RenderJob{}, false, err
	}
	if err := domain.ValidateResourceID("timeline_id", request.TimelineID); err != nil {
		return domain.RenderJob{}, false, err
	}

	existing, getErr := s.repo.GetJob(id)
	if getErr == nil {
		if existing.TimelineID != request.TimelineID {
			return domain.RenderJob{}, false, domain.E(domain.KindConflict, "render_conflict", "render_id", "identifier is already bound to a different timeline")
		}
		return existing, false, nil
	}
	if appErr := domain.AsAppError(getErr); appErr.Kind != domain.KindNotFound {
		return domain.RenderJob{}, false, getErr
	}

	plan, err := s.repo.GetTimeline(request.TimelineID)
	if err != nil {
		return domain.RenderJob{}, false, err
	}
	now := time.Now().UTC()
	job := domain.RenderJob{
		ID:         id,
		TimelineID: plan.ID,
		State:      domain.RenderQueued,
		CreatedAt:  now,
		UpdatedAt:  now,
	}
	if err := s.repo.CreateJob(job); err != nil {
		if errors.Is(err, domain.ErrJobExists) {
			// Another process (or a concurrent request) claimed this render
			// identifier first. Resolve its durable record instead of racing
			// it; the caller sees the same idempotent resource.
			existing, getErr := s.repo.GetJob(id)
			if getErr != nil {
				return domain.RenderJob{}, false, getErr
			}
			if existing.TimelineID != request.TimelineID {
				return domain.RenderJob{}, false, domain.E(domain.KindConflict, "render_conflict", "render_id", "identifier is already bound to a different timeline")
			}
			return existing, false, nil
		}
		return domain.RenderJob{}, false, err
	}
	// Once CreateJob succeeds this request owns the job's lifecycle. Each
	// transition re-reads the durable record under a cross-process guard; if a
	// peer already made the job terminal (for example a restart that could not
	// recover it), this request stops and reports the durable result instead
	// of overwriting it.
	if err := s.advanceJob(&job, domain.RenderRendering, nil); err != nil {
		return job, true, err
	}
	if domain.TerminalRenderState(job.State) {
		return job, true, nil
	}

	sources := make(map[string]render.SourceInput)
	for _, span := range plan.Spans {
		if _, loaded := sources[span.SourceID]; loaded {
			continue
		}
		asset, payload, loadErr := s.repo.GetSource(span.SourceID)
		if loadErr != nil {
			return s.failRender(job, loadErr)
		}
		sources[span.SourceID] = render.SourceInput{Asset: asset, WAV: payload}
	}
	result, renderErr := render.Render(
		ctx,
		id,
		plan,
		sources,
		render.Limits{MaxOutputFrames: s.config.MaxOutputFrames},
		time.Now().UTC(),
	)
	if renderErr != nil {
		return s.failRender(job, domain.Wrap(domain.KindInternal, "render_failed", "", "timeline rendering failed", renderErr))
	}
	if err := s.advanceJob(&job, domain.RenderFinalizing, nil); err != nil {
		return job, true, err
	}
	if domain.TerminalRenderState(job.State) {
		return job, true, nil
	}
	if err := s.repo.CommitRenderArtifacts(id, result.WAV, result.Receipt); err != nil {
		// A peer may have published this same render's artifacts between our
		// finalizing transition and our commit. Adopt its already-published
		// output instead of failing the job and stranding the folder.
		if appErr := domain.AsAppError(err); appErr.Kind == domain.KindConflict {
			if adopted, adoptErr := s.adoptPublishedOutput(&job); adoptErr == nil && adopted {
				return job, true, nil
			}
		}
		return s.failRender(job, err)
	}
	if err := s.advanceJobWith(&job, domain.RenderCompleted, nil, func(current *domain.RenderJob) {
		current.Output = &result.Output
		current.ReceiptDigest = result.Output.Digest
	}); err != nil {
		return job, true, err
	}
	return job, true, nil
}

// adoptPublishedOutput completes a job from artifacts that a peer process
// already committed for this identifier. It reports whether adoption happened.
func (s *Service) adoptPublishedOutput(job *domain.RenderJob) (bool, error) {
	_, receipt, err := s.repo.GetRenderOutput(job.ID)
	if err != nil {
		return false, err
	}
	output := receipt.Output
	err = s.advanceJobWith(job, domain.RenderCompleted, nil, func(current *domain.RenderJob) {
		current.Output = &output
		current.ReceiptDigest = receipt.Output.Digest
	})
	if err != nil {
		return false, err
	}
	return domain.TerminalRenderState(job.State), nil
}

func (s *Service) advanceJob(job *domain.RenderJob, state domain.RenderState, detail *domain.ErrorDetail) error {
	return s.advanceJobWith(job, state, detail, nil)
}

// advanceJobWith applies an explicit state transition together with optional
// extra fields (such as output metadata) in one guarded read-modify-write.
func (s *Service) advanceJobWith(
	job *domain.RenderJob,
	state domain.RenderState,
	detail *domain.ErrorDetail,
	mutate func(*domain.RenderJob),
) error {
	updated, err := s.repo.TransitionJob(job.ID, func(current *domain.RenderJob) error {
		if err := current.Transition(state, detail, time.Now().UTC()); err != nil {
			return err
		}
		if mutate != nil {
			mutate(current)
		}
		return nil
	})
	if err != nil {
		return s.resolveTerminal(job, err)
	}
	*job = updated
	return nil
}

// resolveTerminal turns a late transition into a no-op when another process
// already finished the job. A completed or failed job is never overwritten,
// so the other process's receipt and output stay intact.
func (s *Service) resolveTerminal(job *domain.RenderJob, err error) error {
	appErr := domain.AsAppError(err)
	if appErr.Detail.Code != "render_already_terminal" {
		return err
	}
	existing, getErr := s.repo.GetJob(job.ID)
	if getErr != nil {
		return err
	}
	*job = existing
	return nil
}

func (s *Service) failRender(job domain.RenderJob, cause error) (domain.RenderJob, bool, error) {
	appErr := domain.AsAppError(cause)
	detail := &domain.ErrorDetail{
		Code:    appErr.Detail.Code,
		Message: appErr.Detail.Message,
		Field:   appErr.Detail.Field,
	}
	updated, saveErr := s.repo.TransitionJob(job.ID, func(current *domain.RenderJob) error {
		if err := current.Transition(domain.RenderFailed, detail, time.Now().UTC()); err != nil {
			return err
		}
		current.Output = nil
		current.ReceiptDigest = ""
		return nil
	})
	if saveErr != nil {
		if appLate := domain.AsAppError(saveErr); appLate.Detail.Code == "render_already_terminal" {
			// Another process already finished the job. Do not report this
			// request as the owner of a failure it did not cause.
			existing, getErr := s.repo.GetJob(job.ID)
			if getErr == nil {
				return existing, true, nil
			}
		}
		return job, true, saveErr
	}
	return updated, true, appErr
}
