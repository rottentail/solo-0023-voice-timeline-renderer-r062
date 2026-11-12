package service

import (
	"context"
	"encoding/base64"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"voice-timeline-renderer/internal/audiocodec"
	"voice-timeline-renderer/internal/domain"
	"voice-timeline-renderer/internal/storage"
)

func testService(t *testing.T) (*Service, string) {
	t.Helper()
	root := t.TempDir()
	repo, err := storage.NewRepository(root)
	if err != nil {
		t.Fatalf("NewRepository: %v", err)
	}
	return New(repo, DefaultConfig()), root
}

func sineWAV(t *testing.T, sampleRate, channels, frames int) []byte {
	t.Helper()
	samples := make([]float32, frames*channels)
	for index := range samples {
		samples[index] = 0.2
	}
	encoded, err := audiocodec.EncodePCM16(sampleRate, channels, samples)
	if err != nil {
		t.Fatalf("EncodePCM16: %v", err)
	}
	return encoded
}

func registerSource(service *Service, t *testing.T, id string, wav []byte) (domain.SourceAsset, bool, error) {
	t.Helper()
	return service.CreateSource(context.Background(), id, SourceWriteRequest{
		Name:      "take",
		WAVBase64: base64.StdEncoding.EncodeToString(wav),
	})
}

func TestConcurrentIdenticalSourceRegistrationIsIdempotent(t *testing.T) {
	serviceA, root := testService(t)
	repoB, err := storage.NewRepository(root)
	if err != nil {
		t.Fatalf("NewRepository: %v", err)
	}
	serviceB := New(repoB, DefaultConfig())
	wav := sineWAV(t, 48000, 1, 4800)

	var wait sync.WaitGroup
	type outcome struct {
		asset   domain.SourceAsset
		created bool
		err     error
	}
	results := make([]outcome, 2)
	services := []*Service{serviceA, serviceB}
	for index := range services {
		wait.Add(1)
		go func(slot int) {
			defer wait.Done()
			asset, created, err := registerSource(services[slot], t, "shared_source", wav)
			results[slot] = outcome{asset, created, err}
		}(index)
	}
	wait.Wait()

	for _, result := range results {
		if result.err != nil {
			t.Fatalf("registration failed: %v", result.err)
		}
		if result.asset.Digest == "" {
			t.Fatalf("registration returned empty metadata")
		}
	}
	if results[0].asset.Digest != results[1].asset.Digest {
		t.Fatalf("digests differ: %s vs %s", results[0].asset.Digest, results[1].asset.Digest)
	}
	if _, err := os.Stat(filepath.Join(root, "sources", "shared_source", "source.wav")); err != nil {
		t.Fatalf("published source missing: %v", err)
	}
}

func TestConcurrentConflictingSourceRegistrationKeepsWinner(t *testing.T) {
	serviceA, root := testService(t)
	repoB, err := storage.NewRepository(root)
	if err != nil {
		t.Fatalf("NewRepository: %v", err)
	}
	serviceB := New(repoB, DefaultConfig())

	first := sineWAV(t, 48000, 1, 4800)
	second := sineWAV(t, 44100, 2, 4800)
	var wait sync.WaitGroup
	errs := make([]error, 2)
	wait.Add(2)
	go func() {
		defer wait.Done()
		_, _, errs[0] = registerSource(serviceA, t, "conflict_source", first)
	}()
	go func() {
		defer wait.Done()
		_, _, errs[1] = registerSource(serviceB, t, "conflict_source", second)
	}()
	wait.Wait()

	failed := 0
	for _, err := range errs {
		if err != nil {
			failed++
			if appErr := domain.AsAppError(err); appErr.Detail.Code != "source_conflict" {
				t.Fatalf("expected source_conflict, got %v", err)
			}
		}
	}
	if failed != 1 {
		t.Fatalf("expected exactly one conflict, got %d", failed)
	}
	// The published source must match exactly one of the submitted payloads.
	repo, err := storage.NewRepository(root)
	if err != nil {
		t.Fatalf("reopen repository: %v", err)
	}
	asset, payload, err := repo.GetSource("conflict_source")
	if err != nil {
		t.Fatalf("GetSource: %v", err)
	}
	if string(payload) != string(first) && string(payload) != string(second) {
		t.Fatalf("published bytes match neither submission")
	}
	_ = asset
}

func TestTerminalRenderStateIsNotOverwritten(t *testing.T) {
	service, root := testService(t)
	wav := sineWAV(t, 48000, 1, 4800)
	if _, _, err := registerSource(service, t, "terminal_source", wav); err != nil {
		t.Fatalf("register source: %v", err)
	}
	spans := []domain.TimelineSpan{{SourceID: "terminal_source", SourceStartFrame: 0, SourceEndFrame: 4800, GainDB: 0}}
	if _, _, err := service.CreateTimeline(context.Background(), "terminal_timeline", TimelineWriteRequest{
		Name: "single span", SampleRate: 48000, OutputChannels: 2, TargetPeakDBFS: -1, Spans: spans,
	}); err != nil {
		t.Fatalf("create timeline: %v", err)
	}
	job, created, err := service.CreateRender(context.Background(), "terminal_render", RenderWriteRequest{TimelineID: "terminal_timeline"})
	if err != nil || !created {
		t.Fatalf("create render: created=%v err=%v", created, err)
	}
	if job.State != domain.RenderCompleted {
		t.Fatalf("render state = %s, want completed", job.State)
	}

	// A peer repository that shares the data directory must not be able to
	// transition the completed job back to an active or failed state.
	repoB, err := storage.NewRepository(root)
	if err != nil {
		t.Fatalf("NewRepository: %v", err)
	}
	_, err = repoB.TransitionJob("terminal_render", func(current *domain.RenderJob) error {
		return current.Transition(domain.RenderFailed, &domain.ErrorDetail{Code: "late_failure", Message: "late"}, time.Now().UTC())
	})
	if err == nil {
		t.Fatalf("expected terminal job transition to be refused")
	}
	if appErr := domain.AsAppError(err); appErr.Detail.Code != "render_already_terminal" {
		t.Fatalf("unexpected error code %q", appErr.Detail.Code)
	}
	job, err = repoB.GetJob("terminal_render")
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if job.State != domain.RenderCompleted || job.Output == nil {
		t.Fatalf("completed job was clobbered: state=%s output=%v", job.State, job.Output)
	}
	if _, _, err := repoB.GetRenderOutput("terminal_render"); err != nil {
		t.Fatalf("completed output was lost: %v", err)
	}
}

func TestConcurrentDuplicateRenderCreation(t *testing.T) {
	serviceA, root := testService(t)
	repoB, err := storage.NewRepository(root)
	if err != nil {
		t.Fatalf("NewRepository: %v", err)
	}
	serviceB := New(repoB, DefaultConfig())
	wav := sineWAV(t, 48000, 1, 4800)
	if _, _, err := registerSource(serviceA, t, "dup_source", wav); err != nil {
		t.Fatalf("register source: %v", err)
	}
	spans := []domain.TimelineSpan{{SourceID: "dup_source", SourceStartFrame: 0, SourceEndFrame: 4800, GainDB: 0}}
	if _, _, err := serviceA.CreateTimeline(context.Background(), "dup_timeline", TimelineWriteRequest{
		Name: "single span", SampleRate: 48000, OutputChannels: 2, TargetPeakDBFS: -1, Spans: spans,
	}); err != nil {
		t.Fatalf("create timeline: %v", err)
	}

	var wait sync.WaitGroup
	errs := make([]error, 2)
	jobs := make([]domain.RenderJob, 2)
	wait.Add(2)
	go func() {
		defer wait.Done()
		jobs[0], _, errs[0] = serviceA.CreateRender(context.Background(), "dup_render", RenderWriteRequest{TimelineID: "dup_timeline"})
	}()
	go func() {
		defer wait.Done()
		jobs[1], _, errs[1] = serviceB.CreateRender(context.Background(), "dup_render", RenderWriteRequest{TimelineID: "dup_timeline"})
	}()
	wait.Wait()

	for index, err := range errs {
		if err != nil {
			t.Fatalf("duplicate render %d failed: %v (job=%+v)", index, err, jobs[index])
		}
	}
	// At least one request completed the render. The other must either
	// observe the completed job or finish it; both must be consistent and the
	// published output must exist exactly once.
	final, err := repoB.GetJob("dup_render")
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if final.State != domain.RenderCompleted || final.Output == nil {
		t.Fatalf("final job not completed: %+v", final)
	}
	if _, _, err := repoB.GetRenderOutput("dup_render"); err != nil {
		t.Fatalf("published output missing: %v", err)
	}
	entries, err := os.ReadDir(filepath.Join(root, "renders"))
	if err != nil {
		t.Fatalf("ReadDir renders: %v", err)
	}
	count := 0
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		if entry.Name() == "dup_render" {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("published output folder count = %d, want 1", count)
	}
}

func TestAdoptPublishedOutputCompletesJob(t *testing.T) {
	service, root := testService(t)
	wav := sineWAV(t, 48000, 1, 4800)
	if _, _, err := registerSource(service, t, "adopt_source", wav); err != nil {
		t.Fatalf("register source: %v", err)
	}
	spans := []domain.TimelineSpan{{SourceID: "adopt_source", SourceStartFrame: 0, SourceEndFrame: 4800, GainDB: 0}}
	if _, _, err := service.CreateTimeline(context.Background(), "adopt_timeline", TimelineWriteRequest{
		Name: "single span", SampleRate: 48000, OutputChannels: 2, TargetPeakDBFS: -1, Spans: spans,
	}); err != nil {
		t.Fatalf("create timeline: %v", err)
	}
	job, created, err := service.CreateRender(context.Background(), "adopt_render", RenderWriteRequest{TimelineID: "adopt_timeline"})
	if err != nil || !created || job.State != domain.RenderCompleted {
		t.Fatalf("create render: created=%v job=%+v err=%v", created, job, err)
	}

	// Simulate the peer state: same terminal job and published artifacts.
	repoB, err := storage.NewRepository(root)
	if err != nil {
		t.Fatalf("NewRepository: %v", err)
	}
	peerService := New(repoB, DefaultConfig())
	_ = peerService
	if _, _, err := repoB.GetRenderOutput("adopt_render"); err != nil {
		t.Fatalf("output missing: %v", err)
	}
	// The adoption helper must set output metadata from the published receipt
	// without republishing.
	adopted := domain.RenderJob{ID: "adopt_render", TimelineID: "adopt_timeline", State: domain.RenderFinalizing}
	okAdopt, err := service.adoptPublishedOutput(&adopted)
	if err != nil || !okAdopt {
		t.Fatalf("adoption failed: ok=%v err=%v", okAdopt, err)
	}
	if adopted.State != domain.RenderCompleted || adopted.Output == nil || adopted.ReceiptDigest == "" {
		t.Fatalf("adopted job incomplete: %+v", adopted)
	}
}

func TestIdempotentRenderRetryReturnsCompletedJob(t *testing.T) {
	service, _ := testService(t)
	wav := sineWAV(t, 48000, 1, 4800)
	if _, _, err := registerSource(service, t, "retry_source", wav); err != nil {
		t.Fatalf("register source: %v", err)
	}
	spans := []domain.TimelineSpan{{SourceID: "retry_source", SourceStartFrame: 0, SourceEndFrame: 4800, GainDB: 0}}
	if _, _, err := service.CreateTimeline(context.Background(), "retry_timeline", TimelineWriteRequest{
		Name: "single span", SampleRate: 48000, OutputChannels: 2, TargetPeakDBFS: -1, Spans: spans,
	}); err != nil {
		t.Fatalf("create timeline: %v", err)
	}
	first, created, err := service.CreateRender(context.Background(), "retry_render", RenderWriteRequest{TimelineID: "retry_timeline"})
	if err != nil || !created || first.State != domain.RenderCompleted {
		t.Fatalf("first render: created=%v state=%s err=%v", created, first.State, err)
	}
	second, created, err := service.CreateRender(context.Background(), "retry_render", RenderWriteRequest{TimelineID: "retry_timeline"})
	if err != nil {
		t.Fatalf("retry render: %v", err)
	}
	if created {
		t.Fatalf("retry reported a new creation")
	}
	if second.State != domain.RenderCompleted || second.Output == nil {
		t.Fatalf("retry did not return completed job: %+v", second)
	}
}
