package storage

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"voice-timeline-renderer/internal/domain"
)

func newTestRepository(t *testing.T, root string) *Repository {
	t.Helper()
	if root == "" {
		root = t.TempDir()
	}
	repo, err := NewRepository(root)
	if err != nil {
		t.Fatalf("NewRepository: %v", err)
	}
	return repo
}

// manuallyCreateStaging writes a staging directory whose media file exists, so
// reclamation has something it could wrongly delete.
func manuallyCreateStaging(t *testing.T, repo *Repository, kind, owner string) string {
	t.Helper()
	prefix := kind + kindSeparator + owner + kindSeparator
	directory, err := os.MkdirTemp(repo.path("tmp"), prefix)
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	media := "source.wav"
	if kind == "render" {
		media = "output.wav"
	}
	if err := os.WriteFile(filepath.Join(directory, media), []byte("audio"), 0o640); err != nil {
		t.Fatalf("write media: %v", err)
	}
	return directory
}

func TestLiveOwnerStagingIsPreserved(t *testing.T) {
	root := t.TempDir()
	owner := newTestRepository(t, root)
	peer := newTestRepository(t, root)

	lease, err := peer.acquireLease()
	if err != nil || lease == nil {
		t.Fatalf("peer lease unavailable: err=%v lease=%v", err, lease)
	}
	staging := manuallyCreateStaging(t, owner, "source", lease.token)
	lease.track(staging)
	defer func() {
		lease.release(staging)
		_ = os.RemoveAll(staging)
	}()

	if err := owner.reclaimStaging("source", false); err != nil {
		t.Fatalf("reclaimStaging: %v", err)
	}
	if _, err := os.Stat(filepath.Join(staging, "source.wav")); err != nil {
		t.Fatalf("live staging was reclaimed: %v", err)
	}

	// A concurrent publication through the other repository must also leave
	// the live staging untouched.
	if err := owner.SaveSource(domain.SourceAsset{ID: "other_source"}, []byte("wav")); err != nil {
		t.Fatalf("SaveSource: %v", err)
	}
	if _, err := os.Stat(filepath.Join(staging, "source.wav")); err != nil {
		t.Fatalf("staging removed by concurrent save: %v", err)
	}
}

func TestAbandonedStagingIsReclaimed(t *testing.T) {
	repo := newTestRepository(t, "")
	staging := manuallyCreateStaging(t, repo, "source", "dead_owner")

	if err := repo.reclaimStaging("source", false); err != nil {
		t.Fatalf("reclaimStaging: %v", err)
	}
	if _, err := os.Stat(staging); !os.IsNotExist(err) {
		t.Fatalf("abandoned staging still present: err=%v", err)
	}
}

func TestAbandonedRenderStagingIsReclaimed(t *testing.T) {
	repo := newTestRepository(t, "")
	staging := manuallyCreateStaging(t, repo, "render", "dead_owner")

	if err := repo.reclaimStaging("render", false); err != nil {
		t.Fatalf("reclaimStaging: %v", err)
	}
	if _, err := os.Stat(staging); !os.IsNotExist(err) {
		t.Fatalf("abandoned render staging still present: err=%v", err)
	}
}

func TestDeadLeaseFileIsRemovedAndOwnerStagingReclaimed(t *testing.T) {
	if !processLocksSupported() {
		t.Skip("advisory locks unavailable")
	}
	repo := newTestRepository(t, "")
	// Simulate a process that exited: a lease file exists but its lock is not
	// held, and its staging directory still holds media.
	leasePath := repo.path("tmp", leasePrefix+"dead_beef"+leaseSuffix)
	if err := os.WriteFile(leasePath, nil, 0o640); err != nil {
		t.Fatalf("write lease: %v", err)
	}
	staging := manuallyCreateStaging(t, repo, "source", "dead_beef")

	if err := repo.reclaimStaging("source", false); err != nil {
		t.Fatalf("reclaimStaging: %v", err)
	}
	if _, err := os.Stat(staging); !os.IsNotExist(err) {
		t.Fatalf("staging of dead lease still present: err=%v", err)
	}
	if _, err := os.Stat(leasePath); !os.IsNotExist(err) {
		t.Fatalf("dead lease file still present: err=%v", err)
	}
}

func TestLegacyOwnerlessStagingOnlyReclaimedWhenOld(t *testing.T) {
	repo := newTestRepository(t, "")
	// A pre-fix staging directory uses the old "<kind>-<rand>" naming and has
	// no owner token.
	legacy, err := os.MkdirTemp(repo.path("tmp"), "source-*")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	if err := os.WriteFile(filepath.Join(legacy, "source.wav"), []byte("audio"), 0o640); err != nil {
		t.Fatalf("write media: %v", err)
	}

	// Fresh ownerless staging could belong to a live pre-fix process.
	if err := repo.reclaimStaging("source", true); err != nil {
		t.Fatalf("startup reclaimStaging: %v", err)
	}
	if _, err := os.Stat(legacy); err != nil {
		t.Fatalf("fresh legacy staging reclaimed too eagerly: %v", err)
	}

	// Once it is old enough to be certainly abandoned, even a conservative
	// global sweep reclaims it.
	old := time.Now().Add(-2 * legacyStagingMinAge)
	if err := os.Chtimes(filepath.Join(legacy, "source.wav"), old, old); err != nil {
		t.Fatalf("chtimes: %v", err)
	}
	if err := os.Chtimes(legacy, old, old); err != nil {
		t.Fatalf("chtimes dir: %v", err)
	}
	if err := repo.reclaimStaging("source", true); err != nil {
		t.Fatalf("startup reclaimStaging: %v", err)
	}
	if _, err := os.Stat(legacy); !os.IsNotExist(err) {
		t.Fatalf("old legacy staging survived startup reclamation: %v", err)
	}

	// Live per-request reclamation never touches ownerless staging.
	staleLive, err := os.MkdirTemp(repo.path("tmp"), "source-*")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	if err := os.WriteFile(filepath.Join(staleLive, "source.wav"), []byte("audio"), 0o640); err != nil {
		t.Fatalf("write media: %v", err)
	}
	if err := os.Chtimes(filepath.Join(staleLive, "source.wav"), old, old); err != nil {
		t.Fatalf("chtimes: %v", err)
	}
	if err := os.Chtimes(staleLive, old, old); err != nil {
		t.Fatalf("chtimes dir: %v", err)
	}
	if err := repo.reclaimStaging("source", false); err != nil {
		t.Fatalf("live reclaimStaging: %v", err)
	}
	if _, err := os.Stat(staleLive); err != nil {
		t.Fatalf("ownerless staging reclaimed during live operation: %v", err)
	}
}

func TestAnonymousStagingIsNeverReclaimed(t *testing.T) {
	repo := newTestRepository(t, "")
	staging := manuallyCreateStaging(t, repo, "source", anonymousOwner)
	for _, allowLegacy := range []bool{false, true} {
		if err := repo.reclaimStaging("source", allowLegacy); err != nil {
			t.Fatalf("reclaimStaging: %v", err)
		}
		if _, err := os.Stat(staging); err != nil {
			t.Fatalf("anonymous staging reclaimed (allowLegacy=%v): %v", allowLegacy, err)
		}
	}
}

func TestStaleWriteTempReclaimedButFreshPreserved(t *testing.T) {
	repo := newTestRepository(t, "")
	directory := repo.path("timelines")
	fresh := filepath.Join(directory, ".write-fresh")
	stale := filepath.Join(directory, ".write-stale")
	for _, path := range []string{fresh, stale} {
		if err := os.WriteFile(path, []byte("partial"), 0o640); err != nil {
			t.Fatalf("write temp: %v", err)
		}
	}
	old := time.Now().Add(-2 * staleWriteTempAge)
	if err := os.Chtimes(stale, old, old); err != nil {
		t.Fatalf("chtimes: %v", err)
	}
	if err := repo.reclaimStaleWriteTemps(); err != nil {
		t.Fatalf("reclaimStaleWriteTemps: %v", err)
	}
	if _, err := os.Stat(stale); !os.IsNotExist(err) {
		t.Fatalf("stale write temp survived: %v", err)
	}
	if _, err := os.Stat(fresh); err != nil {
		t.Fatalf("fresh write temp was removed: %v", err)
	}
}

func TestPublishedArtifactsAreNeverReclaimed(t *testing.T) {
	root := t.TempDir()
	publisher := newTestRepository(t, root)
	peer := newTestRepository(t, root)

	if err := publisher.SaveSource(domain.SourceAsset{ID: "kept_source"}, []byte("wav")); err != nil {
		t.Fatalf("SaveSource: %v", err)
	}
	if err := peer.ReclaimUnpublished(); err != nil {
		t.Fatalf("ReclaimUnpublished: %v", err)
	}
	if _, err := os.Stat(publisher.path("sources", "kept_source", "source.wav")); err != nil {
		t.Fatalf("published source was removed: %v", err)
	}
}

func TestReclaimUnpublishedLeavesLivePeerStaging(t *testing.T) {
	root := t.TempDir()
	starter := newTestRepository(t, root)
	peer := newTestRepository(t, root)

	lease, err := peer.acquireLease()
	if err != nil || lease == nil {
		t.Fatalf("peer lease unavailable: err=%v lease=%v", err, lease)
	}
	staging := manuallyCreateStaging(t, starter, "render", lease.token)
	lease.track(staging)
	defer func() {
		lease.release(staging)
		_ = os.RemoveAll(staging)
	}()

	if err := starter.ReclaimUnpublished(); err != nil {
		t.Fatalf("ReclaimUnpublished: %v", err)
	}
	if _, err := os.Stat(staging); err != nil {
		t.Fatalf("live peer staging was reclaimed at startup: %v", err)
	}
}

func TestStagingNamesCarryOwner(t *testing.T) {
	repo := newTestRepository(t, "")
	lease, err := repo.acquireLease()
	if err != nil || lease == nil {
		t.Fatalf("lease unavailable: err=%v lease=%v", err, lease)
	}
	pattern, gotLease, err := repo.stagingPattern("source")
	if err != nil {
		t.Fatalf("stagingPattern: %v", err)
	}
	if gotLease != lease {
		t.Fatalf("stagingPattern returned a different lease")
	}
	directory, err := os.MkdirTemp(repo.path("tmp"), pattern)
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	defer os.RemoveAll(directory)
	name := filepath.Base(directory)
	owner, ok := ownerOfStaging(name)
	if !ok || owner != lease.token {
		t.Fatalf("owner of %q = %q (ok=%v), want %q", name, owner, ok, lease.token)
	}
	if !repo.ownerIsAlive(name, false, time.Now()) {
		t.Fatalf("own live staging reported as abandoned")
	}
}

func TestLeaseTokenIsStable(t *testing.T) {
	repo := newTestRepository(t, "")
	first, err := repo.acquireLease()
	if err != nil || first == nil {
		t.Fatalf("lease unavailable: err=%v", err)
	}
	second, err := repo.acquireLease()
	if err != nil {
		t.Fatalf("acquireLease again: %v", err)
	}
	if first != second || first.token == "" {
		t.Fatalf("lease token not stable: %v %v", first, second)
	}
	// Give the filesystem a moment to settle, then confirm the lease file
	// exists under the stable token-derived name.
	time.Sleep(10 * time.Millisecond)
	if _, err := os.Stat(repo.leasePathForToken(first.token)); err != nil {
		t.Fatalf("lease file missing: %v", err)
	}
}

func TestStagingSurvivesSequentialCrossRepositorySaves(t *testing.T) {
	root := t.TempDir()
	publisher := newTestRepository(t, root)
	other := newTestRepository(t, root)

	if err := publisher.SaveSource(domain.SourceAsset{ID: "source_a"}, []byte("wav")); err != nil {
		t.Fatalf("first save: %v", err)
	}
	if err := other.SaveSource(domain.SourceAsset{ID: "source_b"}, []byte("wav")); err != nil {
		t.Fatalf("second save: %v", err)
	}
	for _, id := range []string{"source_a", "source_b"} {
		if _, err := os.Stat(publisher.path("sources", id, "source.wav")); err != nil {
			t.Fatalf("source %s missing: %v", id, err)
		}
	}
}
