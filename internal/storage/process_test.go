package storage

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"voice-timeline-renderer/internal/domain"
)

// TestConcurrentProcessesDoNotReclaimEachOthersStaging reproduces the reported
// failure: two independent processes share one data directory. One process
// stages media and stays alive while the other runs source, render and
// startup reclamation. The other must not delete the live process's staging.
// Once the owner exits, a later reclamation safely cleans the abandoned media.
func TestConcurrentProcessesDoNotReclaimEachOthersStaging(t *testing.T) {
	if os.Getenv("VOICE_STAGING_CHILD") == "hold" {
		runHoldingChild(t)
		return
	}
	if !processLocksSupported() {
		t.Skip("advisory locks unavailable")
	}
	root := t.TempDir()

	child := exec.Command(os.Args[0], "-test.run=TestConcurrentProcessesDoNotReclaimEachOthersStaging")
	child.Env = append(os.Environ(), "VOICE_STAGING_CHILD=hold", "VOICE_STAGING_ROOT="+root)
	var output strings.Builder
	child.Stdout = &output
	child.Stderr = &output
	if err := child.Start(); err != nil {
		t.Fatalf("start holding child: %v", err)
	}
	exited := make(chan struct{})
	go func() {
		_ = child.Wait()
		close(exited)
	}()
	defer func() {
		_ = os.WriteFile(filepath.Join(root, "verified"), []byte("ok\n"), 0o640)
		select {
		case <-exited:
		case <-time.After(10 * time.Second):
			_ = child.Process.Kill()
			<-exited
		}
	}()

	waitForFile(t, filepath.Join(root, "ready"), 15*time.Second)
	staging := strings.TrimSpace(readFile(t, filepath.Join(root, "staging-path")))

	// A second, independent repository opens the same data directory while
	// the holding process is alive.
	peer, err := NewRepository(root)
	if err != nil {
		t.Fatalf("peer NewRepository: %v", err)
	}
	if err := peer.SaveSource(domain.SourceAsset{ID: "peer_source"}, []byte("peer wav")); err != nil {
		t.Fatalf("peer SaveSource: %v", err)
	}
	if err := peer.ReclaimUnpublished(); err != nil {
		t.Fatalf("peer ReclaimUnpublished: %v", err)
	}
	if _, err := os.Stat(filepath.Join(staging, "source.wav")); err != nil {
		t.Fatalf("live staging was reclaimed while its owner was alive: %v", err)
	}
	if _, err := os.Stat(peer.path("sources", "peer_source", "source.wav")); err != nil {
		t.Fatalf("peer published source missing: %v", err)
	}

	// Signal the child to exit. Its staged media is now provably abandoned.
	mustWrite(t, filepath.Join(root, "verified"), "ok\n")
	select {
	case <-exited:
	case <-time.After(10 * time.Second):
		t.Fatalf("holding child did not exit")
	}
	if _, err := os.Stat(staging); err == nil {
		// Expected: reclamation never ran after the owner exited. Reclaiming
		// now must clean it without touching published media.
		if err := peer.ReclaimUnpublished(); err != nil {
			t.Fatalf("post-exit ReclaimUnpublished: %v", err)
		}
		if _, err := os.Stat(staging); !os.IsNotExist(err) {
			t.Fatalf("abandoned staging was not reclaimed: %v", err)
		}
	} else if !os.IsNotExist(err) {
		t.Fatalf("unexpected stat error: %v", err)
	}
	if _, err := os.Stat(peer.path("sources", "peer_source", "source.wav")); err != nil {
		t.Fatalf("published source lost during reclamation: %v", err)
	}
}

func waitForFile(t *testing.T, path string, limit time.Duration) {
	t.Helper()
	deadline := time.Now().Add(limit)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", filepath.Base(path))
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	payload, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(payload)
}

func mustWrite(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o640); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func runHoldingChild(t *testing.T) {
	t.Helper()
	root := os.Getenv("VOICE_STAGING_ROOT")
	repo, err := NewRepository(root)
	if err != nil {
		t.Fatalf("child NewRepository: %v", err)
	}
	pattern, lease, err := repo.stagingPattern("source")
	if err != nil {
		t.Fatalf("child stagingPattern: %v", err)
	}
	staging, err := os.MkdirTemp(repo.path("tmp"), pattern)
	if err != nil {
		t.Fatalf("child MkdirTemp: %v", err)
	}
	lease.track(staging)
	if err := os.WriteFile(filepath.Join(staging, "source.wav"), []byte("live audio"), 0o640); err != nil {
		t.Fatalf("child write media: %v", err)
	}
	mustWrite(t, filepath.Join(root, "staging-path"), staging+"\n")
	mustWrite(t, filepath.Join(root, "ready"), "ok\n")

	// Stay alive until the parent verifies the concurrent reclamation and
	// signals that the child may exit, simulating an interrupted process.
	waitForFile(t, filepath.Join(root, "verified"), 20*time.Second)
}
