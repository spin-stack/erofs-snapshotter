package snapshotter

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/containerd/containerd/v2/core/snapshots"
)

// TestReverseStringsDoesNotMutate verifies reverseStrings returns a new slice.
// This is a property test, not a concurrency test - reverseStrings is safe
// for concurrent use because it allocates a new slice on each call.
func TestReverseStringsDoesNotMutate(t *testing.T) {
	original := []string{"layer5", "layer4", "layer3", "layer2", "layer1"}
	originalCopy := make([]string, len(original))
	copy(originalCopy, original)

	result := reverseStrings(original)

	// Verify result is reversed
	if result[0] != "layer1" || result[4] != "layer5" {
		t.Errorf("reverseStrings returned wrong result: %v", result)
	}

	// Verify original is unchanged
	for i := range original {
		if original[i] != originalCopy[i] {
			t.Errorf("original was modified at index %d: got %s, want %s",
				i, original[i], originalCopy[i])
		}
	}

	// Verify result is a different slice (not aliased)
	result[0] = "changed"
	if original[4] == "changed" {
		t.Error("result slice aliases original slice")
	}
}

// TestConcurrentPrepare verifies concurrent Prepare calls don't race.
func TestConcurrentPrepare(t *testing.T) {
	s := newTestSnapshotter(t)
	ctx := t.Context()
	const numGoroutines = 10

	var wg sync.WaitGroup
	errors := make(chan error, numGoroutines)

	for i := range numGoroutines {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			key := fmt.Sprintf("concurrent-prepare-%d", id)
			_, err := s.Prepare(ctx, key, "")
			if err != nil {
				errors <- fmt.Errorf("prepare %d: %w", id, err)
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Errorf("unexpected error: %v", err)
	}

	// Verify all snapshots exist
	var count int
	err := s.Walk(ctx, func(_ context.Context, info snapshots.Info) error {
		count++
		return nil
	})
	if err != nil {
		t.Fatalf("walk failed: %v", err)
	}
	if count < numGoroutines {
		t.Errorf("expected at least %d snapshots, got %d", numGoroutines, count)
	}
}

// TestConcurrentView verifies concurrent View calls on the same parent don't race.
// This tests the fsmeta generation coordination (placeholder file).
func TestConcurrentView(t *testing.T) {
	s := newTestSnapshotter(t)
	ctx := t.Context()

	// Create a base snapshot to use as parent
	_, err := s.Prepare(ctx, "base-for-views", "")
	if err != nil {
		t.Fatalf("prepare base: %v", err)
	}

	// Commit the base (needed for View)
	if err := s.Commit(ctx, "committed-base", "base-for-views"); err != nil {
		t.Fatalf("commit base: %v", err)
	}

	// Create multiple views concurrently - all trigger fsmeta generation for same parent
	const numGoroutines = 10
	var wg sync.WaitGroup
	errors := make(chan error, numGoroutines)

	for i := range numGoroutines {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			key := fmt.Sprintf("concurrent-view-%d", id)
			_, err := s.View(ctx, key, "committed-base")
			if err != nil {
				errors <- fmt.Errorf("view %d: %w", id, err)
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Errorf("unexpected error: %v", err)
	}
}

// TestConcurrentRemove verifies concurrent Remove calls work correctly.
func TestConcurrentRemove(t *testing.T) {
	s := newTestSnapshotter(t)
	ctx := t.Context()
	const numSnapshots = 10

	// Create snapshots to remove
	keys := make([]string, numSnapshots)
	for i := range numSnapshots {
		key := fmt.Sprintf("to-remove-%d", i)
		keys[i] = key
		if _, err := s.Prepare(ctx, key, ""); err != nil {
			t.Fatalf("prepare %d: %v", i, err)
		}
	}

	// Remove all concurrently
	var wg sync.WaitGroup
	errors := make(chan error, numSnapshots)

	for i, key := range keys {
		wg.Add(1)
		go func(id int, k string) {
			defer wg.Done()
			if err := s.Remove(ctx, k); err != nil {
				errors <- fmt.Errorf("remove %d (%s): %w", id, k, err)
			}
		}(i, key)
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Errorf("unexpected error: %v", err)
	}

	// Verify all snapshots removed
	var remaining int
	err := s.Walk(ctx, func(_ context.Context, _ snapshots.Info) error {
		remaining++
		return nil
	})
	if err != nil {
		t.Fatalf("walk failed: %v", err)
	}
	if remaining != 0 {
		t.Errorf("expected 0 snapshots after removal, got %d", remaining)
	}
}

// TestFsmetaLockFileRace verifies that concurrent fsmeta generation
// uses the lock file correctly (only one wins).
func TestFsmetaLockFileRace(t *testing.T) {
	root := t.TempDir()
	s := &snapshotter{root: root}

	// Create snapshot directory
	snapshotDir := filepath.Join(root, "snapshots", "test-parent")
	if err := os.MkdirAll(snapshotDir, 0755); err != nil {
		t.Fatal(err)
	}

	// Run multiple goroutines trying to acquire the lock file
	const numGoroutines = 20
	var wg sync.WaitGroup
	winners := make(chan int, numGoroutines)

	lockFile := s.fsMetaPath("test-parent") + ".lock"

	for i := range numGoroutines {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			// Try to create lock file atomically (same pattern as generateFsMeta)
			f, err := os.OpenFile(lockFile, os.O_CREATE|os.O_EXCL, 0600)
			if err == nil {
				winners <- id
				f.Close()
			}
			// Others get os.ErrExist - that's expected
		}(i)
	}

	wg.Wait()
	close(winners)

	// Exactly one goroutine should win
	winnerCount := 0
	for range winners {
		winnerCount++
	}

	if winnerCount != 1 {
		t.Errorf("expected exactly 1 winner for lock file creation, got %d", winnerCount)
	}

	// Verify lock file exists
	if _, err := os.Stat(lockFile); err != nil {
		t.Errorf("lock file should exist: %v", err)
	}
}

// TestFsmetaAtomicRename verifies the atomic rename pattern for fsmeta generation.
func TestFsmetaAtomicRename(t *testing.T) {
	root := t.TempDir()
	s := &snapshotter{root: root}

	// Create snapshot directory
	snapshotDir := filepath.Join(root, "snapshots", "test-parent")
	if err := os.MkdirAll(snapshotDir, 0755); err != nil {
		t.Fatal(err)
	}

	fsmetaPath := s.fsMetaPath("test-parent")
	vmdkPath := s.vmdkPath("test-parent")
	tmpMeta := fsmetaPath + ".tmp"
	tmpVmdk := vmdkPath + ".tmp"

	// Simulate successful generation: create temp files
	if err := os.WriteFile(tmpMeta, []byte("fsmeta content"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(tmpVmdk, []byte("vmdk content"), 0644); err != nil {
		t.Fatal(err)
	}

	// Atomic rename (same order as generateFsMeta)
	if err := os.Rename(tmpMeta, fsmetaPath); err != nil {
		t.Fatalf("rename fsmeta: %v", err)
	}
	if err := os.Rename(tmpVmdk, vmdkPath); err != nil {
		t.Fatalf("rename vmdk: %v", err)
	}

	// Verify final files exist
	if _, err := os.Stat(fsmetaPath); err != nil {
		t.Errorf("fsmeta should exist: %v", err)
	}
	if _, err := os.Stat(vmdkPath); err != nil {
		t.Errorf("vmdk should exist: %v", err)
	}

	// Verify temp files are gone
	if _, err := os.Stat(tmpMeta); !os.IsNotExist(err) {
		t.Error("temp fsmeta should not exist after rename")
	}
	if _, err := os.Stat(tmpVmdk); !os.IsNotExist(err) {
		t.Error("temp vmdk should not exist after rename")
	}
}

// TestFixVmdkPaths verifies the VMDK path replacement function.
func TestFixVmdkPaths(t *testing.T) {
	tmpDir := t.TempDir()
	vmdkFile := filepath.Join(tmpDir, "test.vmdk")

	// Sample VMDK content with temp path
	vmdkContent := `# Disk DescriptorFile
version=1
CID=91702505
parentCID=ffffffff
createType="twoGbMaxExtentFlat"
# Extent description
RW 232 FLAT "/var/lib/snapshots/11/fsmeta.erofs.tmp" 0
RW 15944 FLAT "/var/lib/snapshots/7/layer1.erofs" 0
# The Disk Data Base
#DDB
ddb.virtualHWVersion = "4"
`

	if err := os.WriteFile(vmdkFile, []byte(vmdkContent), 0644); err != nil {
		t.Fatal(err)
	}

	// Fix the path
	oldPath := "/var/lib/snapshots/11/fsmeta.erofs.tmp"
	newPath := "/var/lib/snapshots/11/fsmeta.erofs"
	if err := fixVmdkPaths(vmdkFile, oldPath, newPath); err != nil {
		t.Fatalf("fixVmdkPaths: %v", err)
	}

	// Read back and verify
	content, err := os.ReadFile(vmdkFile)
	if err != nil {
		t.Fatal(err)
	}

	contentStr := string(content)
	if strings.Contains(contentStr, ".tmp") {
		t.Error("VMDK should not contain .tmp after fix")
	}
	if !strings.Contains(contentStr, "fsmeta.erofs") {
		t.Error("VMDK should contain final fsmeta.erofs path")
	}
	// Verify other paths are unchanged
	if !strings.Contains(contentStr, "layer1.erofs") {
		t.Error("VMDK should still contain layer1.erofs path")
	}
}
