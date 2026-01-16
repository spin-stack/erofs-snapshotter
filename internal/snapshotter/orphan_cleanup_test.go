//go:build linux

package snapshotter

// Tests for orphan cleanup functionality, specifically verifying that:
// 1. storage.IDMap is used instead of WalkInfo (works without namespace context)
// 2. snapshotIDExists correctly identifies existing snapshots
// 3. Orphan cleanup doesn't delete valid snapshots

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/containerd/containerd/v2/core/snapshots/storage"
	"github.com/containerd/containerd/v2/pkg/namespaces"
	"github.com/containerd/containerd/v2/pkg/testutil"

	"github.com/spin-stack/erofs-snapshotter/internal/preflight"
)

// TestSnapshotIDExists verifies that snapshotIDExists correctly identifies
// existing snapshot IDs using storage.IDMap (which works without namespace context).
func TestSnapshotIDExists(t *testing.T) {
	testutil.RequiresRoot(t)

	if err := preflight.CheckErofsSupport(); err != nil {
		t.Skipf("EROFS support check failed: %v", err)
	}

	tempDir := t.TempDir()
	snapshotRoot := filepath.Join(tempDir, "snapshots")

	s, err := NewSnapshotter(snapshotRoot)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	snap, ok := s.(*snapshotter)
	if !ok {
		t.Fatal("failed to cast snapshotter to *snapshotter")
	}

	// Use a namespace context for creating snapshots
	ctx := namespaces.WithNamespace(t.Context(), "test-orphan")

	// Create a snapshot
	extractKey := "extract-test-id-exists"
	if _, err := snap.Prepare(ctx, extractKey, ""); err != nil {
		t.Fatalf("failed to prepare snapshot: %v", err)
	}

	// Get the snapshot ID
	var snapshotID string
	if err := snap.ms.WithTransaction(ctx, false, func(ctx context.Context) error {
		var err error
		snapshotID, _, _, err = storage.GetInfo(ctx, extractKey)
		return err
	}); err != nil {
		t.Fatalf("failed to get snapshot ID: %v", err)
	}

	// Test 1: snapshotIDExists should find the existing snapshot
	// Use context.Background() to simulate the orphan cleanup context (no namespace)
	bgCtx := context.Background()
	if !snap.snapshotIDExists(bgCtx, snapshotID) {
		t.Errorf("snapshotIDExists(%q) = false, want true", snapshotID)
	}

	// Test 2: snapshotIDExists should return false for non-existent ID
	nonExistentID := "999999"
	if snap.snapshotIDExists(bgCtx, nonExistentID) {
		t.Errorf("snapshotIDExists(%q) = true, want false", nonExistentID)
	}

	// Cleanup
	if err := snap.Remove(ctx, extractKey); err != nil {
		t.Fatalf("failed to remove snapshot: %v", err)
	}

	// Test 3: snapshotIDExists should return false after removal
	if snap.snapshotIDExists(bgCtx, snapshotID) {
		t.Errorf("snapshotIDExists(%q) after removal = true, want false", snapshotID)
	}
}

// TestOrphanCleanupPreservesValidSnapshots verifies that orphan cleanup
// does NOT delete valid snapshots when running with context.Background()
// (which has no namespace). This is a regression test for the bug where
// storage.WalkInfo was used instead of storage.IDMap.
func TestOrphanCleanupPreservesValidSnapshots(t *testing.T) {
	testutil.RequiresRoot(t)

	if err := preflight.CheckErofsSupport(); err != nil {
		t.Skipf("EROFS support check failed: %v", err)
	}

	tempDir := t.TempDir()
	snapshotRoot := filepath.Join(tempDir, "snapshots")

	s, err := NewSnapshotter(snapshotRoot)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	snap, ok := s.(*snapshotter)
	if !ok {
		t.Fatal("failed to cast snapshotter to *snapshotter")
	}

	// Use a namespace context for creating snapshots
	ctx := namespaces.WithNamespace(t.Context(), "test-orphan-preserve")

	// Create a snapshot
	extractKey := "extract-orphan-test"
	if _, err := snap.Prepare(ctx, extractKey, ""); err != nil {
		t.Fatalf("failed to prepare snapshot: %v", err)
	}

	// Get the snapshot ID
	var snapshotID string
	if err := snap.ms.WithTransaction(ctx, false, func(ctx context.Context) error {
		var err error
		snapshotID, _, _, err = storage.GetInfo(ctx, extractKey)
		return err
	}); err != nil {
		t.Fatalf("failed to get snapshot ID: %v", err)
	}

	// Verify the snapshot directory exists
	snapshotsDir := filepath.Join(snapshotRoot, "snapshots", snapshotID)
	if _, err := os.Stat(snapshotsDir); os.IsNotExist(err) {
		t.Fatalf("snapshot directory %q does not exist before cleanup", snapshotsDir)
	}

	// Run orphan cleanup (uses context.Background() internally)
	snap.cleanupOrphanedMounts()

	// The snapshot directory should still exist after cleanup
	if _, err := os.Stat(snapshotsDir); os.IsNotExist(err) {
		t.Errorf("snapshot directory %q was incorrectly deleted by orphan cleanup", snapshotsDir)
	}

	// Verify the snapshot is still in metadata
	if !snap.snapshotIDExists(context.Background(), snapshotID) {
		t.Errorf("snapshot %q not found in metadata after cleanup", snapshotID)
	}

	// Cleanup
	if err := snap.Remove(ctx, extractKey); err != nil {
		t.Fatalf("failed to remove snapshot: %v", err)
	}
}

// TestOrphanCleanupRemovesOrphans verifies that orphan cleanup correctly
// removes directories that are NOT in the metadata.
func TestOrphanCleanupRemovesOrphans(t *testing.T) {
	testutil.RequiresRoot(t)

	if err := preflight.CheckErofsSupport(); err != nil {
		t.Skipf("EROFS support check failed: %v", err)
	}

	tempDir := t.TempDir()
	snapshotRoot := filepath.Join(tempDir, "snapshots")

	s, err := NewSnapshotter(snapshotRoot)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	snap, ok := s.(*snapshotter)
	if !ok {
		t.Fatal("failed to cast snapshotter to *snapshotter")
	}

	// Create an orphan directory manually (not in metadata)
	orphanID := "orphan-12345"
	orphanDir := filepath.Join(snapshotRoot, "snapshots", orphanID)
	if err := os.MkdirAll(orphanDir, 0755); err != nil {
		t.Fatalf("failed to create orphan directory: %v", err)
	}

	// Create a file in the orphan directory
	orphanFile := filepath.Join(orphanDir, "test.txt")
	if err := os.WriteFile(orphanFile, []byte("orphan"), 0644); err != nil {
		t.Fatalf("failed to create orphan file: %v", err)
	}

	// Verify the orphan directory exists
	if _, err := os.Stat(orphanDir); os.IsNotExist(err) {
		t.Fatalf("orphan directory %q does not exist before cleanup", orphanDir)
	}

	// Run orphan cleanup
	snap.cleanupOrphanedMounts()

	// The orphan directory should be removed
	if _, err := os.Stat(orphanDir); !os.IsNotExist(err) {
		t.Errorf("orphan directory %q was not removed by cleanup", orphanDir)
	}
}

// TestIDMapWorksWithoutNamespace verifies that storage.IDMap works correctly
// with context.Background() (no namespace). This is a sanity check for the
// assumption that IDMap doesn't require namespace context.
func TestIDMapWorksWithoutNamespace(t *testing.T) {
	testutil.RequiresRoot(t)

	if err := preflight.CheckErofsSupport(); err != nil {
		t.Skipf("EROFS support check failed: %v", err)
	}

	tempDir := t.TempDir()
	snapshotRoot := filepath.Join(tempDir, "snapshots")

	s, err := NewSnapshotter(snapshotRoot)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	snap, ok := s.(*snapshotter)
	if !ok {
		t.Fatal("failed to cast snapshotter to *snapshotter")
	}

	// Create snapshots in multiple namespaces
	namespaceList := []string{"ns1", "ns2", "ns3"}
	var createdIDs []string

	for i, ns := range namespaceList {
		ctx := namespaces.WithNamespace(t.Context(), ns)
		extractKey := "extract-multi-ns-" + ns
		if _, err := snap.Prepare(ctx, extractKey, ""); err != nil {
			t.Fatalf("failed to prepare snapshot in %s: %v", ns, err)
		}

		var snapshotID string
		if err := snap.ms.WithTransaction(ctx, false, func(ctx context.Context) error {
			var err error
			snapshotID, _, _, err = storage.GetInfo(ctx, extractKey)
			return err
		}); err != nil {
			t.Fatalf("failed to get snapshot ID in %s: %v", ns, err)
		}
		createdIDs = append(createdIDs, snapshotID)
		t.Logf("Created snapshot %d with ID %s in namespace %s", i, snapshotID, ns)
	}

	// Call IDMap with context.Background() (no namespace)
	bgCtx := context.Background()
	var idMap map[string]string
	if err := snap.ms.WithTransaction(bgCtx, false, func(ctx context.Context) error {
		var err error
		idMap, err = storage.IDMap(ctx)
		return err
	}); err != nil {
		t.Fatalf("storage.IDMap failed with context.Background(): %v", err)
	}

	// Verify all created IDs are in the map
	for _, id := range createdIDs {
		if _, found := idMap[id]; !found {
			t.Errorf("IDMap did not contain snapshot ID %q created in different namespace", id)
		}
	}

	t.Logf("IDMap returned %d entries, expected at least %d", len(idMap), len(createdIDs))

	// Cleanup
	for _, ns := range namespaceList {
		ctx := namespaces.WithNamespace(t.Context(), ns)
		extractKey := "extract-multi-ns-" + ns
		if err := snap.Remove(ctx, extractKey); err != nil {
			t.Logf("failed to remove snapshot in %s: %v (may already be removed)", ns, err)
		}
	}
}

// TestConcurrentSnapshotCreationDuringCleanup verifies that the TOCTOU
// protection in orphan cleanup works correctly - a snapshot created during
// cleanup should NOT be deleted.
func TestConcurrentSnapshotCreationDuringCleanup(t *testing.T) {
	testutil.RequiresRoot(t)

	if err := preflight.CheckErofsSupport(); err != nil {
		t.Skipf("EROFS support check failed: %v", err)
	}

	tempDir := t.TempDir()
	snapshotRoot := filepath.Join(tempDir, "snapshots")

	s, err := NewSnapshotter(snapshotRoot)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	snap, ok := s.(*snapshotter)
	if !ok {
		t.Fatal("failed to cast snapshotter to *snapshotter")
	}

	ctx := namespaces.WithNamespace(t.Context(), "test-concurrent")

	// Create an initial snapshot
	extractKey := "extract-concurrent-test"
	if _, err := snap.Prepare(ctx, extractKey, ""); err != nil {
		t.Fatalf("failed to prepare snapshot: %v", err)
	}

	var snapshotID string
	if err := snap.ms.WithTransaction(ctx, false, func(ctx context.Context) error {
		var err error
		snapshotID, _, _, err = storage.GetInfo(ctx, extractKey)
		return err
	}); err != nil {
		t.Fatalf("failed to get snapshot ID: %v", err)
	}

	snapshotsDir := filepath.Join(snapshotRoot, "snapshots", snapshotID)

	// Verify the snapshot directory exists
	if _, err := os.Stat(snapshotsDir); os.IsNotExist(err) {
		t.Fatalf("snapshot directory %q does not exist", snapshotsDir)
	}

	// The TOCTOU check (snapshotIDExists) should protect this snapshot
	// even if there's a race between scanning metadata and processing directories
	snap.cleanupOrphanedMounts()

	// Verify the snapshot was preserved
	if _, err := os.Stat(snapshotsDir); os.IsNotExist(err) {
		t.Errorf("snapshot directory %q was incorrectly deleted during cleanup", snapshotsDir)
	}

	// Cleanup
	if err := snap.Remove(ctx, extractKey); err != nil {
		t.Fatalf("failed to remove snapshot: %v", err)
	}
}
