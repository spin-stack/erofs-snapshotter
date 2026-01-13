package snapshotter

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/containerd/containerd/v2/core/snapshots"
)

// TestLayerBlobNotFoundErrorAs verifies errors.As works correctly for type matching.
// Note: We use errors.As (not errors.Is) for structural error types per Go idioms.
func TestLayerBlobNotFoundErrorAs(t *testing.T) {
	err := &LayerBlobNotFoundError{
		ID:       "test-123",
		Dir:      "/test/path",
		Searched: []string{"*.erofs"},
	}

	// Test errors.As for type-based matching
	var target *LayerBlobNotFoundError
	if !errors.As(err, &target) {
		t.Error("errors.As should match LayerBlobNotFoundError")
	}
	if target.ID != "test-123" {
		t.Errorf("expected snapshot ID test-123, got %s", target.ID)
	}

	// Test that wrapped error can be unwrapped with errors.As
	wrapped := &CommitConversionError{
		ID:       "commit-test",
		UpperDir: "/upper",
		Mode:     CommitModeOverlay,
		Cause:    err,
	}

	var wrappedTarget *LayerBlobNotFoundError
	if !errors.As(wrapped, &wrappedTarget) {
		t.Error("errors.As should find LayerBlobNotFoundError in chain")
	}
	if wrappedTarget.ID != "test-123" {
		t.Errorf("expected snapshot ID test-123, got %s", wrappedTarget.ID)
	}
}

// TestErrorChainDepth verifies deep error chains work correctly.
func TestErrorChainDepth(t *testing.T) {
	// Create a 2-level error chain
	level1 := errors.New("root cause: filesystem full")
	level2 := &CommitConversionError{
		ID:       "snap-abc",
		UpperDir: "/var/lib/snapshotter/abc/upper",
		Mode:     CommitModeBlock,
		Cause:    level1,
	}

	// Should find root cause
	if !errors.Is(level2, level1) {
		t.Error("should find root error through error chain")
	}

	// Error message should include context
	msg := level2.Error()
	if !strings.Contains(msg, "snap-abc") {
		t.Error("error message should contain snapshot ID")
	}
}

// TestReverseStringsEmpty verifies reverseStrings handles empty/nil slices.
func TestReverseStringsEmpty(t *testing.T) {
	// Empty slice
	result := reverseStrings([]string{})
	if result != nil {
		t.Errorf("expected nil for empty slice, got %v", result)
	}

	// Nil slice
	result = reverseStrings(nil)
	if result != nil {
		t.Errorf("expected nil for nil slice, got %v", result)
	}
}

// TestFindLayerBlobNotFound verifies findLayerBlob returns correct error type.
func TestFindLayerBlobNotFound(t *testing.T) {
	root := t.TempDir()
	s := newTestSnapshotterWithRoot(t, root)

	// Create empty snapshot directory
	snapshotDir := filepath.Join(root, "snapshots", "missing-blob")
	if err := os.MkdirAll(snapshotDir, 0755); err != nil {
		t.Fatal(err)
	}

	// Try to find non-existent layer blob
	_, err := s.findLayerBlob("missing-blob")
	if err == nil {
		t.Fatal("expected error for missing layer blob")
	}

	// Should be LayerBlobNotFoundError
	var notFoundErr *LayerBlobNotFoundError
	if !errors.As(err, &notFoundErr) {
		t.Errorf("expected LayerBlobNotFoundError, got %T: %v", err, err)
	}

	if notFoundErr.ID != "missing-blob" {
		t.Errorf("expected snapshot ID 'missing-blob', got %q", notFoundErr.ID)
	}

	if len(notFoundErr.Searched) == 0 {
		t.Error("expected Searched patterns to be populated")
	}
}

// TestFindLayerBlobDigestNaming verifies digest-based blob naming works.
func TestFindLayerBlobDigestNaming(t *testing.T) {
	root := t.TempDir()
	s := newTestSnapshotterWithRoot(t, root)

	// Create snapshot directory with digest-based layer blob
	snapshotDir := filepath.Join(root, "snapshots", "digest-test")
	if err := os.MkdirAll(snapshotDir, 0755); err != nil {
		t.Fatal(err)
	}

	// Create digest-named layer blob (64 hex chars for sha256)
	digestBlob := filepath.Join(snapshotDir, "sha256-a3ed95caeb02ffe68cdd9fd84406680ae93d633cb16422d00e8a7c22955b46d4.erofs")
	if err := os.WriteFile(digestBlob, []byte("fake erofs"), 0644); err != nil {
		t.Fatal(err)
	}

	// Should find the digest-named blob
	found, err := s.findLayerBlob("digest-test")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if found != digestBlob {
		t.Errorf("expected %q, got %q", digestBlob, found)
	}
}

// TestFindLayerBlobFallbackNaming verifies fallback naming works.
func TestFindLayerBlobFallbackNaming(t *testing.T) {
	root := t.TempDir()
	s := newTestSnapshotterWithRoot(t, root)

	// Create snapshot directory with fallback-named layer blob
	snapshotDir := filepath.Join(root, "snapshots", "fallback-test")
	if err := os.MkdirAll(snapshotDir, 0755); err != nil {
		t.Fatal(err)
	}

	// Create fallback-named layer blob
	fallbackBlob := filepath.Join(snapshotDir, "snapshot-fallback-test.erofs")
	if err := os.WriteFile(fallbackBlob, []byte("fake erofs"), 0644); err != nil {
		t.Fatal(err)
	}

	// Should find the fallback-named blob
	found, err := s.findLayerBlob("fallback-test")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if found != fallbackBlob {
		t.Errorf("expected %q, got %q", fallbackBlob, found)
	}
}

// TestFindLayerBlobDigestPriority verifies digest-based naming takes priority.
func TestFindLayerBlobDigestPriority(t *testing.T) {
	root := t.TempDir()
	s := newTestSnapshotterWithRoot(t, root)

	// Create snapshot directory with both naming styles
	snapshotDir := filepath.Join(root, "snapshots", "priority-test")
	if err := os.MkdirAll(snapshotDir, 0755); err != nil {
		t.Fatal(err)
	}

	// Create both types of blobs
	digestBlob := filepath.Join(snapshotDir, "sha256-a3ed95caeb02ffe68cdd9fd84406680ae93d633cb16422d00e8a7c22955b46d4.erofs")
	fallbackBlob := filepath.Join(snapshotDir, "snapshot-priority-test.erofs")

	for _, blob := range []string{digestBlob, fallbackBlob} {
		if err := os.WriteFile(blob, []byte("fake erofs"), 0644); err != nil {
			t.Fatal(err)
		}
	}

	// Should prefer digest-named blob
	found, err := s.findLayerBlob("priority-test")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if found != digestBlob {
		t.Errorf("expected digest blob %q to take priority, got %q", digestBlob, found)
	}
}

// TestRemoveWithChildren verifies removing a parent with children fails.
func TestRemoveWithChildren(t *testing.T) {
	s := newTestSnapshotter(t)
	ctx := t.Context()

	// Create parent snapshot
	_, err := s.Prepare(ctx, "parent-snap", "")
	if err != nil {
		t.Fatalf("prepare parent: %v", err)
	}

	// Commit parent
	if err := s.Commit(ctx, "committed-parent", "parent-snap"); err != nil {
		t.Fatalf("commit parent: %v", err)
	}

	// Create child snapshot
	_, err = s.Prepare(ctx, "child-snap", "committed-parent")
	if err != nil {
		t.Fatalf("prepare child: %v", err)
	}

	// Try to remove parent - should fail
	err = s.Remove(ctx, "committed-parent")
	if err == nil {
		t.Error("expected error when removing parent with child")
	}

	// Child should still exist
	_, err = s.Stat(ctx, "child-snap")
	if err != nil {
		t.Errorf("child should still exist: %v", err)
	}
}

// TestStatNonExistent verifies Stat returns proper error for non-existent snapshot.
func TestStatNonExistent(t *testing.T) {
	s := newTestSnapshotter(t)
	ctx := t.Context()

	_, err := s.Stat(ctx, "does-not-exist")
	if err == nil {
		t.Error("expected error for non-existent snapshot")
	}
}

// TestMountsNonExistent verifies Mounts returns proper error for non-existent snapshot.
func TestMountsNonExistent(t *testing.T) {
	s := newTestSnapshotter(t)
	ctx := t.Context()

	_, err := s.Mounts(ctx, "does-not-exist")
	if err == nil {
		t.Error("expected error for non-existent snapshot")
	}
}

// TestViewWithNonExistentParent verifies View fails with non-existent parent.
func TestViewWithNonExistentParent(t *testing.T) {
	s := newTestSnapshotter(t)
	ctx := t.Context()

	_, err := s.View(ctx, "view-1", "non-existent-parent")
	if err == nil {
		t.Error("expected error for non-existent parent")
	}
}

// TestCommitNonExistent verifies Commit fails for non-existent active snapshot.
func TestCommitNonExistent(t *testing.T) {
	s := newTestSnapshotter(t)
	ctx := t.Context()

	err := s.Commit(ctx, "committed-name", "non-existent-active")
	if err == nil {
		t.Error("expected error for non-existent active snapshot")
	}
}

// TestCommitAlreadyCommitted verifies double commit fails.
func TestCommitAlreadyCommitted(t *testing.T) {
	s := newTestSnapshotter(t)
	ctx := t.Context()

	// Create and commit
	_, err := s.Prepare(ctx, "to-commit", "")
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}

	if err := s.Commit(ctx, "committed", "to-commit"); err != nil {
		t.Fatalf("first commit: %v", err)
	}

	// Second commit should fail (active snapshot no longer exists)
	err = s.Commit(ctx, "committed-again", "to-commit")
	if err == nil {
		t.Error("expected error for double commit")
	}
}

// TestPrepareAfterCommit verifies Prepare can use committed snapshot as parent.
func TestPrepareAfterCommit(t *testing.T) {
	s := newTestSnapshotter(t)
	ctx := t.Context()

	// Create and commit base layer
	_, err := s.Prepare(ctx, "base-active", "")
	if err != nil {
		t.Fatalf("prepare base: %v", err)
	}

	if err := s.Commit(ctx, "base-committed", "base-active"); err != nil {
		t.Fatalf("commit base: %v", err)
	}

	// Prepare child using committed parent
	mounts, err := s.Prepare(ctx, "child-active", "base-committed")
	if err != nil {
		t.Fatalf("prepare child: %v", err)
	}

	if len(mounts) == 0 {
		t.Error("expected at least one mount")
	}

	// Verify child info
	info, err := s.Stat(ctx, "child-active")
	if err != nil {
		t.Fatalf("stat child: %v", err)
	}

	if info.Kind != snapshots.KindActive {
		t.Errorf("expected KindActive, got %v", info.Kind)
	}
	if info.Parent != "base-committed" {
		t.Errorf("expected parent 'base-committed', got %q", info.Parent)
	}
}

// TestCleanupRemovesOrphanedDirectories verifies Cleanup removes orphaned snapshot directories.
func TestCleanupRemovesOrphanedDirectories(t *testing.T) {
	if !checkBlockModeRequirements(t) {
		t.Skip("mkfs.ext4 not available")
	}

	root := t.TempDir()
	ss, err := NewSnapshotter(root, WithDefaultSize(1024*1024))
	if err != nil {
		t.Fatalf("create snapshotter: %v", err)
	}
	defer ss.Close()

	// Get internal snapshotter for Cleanup method
	internal := ss.(*snapshotter)
	ctx := t.Context()

	// Create a snapshot through normal means
	_, err = ss.Prepare(ctx, "normal-snapshot", "")
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}

	// Create an orphaned directory (not in metadata)
	orphanDir := filepath.Join(root, "snapshots", "orphan-123")
	if err := os.MkdirAll(orphanDir, 0755); err != nil {
		t.Fatalf("create orphan dir: %v", err)
	}

	// Verify orphan exists
	if _, err := os.Stat(orphanDir); err != nil {
		t.Fatalf("orphan should exist: %v", err)
	}

	// Run cleanup
	if err := internal.Cleanup(ctx); err != nil {
		t.Fatalf("cleanup: %v", err)
	}

	// Verify orphan is removed
	if _, err := os.Stat(orphanDir); !os.IsNotExist(err) {
		t.Error("orphan directory should be removed after cleanup")
	}

	// Normal snapshot should still exist
	_, err = ss.Stat(ctx, "normal-snapshot")
	if err != nil {
		t.Errorf("normal snapshot should still exist: %v", err)
	}
}
