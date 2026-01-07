package snapshotter

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestResolveCommitSource(t *testing.T) {
	// This test verifies that resolveCommitSource correctly determines
	// block vs overlay mode based on rwlayer.img existence.

	t.Run("overlay mode when no rwlayer.img", func(t *testing.T) {
		root := t.TempDir()
		s := &snapshotter{root: root}

		// Create snapshot directory without rwlayer.img
		snapshotDir := filepath.Join(root, "snapshots", "test-id")
		fsDir := filepath.Join(snapshotDir, "fs")
		if err := os.MkdirAll(fsDir, 0755); err != nil {
			t.Fatal(err)
		}

		source, err := s.resolveCommitSource(context.Background(), "test-id")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Should return overlay upper dir (fs/)
		expectedUpper := filepath.Join(snapshotDir, "fs")
		if source.upperDir != expectedUpper {
			t.Errorf("upperDir = %q, want %q", source.upperDir, expectedUpper)
		}

		// Cleanup should be a no-op (returns nil)
		if err := source.cleanup(); err != nil {
			t.Errorf("cleanup() should not error: %v", err)
		}
	})

	t.Run("block mode when rwlayer.img exists", func(t *testing.T) {
		root := t.TempDir()
		s := &snapshotter{root: root}

		// Create snapshot directory with rwlayer.img
		snapshotDir := filepath.Join(root, "snapshots", "test-id")
		rwDir := filepath.Join(snapshotDir, "rw")
		if err := os.MkdirAll(rwDir, 0755); err != nil {
			t.Fatal(err)
		}

		rwLayer := filepath.Join(snapshotDir, "rwlayer.img")
		if err := os.WriteFile(rwLayer, []byte("fake ext4"), 0644); err != nil {
			t.Fatal(err)
		}

		// Note: This will fail at mount step because we don't have a real ext4 image,
		// but the important thing is that it attempts block mode (not overlay).
		_, err := s.resolveCommitSource(context.Background(), "test-id")

		// We expect an error because mounting a fake file fails,
		// but verify it's a mount error (block mode was attempted)
		if err == nil {
			t.Fatal("expected error when mounting fake rwlayer.img")
		}

		// Should be a BlockMountError (indicating block mode was attempted)
		var blockErr *BlockMountError
		if !errors.As(err, &blockErr) {
			t.Errorf("expected BlockMountError in error chain, got: %v", err)
		}
	})
}

func TestCommitSourceFromOverlay(t *testing.T) {
	root := t.TempDir()
	s := &snapshotter{root: root}

	// Create snapshot with fs directory
	snapshotDir := filepath.Join(root, "snapshots", "test-id")
	fsDir := filepath.Join(snapshotDir, "fs")
	if err := os.MkdirAll(fsDir, 0755); err != nil {
		t.Fatal(err)
	}

	source, err := s.commitSourceFromOverlay(context.Background(), "test-id")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify upper dir is fs/
	if source.upperDir != fsDir {
		t.Errorf("upperDir = %q, want %q", source.upperDir, fsDir)
	}

	// Cleanup should be no-op
	if err := source.cleanup(); err != nil {
		t.Errorf("cleanup() should return nil: %v", err)
	}
}
