package snapshotter

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestGetCommitUpperDir(t *testing.T) {
	// This test verifies that getCommitUpperDir correctly determines
	// block vs overlay mode based on rwlayer.img existence and mount state.
	ctx := context.Background()

	t.Run("overlay mode when no rwlayer.img", func(t *testing.T) {
		root := t.TempDir()
		s := newTestSnapshotterWithRoot(t, root)

		// Create snapshot directory without rwlayer.img
		snapshotDir := filepath.Join(root, "snapshots", "test-id")
		fsDir := filepath.Join(snapshotDir, "fs")
		if err := os.MkdirAll(fsDir, 0o755); err != nil {
			t.Fatal(err)
		}

		upperDir, cleanup, prune, err := s.getCommitUpperDir(ctx, "test-id")
		if err != nil {
			t.Fatalf("getCommitUpperDir: %v", err)
		}
		defer cleanup()

		// Should return overlay upper dir (fs/)
		expectedUpper := filepath.Join(snapshotDir, "fs")
		if upperDir != expectedUpper {
			t.Errorf("upperDir = %q, want %q", upperDir, expectedUpper)
		}
		if !prune {
			t.Error("prune = false, want true for overlay mode")
		}
	})

	t.Run("block mode when rwlayer.img exists and upper dir exists", func(t *testing.T) {
		root := t.TempDir()
		s := newTestSnapshotterWithRoot(t, root)

		// Create snapshot directory with rwlayer.img and rw/upper/
		snapshotDir := filepath.Join(root, "snapshots", "test-id")
		rwDir := filepath.Join(snapshotDir, "rw")
		upperDir := filepath.Join(rwDir, "upper")
		if err := os.MkdirAll(upperDir, 0o755); err != nil {
			t.Fatal(err)
		}

		rwLayer := filepath.Join(snapshotDir, "rwlayer.img")
		if err := os.WriteFile(rwLayer, []byte("fake ext4"), 0o644); err != nil {
			t.Fatal(err)
		}

		result, cleanup, prune, err := s.getCommitUpperDir(ctx, "test-id")
		if err != nil {
			t.Fatalf("getCommitUpperDir: %v", err)
		}
		defer cleanup()

		// Should return block upper dir (rw/upper/)
		if result != upperDir {
			t.Errorf("upperDir = %q, want %q", result, upperDir)
		}
		if !prune {
			t.Error("prune = false, want true for mounted extract upper")
		}
	})

	// In block mode without an accessible rw/upper, getCommitUpperDir must
	// NOT fall back to the (empty) fs/ directory: doing so would silently
	// commit an empty layer and lose the container's changes. It mounts the
	// ext4 instead, which fails loudly for an invalid image. The same
	// scenarios used to return fs/ (or rw/) before this was fixed.
	for _, tc := range []struct {
		name  string
		setup func(t *testing.T, snapshotDir string)
	}{
		{
			name: "errors when rw has content but no upper subdir",
			setup: func(t *testing.T, snapshotDir string) {
				rwDir := filepath.Join(snapshotDir, "rw")
				if err := os.MkdirAll(rwDir, 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(rwDir, "somefile"), []byte("data"), 0o644); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "errors when rw is empty (ext4 not mounted)",
			setup: func(t *testing.T, snapshotDir string) {
				if err := os.MkdirAll(filepath.Join(snapshotDir, "rw"), 0o755); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name:  "errors when rw does not exist",
			setup: func(t *testing.T, snapshotDir string) {},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			s := newTestSnapshotterWithRoot(t, root)

			snapshotDir := filepath.Join(root, "snapshots", "test-id")
			fsDir := filepath.Join(snapshotDir, "fs")
			if err := os.MkdirAll(fsDir, 0o755); err != nil {
				t.Fatal(err)
			}
			tc.setup(t, snapshotDir)

			// rwlayer.img is not a valid ext4 image, so the read-only mount
			// attempt must fail; what matters is that no fs/ fallback happens.
			rwLayer := filepath.Join(snapshotDir, "rwlayer.img")
			if err := os.WriteFile(rwLayer, []byte("fake ext4"), 0o644); err != nil {
				t.Fatal(err)
			}

			result, cleanup, _, err := s.getCommitUpperDir(ctx, "test-id")
			if err == nil {
				cleanup()
				t.Fatalf("getCommitUpperDir = %q, want error (no silent fallback to fs/)", result)
			}
		})
	}
}
