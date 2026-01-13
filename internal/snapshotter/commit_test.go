package snapshotter

import (
	"os"
	"path/filepath"
	"testing"
)

func TestGetCommitUpperDir(t *testing.T) {
	// This test verifies that getCommitUpperDir correctly determines
	// block vs overlay mode based on rwlayer.img existence and mount state.

	t.Run("overlay mode when no rwlayer.img", func(t *testing.T) {
		root := t.TempDir()
		s := newTestSnapshotterWithRoot(t, root)

		// Create snapshot directory without rwlayer.img
		snapshotDir := filepath.Join(root, "snapshots", "test-id")
		fsDir := filepath.Join(snapshotDir, "fs")
		if err := os.MkdirAll(fsDir, 0755); err != nil {
			t.Fatal(err)
		}

		upperDir := s.getCommitUpperDir("test-id")

		// Should return overlay upper dir (fs/)
		expectedUpper := filepath.Join(snapshotDir, "fs")
		if upperDir != expectedUpper {
			t.Errorf("upperDir = %q, want %q", upperDir, expectedUpper)
		}
	})

	t.Run("block mode when rwlayer.img exists and upper dir exists", func(t *testing.T) {
		root := t.TempDir()
		s := newTestSnapshotterWithRoot(t, root)

		// Create snapshot directory with rwlayer.img and rw/upper/
		snapshotDir := filepath.Join(root, "snapshots", "test-id")
		rwDir := filepath.Join(snapshotDir, "rw")
		upperDir := filepath.Join(rwDir, "upper")
		if err := os.MkdirAll(upperDir, 0755); err != nil {
			t.Fatal(err)
		}

		rwLayer := filepath.Join(snapshotDir, "rwlayer.img")
		if err := os.WriteFile(rwLayer, []byte("fake ext4"), 0644); err != nil {
			t.Fatal(err)
		}

		result := s.getCommitUpperDir("test-id")

		// Should return block upper dir (rw/upper/)
		if result != upperDir {
			t.Errorf("upperDir = %q, want %q", result, upperDir)
		}
	})

	t.Run("block mode when rw has content but no upper subdir", func(t *testing.T) {
		root := t.TempDir()
		s := newTestSnapshotterWithRoot(t, root)

		// Create snapshot directory with rwlayer.img and rw/ with some content
		snapshotDir := filepath.Join(root, "snapshots", "test-id")
		rwDir := filepath.Join(snapshotDir, "rw")
		if err := os.MkdirAll(rwDir, 0755); err != nil {
			t.Fatal(err)
		}

		// Add content to rw/ to simulate a mounted filesystem
		if err := os.WriteFile(filepath.Join(rwDir, "somefile"), []byte("data"), 0644); err != nil {
			t.Fatal(err)
		}

		rwLayer := filepath.Join(snapshotDir, "rwlayer.img")
		if err := os.WriteFile(rwLayer, []byte("fake ext4"), 0644); err != nil {
			t.Fatal(err)
		}

		result := s.getCommitUpperDir("test-id")

		// Should return mount root (rw/) when it has content but no upper/
		if result != rwDir {
			t.Errorf("upperDir = %q, want %q", result, rwDir)
		}
	})

	t.Run("falls back to overlay when rw is empty", func(t *testing.T) {
		root := t.TempDir()
		s := newTestSnapshotterWithRoot(t, root)

		// Create snapshot directory with rwlayer.img but empty rw/
		snapshotDir := filepath.Join(root, "snapshots", "test-id")
		fsDir := filepath.Join(snapshotDir, "fs")
		rwDir := filepath.Join(snapshotDir, "rw")
		if err := os.MkdirAll(fsDir, 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.MkdirAll(rwDir, 0755); err != nil {
			t.Fatal(err)
		}

		rwLayer := filepath.Join(snapshotDir, "rwlayer.img")
		if err := os.WriteFile(rwLayer, []byte("fake ext4"), 0644); err != nil {
			t.Fatal(err)
		}

		result := s.getCommitUpperDir("test-id")

		// Should fall back to overlay (fs/) when rw/ is empty (not mounted)
		if result != fsDir {
			t.Errorf("upperDir = %q, want %q", result, fsDir)
		}
	})

	t.Run("falls back to overlay when rw does not exist", func(t *testing.T) {
		root := t.TempDir()
		s := newTestSnapshotterWithRoot(t, root)

		// Create snapshot directory with rwlayer.img but no rw/ directory
		snapshotDir := filepath.Join(root, "snapshots", "test-id")
		fsDir := filepath.Join(snapshotDir, "fs")
		if err := os.MkdirAll(fsDir, 0755); err != nil {
			t.Fatal(err)
		}

		rwLayer := filepath.Join(snapshotDir, "rwlayer.img")
		if err := os.WriteFile(rwLayer, []byte("fake ext4"), 0644); err != nil {
			t.Fatal(err)
		}

		result := s.getCommitUpperDir("test-id")

		// Should fall back to overlay (fs/) when rw/ doesn't exist
		if result != fsDir {
			t.Errorf("upperDir = %q, want %q", result, fsDir)
		}
	})
}
