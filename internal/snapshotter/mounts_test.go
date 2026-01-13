package snapshotter

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/containerd/containerd/v2/core/snapshots"
	"github.com/containerd/containerd/v2/core/snapshots/storage"
)

// Mount type constants for tests (prefixed to avoid conflicts with other test files)
const (
	testMountErofs       = "erofs"
	testMountFormatErofs = "format/erofs"
	testMountExt4        = "ext4"
	testMountBind        = "bind"
)

func TestMultiLayerMountsFallbackToIndividualLayers(t *testing.T) {
	// This test verifies that multiLayerMounts falls back to individual EROFS mounts
	// when fsmeta is not available (common during async generation or failures).

	root := t.TempDir()
	s := newTestSnapshotterWithRoot(t, root)

	// Create 3 parent snapshot directories with layer blobs but NO fsmeta
	parentIDs := []string{"parent3", "parent2", "parent1"}

	for _, pid := range parentIDs {
		snapshotDir := filepath.Join(root, "snapshots", pid)
		if err := os.MkdirAll(snapshotDir, 0755); err != nil {
			t.Fatal(err)
		}
		// Create layer blob with digest-based name
		layerPath := filepath.Join(snapshotDir, "sha256-"+pid+pid+pid+pid+pid+pid+pid+pid+".erofs")
		if err := os.WriteFile(layerPath, []byte("fake"), 0644); err != nil {
			t.Fatal(err)
		}
	}

	// NOTE: No fsmeta.erofs or merged.vmdk created - forces fallback

	snap := storage.Snapshot{
		ID:        "child",
		Kind:      snapshots.KindView,
		ParentIDs: parentIDs,
	}

	mounts, err := s.multiLayerMounts(snap)
	if err != nil {
		t.Fatalf("multiLayerMounts failed: %v", err)
	}

	// Should get 3 individual EROFS mounts (fallback path)
	if len(mounts) != 3 {
		t.Fatalf("expected 3 mounts (fallback), got %d", len(mounts))
	}

	// All should be type "erofs" (not "format/erofs")
	for i, m := range mounts {
		if m.Type != testMountErofs {
			t.Errorf("mount[%d].Type = %q, want %q", i, m.Type, testMountErofs)
		}
	}
}

func TestActiveMountsFallbackToIndividualLayers(t *testing.T) {
	// This test verifies that mounts() for active snapshots falls back to
	// individual EROFS mounts when fsmeta is not available.

	root := t.TempDir()
	s := newTestSnapshotterWithRoot(t, root)

	// Create parent snapshot directory with layer blob but NO fsmeta
	snapshotDir := filepath.Join(root, "snapshots", "parent1")
	if err := os.MkdirAll(snapshotDir, 0755); err != nil {
		t.Fatal(err)
	}
	layerPath := filepath.Join(snapshotDir, "sha256-a3ed95caeb02ffe68cdd9fd84406680ae93d633cb16422d00e8a7c22955b46d4.erofs")
	if err := os.WriteFile(layerPath, []byte("fake"), 0644); err != nil {
		t.Fatal(err)
	}

	// Create active snapshot directory with rwlayer.img
	activeDir := filepath.Join(root, "snapshots", "active")
	if err := os.MkdirAll(activeDir, 0755); err != nil {
		t.Fatal(err)
	}
	rwLayer := filepath.Join(activeDir, "rwlayer.img")
	if err := os.WriteFile(rwLayer, []byte("fake"), 0644); err != nil {
		t.Fatal(err)
	}

	snap := storage.Snapshot{
		ID:        "active",
		Kind:      snapshots.KindActive,
		ParentIDs: []string{"parent1"},
	}
	info := snapshots.Info{} // Empty info = not extract snapshot

	mounts, err := s.mounts(snap, info)
	if err != nil {
		t.Fatalf("mounts failed: %v", err)
	}

	// Should get 2 mounts: 1 EROFS (fallback) + 1 ext4 (writable)
	if len(mounts) != 2 {
		t.Fatalf("expected 2 mounts, got %d", len(mounts))
	}

	// First should be EROFS (read-only layer)
	if mounts[0].Type != testMountErofs {
		t.Errorf("mounts[0].Type = %q, want %q", mounts[0].Type, testMountErofs)
	}

	// Last should be ext4 (writable layer)
	if mounts[1].Type != testMountExt4 {
		t.Errorf("mounts[1].Type = %q, want %q", mounts[1].Type, testMountExt4)
	}
	if mounts[1].Source != rwLayer {
		t.Errorf("mounts[1].Source = %q, want %q", mounts[1].Source, rwLayer)
	}
}

func TestLayerMountsDecisionTree(t *testing.T) {
	// This test verifies the complete decision tree for layerMounts.

	t.Run("0 parents returns bind mount", func(t *testing.T) {
		root := t.TempDir()
		s := newTestSnapshotterWithRoot(t, root)

		// Create snapshot directory
		snapshotDir := filepath.Join(root, "snapshots", "empty")
		if err := os.MkdirAll(snapshotDir, 0755); err != nil {
			t.Fatal(err)
		}

		snap := storage.Snapshot{
			ID:        "empty",
			Kind:      snapshots.KindView,
			ParentIDs: []string{}, // No parents
		}

		mounts, err := s.layerMounts(snap)
		if err != nil {
			t.Fatalf("layerMounts failed: %v", err)
		}

		if len(mounts) != 1 {
			t.Fatalf("expected 1 mount, got %d", len(mounts))
		}

		if mounts[0].Type != testMountBind {
			t.Errorf("mount.Type = %q, want %q", mounts[0].Type, testMountBind)
		}
	})

	t.Run("1 parent returns single erofs mount", func(t *testing.T) {
		root := t.TempDir()
		s := newTestSnapshotterWithRoot(t, root)

		// Create parent with layer blob
		snapshotDir := filepath.Join(root, "snapshots", "parent1")
		if err := os.MkdirAll(snapshotDir, 0755); err != nil {
			t.Fatal(err)
		}
		layerPath := filepath.Join(snapshotDir, "sha256-a3ed95caeb02ffe68cdd9fd84406680ae93d633cb16422d00e8a7c22955b46d4.erofs")
		if err := os.WriteFile(layerPath, []byte("fake"), 0644); err != nil {
			t.Fatal(err)
		}

		snap := storage.Snapshot{
			ID:        "child",
			Kind:      snapshots.KindView,
			ParentIDs: []string{"parent1"},
		}

		mounts, err := s.layerMounts(snap)
		if err != nil {
			t.Fatalf("layerMounts failed: %v", err)
		}

		if len(mounts) != 1 {
			t.Fatalf("expected 1 mount, got %d", len(mounts))
		}

		if mounts[0].Type != testMountErofs {
			t.Errorf("mount.Type = %q, want %q", mounts[0].Type, testMountErofs)
		}
		if mounts[0].Source != layerPath {
			t.Errorf("mount.Source = %q, want %q", mounts[0].Source, layerPath)
		}
	})

	t.Run("N parents with fsmeta returns format/erofs", func(t *testing.T) {
		root := t.TempDir()
		s := newTestSnapshotterWithRoot(t, root)

		// Create parent directories with layer blobs
		parentIDs := []string{"parent2", "parent1"}
		for _, pid := range parentIDs {
			snapshotDir := filepath.Join(root, "snapshots", pid)
			if err := os.MkdirAll(snapshotDir, 0755); err != nil {
				t.Fatal(err)
			}
			layerPath := filepath.Join(snapshotDir, "sha256-"+pid+pid+pid+pid+pid+pid+pid+pid+".erofs")
			if err := os.WriteFile(layerPath, []byte("fake"), 0644); err != nil {
				t.Fatal(err)
			}
		}

		// Create fsmeta and vmdk in newest parent
		newestDir := filepath.Join(root, "snapshots", "parent2")
		for _, name := range []string{"fsmeta.erofs", "merged.vmdk"} {
			if err := os.WriteFile(filepath.Join(newestDir, name), []byte("fake"), 0644); err != nil {
				t.Fatal(err)
			}
		}

		snap := storage.Snapshot{
			ID:        "child",
			Kind:      snapshots.KindView,
			ParentIDs: parentIDs,
		}

		mounts, err := s.layerMounts(snap)
		if err != nil {
			t.Fatalf("layerMounts failed: %v", err)
		}

		if len(mounts) != 1 {
			t.Fatalf("expected 1 fsmeta mount, got %d", len(mounts))
		}

		if mounts[0].Type != testMountFormatErofs {
			t.Errorf("mount.Type = %q, want %q", mounts[0].Type, testMountFormatErofs)
		}
	})
}

func TestActiveSnapshotNoParentsReturnsExt4Only(t *testing.T) {
	root := t.TempDir()
	s := newTestSnapshotterWithRoot(t, root)

	// Create snapshot directory with rwlayer.img
	snapshotDir := filepath.Join(root, "snapshots", "active")
	if err := os.MkdirAll(snapshotDir, 0755); err != nil {
		t.Fatal(err)
	}
	rwLayer := filepath.Join(snapshotDir, "rwlayer.img")
	if err := os.WriteFile(rwLayer, []byte("fake"), 0644); err != nil {
		t.Fatal(err)
	}

	snap := storage.Snapshot{
		ID:        "active",
		Kind:      snapshots.KindActive,
		ParentIDs: []string{}, // No parents
	}
	info := snapshots.Info{} // Empty info = not extract snapshot

	mounts, err := s.mounts(snap, info)
	if err != nil {
		t.Fatalf("mounts failed: %v", err)
	}

	// Should get 2 mounts: 1 bind (empty layer) + 1 ext4 (writable)
	if len(mounts) != 2 {
		t.Fatalf("expected 2 mounts, got %d", len(mounts))
	}

	// First should be bind mount (empty read-only layer)
	if mounts[0].Type != testMountBind {
		t.Errorf("mounts[0].Type = %q, want %q", mounts[0].Type, testMountBind)
	}

	// Last should be ext4 (writable layer)
	if mounts[1].Type != testMountExt4 {
		t.Errorf("mounts[1].Type = %q, want %q", mounts[1].Type, testMountExt4)
	}
}

func TestWritableMountReturnsExt4(t *testing.T) {
	root := t.TempDir()
	s := newTestSnapshotterWithRoot(t, root)

	// Create snapshot directory with rwlayer.img
	snapshotDir := filepath.Join(root, "snapshots", "test")
	if err := os.MkdirAll(snapshotDir, 0755); err != nil {
		t.Fatal(err)
	}

	mount := s.writableMount("test")

	if mount.Type != testMountExt4 {
		t.Errorf("mount.Type = %q, want %q", mount.Type, testMountExt4)
	}
	if mount.Options[0] != "rw" {
		t.Errorf("mount.Options[0] = %q, want %q", mount.Options[0], "rw")
	}
}
