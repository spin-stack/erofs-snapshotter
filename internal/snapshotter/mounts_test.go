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

func TestViewMountsFallbackToIndividualLayers(t *testing.T) {
	// This test verifies that viewMounts falls back to individual EROFS mounts
	// when fsmeta is not available (common during async generation or failures).

	root := t.TempDir()
	s := &snapshotter{root: root}

	// Create 3 parent snapshot directories with layer blobs but NO fsmeta
	parentIDs := []string{"parent3", "parent2", "parent1"}
	layerPaths := make(map[string]string)

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
		layerPaths[pid] = layerPath
	}

	// NOTE: No fsmeta.erofs or merged.vmdk created - forces fallback

	snap := storage.Snapshot{
		ID:        "child",
		Kind:      snapshots.KindView,
		ParentIDs: parentIDs,
	}

	mounts, err := s.viewMounts(snap)
	if err != nil {
		t.Fatalf("viewMounts failed: %v", err)
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
	// This test verifies that activeMounts falls back to individual EROFS mounts
	// when fsmeta is not available.

	root := t.TempDir()
	s := &snapshotter{root: root}

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

	mounts, err := s.activeMounts(snap)
	if err != nil {
		t.Fatalf("activeMounts failed: %v", err)
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

func TestViewMountsForKindDecisionTree(t *testing.T) {
	// This test verifies the complete decision tree for view mounts.

	t.Run("0 parents returns bind mount", func(t *testing.T) {
		root := t.TempDir()
		s := &snapshotter{root: root}

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

		mounts, err := s.viewMountsForKind(snap)
		if err != nil {
			t.Fatalf("viewMountsForKind failed: %v", err)
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
		s := &snapshotter{root: root}

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

		mounts, err := s.viewMountsForKind(snap)
		if err != nil {
			t.Fatalf("viewMountsForKind failed: %v", err)
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
		s := &snapshotter{root: root}

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

		mounts, err := s.viewMountsForKind(snap)
		if err != nil {
			t.Fatalf("viewMountsForKind failed: %v", err)
		}

		if len(mounts) != 1 {
			t.Fatalf("expected 1 fsmeta mount, got %d", len(mounts))
		}

		if mounts[0].Type != testMountFormatErofs {
			t.Errorf("mount.Type = %q, want %q", mounts[0].Type, testMountFormatErofs)
		}
	})
}

func TestActiveMountsForKindDecisionTree(t *testing.T) {
	t.Run("0 parents returns ext4 only", func(t *testing.T) {
		root := t.TempDir()
		s := &snapshotter{root: root}

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

		mounts, err := s.activeMountsForKind(snap)
		if err != nil {
			t.Fatalf("activeMountsForKind failed: %v", err)
		}

		if len(mounts) != 1 {
			t.Fatalf("expected 1 mount, got %d", len(mounts))
		}

		if mounts[0].Type != testMountExt4 {
			t.Errorf("mount.Type = %q, want %q", mounts[0].Type, testMountExt4)
		}
	})
}

func TestSingleLayerMountsRequiresActive(t *testing.T) {
	root := t.TempDir()
	s := &snapshotter{root: root}

	snap := storage.Snapshot{
		ID:        "view",
		Kind:      snapshots.KindView, // Not Active
		ParentIDs: []string{},
	}

	_, err := s.singleLayerMounts(snap)
	if err == nil {
		t.Error("singleLayerMounts should reject non-Active snapshots")
	}
}
