package snapshotter

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

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
		if err := os.MkdirAll(snapshotDir, 0o755); err != nil {
			t.Fatal(err)
		}
		// Create layer blob with digest-based name
		layerPath := filepath.Join(snapshotDir, "sha256-"+pid+pid+pid+pid+pid+pid+pid+pid+".erofs")
		if err := os.WriteFile(layerPath, []byte("fake"), 0o644); err != nil {
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

	// All should be type "erofs" (not "format/erofs"), and sources must follow
	// ParentIDs order: newest-first (unlike mountFsMeta, which emits device=
	// options oldest-first). A VM runtime stacking these layers depends on
	// this order.
	for i, m := range mounts {
		if m.Type != testMountErofs {
			t.Errorf("mount[%d].Type = %q, want %q", i, m.Type, testMountErofs)
		}
		if want := layerPaths[parentIDs[i]]; m.Source != want {
			t.Errorf("mount[%d].Source = %q, want %q", i, m.Source, want)
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
	if err := os.MkdirAll(snapshotDir, 0o755); err != nil {
		t.Fatal(err)
	}
	layerPath := filepath.Join(snapshotDir, "sha256-a3ed95caeb02ffe68cdd9fd84406680ae93d633cb16422d00e8a7c22955b46d4.erofs")
	if err := os.WriteFile(layerPath, []byte("fake"), 0o644); err != nil {
		t.Fatal(err)
	}

	// Create active snapshot directory with rwlayer.img
	activeDir := filepath.Join(root, "snapshots", "active")
	if err := os.MkdirAll(activeDir, 0o755); err != nil {
		t.Fatal(err)
	}
	rwLayer := filepath.Join(activeDir, "rwlayer.img")
	if err := os.WriteFile(rwLayer, []byte("fake"), 0o644); err != nil {
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

	// First should be EROFS (read-only layer) pointing at the parent's blob
	if mounts[0].Type != testMountErofs {
		t.Errorf("mounts[0].Type = %q, want %q", mounts[0].Type, testMountErofs)
	}
	if mounts[0].Source != layerPath {
		t.Errorf("mounts[0].Source = %q, want %q", mounts[0].Source, layerPath)
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
		if err := os.MkdirAll(snapshotDir, 0o755); err != nil {
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
		if err := os.MkdirAll(snapshotDir, 0o755); err != nil {
			t.Fatal(err)
		}
		layerPath := filepath.Join(snapshotDir, "sha256-a3ed95caeb02ffe68cdd9fd84406680ae93d633cb16422d00e8a7c22955b46d4.erofs")
		if err := os.WriteFile(layerPath, []byte("fake"), 0o644); err != nil {
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
			if err := os.MkdirAll(snapshotDir, 0o755); err != nil {
				t.Fatal(err)
			}
			layerPath := filepath.Join(snapshotDir, "sha256-"+pid+pid+pid+pid+pid+pid+pid+pid+".erofs")
			if err := os.WriteFile(layerPath, []byte("fake"), 0o644); err != nil {
				t.Fatal(err)
			}
		}

		// Create fsmeta and vmdk in newest parent
		newestDir := filepath.Join(root, "snapshots", "parent2")
		for _, name := range []string{"fsmeta.erofs", "merged.vmdk"} {
			if err := os.WriteFile(filepath.Join(newestDir, name), []byte("fake"), 0o644); err != nil {
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
		if err := os.MkdirAll(snapshotDir, 0o755); err != nil {
			t.Fatal(err)
		}
		rwLayer := filepath.Join(snapshotDir, "rwlayer.img")
		if err := os.WriteFile(rwLayer, []byte("fake"), 0o644); err != nil {
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

func TestBuildErofsLayerMountsRefusesTooManyLayersWithoutFsmeta(t *testing.T) {
	// When fsmeta is unavailable and the parent chain exceeds maxLayersForFallback,
	// we must fail loud rather than return many individual mounts that a microVM
	// would later fail to attach.

	root := t.TempDir()
	s := &snapshotter{root: root}

	layerCount := maxLayersForFallback + 1
	parentIDs := make([]string, layerCount)
	for i := range parentIDs {
		pid := "p" + string(rune('a'+i%26)) + string(rune('a'+(i/26)%26))
		// Disambiguate when index repeats (keeps fake digests unique enough)
		pid += string(rune('0' + i%10))
		parentIDs[i] = pid

		snapshotDir := filepath.Join(root, "snapshots", pid)
		if err := os.MkdirAll(snapshotDir, 0o755); err != nil {
			t.Fatal(err)
		}
		// Pad the fake digest to 64 hex chars so glob matching mimics real blobs
		fakeDigest := pid
		for len(fakeDigest) < 64 {
			fakeDigest += "0"
		}
		layerPath := filepath.Join(snapshotDir, "sha256-"+fakeDigest[:64]+".erofs")
		if err := os.WriteFile(layerPath, []byte("fake"), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	snap := storage.Snapshot{
		ID:        "child",
		Kind:      snapshots.KindView,
		ParentIDs: parentIDs,
	}

	_, err := s.buildErofsLayerMounts(snap)
	if err == nil {
		t.Fatal("expected FsmetaFallbackTooManyLayersError, got nil")
	}
	var tooMany *FsmetaFallbackTooManyLayersError
	if !errors.As(err, &tooMany) {
		t.Fatalf("expected *FsmetaFallbackTooManyLayersError, got %T: %v", err, err)
	}
	if tooMany.LayerCount != layerCount {
		t.Errorf("LayerCount = %d, want %d", tooMany.LayerCount, layerCount)
	}
	if tooMany.Limit != maxLayersForFallback {
		t.Errorf("Limit = %d, want %d", tooMany.Limit, maxLayersForFallback)
	}
	if tooMany.ParentID != parentIDs[0] {
		t.Errorf("ParentID = %q, want %q", tooMany.ParentID, parentIDs[0])
	}
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

func TestWaitForFsmeta(t *testing.T) {
	newSnap := func(t *testing.T) *snapshotter {
		root := t.TempDir()
		s := &snapshotter{root: root}
		if err := os.MkdirAll(filepath.Join(root, "snapshots", "parent"), 0o755); err != nil {
			t.Fatal(err)
		}
		return s
	}

	t.Run("returns immediately when fsmeta exists", func(t *testing.T) {
		s := newSnap(t)
		if err := os.WriteFile(s.fsMetaPath("parent"), []byte("fsmeta"), 0o644); err != nil {
			t.Fatal(err)
		}
		start := time.Now()
		if !s.waitForFsmeta("parent", 5*time.Second) {
			t.Fatal("waitForFsmeta = false, want true when fsmeta exists")
		}
		if elapsed := time.Since(start); elapsed > time.Second {
			t.Fatalf("waitForFsmeta took %v, expected immediate return", elapsed)
		}
	})

	t.Run("returns quickly when no generation is running", func(t *testing.T) {
		s := newSnap(t)
		start := time.Now()
		if s.waitForFsmeta("parent", 5*time.Second) {
			t.Fatal("waitForFsmeta = true, want false when fsmeta never appears")
		}
		// Without a lock holder the miss is permanent: it must not burn the
		// full timeout.
		if elapsed := time.Since(start); elapsed > time.Second {
			t.Fatalf("waitForFsmeta took %v, expected early exit with no generation in flight", elapsed)
		}
	})

	t.Run("waits while generation lock is held then times out", func(t *testing.T) {
		s := newSnap(t)
		lock, err := s.acquireFsmetaLock("parent")
		if err != nil || lock == nil {
			t.Fatalf("acquireFsmetaLock: lock=%v err=%v", lock, err)
		}
		defer lock.release()
		start := time.Now()
		if s.waitForFsmeta("parent", 300*time.Millisecond) {
			t.Fatal("waitForFsmeta = true, want false (lock held, fsmeta never written)")
		}
		if elapsed := time.Since(start); elapsed < 300*time.Millisecond {
			t.Fatalf("waitForFsmeta returned after %v, expected it to wait for the timeout", elapsed)
		}
	})

	t.Run("picks up fsmeta written mid-wait", func(t *testing.T) {
		s := newSnap(t)
		lock, err := s.acquireFsmetaLock("parent")
		if err != nil || lock == nil {
			t.Fatalf("acquireFsmetaLock: lock=%v err=%v", lock, err)
		}
		go func() {
			time.Sleep(150 * time.Millisecond)
			_ = os.WriteFile(s.fsMetaPath("parent"), []byte("fsmeta"), 0o644)
			lock.release()
		}()
		if !s.waitForFsmeta("parent", 5*time.Second) {
			t.Fatal("waitForFsmeta = false, want true once fsmeta appears")
		}
	})
}
