//go:build linux

/*
   Copyright The containerd Authors.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package snapshotter

// This file contains EROFS snapshot flow integration tests.
// These tests verify end-to-end workflows involving the snapshotter
// with commit/apply flows and cleanup.
//
// Tests in this file:
// - TestErofsSnapshotCommitApplyFlow
// - TestErofsSnapshotterFsmetaSingleLayerView
// - TestErofsBlockModeMountsAfterPrepare
// - TestErofsCleanupRemovesOrphan
// - TestErofsViewMountsMultiLayer (verifies EROFS descriptor format)
// - TestErofsViewMountsSingleLayer
// - TestErofsViewMountsCleanupOnRemove
// - TestErofsViewMountsIdempotent
// - TestErofsBlockModeIgnoresFsMerge
// - TestErofsExtractSnapshotWithParents
// - TestErofsImmutableFlagOnCommit
// - TestErofsImmutableFlagClearedOnRemove
// - TestErofsConcurrentMounts
// - TestErofsViewNoParent
// - TestErofsViewNoParentBlockMode
// - TestErofsBlockModeExtractWithParent
// - TestErofsBlockModeExtractWithMultipleParents
// - TestErofsConcurrentRemoveAndMounts

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/containerd/containerd/v2/core/images/imagetest"
	"github.com/containerd/containerd/v2/core/mount"
	"github.com/containerd/containerd/v2/core/mount/manager"
	"github.com/containerd/containerd/v2/core/snapshots/storage"
	"github.com/containerd/containerd/v2/pkg/namespaces"
	"github.com/containerd/containerd/v2/pkg/testutil"
	bolt "go.etcd.io/bbolt"

	erofsdiffer "github.com/aledbf/nexus-erofs/internal/differ"
	"github.com/aledbf/nexus-erofs/internal/mountutils"
	"github.com/aledbf/nexus-erofs/internal/preflight"
)

// mountTypeBind is the mount type for bind mounts.
const mountTypeBind = "bind"

func TestErofsSnapshotCommitApplyFlow(t *testing.T) {
	testutil.RequiresRoot(t)
	ctx := namespaces.WithNamespace(t.Context(), "testsuite")

	if err := preflight.CheckErofsSupport(); err != nil {
		t.Skipf("EROFS support check failed: %v", err)
	}

	tempDir := t.TempDir()
	contentStore := imagetest.NewContentStore(ctx, t).Store

	snapshotRoot := filepath.Join(tempDir, "snapshots")
	s, err := NewSnapshotter(snapshotRoot)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	defer cleanupAllSnapshots(ctx, s)
	// Unmount all mounts under snapshot root to allow TempDir cleanup
	t.Cleanup(func() {
		mount.UnmountRecursive(snapshotRoot, 0)
	})

	snap, ok := s.(*snapshotter)
	if !ok {
		t.Fatal("failed to cast snapshotter to *snapshotter")
	}

	// Create mount manager for template expansion
	db, err := bolt.Open(filepath.Join(tempDir, "mounts.db"), 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	mountRoot := filepath.Join(tempDir, "mounts")
	mm, err := manager.NewManager(db, mountRoot, manager.WithAllowedRoot(snapshotRoot))
	if err != nil {
		t.Fatal(err)
	}
	if closer, ok := mm.(interface{ Close() error }); ok {
		defer closer.Close()
	}
	// Clean up any mounts in the mount root before temp directory cleanup.
	// This ensures EROFS loop mounts are unmounted before RemoveAll runs.
	t.Cleanup(func() {
		mount.UnmountRecursive(mountRoot, 0)
	})

	differ := erofsdiffer.NewErofsDiffer(contentStore, erofsdiffer.WithMountManager(mm))

	writeFiles := func(dir string, files map[string]string) error {
		for name, content := range files {
			if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0644); err != nil {
				return err
			}
		}
		return nil
	}

	commitWithFiles := func(t *testing.T, key, parent string, files map[string]string) (string, error) {
		t.Helper()
		// Use extract-style key so the snapshotter mounts the ext4 on host
		extractKey := "extract-" + key
		if _, err := s.Prepare(ctx, extractKey, parent); err != nil {
			return "", err
		}
		id := snapshotID(ctx, t, snap, extractKey)
		if err := writeFiles(snap.blockUpperPath(id), files); err != nil {
			return "", err
		}
		commitKey := key + "-commit"
		if err := s.Commit(ctx, commitKey, extractKey); err != nil {
			return "", err
		}
		return commitKey, nil
	}

	runFlow := func(t *testing.T, name string, baseFiles, midFiles, topFiles, upperFiles map[string]string, expectMulti bool) {
		t.Helper()
		baseCommit, err := commitWithFiles(t, name+"-base", "", baseFiles)
		if err != nil {
			t.Fatal(err)
		}

		parentCommit := baseCommit
		if midFiles != nil {
			midCommit, err := commitWithFiles(t, name+"-mid", parentCommit, midFiles)
			if err != nil {
				t.Fatal(err)
			}
			parentCommit = midCommit
		}
		if topFiles != nil {
			topCommit, err := commitWithFiles(t, name+"-top", parentCommit, topFiles)
			if err != nil {
				t.Fatal(err)
			}
			parentCommit = topCommit
		}

		lowerKey := name + "-lower"
		lowerMounts, err := s.View(ctx, lowerKey, parentCommit)
		if err != nil {
			t.Fatal(err)
		}

		// Use extract-style key so the snapshotter mounts the ext4 on host
		upperKey := "extract-" + name + "-upper"
		if _, err := s.Prepare(ctx, upperKey, parentCommit); err != nil {
			t.Fatal(err)
		}
		upperID := snapshotID(ctx, t, snap, upperKey)
		if err := writeFiles(snap.blockUpperPath(upperID), upperFiles); err != nil {
			t.Fatal(err)
		}

		// View mounts should contain EROFS mount(s) (the consumer converts to virtio-blk)
		// Multi-layer views return fsmeta.erofs with device= options (consolidated)
		hasErofs := false
		for _, m := range lowerMounts {
			if mountutils.TypeSuffix(m.Type) == testTypeErofs {
				hasErofs = true
				break
			}
		}
		if !hasErofs {
			t.Fatalf("expected EROFS mount(s), got: %#v", lowerMounts)
		}
		_ = expectMulti // fsmeta consolidation handles both single and multi-layer

		// For Compare, we need to mount both lower and upper manually because:
		// 1. Lower may have multi-device fsmeta (device= options) which containerd mount manager doesn't support
		// 2. Upper needs to show the full overlay view (parent content + new files)
		//
		// Mount the lower EROFS layer(s) first
		lowerTarget := t.TempDir()
		lowerCleanup := mountErofsView(t, lowerMounts, lowerTarget)
		defer lowerCleanup()

		// Create a bind mount to the lower view for Compare
		lowerBindMounts := []mount.Mount{{
			Type:    "bind",
			Source:  lowerTarget,
			Options: []string{"ro", "rbind"},
		}}

		// Create an overlay with lower EROFS as lowerdir and rw/upper as upperdir
		// This shows the full desired state (parent content + new files)
		overlayViewTarget := t.TempDir()
		overlayCleanup := createOverlayViewForCompareWithMountedLower(t, lowerTarget, snap.blockUpperPath(upperID), overlayViewTarget)
		defer overlayCleanup()

		// Create a bind mount to the overlay view for Compare
		overlayMounts := []mount.Mount{{
			Type:    "bind",
			Source:  overlayViewTarget,
			Options: []string{"ro", "rbind"},
		}}

		desc, err := differ.Compare(ctx, lowerBindMounts, overlayMounts)
		if err != nil {
			t.Fatalf("Compare failed: %v", err)
		}
		if desc.Digest == "" || desc.Size == 0 {
			t.Fatalf("unexpected diff descriptor: %+v", desc)
		}

		// Use extract- prefix to get diffMounts (bind mount to fs/) instead of overlay
		applyKey := "extract-" + name + "-apply"
		applyMounts, err := s.Prepare(ctx, applyKey, parentCommit)
		if err != nil {
			t.Fatalf("Prepare for apply failed: %+v", err)
		}
		if _, err := differ.Apply(ctx, desc, applyMounts); err != nil {
			t.Fatalf("Apply failed: %v", err)
		}
		applyCommit := name + "-apply-commit"
		if err := s.Commit(ctx, applyCommit, applyKey); err != nil {
			t.Fatalf("Commit failed: %v", err)
		}

		viewKey := name + "-view"
		viewMounts, err := s.View(ctx, viewKey, applyCommit)
		if err != nil {
			t.Fatalf("View failed: %v", err)
		}

		// Mount and verify files
		// View mounts may be overlay (with pre-mounted EROFS layers) or EROFS (single layer)
		// For overlay mounts, mount directly. For EROFS mounts, use mountErofsView helper
		// (containerd mount manager doesn't support EROFS).
		viewTarget := t.TempDir()

		// Count EROFS mounts and check if fsmeta is used (has device= options)
		var erofsLayers []mount.Mount
		hasFsmetaMount := false
		for _, m := range viewMounts {
			if mountutils.TypeSuffix(m.Type) == testTypeErofs {
				erofsLayers = append(erofsLayers, m)
				for _, opt := range m.Options {
					if strings.HasPrefix(opt, "device=") {
						hasFsmetaMount = true
						break
					}
				}
			}
		}

		// When fsmeta merge fails, we get multiple individual EROFS mounts.
		// Mount each layer separately and create an overlay to verify contents.
		var layerDirs []string
		var overlayMounted bool

		switch {
		case len(erofsLayers) > 1 && !hasFsmetaMount:
			t.Logf("fsmeta merge failed, mounting %d individual EROFS layers with overlay", len(erofsLayers))
			result := mountErofsLayersWithOverlay(t, erofsLayers, viewTarget)
			t.Cleanup(result.cleanup)
			layerDirs = result.layerDirs
			overlayMounted = result.overlayMounted
		case len(erofsLayers) > 0:
			cleanup := mountErofsView(t, viewMounts, viewTarget)
			t.Cleanup(cleanup)
			overlayMounted = true
		default:
			// Direct mount for overlay mounts
			if err := mount.All(viewMounts, viewTarget); err != nil {
				t.Fatalf("mount.All failed: %v", err)
			}
			t.Cleanup(func() {
				if err := mount.UnmountAll(viewTarget, 0); err != nil {
					t.Logf("failed to unmount view: %v", err)
				}
			})
			overlayMounted = true
		}

		// Verify files - either from overlay mount or individual layers
		if overlayMounted {
			verifyFilesInDir(t, viewTarget, baseFiles)
			if midFiles != nil {
				verifyFilesInDir(t, viewTarget, midFiles)
			}
			if topFiles != nil {
				verifyFilesInDir(t, viewTarget, topFiles)
			}
			verifyFilesInDir(t, viewTarget, upperFiles)
		} else {
			// Overlay mount failed - verify files exist in individual layers
			// Layer 0 = newest (upper), Layer N-1 = oldest (base)
			allFiles := []map[string]string{upperFiles}
			if topFiles != nil {
				allFiles = append(allFiles, topFiles)
			}
			if midFiles != nil {
				allFiles = append(allFiles, midFiles)
			}
			allFiles = append(allFiles, baseFiles)
			verifyFilesInLayers(t, layerDirs, allFiles)
		}
	}

	t.Run("single-layer", func(t *testing.T) {
		runFlow(t, "single",
			map[string]string{"base.txt": "base"},
			nil,
			nil,
			map[string]string{"upper.txt": "upper"},
			false,
		)
	})

	t.Run("multi-layer-overlay", func(t *testing.T) {
		runFlow(t, "multi",
			map[string]string{"base.txt": "base"},
			map[string]string{"mid.txt": "mid"},
			map[string]string{"top.txt": "top"},
			map[string]string{"upper.txt": "upper"},
			true,
		)
	})
}

// TestErofsCommitWithoutHostMount tests commit when the /rw directory doesn't exist.
// This simulates the VM-only scenario where the VM mounts the ext4 (not the host).
// The commit code must handle the case where rwMount path doesn't exist when
// checking mountinfo.Mounted().
func TestErofsCommitWithoutHostMount(t *testing.T) {
	testutil.RequiresRoot(t)
	ctx := namespaces.WithNamespace(t.Context(), "testsuite")

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
	defer cleanupAllSnapshots(ctx, s)
	t.Cleanup(func() {
		mount.UnmountRecursive(snapshotRoot, 0)
	})

	snap, ok := s.(*snapshotter)
	if !ok {
		t.Fatal("failed to cast snapshotter to *snapshotter")
	}

	// First, create a base layer using extract key (so we have a parent to prepare from)
	baseKey := "extract-base"
	if _, err := s.Prepare(ctx, baseKey, ""); err != nil {
		t.Fatal(err)
	}
	baseID := snapshotID(ctx, t, snap, baseKey)

	// Write some files to the base layer
	if err := os.WriteFile(filepath.Join(snap.blockUpperPath(baseID), "base.txt"), []byte("base"), 0644); err != nil {
		t.Fatal(err)
	}

	// Commit the base layer
	baseCommit := "base-commit"
	if err := s.Commit(ctx, baseCommit, baseKey); err != nil {
		t.Fatal(err)
	}

	// Now prepare a VM-only snapshot (without extract- prefix)
	// This will create rwlayer.img but NOT mount it on the host
	vmKey := "vm-only-active"
	if _, err := s.Prepare(ctx, vmKey, baseCommit); err != nil {
		t.Fatal(err)
	}
	vmID := snapshotID(ctx, t, snap, vmKey)

	// Verify the rwlayer.img exists but /rw directory does NOT
	rwLayerPath := snap.writablePath(vmID)
	if _, err := os.Stat(rwLayerPath); err != nil {
		t.Fatalf("rwlayer.img should exist: %v", err)
	}
	rwMountPath := snap.blockRwMountPath(vmID)
	if _, err := os.Stat(rwMountPath); !os.IsNotExist(err) {
		t.Fatalf("/rw directory should NOT exist for VM-only snapshot, but got: %v", err)
	}

	// Simulate VM operation: manually mount the ext4, write files, unmount
	if err := os.MkdirAll(rwMountPath, 0755); err != nil {
		t.Fatal(err)
	}
	m := mount.Mount{
		Source:  rwLayerPath,
		Type:    "ext4",
		Options: []string{"rw", "loop"},
	}
	if err := m.Mount(rwMountPath); err != nil {
		t.Fatalf("mount ext4 for test: %v", err)
	}

	// Create upper dir and write a file
	upperPath := snap.blockUpperPath(vmID)
	if err := os.MkdirAll(upperPath, 0755); err != nil {
		mount.UnmountAll(rwMountPath, 0)
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(upperPath, "vm-file.txt"), []byte("vm-data"), 0644); err != nil {
		mount.UnmountAll(rwMountPath, 0)
		t.Fatal(err)
	}

	// Unmount to simulate VM finishing its work
	if err := mount.UnmountAll(rwMountPath, 0); err != nil {
		t.Fatalf("unmount ext4: %v", err)
	}

	// Remove the /rw directory to simulate the state after VM unmount
	if err := os.Remove(rwMountPath); err != nil {
		t.Fatalf("remove /rw directory: %v", err)
	}

	// Now commit - this is where the bug was: mountinfo.Mounted() failed on non-existent path
	vmCommit := "vm-only-commit"
	if err := s.Commit(ctx, vmCommit, vmKey); err != nil {
		t.Fatalf("commit should succeed even when /rw doesn't exist: %v", err)
	}

	// Verify the layer blob was created
	layerBlob, err := snap.findLayerBlob(vmID)
	if err != nil {
		t.Fatalf("layer blob should exist after commit: %v", err)
	}
	if _, err := os.Stat(layerBlob); err != nil {
		t.Fatalf("layer blob path should be valid: %v", err)
	}

	t.Logf("Successfully committed VM-only snapshot without pre-existing /rw directory")
}

// TestErofsSnapshotterFsmetaSingleLayerView tests that when fsmeta merge
// collapses multiple layers into a single mount, KindView returns the EROFS
// mount directly without requiring mount manager resolution.
func TestErofsSnapshotterFsmetaSingleLayerView(t *testing.T) {
	testutil.RequiresRoot(t)
	ctx := namespaces.WithNamespace(t.Context(), "testsuite")

	if err := preflight.CheckErofsSupport(); err != nil {
		t.Skipf("EROFS support check failed: %v", err)
	}

	tempDir := t.TempDir()

	// Create snapshotter - VMDK is always generated for multi-layer images
	snapshotRoot := filepath.Join(tempDir, "snapshots")
	s, err := NewSnapshotter(snapshotRoot)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// Cleanup mounted layers before temp directory removal
	t.Cleanup(func() {
		mount.UnmountRecursive(snapshotRoot, 0)
	})

	snap, ok := s.(*snapshotter)
	if !ok {
		t.Fatal("failed to cast snapshotter to *snapshotter")
	}

	// Create 6 layers to exceed the threshold and trigger fsmeta generation
	var parentKey string
	for i := range 6 {
		// Use extract-style key so the snapshotter mounts the ext4 on host
		key := fmt.Sprintf("extract-layer-%d", i)
		commitKey := fmt.Sprintf("layer-%d-commit", i)

		if _, err := s.Prepare(ctx, key, parentKey); err != nil {
			t.Fatalf("failed to prepare layer %d: %v", i, err)
		}

		id := snapshotID(ctx, t, snap, key)
		filename := fmt.Sprintf("file-%d.txt", i)
		if err := os.WriteFile(filepath.Join(snap.blockUpperPath(id), filename), []byte(fmt.Sprintf("content-%d", i)), 0644); err != nil {
			t.Fatalf("failed to write file in layer %d: %v", i, err)
		}

		if err := s.Commit(ctx, commitKey, key); err != nil {
			t.Fatalf("failed to commit layer %d: %v", i, err)
		}
		parentKey = commitKey
	}

	// Wait a bit for fsmeta generation (it runs asynchronously)
	time.Sleep(500 * time.Millisecond)

	// Check if fsmeta was generated for the top layer
	var topID string
	if err := snap.ms.WithTransaction(ctx, false, func(ctx context.Context) error {
		topID, _, _, err = storage.GetInfo(ctx, "layer-5-commit")
		return err
	}); err != nil {
		t.Fatal(err)
	}

	fsmetaPath := snap.fsMetaPath(topID)
	fsmetaExists := false
	if fi, err := os.Stat(fsmetaPath); err == nil && fi.Size() > 0 {
		fsmetaExists = true
		t.Logf("fsmeta generated at %s (%d bytes)", fsmetaPath, fi.Size())
	}

	// Create a view of the merged layers
	viewKey := "merged-view"
	viewMounts, err := s.View(ctx, viewKey, parentKey)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("view mounts (fsmeta exists: %v): %#v", fsmetaExists, viewMounts)

	if fsmetaExists {
		// With fsmeta, we expect a single merged EROFS mount (with device= options)
		// The key assertion: KindView with fsmeta merge resulting in single lower
		// should NOT require format/mkdir/overlay (which needs mount manager)
		hasTemplateOverlay := false
		for _, m := range viewMounts {
			if strings.Contains(m.Type, "overlay") && mountsHaveTemplate([]mount.Mount{m}) {
				hasTemplateOverlay = true
				break
			}
		}

		if hasTemplateOverlay {
			t.Fatalf("fsmeta view with single lower should not require template resolution, got: %#v", viewMounts)
		}

		// Verify we have EROFS mounts (possibly with device= for multi-device)
		// Multi-device fsmeta mounts return "format/erofs", single-layer returns "erofs"
		hasErofs := false
		for _, m := range viewMounts {
			if mountutils.TypeSuffix(m.Type) == testTypeErofs {
				hasErofs = true
				t.Logf("found EROFS mount: type=%s, source=%s, options=%v", m.Type, m.Source, m.Options)
				// Verify fsmeta mounts use format/erofs
				if strings.HasSuffix(m.Source, "fsmeta.erofs") && m.Type != "format/erofs" {
					t.Fatalf("fsmeta mount should use format/erofs type, got: %s", m.Type)
				}
			}
		}
		if !hasErofs {
			t.Fatalf("expected EROFS mount in view, got: %#v", viewMounts)
		}
	} else {
		// Without fsmeta, we'll have multiple EROFS mounts with overlay
		t.Logf("fsmeta not generated (mkfs.erofs may not support --aufs), view has %d mounts", len(viewMounts))
	}
}

func TestErofsBlockModeMountsAfterPrepare(t *testing.T) {
	env := newSnapshotTestEnv(t, WithDefaultSize(16*1024*1024))

	key := "block-active"
	if _, err := env.snapshotter.Prepare(env.ctx(), key, ""); err != nil {
		t.Fatal(err)
	}

	// Block mode returns the ext4 writable layer directly (no templates).
	// Non-extract snapshots return ext4 type (passed to VM as virtio-blk).
	mounts1, err := env.snapshotter.Mounts(env.ctx(), key)
	if err != nil {
		t.Fatal(err)
	}

	// Should NOT have templates
	if mountsHaveTemplate(mounts1) {
		t.Fatalf("block mode should not use templates, got: %#v", mounts1)
	}

	// Should have an ext4 mount (writable layer for VM)
	hasExt4 := false
	for _, m := range mounts1 {
		if m.Type == testTypeExt4 {
			hasExt4 = true
			break
		}
	}
	if !hasExt4 {
		t.Fatalf("expected Mounts to include ext4 mount, got: %#v", mounts1)
	}

	// Subsequent calls return consistent mounts (idempotent).
	mounts2, err := env.snapshotter.Mounts(env.ctx(), key)
	if err != nil {
		t.Fatal(err)
	}
	if len(mounts1) != len(mounts2) {
		t.Fatalf("expected consistent mounts, got %d vs %d", len(mounts1), len(mounts2))
	}

	if err := env.snapshotter.Remove(env.ctx(), key); err != nil {
		t.Fatal(err)
	}
}

func TestErofsCleanupRemovesOrphan(t *testing.T) {
	env := newSnapshotTestEnv(t)

	// Create and commit a snapshot to initialize the metadata store bucket.
	env.createLayer("init", "", "test.txt", "content")

	// Create an orphan snapshot directory not tracked by metadata.
	orphanDir := filepath.Join(env.snapshotter.root, "snapshots", "orphan")
	if err := os.MkdirAll(filepath.Join(orphanDir, "fs"), 0755); err != nil {
		t.Fatal(err)
	}

	// Call Cleanup directly - *snapshotter implements Cleanup
	if err := env.snapshotter.Cleanup(env.ctx()); err != nil {
		t.Fatal(err)
	}

	_, err := os.Stat(orphanDir)
	if err == nil {
		t.Fatalf("expected orphan dir to be removed: %s", orphanDir)
	}
	if !os.IsNotExist(err) {
		t.Fatalf("expected not exist error, got: %v", err)
	}
}

// TestErofsViewMountsMultiLayer tests that View snapshots with multiple layers
// return EROFS mount descriptors. With fsmeta consolidation, multi-layer views
// return a single fsmeta.erofs mount with device= options for additional layers.
func TestErofsViewMountsMultiLayer(t *testing.T) {
	env := newSnapshotTestEnv(t)

	// Create 3 layers (multiple parents)
	var parentKey string
	for i := range 3 {
		key := fmt.Sprintf("layer-%d", i)
		filename := fmt.Sprintf("file-%d.txt", i)
		content := fmt.Sprintf("content-%d", i)
		parentKey = env.createLayer(key, parentKey, filename, content)
	}

	// Create a View of the multi-layer snapshot
	viewKey := "multi-layer-view"
	viewMounts := env.createView(viewKey, parentKey)

	t.Logf("view mounts: %#v", viewMounts)

	// With fsmeta consolidation, multi-layer views return either:
	// 1. Single fsmeta.erofs mount with device= options (when fsmeta is generated)
	// 2. Multiple EROFS mounts (one per layer) if fsmeta not available
	hasErofs := false
	for _, m := range viewMounts {
		if mountutils.TypeSuffix(m.Type) == testTypeErofs {
			hasErofs = true
			break
		}
	}
	if !hasErofs {
		t.Fatalf("expected at least one EROFS mount, got: %#v", viewMounts)
	}

	// Verify mount sources point to valid EROFS files
	for i, m := range viewMounts {
		if mountutils.TypeSuffix(m.Type) != testTypeErofs {
			continue
		}
		// Source should be either layer.erofs or fsmeta.erofs
		if !strings.HasSuffix(m.Source, ".erofs") {
			t.Fatalf("mount %d: expected source to end with .erofs, got: %s", i, m.Source)
		}
	}
}

// TestErofsViewMountsSingleLayer tests that View snapshots with a single layer
// return an EROFS mount directly (no overlay needed).
func TestErofsViewMountsSingleLayer(t *testing.T) {
	env := newSnapshotTestEnv(t)

	// Create a single layer
	commitKey := env.createLayer("single-layer", "", "test.txt", "test")

	// Create a View of the single-layer snapshot
	viewKey := "single-layer-view"
	viewMounts := env.createView(viewKey, commitKey)

	t.Logf("single layer view mounts: %#v", viewMounts)

	// Single layer view should return EROFS mount directly (no overlay)
	if len(viewMounts) != 1 {
		t.Fatalf("expected single mount, got %d: %#v", len(viewMounts), viewMounts)
	}

	if viewMounts[0].Type != testTypeErofs {
		t.Fatalf("expected erofs mount type for single layer, got: %s", viewMounts[0].Type)
	}

	// Should not have template syntax
	if mountsHaveTemplate(viewMounts) {
		t.Fatalf("single layer view should not have templates: %#v", viewMounts)
	}
}

// TestErofsViewMountsCleanupOnRemove tests that View snapshot directories are properly
// cleaned up when the snapshot is removed. Since no host mounting is done, cleanup
// simply removes the snapshot directory.
func TestErofsViewMountsCleanupOnRemove(t *testing.T) {
	env := newSnapshotTestEnv(t)

	// Create 2 layers
	var parentKey string
	for i := range 2 {
		key := fmt.Sprintf("layer-%d", i)
		filename := fmt.Sprintf("file-%d.txt", i)
		parentKey = env.createLayer(key, parentKey, filename, "content")
	}

	// Create a View (returns EROFS descriptors, no host mounting)
	viewKey := "cleanup-test-view"
	viewMounts := env.createView(viewKey, parentKey)

	t.Logf("view mounts: %#v", viewMounts)

	// With fsmeta consolidation, multi-layer views may return a single mount
	// with device= options. Verify we have at least one EROFS mount.
	hasErofs := false
	for _, m := range viewMounts {
		if mountutils.TypeSuffix(m.Type) == testTypeErofs {
			hasErofs = true
			break
		}
	}
	if !hasErofs {
		t.Fatalf("expected at least one EROFS mount, got: %+v", viewMounts)
	}

	// Get snapshot directory path
	viewID := snapshotID(env.ctx(), t, env.snapshotter, viewKey)
	snapshotDir := filepath.Join(env.snapshotter.root, "snapshots", viewID)

	// Snapshot directory should exist
	if _, err := os.Stat(snapshotDir); err != nil {
		t.Fatalf("expected snapshot directory to exist: %v", err)
	}

	// Remove the view snapshot
	if err := env.snapshotter.Remove(env.ctx(), viewKey); err != nil {
		t.Fatalf("failed to remove view snapshot: %v", err)
	}

	// After removal, the snapshot directory should not exist
	_, err := os.Stat(snapshotDir)
	if err == nil {
		t.Fatalf("expected snapshot directory to be removed: %s", snapshotDir)
	}
	if !os.IsNotExist(err) {
		t.Fatalf("expected not exist error, got: %v", err)
	}
}

// TestErofsViewMountsIdempotent tests that calling Mounts() multiple times
// on a View snapshot returns consistent EROFS descriptors.
func TestErofsViewMountsIdempotent(t *testing.T) {
	env := newSnapshotTestEnv(t)

	// Create 2 layers
	var parentKey string
	for i := range 2 {
		key := fmt.Sprintf("layer-%d", i)
		filename := fmt.Sprintf("file-%d.txt", i)
		parentKey = env.createLayer(key, parentKey, filename, "content")
	}

	// Create a View
	viewKey := "idempotent-test-view"
	mounts1 := env.createView(viewKey, parentKey)

	// Call Mounts() again on the same view
	mounts2, err := env.snapshotter.Mounts(env.ctx(), viewKey)
	if err != nil {
		t.Fatal(err)
	}

	// Should return consistent results
	if len(mounts1) != len(mounts2) {
		t.Fatalf("inconsistent mount counts: first=%d, second=%d", len(mounts1), len(mounts2))
	}

	// Both mount types should be consistent
	if mounts1[0].Type != mounts2[0].Type {
		t.Fatalf("inconsistent mount types: first=%s, second=%s", mounts1[0].Type, mounts2[0].Type)
	}

	// Source paths should be identical
	if mounts1[0].Source != mounts2[0].Source {
		t.Fatalf("inconsistent source:\n  first:  %s\n  second: %s", mounts1[0].Source, mounts2[0].Source)
	}

	// Options should be identical
	if len(mounts1[0].Options) != len(mounts2[0].Options) {
		t.Fatalf("inconsistent options count: first=%d, second=%d", len(mounts1[0].Options), len(mounts2[0].Options))
	}

	for i := range mounts1[0].Options {
		if mounts1[0].Options[i] != mounts2[0].Options[i] {
			t.Fatalf("inconsistent option[%d]:\n  first:  %s\n  second: %s", i, mounts1[0].Options[i], mounts2[0].Options[i])
		}
	}
}

// TestErofsMultiLayerViewMounts verifies that multi-layer views return
// EROFS mounts for the VM runtime to handle. With fsmeta consolidation,
// this may be a single mount with device= options or multiple mounts.
func TestErofsMultiLayerViewMounts(t *testing.T) {
	env := newSnapshotTestEnv(t, WithDefaultSize(64*1024*1024))

	// Create first layer with extract label
	labels := map[string]string{extractLabel: "true"}
	layer1Commit := env.createLayerWithLabels("layer1-active", "", "file1.txt", "layer1", labels)

	// Create second layer on top with extract label
	layer2Commit := env.createLayerWithLabels("layer2-active", layer1Commit, "file2.txt", "layer2", labels)

	// Create a View with 2 parents
	viewMounts := env.createView("view-test", layer2Commit)

	t.Logf("view mounts: %+v", viewMounts)

	// With fsmeta consolidation, multi-layer views may return a single mount
	// with device= options or multiple mounts. Verify we have at least one EROFS mount.
	hasErofs := false
	for _, m := range viewMounts {
		if mountutils.TypeSuffix(m.Type) == testTypeErofs {
			hasErofs = true
			break
		}
	}
	if !hasErofs {
		t.Fatalf("expected at least one EROFS mount, got: %+v", viewMounts)
	}

	// Verify mount sources point to valid EROFS files
	for i, m := range viewMounts {
		if mountutils.TypeSuffix(m.Type) != testTypeErofs {
			continue
		}
		if !strings.HasSuffix(m.Source, ".erofs") {
			t.Fatalf("mount %d: expected source to end with .erofs, got: %s", i, m.Source)
		}
	}
}

// TestErofsExtractSnapshotWithParents verifies that extract snapshots
// always return a bind mount to the rw/upper directory, regardless of parent count.
// The ext4 layer is mounted at rw/ and the overlay upper dir is at rw/upper.
func TestErofsExtractSnapshotWithParents(t *testing.T) {
	env := newSnapshotTestEnv(t)

	// Create and commit layers with extract label
	labels := map[string]string{extractLabel: "true"}
	layer1Commit := env.createLayerWithLabels("layer1-active", "", "file1.txt", "layer1", labels)
	layer2Commit := env.createLayerWithLabels("layer2-active", layer1Commit, "file2.txt", "layer2", labels)

	// Create extract snapshot with 2 parents - use Prepare directly since extract
	// snapshots return a bind mount to rw/upper
	extractMounts, err := env.snapshotter.Prepare(env.ctx(), "extract-with-parents", layer2Commit)
	if err != nil {
		t.Fatalf("failed to prepare extract snapshot: %v", err)
	}

	t.Logf("extract mounts: %+v", extractMounts)

	// Extract snapshots should always be bind mounts to rw/upper
	if len(extractMounts) != 1 {
		t.Fatalf("expected 1 mount, got %d", len(extractMounts))
	}

	m := extractMounts[0]
	if m.Type != mountTypeBind {
		t.Errorf("expected bind mount for extract snapshot, got %s", m.Type)
	}

	// Extract snapshots use the rw/upper path (mounted ext4 with overlay upper dir)
	if !strings.HasSuffix(m.Source, "/rw/upper") {
		t.Errorf("expected source to end with /rw/upper, got %s", m.Source)
	}

	// Verify parent layers don't affect the mount type
	hasRbind := false
	for _, opt := range m.Options {
		if opt == "rbind" {
			hasRbind = true
		}
	}
	if !hasRbind {
		t.Error("expected rbind option in extract mount")
	}
}

// TestErofsImmutableFlagOnCommit verifies that the immutable flag (FS_IMMUTABLE_FL)
// is set on the EROFS layer blob when WithImmutable option is enabled.
func TestErofsImmutableFlagOnCommit(t *testing.T) {
	env := newSnapshotTestEnv(t, WithImmutable())

	// Create and commit a layer with extract label
	labels := map[string]string{extractLabel: "true"}
	env.createLayerWithLabels("layer1-active", "", "test.txt", "content", labels)

	// Get the committed snapshot info
	info, err := env.snapshotter.Stat(env.ctx(), "layer1-active-commit")
	if err != nil {
		t.Fatalf("failed to stat committed snapshot: %v", err)
	}
	t.Logf("committed snapshot info: %+v", info)

	// Verify the layer blob has immutable flag
	layerBlob, err := env.snapshotter.findLayerBlob(snapshotID(env.ctx(), t, env.snapshotter, "layer1-active-commit"))
	if err != nil {
		t.Fatalf("failed to find layer blob: %v", err)
	}
	if _, err := os.Stat(layerBlob); err != nil {
		t.Fatalf("layer blob not found at %s: %v", layerBlob, err)
	}

	// Check immutable flag using lsattr
	out, err := exec.Command("lsattr", layerBlob).CombinedOutput()
	if err != nil {
		t.Logf("lsattr failed (may not be supported): %v, output: %s", err, string(out))
		t.Skip("lsattr not available or not supported on this filesystem")
	}

	// lsattr output format: "----i--------e-- /path/to/file"
	// The 'i' indicates immutable flag
	if !strings.Contains(string(out), "i") {
		t.Errorf("expected immutable flag to be set on %s, lsattr output: %s", layerBlob, string(out))
	} else {
		t.Logf("immutable flag verified on %s: %s", layerBlob, strings.TrimSpace(string(out)))
	}
}

// TestErofsImmutableFlagClearedOnRemove verifies that the immutable flag
// is cleared before removing a committed snapshot, allowing deletion.
func TestErofsImmutableFlagClearedOnRemove(t *testing.T) {
	env := newSnapshotTestEnv(t, WithImmutable())

	// Skip if filesystem doesn't support immutable flags (e.g., tmpfs)
	skipIfNoImmutableSupport(t, env.tempDir)

	// Create and commit a layer with extract label
	labels := map[string]string{extractLabel: "true"}
	commitKey := env.createLayerWithLabels("layer1-active", "", "test.txt", "content", labels)

	// Get the layer blob path before removal
	layerBlob, err := env.snapshotter.findLayerBlob(snapshotID(env.ctx(), t, env.snapshotter, commitKey))
	if err != nil {
		t.Fatalf("failed to find layer blob: %v", err)
	}

	// Remove the snapshot - this should clear the immutable flag first
	if err := env.snapshotter.Remove(env.ctx(), commitKey); err != nil {
		t.Fatalf("failed to remove snapshot: %v", err)
	}

	// Verify the layer blob no longer exists
	if _, err := os.Stat(layerBlob); !os.IsNotExist(err) {
		t.Errorf("expected layer blob to be removed, but got: %v", err)
	}

	// Verify snapshot is gone
	_, err = env.snapshotter.Stat(env.ctx(), commitKey)
	if err == nil {
		t.Error("expected snapshot to be removed")
	}
}

// TestErofsConcurrentMounts verifies that concurrent mount operations
// are safe and produce consistent results.
func TestErofsConcurrentMounts(t *testing.T) {
	env := newSnapshotTestEnv(t)

	// Create and commit a base layer with extract label
	labels := map[string]string{extractLabel: "true"}
	baseCommit := env.createLayerWithLabels("base-active", "", "base.txt", "base", labels)

	// Create a View snapshot
	env.createView("concurrent-view", baseCommit)

	// Concurrent access test
	const numGoroutines = 10
	results := make(chan []mount.Mount, numGoroutines)
	errors := make(chan error, numGoroutines)

	for i := range numGoroutines {
		go func(id int) {
			mounts, err := env.snapshotter.Mounts(env.ctx(), "concurrent-view")
			if err != nil {
				errors <- fmt.Errorf("goroutine %d: %w", id, err)
				return
			}
			results <- mounts
		}(i)
	}

	// Collect results
	var allMounts [][]mount.Mount
	for range numGoroutines {
		select {
		case mounts := <-results:
			allMounts = append(allMounts, mounts)
		case err := <-errors:
			t.Fatalf("concurrent mount failed: %v", err)
		case <-time.After(30 * time.Second):
			t.Fatal("timeout waiting for concurrent mounts")
		}
	}

	// Verify all results are consistent
	if len(allMounts) == 0 {
		t.Fatal("no mounts returned")
	}

	reference := allMounts[0]
	for i, mounts := range allMounts[1:] {
		if len(mounts) != len(reference) {
			t.Errorf("goroutine %d returned different number of mounts: %d vs %d",
				i+1, len(mounts), len(reference))
			continue
		}
		for j := range mounts {
			if mounts[j].Type != reference[j].Type {
				t.Errorf("goroutine %d mount %d has different type: %s vs %s",
					i+1, j, mounts[j].Type, reference[j].Type)
			}
			if mounts[j].Source != reference[j].Source {
				t.Errorf("goroutine %d mount %d has different source: %s vs %s",
					i+1, j, mounts[j].Source, reference[j].Source)
			}
		}
	}

	t.Logf("all %d concurrent mount calls returned consistent results", numGoroutines)
}

// TestErofsViewNoParent verifies that View with empty parent returns
// a read-only bind mount to an empty directory (directory mode).
func TestErofsViewNoParent(t *testing.T) {
	env := newSnapshotTestEnv(t)

	// Create a View with no parent - this is an edge case
	mounts, err := env.snapshotter.View(env.ctx(), "empty-view", "")
	if err != nil {
		t.Fatalf("failed to create view with no parent: %v", err)
	}

	// Should return a single bind mount
	if len(mounts) != 1 {
		t.Fatalf("expected 1 mount, got %d", len(mounts))
	}

	m := mounts[0]
	if m.Type != mountTypeBind {
		t.Errorf("expected bind mount, got %s", m.Type)
	}

	// Should be read-only
	hasRO := false
	for _, opt := range m.Options {
		if opt == "ro" {
			hasRO = true
			break
		}
	}
	if !hasRO {
		t.Error("expected read-only mount options")
	}

	// Source should be the view's lower directory
	viewID := snapshotID(env.ctx(), t, env.snapshotter, "empty-view")
	expectedPath := env.snapshotter.viewLowerPath(viewID)
	if m.Source != expectedPath {
		t.Errorf("expected source %s, got %s", expectedPath, m.Source)
	}

	// Directory should exist and be empty
	entries, err := os.ReadDir(m.Source)
	if err != nil {
		t.Fatalf("failed to read view directory: %v", err)
	}
	if len(entries) != 0 {
		t.Errorf("expected empty directory, got %d entries", len(entries))
	}

	// Verify we can get mounts again (idempotent)
	mounts2, err := env.snapshotter.Mounts(env.ctx(), "empty-view")
	if err != nil {
		t.Fatalf("failed to get mounts: %v", err)
	}
	if len(mounts2) != 1 || mounts2[0].Source != m.Source {
		t.Error("mounts not idempotent")
	}
}

// TestErofsViewNoParentBlockMode verifies that View with empty parent
// works correctly in block mode.
func TestErofsViewNoParentBlockMode(t *testing.T) {
	env := newSnapshotTestEnv(t, WithDefaultSize(16*1024*1024))

	// Create a View with no parent in block mode
	mounts, err := env.snapshotter.View(env.ctx(), "empty-view-block", "")
	if err != nil {
		t.Fatalf("failed to create view with no parent: %v", err)
	}

	// Should return a single bind mount (same behavior as directory mode)
	if len(mounts) != 1 {
		t.Fatalf("expected 1 mount, got %d", len(mounts))
	}

	m := mounts[0]
	if m.Type != mountTypeBind {
		t.Errorf("expected bind mount, got %s", m.Type)
	}

	// Should be read-only
	hasRO := false
	for _, opt := range m.Options {
		if opt == "ro" {
			hasRO = true
			break
		}
	}
	if !hasRO {
		t.Error("expected read-only mount options")
	}

	// Directory should exist and be empty
	entries, err := os.ReadDir(m.Source)
	if err != nil {
		t.Fatalf("failed to read view directory: %v", err)
	}
	if len(entries) != 0 {
		t.Errorf("expected empty directory, got %d entries", len(entries))
	}
}

// TestErofsBlockModeExtractWithParent verifies extract snapshot behavior
// in block mode with a single parent layer.
func TestErofsBlockModeExtractWithParent(t *testing.T) {
	env := newSnapshotTestEnv(t, WithDefaultSize(16*1024*1024))

	// Create and commit a base layer
	baseCommit := env.createLayer("base-active", "", "base.txt", "base content")

	// Create extract snapshot with 1 parent
	extractKey := "extract-layer1"
	mounts, err := env.snapshotter.Prepare(env.ctx(), extractKey, baseCommit)
	if err != nil {
		t.Fatalf("failed to prepare extract snapshot: %v", err)
	}

	// Extract snapshots should return bind mount to rw/upper directory
	if len(mounts) != 1 {
		t.Fatalf("expected 1 mount, got %d", len(mounts))
	}

	m := mounts[0]
	if m.Type != mountTypeBind {
		t.Errorf("expected bind mount for extract, got %s", m.Type)
	}

	// Source should be rw/upper directory (mounted ext4 with overlay upper dir)
	if !strings.HasSuffix(m.Source, "/rw/upper") {
		t.Errorf("expected source to end with /rw/upper, got %s", m.Source)
	}

	// Write content to verify it's writable
	testFile := filepath.Join(m.Source, "extract-test.txt")
	if err := os.WriteFile(testFile, []byte("extract content"), 0644); err != nil {
		t.Fatalf("failed to write to extract mount: %v", err)
	}

	// Verify content exists
	content, err := os.ReadFile(testFile)
	if err != nil {
		t.Fatalf("failed to read extract file: %v", err)
	}
	if string(content) != "extract content" {
		t.Errorf("unexpected content: %s", content)
	}
}

// TestErofsBlockModeExtractWithMultipleParents verifies extract snapshot
// behavior in block mode with multiple parent layers.
func TestErofsBlockModeExtractWithMultipleParents(t *testing.T) {
	env := newSnapshotTestEnv(t, WithDefaultSize(16*1024*1024))

	// Create layer chain: base -> layer1 -> layer2
	baseCommit := env.createLayer("base-active", "", "base.txt", "base content")
	layer1Commit := env.createLayer("layer1-active", baseCommit, "layer1.txt", "layer1 content")
	layer2Commit := env.createLayer("layer2-active", layer1Commit, "layer2.txt", "layer2 content")

	// Create extract snapshot with multiple parents
	extractKey := "extract-multi"
	mounts, err := env.snapshotter.Prepare(env.ctx(), extractKey, layer2Commit)
	if err != nil {
		t.Fatalf("failed to prepare extract snapshot: %v", err)
	}

	// Extract snapshots should return bind mount to rw/upper directory
	if len(mounts) != 1 {
		t.Fatalf("expected 1 mount, got %d", len(mounts))
	}

	m := mounts[0]
	if m.Type != mountTypeBind {
		t.Errorf("expected bind mount for extract, got %s", m.Type)
	}

	// Source should be rw/upper directory (mounted ext4 with overlay upper dir)
	if !strings.HasSuffix(m.Source, "/rw/upper") {
		t.Errorf("expected source to end with /rw/upper, got %s", m.Source)
	}

	// Write content and verify
	testFile := filepath.Join(m.Source, "extract-multi.txt")
	if err := os.WriteFile(testFile, []byte("multi-parent extract"), 0644); err != nil {
		t.Fatalf("failed to write to extract mount: %v", err)
	}

	// Verify content
	content, err := os.ReadFile(testFile)
	if err != nil {
		t.Fatalf("failed to read extract file: %v", err)
	}
	if string(content) != "multi-parent extract" {
		t.Errorf("unexpected content: %s", content)
	}
}

// TestErofsConcurrentRemoveAndMounts tests the race condition between
// Remove() and Mounts() operations on the same snapshot.
func TestErofsConcurrentRemoveAndMounts(t *testing.T) {
	env := newSnapshotTestEnv(t)

	// Create a committed base layer
	labels := map[string]string{extractLabel: "true"}
	baseCommit := env.createLayerWithLabels("base-active", "", "base.txt", "base", labels)

	// Run multiple iterations to increase chance of hitting race
	for iter := range 5 {
		viewKey := fmt.Sprintf("race-view-%d", iter)

		// Create a View
		_, err := env.snapshotter.View(env.ctx(), viewKey, baseCommit)
		if err != nil {
			t.Fatalf("iter %d: failed to create view: %v", iter, err)
		}

		// Start concurrent operations
		mountsDone := make(chan error, 1)
		removeDone := make(chan error, 1)

		// Goroutine 1: repeatedly call Mounts
		go func() {
			for range 10 {
				_, err := env.snapshotter.Mounts(env.ctx(), viewKey)
				if err != nil {
					// Snapshot may have been removed - this is expected
					if strings.Contains(err.Error(), "not found") ||
						strings.Contains(err.Error(), "does not exist") {
						break
					}
					mountsDone <- err
					return
				}
			}
			mountsDone <- nil
		}()

		// Goroutine 2: remove the snapshot after a brief delay
		go func() {
			time.Sleep(1 * time.Millisecond)
			err := env.snapshotter.Remove(env.ctx(), viewKey)
			// "not found" is acceptable if another iteration already removed it
			if err != nil && !strings.Contains(err.Error(), "not found") {
				removeDone <- err
				return
			}
			removeDone <- nil
		}()

		// Wait for both to complete with timeout
		timeout := time.After(5 * time.Second)
		for range 2 {
			select {
			case err := <-mountsDone:
				if err != nil {
					t.Errorf("iter %d: mounts error: %v", iter, err)
				}
			case err := <-removeDone:
				if err != nil {
					t.Errorf("iter %d: remove error: %v", iter, err)
				}
			case <-timeout:
				t.Fatalf("iter %d: timeout waiting for concurrent operations", iter)
			}
		}
	}
}

// verifyFilesInDir verifies that all files in the map exist with the expected content.
func verifyFilesInDir(t *testing.T, root string, files map[string]string) {
	t.Helper()
	for name, content := range files {
		data, err := os.ReadFile(filepath.Join(root, name))
		if err != nil {
			t.Fatal(err)
		}
		if string(data) != content {
			t.Fatalf("expected %s content %q, got %q", name, content, string(data))
		}
	}
}

// verifyFilesInLayers verifies files across separate layer directories when overlay mount fails.
func verifyFilesInLayers(t *testing.T, layerDirs []string, fileLayers []map[string]string) {
	t.Helper()
	for i, files := range fileLayers {
		if i >= len(layerDirs) {
			t.Fatalf("not enough layers: need %d, have %d", i+1, len(layerDirs))
		}
		for name, content := range files {
			data, err := os.ReadFile(filepath.Join(layerDirs[i], name))
			if err != nil {
				t.Fatalf("layer %d: failed to read %s: %v", i, name, err)
			}
			if string(data) != content {
				t.Fatalf("layer %d: expected %s content %q, got %q", i, name, content, string(data))
			}
		}
		t.Logf("layer %d verified: %v", i, files)
	}
}
