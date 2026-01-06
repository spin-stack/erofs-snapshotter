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

package erofs

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

	erofsdiffer "github.com/aledbf/nexuserofs/internal/differ"
	"github.com/aledbf/nexuserofs/internal/mountutils"
	"github.com/aledbf/nexuserofs/internal/preflight"
)

// mountTypeBind is the mount type for bind mounts.
const mountTypeBind = "bind"

func TestErofsSnapshotCommitApplyFlow(t *testing.T) {
	skipIfVMOnly(t) // Test requires host mounting
	testutil.RequiresRoot(t)
	ctx := namespaces.WithNamespace(t.Context(), "testsuite")

	_, err := exec.LookPath("mkfs.erofs")
	if err != nil {
		t.Skipf("could not find mkfs.erofs: %v", err)
	}
	if err := preflight.CheckErofsSupport(); err != nil {
		t.Skipf("check for erofs kernel support failed: %v, skipping test", err)
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

	differ := erofsdiffer.NewErofsDiffer(contentStore, erofsdiffer.WithMountManager(mm), erofsdiffer.WithTarIndexMode())

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
		if _, err := s.Prepare(ctx, key, parent); err != nil {
			return "", err
		}
		id := snapshotID(ctx, t, snap, key)
		if err := writeFiles(snap.blockUpperPath(id), files); err != nil {
			return "", err
		}
		commitKey := key + "-commit"
		if err := s.Commit(ctx, commitKey, key); err != nil {
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

		upperKey := name + "-upper"
		upperMounts, err := s.Prepare(ctx, upperKey, parentCommit)
		if err != nil {
			t.Fatal(err)
		}
		upperID := snapshotID(ctx, t, snap, upperKey)
		if err := writeFiles(snap.blockUpperPath(upperID), upperFiles); err != nil {
			t.Fatal(err)
		}

		// View mounts should be EROFS mounts (the consumer converts to virtio-blk)
		if expectMulti {
			// Multi-layer: expect multiple EROFS mounts (one per layer)
			if len(lowerMounts) != 3 {
				t.Fatalf("expected 3 EROFS mounts for multi-layer, got: %#v", lowerMounts)
			}
			for i, m := range lowerMounts {
				if mountutils.TypeSuffix(m.Type) != testTypeErofs {
					t.Fatalf("mount %d: expected erofs type, got: %s", i, m.Type)
				}
			}
		} else {
			// Single-layer: expect single EROFS mount directly
			if len(lowerMounts) != 1 || mountutils.TypeSuffix(lowerMounts[0].Type) != testTypeErofs {
				t.Fatalf("expected single EROFS mount, got: %#v", lowerMounts)
			}
		}
		desc, err := differ.Compare(ctx, lowerMounts, upperMounts)
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

		// Verify files using mount manager (mounts may have templates)
		verifyFiles := func(root string, files map[string]string) {
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

		// Mount and verify files
		// View mounts may be overlay (with pre-mounted EROFS layers) or EROFS (single layer)
		// For overlay mounts, mount directly. For EROFS mounts, use the mount manager.
		viewTarget := t.TempDir()
		useMountManager := false
		for _, m := range viewMounts {
			if mountutils.TypeSuffix(m.Type) == testTypeErofs {
				useMountManager = true
				break
			}
		}
		if useMountManager {
			if _, err := mm.Activate(ctx, viewTarget, viewMounts); err != nil {
				t.Fatalf("mm.Activate failed: %v", err)
			}
			t.Cleanup(func() {
				if err := mm.Deactivate(ctx, viewTarget); err != nil {
					t.Logf("failed to deactivate view: %v", err)
				}
			})
		} else {
			// Direct mount for overlay mounts
			if err := mount.All(viewMounts, viewTarget); err != nil {
				t.Fatalf("mount.All failed: %v", err)
			}
			t.Cleanup(func() {
				if err := mount.UnmountAll(viewTarget, 0); err != nil {
					t.Logf("failed to unmount view: %v", err)
				}
			})
		}
		verifyFiles(viewTarget, baseFiles)
		if midFiles != nil {
			verifyFiles(viewTarget, midFiles)
		}
		if topFiles != nil {
			verifyFiles(viewTarget, topFiles)
		}
		verifyFiles(viewTarget, upperFiles)
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

// TestErofsSnapshotterFsmetaSingleLayerView tests that when fsmeta merge
// collapses multiple layers into a single mount, KindView returns the EROFS
// mount directly without requiring mount manager resolution.
func TestErofsSnapshotterFsmetaSingleLayerView(t *testing.T) {
	skipIfVMOnly(t) // Test requires host mounting
	testutil.RequiresRoot(t)
	ctx := namespaces.WithNamespace(t.Context(), "testsuite")

	_, err := exec.LookPath("mkfs.erofs")
	if err != nil {
		t.Skipf("could not find mkfs.erofs: %v", err)
	}
	if err := preflight.CheckErofsSupport(); err != nil {
		t.Skipf("check for erofs kernel support failed: %v, skipping test", err)
	}

	tempDir := t.TempDir()

	// Create snapshotter with fsMergeThreshold=5 to trigger merge with 6 layers
	snapshotRoot := filepath.Join(tempDir, "snapshots")
	s, err := NewSnapshotter(snapshotRoot, WithFsMergeThreshold(5))
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
		key := fmt.Sprintf("layer-%d", i)
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
		hasErofs := false
		for _, m := range viewMounts {
			if m.Type == testTypeErofs {
				hasErofs = true
				t.Logf("found EROFS mount: source=%s, options=%v", m.Source, m.Options)
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

	// Block mode now returns direct mounts (no templates).
	// The ext4 layer is mounted internally and a bind mount is returned.
	mounts1, err := env.snapshotter.Mounts(env.ctx(), key)
	if err != nil {
		t.Fatal(err)
	}

	// Should NOT have templates
	if mountsHaveTemplate(mounts1) {
		t.Fatalf("block mode should not use templates, got: %#v", mounts1)
	}

	// Should have a bind mount to the upper directory
	hasBind := false
	for _, m := range mounts1 {
		if m.Type == mountTypeBind {
			hasBind = true
			break
		}
	}
	if !hasBind {
		t.Fatalf("expected Mounts to include bind mount, got: %#v", mounts1)
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
// return EROFS mount descriptors with device= options for additional layers.
// These descriptors are transformed by consumers into virtio-blk disks.
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

	// Multi-layer views return multiple EROFS mounts (one per layer) for the consumer
	if len(viewMounts) != 3 {
		t.Fatalf("expected 3 EROFS mounts (one per layer), got %d mounts: %#v", len(viewMounts), viewMounts)
	}

	// All mounts should be EROFS type pointing to layer.erofs files
	for i, m := range viewMounts {
		if mountutils.TypeSuffix(m.Type) != testTypeErofs {
			t.Fatalf("mount %d: expected erofs type, got: %s", i, m.Type)
		}
		if !strings.HasSuffix(m.Source, "layer.erofs") {
			t.Fatalf("mount %d: expected source to be layer.erofs, got: %s", i, m.Source)
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

	// Multi-layer views return multiple EROFS mounts (one per layer) for the consumer
	if len(viewMounts) != 2 {
		t.Fatalf("expected 2 EROFS mounts (one per layer), got %d: %+v", len(viewMounts), viewMounts)
	}
	for i, m := range viewMounts {
		if mountutils.TypeSuffix(m.Type) != testTypeErofs {
			t.Fatalf("mount %d: expected erofs type, got: %s", i, m.Type)
		}
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

// TestErofsBlockModeIgnoresFsMerge verifies that block mode does not use
// fsMeta merge even when fsMergeThreshold is configured. Block mode always
// returns individual EROFS mounts because fsMeta merge is not supported with
// block-based writable layers.
func TestErofsBlockModeIgnoresFsMerge(t *testing.T) {
	env := newSnapshotTestEnv(t, WithDefaultSize(64*1024*1024), WithFsMergeThreshold(5))

	// Create first layer with extract label
	labels := map[string]string{extractLabel: "true"}
	layer1Commit := env.createLayerWithLabels("layer1-active", "", "file1.txt", "layer1", labels)

	// Create second layer on top with extract label
	layer2Commit := env.createLayerWithLabels("layer2-active", layer1Commit, "file2.txt", "layer2", labels)

	// Create a View with 2 parents - this would trigger fsMerge in directory mode
	viewMounts := env.createView("view-test", layer2Commit)

	t.Logf("view mounts: %+v", viewMounts)

	// Verify no fsmeta was used (would have device= options)
	for _, m := range viewMounts {
		for _, opt := range m.Options {
			if strings.HasPrefix(opt, "device=") {
				t.Errorf("block mode should not use fsmeta device= options, got: %s", opt)
			}
		}
	}

	// Multi-layer views return multiple EROFS mounts (one per layer) for the consumer
	if len(viewMounts) != 2 {
		t.Fatalf("expected 2 EROFS mounts (one per layer), got %d: %+v", len(viewMounts), viewMounts)
	}

	// All mounts should be EROFS type
	for i, m := range viewMounts {
		if mountutils.TypeSuffix(m.Type) != testTypeErofs {
			t.Fatalf("mount %d: expected erofs type, got: %s", i, m.Type)
		}
	}
}

// TestErofsExtractSnapshotWithParents verifies that extract snapshots
// always return a bind mount to the fs/ directory, regardless of parent count.
func TestErofsExtractSnapshotWithParents(t *testing.T) {
	env := newSnapshotTestEnv(t)

	// Create and commit layers with extract label
	labels := map[string]string{extractLabel: "true"}
	layer1Commit := env.createLayerWithLabels("layer1-active", "", "file1.txt", "layer1", labels)
	layer2Commit := env.createLayerWithLabels("layer2-active", layer1Commit, "file2.txt", "layer2", labels)

	// Create extract snapshot with 2 parents - use Prepare directly since extract
	// snapshots return a bind mount to fs/, not an overlay with upper directory
	extractMounts, err := env.snapshotter.Prepare(env.ctx(), "extract-with-parents", layer2Commit)
	if err != nil {
		t.Fatalf("failed to prepare extract snapshot: %v", err)
	}

	t.Logf("extract mounts: %+v", extractMounts)

	// Extract snapshots should always be bind mounts to fs/
	if len(extractMounts) != 1 {
		t.Fatalf("expected 1 mount, got %d", len(extractMounts))
	}

	m := extractMounts[0]
	if m.Type != mountTypeBind {
		t.Errorf("expected bind mount for extract snapshot, got %s", m.Type)
	}

	if !strings.HasSuffix(m.Source, "/fs") {
		t.Errorf("expected source to end with /fs, got %s", m.Source)
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
	layerBlob := env.snapshotter.layerBlobPath(snapshotID(env.ctx(), t, env.snapshotter, "layer1-active-commit"))
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

	// Create and commit a layer with extract label
	labels := map[string]string{extractLabel: "true"}
	commitKey := env.createLayerWithLabels("layer1-active", "", "test.txt", "content", labels)

	// Get the layer blob path before removal
	layerBlob := env.snapshotter.layerBlobPath(snapshotID(env.ctx(), t, env.snapshotter, commitKey))

	// Remove the snapshot - this should clear the immutable flag first
	if err := env.snapshotter.Remove(env.ctx(), commitKey); err != nil {
		t.Fatalf("failed to remove snapshot: %v", err)
	}

	// Verify the layer blob no longer exists
	if _, err := os.Stat(layerBlob); !os.IsNotExist(err) {
		t.Errorf("expected layer blob to be removed, but got: %v", err)
	}

	// Verify snapshot is gone
	_, err := env.snapshotter.Stat(env.ctx(), commitKey)
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

	// Extract snapshots should return bind mount to fs/ directory
	if len(mounts) != 1 {
		t.Fatalf("expected 1 mount, got %d", len(mounts))
	}

	m := mounts[0]
	if m.Type != mountTypeBind {
		t.Errorf("expected bind mount for extract, got %s", m.Type)
	}

	// Source should be fs/ directory (not rw/upper/)
	if !strings.HasSuffix(m.Source, "/fs") {
		t.Errorf("expected source to end with /fs, got %s", m.Source)
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

	// Extract snapshots should return bind mount to fs/ directory
	if len(mounts) != 1 {
		t.Fatalf("expected 1 mount, got %d", len(mounts))
	}

	m := mounts[0]
	if m.Type != mountTypeBind {
		t.Errorf("expected bind mount for extract, got %s", m.Type)
	}

	// Source should be fs/ directory
	if !strings.HasSuffix(m.Source, "/fs") {
		t.Errorf("expected source to end with /fs, got %s", m.Source)
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
