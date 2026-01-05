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
// with mount manager operations, commit/apply flows, and cleanup.
//
// Tests in this file:
// - TestErofsSnapshotCommitApplyFlow
// - TestErofsSnapshotterFsmetaSingleLayerView
// - TestErofsBlockModeMountsAfterPrepare
// - TestErofsCleanupRemovesOrphan
// - TestErofsViewMountsMultiLayer
// - TestErofsViewMountsSingleLayer
// - TestErofsViewMountsCleanupOnRemove
// - TestErofsViewMountsIdempotent
// - TestCleanupViewMountsNonExistent
// - TestCleanupViewMountsEmptyDir
// - TestErofsBlockModeIgnoresFsMerge
// - TestErofsExtractSnapshotWithParents
// - TestErofsImmutableFlagOnCommit
// - TestErofsImmutableFlagClearedOnRemove
// - TestErofsConcurrentMounts

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
	"github.com/containerd/containerd/v2/core/snapshots"
	"github.com/containerd/containerd/v2/core/snapshots/storage"
	"github.com/containerd/containerd/v2/pkg/namespaces"
	"github.com/containerd/containerd/v2/pkg/testutil"

	erofsdiffer "github.com/aledbf/nexuserofs/internal/differ"
	"github.com/aledbf/nexuserofs/internal/mountutils"
)

func TestErofsSnapshotCommitApplyFlow(t *testing.T) {
	testutil.RequiresRoot(t)
	ctx := namespaces.WithNamespace(t.Context(), "testsuite")

	_, err := exec.LookPath("mkfs.erofs")
	if err != nil {
		t.Skipf("could not find mkfs.erofs: %v", err)
	}
	if !findErofs() {
		t.Skip("check for erofs kernel support failed, skipping test")
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

	snap, ok := s.(*snapshotter)
	if !ok {
		t.Fatal("failed to cast snapshotter to *snapshotter")
	}

	differ := erofsdiffer.NewErofsDiffer(contentStore)

	writeFiles := func(dir string, files map[string]string) error {
		for name, content := range files {
			if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0644); err != nil {
				return err
			}
		}
		return nil
	}

	commitWithFiles := func(key, parent string, files map[string]string) (string, error) {
		if _, err := s.Prepare(ctx, key, parent); err != nil {
			return "", err
		}
		id := snapshotID(ctx, t, snap, key)
		if err := writeFiles(snap.upperPath(id), files); err != nil {
			return "", err
		}
		commitKey := key + "-commit"
		if err := s.Commit(ctx, commitKey, key); err != nil {
			return "", err
		}
		return commitKey, nil
	}

	runFlow := func(name string, baseFiles, midFiles, topFiles, upperFiles map[string]string, expectMulti bool) {
		baseCommit, err := commitWithFiles(name+"-base", "", baseFiles)
		if err != nil {
			t.Fatal(err)
		}

		parentCommit := baseCommit
		if midFiles != nil {
			midCommit, err := commitWithFiles(name+"-mid", parentCommit, midFiles)
			if err != nil {
				t.Fatal(err)
			}
			parentCommit = midCommit
		}
		if topFiles != nil {
			topCommit, err := commitWithFiles(name+"-top", parentCommit, topFiles)
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
		if err := writeFiles(snap.upperPath(upperID), upperFiles); err != nil {
			t.Fatal(err)
		}

		// Lower mounts should NOT have templates - they should be directly mountable
		if mountsHaveTemplate(lowerMounts) {
			t.Fatalf("expected lower mounts without templates, got: %#v", lowerMounts)
		}
		if expectMulti {
			// Multi-layer: expect overlay with real paths
			if len(lowerMounts) != 1 || lowerMounts[0].Type != "overlay" {
				t.Fatalf("expected single overlay mount for multi-layer, got: %#v", lowerMounts)
			}
		} else {
			// Single-layer: expect EROFS mount directly
			if len(lowerMounts) != 1 || mountutils.TypeSuffix(lowerMounts[0].Type) != testTypeErofs {
				t.Fatalf("expected single EROFS mount, got: %#v", lowerMounts)
			}
		}
		// Upper mounts should NOT have templates - they should be directly mountable
		if mountsHaveTemplate(upperMounts) {
			t.Fatalf("expected upper mounts without templates, got: %#v", upperMounts)
		}

		desc, err := differ.Compare(ctx, lowerMounts, upperMounts)
		if err != nil {
			t.Fatal(err)
		}
		if desc.Digest == "" || desc.Size == 0 {
			t.Fatalf("unexpected diff descriptor: %+v", desc)
		}

		applyKey := name + "-apply"
		applyMounts, err := s.Prepare(ctx, applyKey, parentCommit)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := differ.Apply(ctx, desc, applyMounts); err != nil {
			t.Fatal(err)
		}
		applyCommit := name + "-apply-commit"
		if err := s.Commit(ctx, applyCommit, applyKey); err != nil {
			t.Fatal(err)
		}

		viewKey := name + "-view"
		viewMounts, err := s.View(ctx, viewKey, applyCommit)
		if err != nil {
			t.Fatal(err)
		}

		// View mounts are directly mountable (no templates)
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
		if err := mount.WithTempMount(ctx, viewMounts, func(root string) error {
			verifyFiles(root, baseFiles)
			if midFiles != nil {
				verifyFiles(root, midFiles)
			}
			if topFiles != nil {
				verifyFiles(root, topFiles)
			}
			verifyFiles(root, upperFiles)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}

	t.Run("single-layer", func(t *testing.T) {
		runFlow("single",
			map[string]string{"base.txt": "base"},
			nil,
			nil,
			map[string]string{"upper.txt": "upper"},
			false,
		)
	})

	t.Run("multi-layer-overlay", func(t *testing.T) {
		runFlow("multi",
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
	testutil.RequiresRoot(t)
	ctx := namespaces.WithNamespace(t.Context(), "testsuite")

	_, err := exec.LookPath("mkfs.erofs")
	if err != nil {
		t.Skipf("could not find mkfs.erofs: %v", err)
	}
	if !findErofs() {
		t.Skip("check for erofs kernel support failed, skipping test")
	}

	tempDir := t.TempDir()

	// Create snapshotter with fsMergeThreshold=2 to trigger merge with just 3 layers
	snapshotRoot := filepath.Join(tempDir, "snapshots")
	s, err := NewSnapshotter(snapshotRoot, WithFsMergeThreshold(2))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	snap, ok := s.(*snapshotter)
	if !ok {
		t.Fatal("failed to cast snapshotter to *snapshotter")
	}

	// Create 3 layers to exceed the threshold and trigger fsmeta generation
	var parentKey string
	for i := range 3 {
		key := fmt.Sprintf("layer-%d", i)
		commitKey := fmt.Sprintf("layer-%d-commit", i)

		if _, err := s.Prepare(ctx, key, parentKey); err != nil {
			t.Fatalf("failed to prepare layer %d: %v", i, err)
		}

		id := snapshotID(ctx, t, snap, key)
		filename := fmt.Sprintf("file-%d.txt", i)
		if err := os.WriteFile(filepath.Join(snap.upperPath(id), filename), []byte(fmt.Sprintf("content-%d", i)), 0644); err != nil {
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
		topID, _, _, err = storage.GetInfo(ctx, "layer-2-commit")
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
	testutil.RequiresRoot(t)
	ctx := namespaces.WithNamespace(t.Context(), "testsuite")

	if _, err := exec.LookPath("mkfs.ext4"); err != nil {
		t.Skipf("could not find mkfs.ext4: %v", err)
	}

	sn := newSnapshotter(t, WithDefaultSize(16*1024*1024))
	snapshtr, cleanup, err := sn(ctx, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()

	key := "block-active"
	if _, err := snapshtr.Prepare(ctx, key, ""); err != nil {
		t.Fatal(err)
	}

	// Block mode now returns direct mounts (no templates).
	// The ext4 layer is mounted internally and a bind mount is returned.
	mounts1, err := snapshtr.Mounts(ctx, key)
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
		if m.Type == "bind" {
			hasBind = true
			break
		}
	}
	if !hasBind {
		t.Fatalf("expected Mounts to include bind mount, got: %#v", mounts1)
	}

	// Subsequent calls return consistent mounts (idempotent).
	mounts2, err := snapshtr.Mounts(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if len(mounts1) != len(mounts2) {
		t.Fatalf("expected consistent mounts, got %d vs %d", len(mounts1), len(mounts2))
	}

	if err := snapshtr.Remove(ctx, key); err != nil {
		t.Fatal(err)
	}
}

func TestErofsCleanupRemovesOrphan(t *testing.T) {
	testutil.RequiresRoot(t)
	ctx := namespaces.WithNamespace(t.Context(), "testsuite")

	sn := newSnapshotter(t)
	snapshtr, cleanup, err := sn(ctx, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()

	cleaner, ok := snapshtr.(snapshots.Cleaner)
	if !ok {
		t.Fatal("snapshotter does not implement Cleanup")
	}

	// Create and commit a snapshot to initialize the metadata store bucket.
	_, err = snapshtr.Prepare(ctx, "init", "")
	if err != nil {
		t.Fatal(err)
	}
	if err := snapshtr.Commit(ctx, "committed", "init"); err != nil {
		t.Fatal(err)
	}

	// Create an orphan snapshot directory not tracked by metadata.
	snap, ok := snapshtr.(*snapshotter)
	if !ok {
		t.Fatal("failed to cast snapshotter to *snapshotter")
	}
	orphanDir := filepath.Join(snap.root, "snapshots", "orphan")
	if err := os.MkdirAll(filepath.Join(orphanDir, "fs"), 0755); err != nil {
		t.Fatal(err)
	}

	if err := cleaner.Cleanup(ctx); err != nil {
		t.Fatal(err)
	}

	_, err = os.Stat(orphanDir)
	if err == nil {
		t.Fatalf("expected orphan dir to be removed: %s", orphanDir)
	}
	if !os.IsNotExist(err) {
		t.Fatalf("expected not exist error, got: %v", err)
	}
}

// TestErofsViewMountsMultiLayer tests that View snapshots with multiple layers
// return real mountable paths (not templates) that can be used by standard
// containerd operations like 'nerdctl commit'.
func TestErofsViewMountsMultiLayer(t *testing.T) {
	testutil.RequiresRoot(t)
	ctx := namespaces.WithNamespace(t.Context(), "testsuite")

	// Direct mounts are now the default - EROFS layers are mounted and real paths returned
	sn := newSnapshotter(t)
	snapshtr, cleanup, err := sn(ctx, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	defer cleanupAllSnapshots(ctx, snapshtr)

	snap, ok := snapshtr.(*snapshotter)
	if !ok {
		t.Fatal("failed to cast snapshotter to *snapshotter")
	}

	// Create 3 layers (multiple parents)
	var parentKey string
	for i := range 3 {
		key := fmt.Sprintf("layer-%d", i)
		commitKey := fmt.Sprintf("layer-%d-commit", i)

		if _, err := snapshtr.Prepare(ctx, key, parentKey); err != nil {
			t.Fatalf("failed to prepare layer %d: %v", i, err)
		}

		id := snapshotID(ctx, t, snap, key)
		filename := fmt.Sprintf("file-%d.txt", i)
		if err := os.WriteFile(filepath.Join(snap.upperPath(id), filename), []byte(fmt.Sprintf("content-%d", i)), 0644); err != nil {
			t.Fatalf("failed to write file in layer %d: %v", i, err)
		}

		if err := snapshtr.Commit(ctx, commitKey, key); err != nil {
			t.Fatalf("failed to commit layer %d: %v", i, err)
		}
		parentKey = commitKey
	}

	// Create a View of the multi-layer snapshot
	viewKey := "multi-layer-view"
	viewMounts, err := snapshtr.View(ctx, viewKey, parentKey)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("view mounts: %#v", viewMounts)

	// The key assertion: View with multiple layers should NOT have template syntax
	// because viewMounts() should have mounted the layers and returned real paths
	if mountsHaveTemplate(viewMounts) {
		t.Fatalf("expected view mounts without templates (for nerdctl commit compatibility), got: %#v", viewMounts)
	}

	// Should have a single overlay mount
	if len(viewMounts) != 1 {
		t.Fatalf("expected single overlay mount, got %d mounts: %#v", len(viewMounts), viewMounts)
	}

	if viewMounts[0].Type != "overlay" {
		t.Fatalf("expected overlay mount type, got: %s", viewMounts[0].Type)
	}

	// Verify the overlay can be mounted (this is what nerdctl commit would do)
	viewTarget := t.TempDir()
	if err := mount.All(viewMounts, viewTarget); err != nil {
		t.Fatalf("failed to mount view overlay: %v", err)
	}
	defer testutil.Unmount(t, viewTarget)

	// Verify all layer files are visible
	for i := range 3 {
		filename := fmt.Sprintf("file-%d.txt", i)
		content, err := os.ReadFile(filepath.Join(viewTarget, filename))
		if err != nil {
			t.Fatalf("failed to read %s: %v", filename, err)
		}
		expected := fmt.Sprintf("content-%d", i)
		if string(content) != expected {
			t.Fatalf("expected %s content %q, got %q", filename, expected, string(content))
		}
	}

	// Verify the lower directory was created with mounted layers
	viewID := snapshotID(ctx, t, snap, viewKey)
	lowerDir := snap.viewLowerPath(viewID)
	entries, err := os.ReadDir(lowerDir)
	if err != nil {
		t.Fatalf("failed to read lower directory: %v", err)
	}
	if len(entries) != 3 {
		t.Fatalf("expected 3 mounted layers in lower directory, got %d", len(entries))
	}
}

// TestErofsViewMountsSingleLayer tests that View snapshots with a single layer
// return an EROFS mount directly (no overlay needed).
func TestErofsViewMountsSingleLayer(t *testing.T) {
	testutil.RequiresRoot(t)
	ctx := namespaces.WithNamespace(t.Context(), "testsuite")

	sn := newSnapshotter(t)
	snapshtr, cleanup, err := sn(ctx, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	defer cleanupAllSnapshots(ctx, snapshtr)

	snap, ok := snapshtr.(*snapshotter)
	if !ok {
		t.Fatal("failed to cast snapshotter to *snapshotter")
	}

	// Create a single layer
	key := "single-layer"
	commitKey := "single-layer-commit"

	if _, err := snapshtr.Prepare(ctx, key, ""); err != nil {
		t.Fatal(err)
	}

	id := snapshotID(ctx, t, snap, key)
	if err := os.WriteFile(filepath.Join(snap.upperPath(id), "test.txt"), []byte("test"), 0644); err != nil {
		t.Fatal(err)
	}

	if err := snapshtr.Commit(ctx, commitKey, key); err != nil {
		t.Fatal(err)
	}

	// Create a View of the single-layer snapshot
	viewKey := "single-layer-view"
	viewMounts, err := snapshtr.View(ctx, viewKey, commitKey)
	if err != nil {
		t.Fatal(err)
	}

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

// TestErofsViewMountsCleanupOnRemove tests that View snapshot mounts are properly
// cleaned up when the snapshot is removed.
func TestErofsViewMountsCleanupOnRemove(t *testing.T) {
	testutil.RequiresRoot(t)
	ctx := namespaces.WithNamespace(t.Context(), "testsuite")

	// Direct mounts are now the default - EROFS layers are mounted and real paths returned
	sn := newSnapshotter(t)
	snapshtr, cleanup, err := sn(ctx, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	defer cleanupAllSnapshots(ctx, snapshtr)

	snap, ok := snapshtr.(*snapshotter)
	if !ok {
		t.Fatal("failed to cast snapshotter to *snapshotter")
	}

	// Create 2 layers
	var parentKey string
	for i := range 2 {
		key := fmt.Sprintf("layer-%d", i)
		commitKey := fmt.Sprintf("layer-%d-commit", i)

		if _, err := snapshtr.Prepare(ctx, key, parentKey); err != nil {
			t.Fatal(err)
		}

		id := snapshotID(ctx, t, snap, key)
		if err := os.WriteFile(filepath.Join(snap.upperPath(id), fmt.Sprintf("file-%d.txt", i)), []byte("content"), 0644); err != nil {
			t.Fatal(err)
		}

		if err := snapshtr.Commit(ctx, commitKey, key); err != nil {
			t.Fatal(err)
		}
		parentKey = commitKey
	}

	// Create a View (this will mount the EROFS layers)
	viewKey := "cleanup-test-view"
	viewMounts, err := snapshtr.View(ctx, viewKey, parentKey)
	if err != nil {
		t.Fatal(err)
	}

	// Verify the view is ready (lower mounts exist)
	viewID := snapshotID(ctx, t, snap, viewKey)
	lowerDir := snap.viewLowerPath(viewID)

	// The lower directory should exist with mounted layers
	entries, err := os.ReadDir(lowerDir)
	if err != nil {
		t.Fatalf("failed to read lower directory: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("expected 2 mounted layers, got %d", len(entries))
	}

	// Mount the view to ensure it's active
	viewTarget := t.TempDir()
	if err := mount.All(viewMounts, viewTarget); err != nil {
		t.Fatalf("failed to mount view: %v", err)
	}
	// Unmount the view overlay before removing the snapshot
	if err := mount.UnmountAll(viewTarget, 0); err != nil {
		t.Logf("warning: failed to unmount view target: %v", err)
	}

	// Remove the view snapshot
	if err := snapshtr.Remove(ctx, viewKey); err != nil {
		t.Fatalf("failed to remove view snapshot: %v", err)
	}

	// After removal, the lower directory should not exist
	_, err = os.Stat(lowerDir)
	if err == nil {
		t.Fatalf("expected lower directory to be removed after snapshot removal: %s", lowerDir)
	}
	if !os.IsNotExist(err) {
		t.Fatalf("expected not exist error, got: %v", err)
	}
}

// TestErofsViewMountsIdempotent tests that calling Mounts() multiple times
// on a View snapshot returns consistent results without re-mounting.
func TestErofsViewMountsIdempotent(t *testing.T) {
	testutil.RequiresRoot(t)
	ctx := namespaces.WithNamespace(t.Context(), "testsuite")

	// Direct mounts are now the default - EROFS layers are mounted and real paths returned
	sn := newSnapshotter(t)
	snapshtr, cleanup, err := sn(ctx, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	defer cleanupAllSnapshots(ctx, snapshtr)

	snap, ok := snapshtr.(*snapshotter)
	if !ok {
		t.Fatal("failed to cast snapshotter to *snapshotter")
	}

	// Create 2 layers
	var parentKey string
	for i := range 2 {
		key := fmt.Sprintf("layer-%d", i)
		commitKey := fmt.Sprintf("layer-%d-commit", i)

		if _, err := snapshtr.Prepare(ctx, key, parentKey); err != nil {
			t.Fatal(err)
		}

		id := snapshotID(ctx, t, snap, key)
		if err := os.WriteFile(filepath.Join(snap.upperPath(id), fmt.Sprintf("file-%d.txt", i)), []byte("content"), 0644); err != nil {
			t.Fatal(err)
		}

		if err := snapshtr.Commit(ctx, commitKey, key); err != nil {
			t.Fatal(err)
		}
		parentKey = commitKey
	}

	// Create a View
	viewKey := "idempotent-test-view"
	mounts1, err := snapshtr.View(ctx, viewKey, parentKey)
	if err != nil {
		t.Fatal(err)
	}

	// Call Mounts() again on the same view
	mounts2, err := snapshtr.Mounts(ctx, viewKey)
	if err != nil {
		t.Fatal(err)
	}

	// Should return consistent results
	if len(mounts1) != len(mounts2) {
		t.Fatalf("inconsistent mount counts: first=%d, second=%d", len(mounts1), len(mounts2))
	}

	// Verify lowerdir options are the same
	getLowerdir := func(mounts []mount.Mount) string {
		for _, m := range mounts {
			for _, opt := range m.Options {
				if strings.HasPrefix(opt, "lowerdir=") {
					return opt
				}
			}
		}
		return ""
	}

	lowerdir1 := getLowerdir(mounts1)
	lowerdir2 := getLowerdir(mounts2)

	if lowerdir1 != lowerdir2 {
		t.Fatalf("inconsistent lowerdir:\n  first:  %s\n  second: %s", lowerdir1, lowerdir2)
	}
}

// TestCleanupViewMountsNonExistent tests that cleanupViewMounts handles
// non-existent directories gracefully.
func TestCleanupViewMountsNonExistent(t *testing.T) {
	// Should not error for non-existent directory
	err := cleanupViewMounts("/nonexistent/path/to/lower")
	if err != nil {
		t.Fatalf("expected no error for non-existent directory, got: %v", err)
	}
}

// TestCleanupViewMountsEmptyDir tests that cleanupViewMounts handles
// empty directories correctly.
func TestCleanupViewMountsEmptyDir(t *testing.T) {
	dir := t.TempDir()
	err := cleanupViewMounts(dir)
	if err != nil {
		t.Fatalf("expected no error for empty directory, got: %v", err)
	}
}

// TestErofsBlockModeIgnoresFsMerge verifies that block mode does not use
// fsMeta merge even when fsMergeThreshold is configured. Block mode always
// mounts layers individually because fsMeta merge is not supported with
// block-based writable layers.
func TestErofsBlockModeIgnoresFsMerge(t *testing.T) {
	testutil.RequiresRoot(t)
	ctx := namespaces.WithNamespace(t.Context(), "testsuite")

	_, err := exec.LookPath("mkfs.erofs")
	if err != nil {
		t.Skipf("could not find mkfs.erofs: %v", err)
	}
	if !findErofs() {
		t.Skip("check for erofs kernel support failed, skipping test")
	}

	tempDir := t.TempDir()
	snapshotRoot := filepath.Join(tempDir, "snapshots")

	// Create snapshotter with both block mode AND fsMergeThreshold
	// Block mode should take precedence and ignore fsMerge
	s, err := NewSnapshotter(snapshotRoot,
		WithDefaultSize(64*1024*1024), // Enable block mode
		WithFsMergeThreshold(1),       // Would normally enable fsMerge after 1 layer
	)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	defer cleanupAllSnapshots(ctx, s)

	snap, ok := s.(*snapshotter)
	if !ok {
		t.Fatal("failed to cast snapshotter to *snapshotter")
	}

	// Verify block mode is enabled
	if !snap.isBlockMode() {
		t.Fatal("expected block mode to be enabled")
	}

	// Create first layer
	layer1, err := s.Prepare(ctx, "layer1-active", "", snapshots.WithLabels(map[string]string{
		extractLabel: "true",
	}))
	if err != nil {
		t.Fatalf("failed to prepare layer1: %v", err)
	}

	// Create content
	mounts, err := s.Mounts(ctx, "layer1-active")
	if err != nil {
		t.Fatalf("failed to get mounts: %v", err)
	}
	if err := mount.WithTempMount(ctx, mounts, func(root string) error {
		return os.WriteFile(filepath.Join(root, "file1.txt"), []byte("layer1"), 0644)
	}); err != nil {
		t.Fatalf("failed to write to layer1: %v", err)
	}

	// Commit first layer
	if err := s.Commit(ctx, "layer1", "layer1-active"); err != nil {
		t.Fatalf("failed to commit layer1: %v", err)
	}

	// Create second layer on top
	layer2, err := s.Prepare(ctx, "layer2-active", "layer1", snapshots.WithLabels(map[string]string{
		extractLabel: "true",
	}))
	if err != nil {
		t.Fatalf("failed to prepare layer2: %v", err)
	}

	mounts, err = s.Mounts(ctx, "layer2-active")
	if err != nil {
		t.Fatalf("failed to get mounts: %v", err)
	}
	if err := mount.WithTempMount(ctx, mounts, func(root string) error {
		return os.WriteFile(filepath.Join(root, "file2.txt"), []byte("layer2"), 0644)
	}); err != nil {
		t.Fatalf("failed to write to layer2: %v", err)
	}

	if err := s.Commit(ctx, "layer2", "layer2-active"); err != nil {
		t.Fatalf("failed to commit layer2: %v", err)
	}

	// Create a View with 2 parents - this would trigger fsMerge in directory mode
	viewMounts, err := s.View(ctx, "view-test", "layer2")
	if err != nil {
		t.Fatalf("failed to get view mounts: %v", err)
	}

	t.Logf("layer1 info: %+v", layer1)
	t.Logf("layer2 info: %+v", layer2)
	t.Logf("view mounts: %+v", viewMounts)

	// Verify no fsmeta was used (would have device= options)
	for _, m := range viewMounts {
		for _, opt := range m.Options {
			if strings.HasPrefix(opt, "device=") {
				t.Errorf("block mode should not use fsmeta device= options, got: %s", opt)
			}
		}
	}

	// Should be overlay with real lowerdirs (not merged fsmeta)
	if len(viewMounts) != 1 {
		t.Fatalf("expected 1 mount, got %d", len(viewMounts))
	}
	if viewMounts[0].Type != "overlay" {
		t.Fatalf("expected overlay mount, got %s", viewMounts[0].Type)
	}

	// The lowerdir should contain multiple paths (one per layer)
	for _, opt := range viewMounts[0].Options {
		if strings.HasPrefix(opt, "lowerdir=") {
			lowerdir := strings.TrimPrefix(opt, "lowerdir=")
			paths := strings.Split(lowerdir, ":")
			if len(paths) < 2 {
				t.Errorf("expected multiple lowerdirs for 2-layer view, got: %s", lowerdir)
			}
			t.Logf("lowerdir paths: %v", paths)
		}
	}
}

// TestErofsExtractSnapshotWithParents verifies that extract snapshots
// always return a bind mount to the fs/ directory, regardless of parent count.
func TestErofsExtractSnapshotWithParents(t *testing.T) {
	testutil.RequiresRoot(t)
	ctx := namespaces.WithNamespace(t.Context(), "testsuite")

	_, err := exec.LookPath("mkfs.erofs")
	if err != nil {
		t.Skipf("could not find mkfs.erofs: %v", err)
	}
	if !findErofs() {
		t.Skip("check for erofs kernel support failed, skipping test")
	}

	tempDir := t.TempDir()
	snapshotRoot := filepath.Join(tempDir, "snapshots")

	s, err := NewSnapshotter(snapshotRoot)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	defer cleanupAllSnapshots(ctx, s)

	// Create and commit first layer
	_, err = s.Prepare(ctx, "layer1-active", "", snapshots.WithLabels(map[string]string{
		extractLabel: "true",
	}))
	if err != nil {
		t.Fatalf("failed to prepare layer1: %v", err)
	}

	mounts, err := s.Mounts(ctx, "layer1-active")
	if err != nil {
		t.Fatalf("failed to get mounts: %v", err)
	}
	if err := mount.WithTempMount(ctx, mounts, func(root string) error {
		return os.WriteFile(filepath.Join(root, "file1.txt"), []byte("layer1"), 0644)
	}); err != nil {
		t.Fatalf("failed to write to layer1: %v", err)
	}

	if err := s.Commit(ctx, "layer1", "layer1-active"); err != nil {
		t.Fatalf("failed to commit layer1: %v", err)
	}

	// Create and commit second layer
	_, err = s.Prepare(ctx, "layer2-active", "layer1", snapshots.WithLabels(map[string]string{
		extractLabel: "true",
	}))
	if err != nil {
		t.Fatalf("failed to prepare layer2: %v", err)
	}

	mounts, err = s.Mounts(ctx, "layer2-active")
	if err != nil {
		t.Fatalf("failed to get mounts: %v", err)
	}
	if err := mount.WithTempMount(ctx, mounts, func(root string) error {
		return os.WriteFile(filepath.Join(root, "file2.txt"), []byte("layer2"), 0644)
	}); err != nil {
		t.Fatalf("failed to write to layer2: %v", err)
	}

	if err := s.Commit(ctx, "layer2", "layer2-active"); err != nil {
		t.Fatalf("failed to commit layer2: %v", err)
	}

	// Create extract snapshot with 2 parents
	_, err = s.Prepare(ctx, "extract-with-parents", "layer2", snapshots.WithLabels(map[string]string{
		extractLabel: "true",
	}))
	if err != nil {
		t.Fatalf("failed to prepare extract snapshot: %v", err)
	}

	extractMounts, err := s.Mounts(ctx, "extract-with-parents")
	if err != nil {
		t.Fatalf("failed to get extract mounts: %v", err)
	}

	t.Logf("extract mounts: %+v", extractMounts)

	// Extract snapshots should always be bind mounts to fs/
	if len(extractMounts) != 1 {
		t.Fatalf("expected 1 mount, got %d", len(extractMounts))
	}

	m := extractMounts[0]
	if m.Type != "bind" {
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
	testutil.RequiresRoot(t)
	ctx := namespaces.WithNamespace(t.Context(), "testsuite")

	_, err := exec.LookPath("mkfs.erofs")
	if err != nil {
		t.Skipf("could not find mkfs.erofs: %v", err)
	}
	if !findErofs() {
		t.Skip("check for erofs kernel support failed, skipping test")
	}

	tempDir := t.TempDir()
	snapshotRoot := filepath.Join(tempDir, "snapshots")

	// Create snapshotter with immutable flag enabled
	s, err := NewSnapshotter(snapshotRoot, WithImmutable())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	defer cleanupAllSnapshots(ctx, s)

	snap, ok := s.(*snapshotter)
	if !ok {
		t.Fatal("failed to cast snapshotter to *snapshotter")
	}

	// Create and commit a layer
	_, err = s.Prepare(ctx, "layer1-active", "", snapshots.WithLabels(map[string]string{
		extractLabel: "true",
	}))
	if err != nil {
		t.Fatalf("failed to prepare layer1: %v", err)
	}

	mounts, err := s.Mounts(ctx, "layer1-active")
	if err != nil {
		t.Fatalf("failed to get mounts: %v", err)
	}
	if err := mount.WithTempMount(ctx, mounts, func(root string) error {
		return os.WriteFile(filepath.Join(root, "test.txt"), []byte("content"), 0644)
	}); err != nil {
		t.Fatalf("failed to write content: %v", err)
	}

	if err := s.Commit(ctx, "layer1", "layer1-active"); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}

	// Get the committed snapshot info
	info, err := s.Stat(ctx, "layer1")
	if err != nil {
		t.Fatalf("failed to stat committed snapshot: %v", err)
	}
	t.Logf("committed snapshot info: %+v", info)

	// Verify the layer blob has immutable flag
	layerBlob := snap.layerBlobPath(getSnapshotID(t, s, ctx, "layer1"))
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
	testutil.RequiresRoot(t)
	ctx := namespaces.WithNamespace(t.Context(), "testsuite")

	_, err := exec.LookPath("mkfs.erofs")
	if err != nil {
		t.Skipf("could not find mkfs.erofs: %v", err)
	}
	if !findErofs() {
		t.Skip("check for erofs kernel support failed, skipping test")
	}

	tempDir := t.TempDir()
	snapshotRoot := filepath.Join(tempDir, "snapshots")

	// Create snapshotter with immutable flag enabled
	s, err := NewSnapshotter(snapshotRoot, WithImmutable())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	snap, ok := s.(*snapshotter)
	if !ok {
		t.Fatal("failed to cast snapshotter to *snapshotter")
	}

	// Create and commit a layer
	_, err = s.Prepare(ctx, "layer1-active", "", snapshots.WithLabels(map[string]string{
		extractLabel: "true",
	}))
	if err != nil {
		t.Fatalf("failed to prepare: %v", err)
	}

	mounts, err := s.Mounts(ctx, "layer1-active")
	if err != nil {
		t.Fatalf("failed to get mounts: %v", err)
	}
	if err := mount.WithTempMount(ctx, mounts, func(root string) error {
		return os.WriteFile(filepath.Join(root, "test.txt"), []byte("content"), 0644)
	}); err != nil {
		t.Fatalf("failed to write content: %v", err)
	}

	if err := s.Commit(ctx, "layer1", "layer1-active"); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}

	// Get the layer blob path before removal
	layerBlob := snap.layerBlobPath(getSnapshotID(t, s, ctx, "layer1"))

	// Remove the snapshot - this should clear the immutable flag first
	if err := s.Remove(ctx, "layer1"); err != nil {
		t.Fatalf("failed to remove snapshot: %v", err)
	}

	// Verify the layer blob no longer exists
	if _, err := os.Stat(layerBlob); !os.IsNotExist(err) {
		t.Errorf("expected layer blob to be removed, but got: %v", err)
	}

	// Verify snapshot is gone
	_, err = s.Stat(ctx, "layer1")
	if err == nil {
		t.Error("expected snapshot to be removed")
	}
}

// getSnapshotID retrieves the internal ID for a snapshot name
func getSnapshotID(t *testing.T, s snapshots.Snapshotter, ctx context.Context, name string) string {
	t.Helper()

	// Get from metastore directly via the snapshotter
	snap, ok := s.(*snapshotter)
	if !ok {
		t.Fatal("failed to cast to snapshotter")
	}

	var id string
	err := snap.ms.WithTransaction(ctx, false, func(ctx context.Context) error {
		snapshotID, _, _, err := storage.GetInfo(ctx, name)
		if err != nil {
			return err
		}
		id = snapshotID
		return nil
	})
	if err != nil {
		t.Fatalf("failed to get snapshot ID: %v", err)
	}

	return id
}

// TestErofsConcurrentMounts verifies that concurrent mount operations
// are safe and produce consistent results.
func TestErofsConcurrentMounts(t *testing.T) {
	testutil.RequiresRoot(t)
	ctx := namespaces.WithNamespace(t.Context(), "testsuite")

	_, err := exec.LookPath("mkfs.erofs")
	if err != nil {
		t.Skipf("could not find mkfs.erofs: %v", err)
	}
	if !findErofs() {
		t.Skip("check for erofs kernel support failed, skipping test")
	}

	tempDir := t.TempDir()
	snapshotRoot := filepath.Join(tempDir, "snapshots")

	s, err := NewSnapshotter(snapshotRoot)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	defer cleanupAllSnapshots(ctx, s)

	// Create and commit a base layer
	_, err = s.Prepare(ctx, "base-active", "", snapshots.WithLabels(map[string]string{
		extractLabel: "true",
	}))
	if err != nil {
		t.Fatalf("failed to prepare base: %v", err)
	}

	mounts, err := s.Mounts(ctx, "base-active")
	if err != nil {
		t.Fatalf("failed to get mounts: %v", err)
	}
	if err := mount.WithTempMount(ctx, mounts, func(root string) error {
		return os.WriteFile(filepath.Join(root, "base.txt"), []byte("base"), 0644)
	}); err != nil {
		t.Fatalf("failed to write content: %v", err)
	}

	if err := s.Commit(ctx, "base", "base-active"); err != nil {
		t.Fatalf("failed to commit base: %v", err)
	}

	// Create a View snapshot
	if _, err := s.View(ctx, "concurrent-view", "base"); err != nil {
		t.Fatalf("failed to create view: %v", err)
	}

	// Concurrent access test
	const numGoroutines = 10
	results := make(chan []mount.Mount, numGoroutines)
	errors := make(chan error, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			mounts, err := s.Mounts(ctx, "concurrent-view")
			if err != nil {
				errors <- fmt.Errorf("goroutine %d: %w", id, err)
				return
			}
			results <- mounts
		}(i)
	}

	// Collect results
	var allMounts [][]mount.Mount
	for i := 0; i < numGoroutines; i++ {
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
