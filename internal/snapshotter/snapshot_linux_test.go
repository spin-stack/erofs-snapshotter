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
	"github.com/containerd/containerd/v2/core/snapshots"
	"github.com/containerd/containerd/v2/core/snapshots/storage"
	"github.com/containerd/containerd/v2/pkg/namespaces"
	"github.com/containerd/containerd/v2/pkg/testutil"
	bolt "go.etcd.io/bbolt"

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

	differ := erofsdiffer.NewErofsDiffer(contentStore, erofsdiffer.WithMountManager(mm))

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

		if expectMulti {
			if !mountsHaveTemplate(lowerMounts) {
				t.Fatalf("expected lower mounts to include overlay templates, got: %#v", lowerMounts)
			}
		} else {
			if len(lowerMounts) != 1 || mountutils.TypeSuffix(lowerMounts[0].Type) != "erofs" {
				t.Fatalf("expected single EROFS mount, got: %#v", lowerMounts)
			}
		}
		if !mountsHaveTemplate(upperMounts) {
			t.Fatalf("expected upper mounts to include overlay templates, got: %#v", upperMounts)
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

		// Use mount manager to process template mounts (overlay templates need to be resolved)
		viewActivation, err := mm.Activate(ctx, viewKey+"-activation", cloneMounts(viewMounts))
		if err != nil {
			t.Fatal(err)
		}
		defer mm.Deactivate(ctx, viewActivation.Name)

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
		if err := mount.WithTempMount(ctx, viewActivation.System, func(root string) error {
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
			if m.Type == "erofs" {
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

	// Block mode returns template mounts when overlay is not mounted on host.
	// VM-based runtimes (like qemubox) need block devices, not bind mounts.
	mounts1, err := snapshtr.Mounts(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	hasExt4 := false
	for _, m := range mounts1 {
		if m.Type == "ext4" {
			hasExt4 = true
			break
		}
	}
	if !hasExt4 {
		t.Fatalf("expected Mounts to include ext4 template, got: %#v", mounts1)
	}

	// Subsequent calls also return template mounts (no overlay mounted on host).
	mounts2, err := snapshtr.Mounts(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if len(mounts1) != len(mounts2) {
		t.Fatalf("expected consistent template mounts, got %d vs %d", len(mounts1), len(mounts2))
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

	if viewMounts[0].Type != "erofs" {
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
