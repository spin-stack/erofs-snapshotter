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

// This file contains EROFS differ integration tests.
// These tests verify the differ's Compare and Apply functionality
// with various mount configurations.
//
// Tests in this file:
// - TestErofsDifferApply
// - TestErofsDifferCompareWithMountManager
// - TestErofsDifferCompareBlockUpperFallback
// - TestErofsDifferComparePreservesWhiteouts
// - TestErofsDifferCompareWithFormattedUpperMounts
// - TestErofsDifferCompareWithoutMountManager
// - TestErofsDifferCompareMultipleStackedLayers
// - TestErofsDifferCompareEmptyLowerMounts
// - TestErofsDifferCompareContextCancellation
// - TestErofsDifferCompareSingleLayerView
// - TestErofsDifferCompareViewWithMultipleLayers

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/containerd/containerd/v2/core/content"
	"github.com/containerd/containerd/v2/core/images/imagetest"
	"github.com/containerd/containerd/v2/core/mount"
	"github.com/containerd/containerd/v2/core/mount/manager"
	"github.com/containerd/containerd/v2/core/snapshots/storage"
	"github.com/containerd/containerd/v2/pkg/namespaces"
	"github.com/containerd/containerd/v2/pkg/testutil"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	bolt "go.etcd.io/bbolt"
	"golang.org/x/sys/unix"

	"github.com/spin-stack/erofs-snapshotter/internal/differ"
	"github.com/spin-stack/erofs-snapshotter/internal/erofs"
	"github.com/spin-stack/erofs-snapshotter/internal/loop"
	"github.com/spin-stack/erofs-snapshotter/internal/mountutils"
	"github.com/spin-stack/erofs-snapshotter/internal/preflight"
)

// Snapshot key constants used across tests
const (
	testKeyBase     = "base"
	testKeyUpper    = "upper"
	testKeyLower    = "lower"
	testTypeExt4    = "ext4"
	testTypeErofs   = "erofs"
	testTypeOverlay = "overlay"
)

// differTestEnv encapsulates the common test environment for differ tests.
// It provides helpers for creating layers, mount managers, and running comparisons.
type differTestEnv struct {
	t            *testing.T
	tempDir      string
	snapshotRoot string
	snapshotter  *snapshotter
	contentStore content.Store
}

// ctx returns the test context with the testsuite namespace.
func (e *differTestEnv) ctx() context.Context {
	return namespaces.WithNamespace(e.t.Context(), "testsuite")
}

// newDifferTestEnv creates a new test environment with all prerequisites checked.
// It skips the test if EROFS support is not available.
//
// NOTE: Tests using this helper require the mount manager to set up loop devices
// for EROFS and ext4 mounts. The containerd mount manager handles this automatically.
func newDifferTestEnv(t *testing.T) *differTestEnv {
	t.Helper()
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

	snap, ok := s.(*snapshotter)
	if !ok {
		t.Fatal("failed to cast snapshotter to *snapshotter")
	}

	env := &differTestEnv{
		t:            t,
		tempDir:      tempDir,
		snapshotRoot: snapshotRoot,
		snapshotter:  snap,
		contentStore: contentStore,
	}

	t.Cleanup(func() {
		cleanupAllSnapshots(ctx, s)
		s.Close()
		mount.UnmountRecursive(snapshotRoot, 0)
	})

	return env
}

// createMountManager creates a mount manager with cleanup registered.
func (e *differTestEnv) createMountManager() mount.Manager {
	e.t.Helper()

	db, err := bolt.Open(filepath.Join(e.tempDir, "mounts.db"), 0o600, nil)
	if err != nil {
		e.t.Fatal(err)
	}
	e.t.Cleanup(func() { db.Close() })

	mountRoot := filepath.Join(e.tempDir, "mounts")
	mm, err := manager.NewManager(db, mountRoot, manager.WithAllowedRoot(e.snapshotRoot))
	if err != nil {
		e.t.Fatal(err)
	}

	if closer, ok := mm.(interface{ Close() error }); ok {
		e.t.Cleanup(func() { closer.Close() })
	}
	e.t.Cleanup(func() { mount.UnmountRecursive(mountRoot, 0) })

	return mm
}

// createLayer creates and commits a layer with a single file.
// Returns the commit key for use as a parent.
// Uses extract-style key to ensure ext4 is mounted on host for writing.
func (e *differTestEnv) createLayer(key, parentKey, filename, content string) string {
	e.t.Helper()

	// Use extract-style key so the snapshotter mounts the ext4 on host
	extractKey := "extract-" + key
	if _, err := e.snapshotter.Prepare(e.ctx(), extractKey, parentKey); err != nil {
		e.t.Fatalf("failed to prepare %s: %v", extractKey, err)
	}

	id := snapshotID(e.ctx(), e.t, e.snapshotter, extractKey)
	filePath := filepath.Join(e.snapshotter.blockUpperPath(id), filename)
	if err := os.WriteFile(filePath, []byte(content), 0o644); err != nil {
		e.t.Fatalf("failed to write %s: %v", filename, err)
	}

	commitKey := key + "-commit"
	if err := e.snapshotter.Commit(e.ctx(), commitKey, extractKey); err != nil {
		e.t.Fatalf("failed to commit %s: %v", extractKey, err)
	}

	return commitKey
}

// prepareActiveLayer prepares an active (uncommitted) layer and writes a file to it.
// Returns the mounts for use in Compare.
// Uses extract-style key to ensure ext4 is mounted on host for writing.
func (e *differTestEnv) prepareActiveLayer(key, parentKey, filename, content string) []mount.Mount {
	e.t.Helper()

	// Use extract-style key so the snapshotter mounts the ext4 on host
	extractKey := "extract-" + key
	mounts, err := e.snapshotter.Prepare(e.ctx(), extractKey, parentKey)
	if err != nil {
		e.t.Fatalf("failed to prepare %s: %v", extractKey, err)
	}

	id := snapshotID(e.ctx(), e.t, e.snapshotter, extractKey)
	filePath := filepath.Join(e.snapshotter.blockUpperPath(id), filename)
	if err := os.WriteFile(filePath, []byte(content), 0o644); err != nil {
		e.t.Fatalf("failed to write %s: %v", filename, err)
	}

	return mounts
}

// populateGuestUpper mounts the ext4 writable layer from an active snapshot's
// mounts on the host and runs populate with the guest-style upper directory,
// mirroring what the guest VM does when it builds its overlay upper layer.
// The ext4 is unmounted and the loop device detached before returning so the
// differ can lock the image afterwards (mountutils.MountExt4 takes flock/OFD
// locks on the image file).
func (e *differTestEnv) populateGuestUpper(mounts []mount.Mount, populate func(upperDir string) error) {
	e.t.Helper()

	var ext4Source string
	for _, m := range mounts {
		if mountutils.TypeSuffix(m.Type) == testTypeExt4 {
			ext4Source = m.Source
			break
		}
	}
	if ext4Source == "" {
		e.t.Fatalf("expected ext4 mount in active snapshot mounts, got: %#v", mounts)
	}

	target, err := os.MkdirTemp(e.tempDir, "ext4-upper-")
	if err != nil {
		e.t.Fatal(err)
	}

	loopDev, err := loop.Setup(ext4Source, loop.Config{})
	if err != nil {
		e.t.Fatalf("failed to set up loop device for %s: %v", ext4Source, err)
	}
	mounted := false
	released := false
	defer func() {
		// Best-effort teardown on failure paths; the success path below
		// unmounts and detaches explicitly with error reporting.
		if !released {
			if mounted {
				_ = unix.Unmount(target, 0)
			}
			_ = loopDev.Detach()
		}
	}()

	if err := unix.Mount(loopDev.Path, target, testTypeExt4, 0, ""); err != nil {
		e.t.Fatalf("failed to mount ext4 %s: %v", ext4Source, err)
	}
	mounted = true

	// The guest creates /upper at the ext4 root (the layout
	// withActiveSnapshotMount in the differ expects).
	upperDir := filepath.Join(target, "upper")
	if err := os.MkdirAll(upperDir, 0o755); err != nil {
		e.t.Fatalf("failed to create guest upper directory: %v", err)
	}
	populateErr := populate(upperDir)

	if err := unix.Unmount(target, 0); err != nil {
		e.t.Fatalf("failed to unmount ext4: %v", err)
	}
	mounted = false
	if err := loopDev.Detach(); err != nil {
		e.t.Fatalf("failed to detach loop device: %v", err)
	}
	released = true

	if populateErr != nil {
		e.t.Fatalf("failed to populate guest upper: %v", populateErr)
	}
}

// createView creates a read-only view of a committed snapshot.
func (e *differTestEnv) createView(key, parentKey string) []mount.Mount {
	e.t.Helper()

	mounts, err := e.snapshotter.View(e.ctx(), key, parentKey)
	if err != nil {
		e.t.Fatalf("failed to create view %s: %v", key, err)
	}

	return mounts
}

// compareAndVerify runs Compare and verifies the result contains expected files.
func (e *differTestEnv) compareAndVerify(differ *differ.ErofsDiff, lower, upper []mount.Mount, expectedFiles ...string) ocispec.Descriptor {
	e.t.Helper()

	desc, err := differ.Compare(e.ctx(), lower, upper)
	if err != nil {
		e.t.Fatalf("Compare failed: %v", err)
	}
	if desc.Digest == "" || desc.Size == 0 {
		e.t.Fatalf("unexpected diff descriptor: %+v", desc)
	}

	for _, file := range expectedFiles {
		found, err := tarHasPath(e.ctx(), e.contentStore, desc, file)
		if err != nil {
			e.t.Fatal(err)
		}
		if !found {
			e.t.Fatalf("expected diff to include %s", file)
		}
	}

	return desc
}

//nolint:cyclop // Integration test with necessary setup complexity
func TestErofsDifferApply(t *testing.T) {
	testutil.RequiresRoot(t)
	ctx := namespaces.WithNamespace(t.Context(), "testsuite")

	if err := preflight.CheckErofsSupport(); err != nil {
		t.Skipf("EROFS support check failed: %v", err)
	}

	// Check if mkfs.erofs supports tar conversion mode (--tar=f)
	supported, err := erofs.SupportGenerateFromTar()
	if err != nil || !supported {
		t.Skip("mkfs.erofs does not support tar conversion mode")
	}

	tempDir := t.TempDir()

	// Create content store for the differ
	contentStore := imagetest.NewContentStore(ctx, t).Store

	// Create EROFS snapshotter first (creates the snapshot root directory).
	// All teardown is registered with t.Cleanup (not defer) so it runs in
	// reverse registration order AFTER the view unmount registered below:
	// unmount view -> mount manager -> db -> snapshots -> snapshotter.
	snapshotRoot := filepath.Join(tempDir, "snapshots")
	s, err := NewSnapshotter(snapshotRoot)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupAllSnapshots(ctx, s)
		s.Close()
		mount.UnmountRecursive(snapshotRoot, 0)
	})

	// Create mount manager for EROFS mounts (after snapshotter creates root)
	db, err := bolt.Open(filepath.Join(tempDir, "mounts.db"), 0o600, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })

	mountRoot := filepath.Join(tempDir, "mounts")
	mm, err := manager.NewManager(db, mountRoot, manager.WithAllowedRoot(snapshotRoot))
	if err != nil {
		t.Fatal(err)
	}
	if closer, ok := mm.(interface{ Close() error }); ok {
		t.Cleanup(func() { closer.Close() })
	}
	t.Cleanup(func() { mount.UnmountRecursive(mountRoot, 0) })

	// Create EROFS differ with mount manager
	differ := differ.NewErofsDiffer(contentStore, differ.WithMountManager(mm))

	// Create test tar content
	tarReader := createTestTarContent()
	defer tarReader.Close()

	// Read the tar content into a buffer for digest calculation and writing
	tarContent, err := io.ReadAll(tarReader)
	if err != nil {
		t.Fatal(err)
	}

	// Write tar content to content store
	desc := ocispec.Descriptor{
		MediaType: ocispec.MediaTypeImageLayerGzip,
		Digest:    digest.FromBytes(tarContent),
		Size:      int64(len(tarContent)),
	}

	writer, err := contentStore.Writer(ctx,
		content.WithRef("test-layer"),
		content.WithDescriptor(desc))
	if err != nil {
		t.Fatal(err)
	}

	if _, err := writer.Write(tarContent); err != nil {
		writer.Close()
		t.Fatal(err)
	}

	if err := writer.Commit(ctx, desc.Size, desc.Digest); err != nil {
		writer.Close()
		t.Fatal(err)
	}
	writer.Close()

	// Prepare a snapshot using the snapshotter with extract-style key
	snapshotKey := "extract-test-snapshot"
	mounts, err := s.Prepare(ctx, snapshotKey, "")
	if err != nil {
		t.Fatal(err)
	}

	// Apply the tar content using the EROFS differ
	appliedDesc, err := differ.Apply(ctx, desc, mounts)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("Applied layer using EROFS differ:")
	t.Logf("  Original: %s (%d bytes)", desc.Digest, desc.Size)
	t.Logf("  Applied:  %s (%d bytes)", appliedDesc.Digest, appliedDesc.Size)
	t.Logf("  MediaType: %s", appliedDesc.MediaType)

	// Commit the snapshot to finalize the EROFS layer creation
	commitKey := "test-commit"
	if err := s.Commit(ctx, commitKey, snapshotKey); err != nil {
		t.Fatal(err)
	}

	// Get the internal snapshot ID to check the EROFS layer file
	snap, ok := s.(*snapshotter)
	if !ok {
		t.Fatal("failed to cast snapshotter to *snapshotter")
	}
	var id string
	if err := snap.ms.WithTransaction(ctx, false, func(ctx context.Context) error {
		id, _, _, err = storage.GetInfo(ctx, commitKey)
		return err
	}); err != nil {
		t.Fatal(err)
	}

	// Verify the EROFS layer file was created
	layerPath, err := snap.findLayerBlob(id)
	if err != nil {
		t.Fatalf("Failed to find layer blob: %v", err)
	}
	if _, err := os.Stat(layerPath); err != nil {
		t.Fatalf("EROFS layer file should exist: %v", err)
	}

	// Verify the layer file is not empty
	stat, err := os.Stat(layerPath)
	if err != nil {
		t.Fatal(err)
	}
	if stat.Size() == 0 {
		t.Fatal("EROFS layer file should not be empty")
	}

	t.Logf("EROFS layer file created: %s (%d bytes)", layerPath, stat.Size())

	// Create a view to verify the content
	viewKey := "test-view"
	viewMounts, err := s.View(ctx, viewKey, commitKey)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("viewMounts: %#v", viewMounts)

	// Mount EROFS directly (containerd mount manager doesn't support EROFS)
	viewTarget := filepath.Join(tempDir, viewKey)
	if err := os.MkdirAll(viewTarget, 0o755); err != nil {
		t.Fatal(err)
	}
	cleanup := mountErofsView(t, viewMounts, viewTarget)
	t.Cleanup(cleanup)

	// Verify we can read the original test data
	testData, err := os.ReadFile(filepath.Join(viewTarget, "test-file.txt"))
	if err != nil {
		t.Fatal(err)
	}
	expected := testFileContent
	if string(testData) != expected {
		t.Fatalf("Expected %q, got %q", expected, string(testData))
	}

	// Verify nested file
	nestedData, err := os.ReadFile(filepath.Join(viewTarget, "testdir", "nested.txt"))
	if err != nil {
		t.Fatal(err)
	}
	expectedNested := testNestedFileContent
	if string(nestedData) != expectedNested {
		t.Fatalf("Expected %q, got %q", expectedNested, string(nestedData))
	}

	t.Logf("Successfully verified EROFS Snapshotter using the differ")
}

func TestErofsDifferCompareWithMountManager(t *testing.T) {
	env := newDifferTestEnv(t)

	// Create two base layers
	baseCommit := env.createLayer(testKeyBase, "", "base.txt", "base")
	childCommit := env.createLayer("child", baseCommit, "child.txt", "child")

	// Create active upper layer and lower view
	upperMounts := env.prepareActiveLayer(testKeyUpper, childCommit, "upper.txt", "upper")
	lowerMounts := env.createView(testKeyLower, childCommit)

	// Verify we have EROFS mount(s)
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

	mm := env.createMountManager()
	differ := differ.NewErofsDiffer(env.contentStore, differ.WithMountManager(mm))
	env.compareAndVerify(differ, lowerMounts, upperMounts, "upper.txt")
}

// TestErofsDifferCompareBlockUpperFallback tests Compare with an extract
// snapshot upper: the snapshotter mounts the ext4 writable layer on the host
// (block commit mode) and returns a bind mount to its upper directory.
func TestErofsDifferCompareBlockUpperFallback(t *testing.T) {
	env := newDifferTestEnv(t)

	// Create empty base layer using extract-style key so ext4 is mounted
	if _, err := env.snapshotter.Prepare(env.ctx(), "extract-"+testKeyBase, ""); err != nil {
		t.Fatal(err)
	}
	if err := env.snapshotter.Commit(env.ctx(), "base-commit", "extract-"+testKeyBase); err != nil {
		t.Fatal(err)
	}

	// Active upper layer written through the host-mounted ext4 (block mode)
	upperMounts := env.prepareActiveLayer(testKeyUpper, "base-commit", "marker.txt", "marker")
	lowerMounts := env.createView(testKeyLower, "base-commit")

	mm := env.createMountManager()
	differ := differ.NewErofsDiffer(env.contentStore, differ.WithMountManager(mm))
	env.compareAndVerify(differ, lowerMounts, upperMounts, "marker.txt")
}

// TestErofsDifferComparePreservesWhiteouts verifies that deletions recorded in
// the guest's overlay upper layer survive Compare. The guest represents a
// deleted file as an overlayfs whiteout (character device 0:0) inside the ext4
// writable layer. The differ mounts active snapshot uppers (EROFS + ext4) with
// the guest upper stacked as the top overlay lowerdir, so the whiteout hides
// the file in the merged view and Compare must emit a .wh. entry in the diff.
func TestErofsDifferComparePreservesWhiteouts(t *testing.T) {
	env := newDifferTestEnv(t)

	// Base layer with a file that the "guest" deletes
	baseCommit := env.createLayer(testKeyBase, "", "gone.txt", "gone")

	// Non-extract active snapshot: EROFS layer(s) + ext4 writable layer
	upperMounts, err := env.snapshotter.Prepare(env.ctx(), testKeyUpper, baseCommit)
	if err != nil {
		t.Fatal(err)
	}
	if !mountutils.HasActiveSnapshotMounts(upperMounts) {
		t.Fatalf("expected EROFS + ext4 active snapshot mounts, got: %#v", upperMounts)
	}

	// Simulate the guest deleting gone.txt and adding added.txt: overlayfs
	// represents the deletion as a 0:0 character device in the upper layer.
	env.populateGuestUpper(upperMounts, func(upperDir string) error {
		if err := os.WriteFile(filepath.Join(upperDir, "added.txt"), []byte("added"), 0o644); err != nil {
			return err
		}
		return unix.Mknod(filepath.Join(upperDir, "gone.txt"), unix.S_IFCHR|0o644, 0)
	})

	lowerMounts := env.createView(testKeyLower, baseCommit)

	mm := env.createMountManager()
	differ := differ.NewErofsDiffer(env.contentStore, differ.WithMountManager(mm))
	env.compareAndVerify(differ, lowerMounts, upperMounts, ".wh.gone.txt", "added.txt")
}

// TestErofsDifferCompareWithFormattedUpperMounts tests Compare with the active
// snapshot mount shape consumed by VM runtimes (EROFS layers + ext4 writable
// layer). The differ must merge the guest upper inside the ext4 with the EROFS
// layers on the host to compute the diff.
func TestErofsDifferCompareWithFormattedUpperMounts(t *testing.T) {
	env := newDifferTestEnv(t)

	baseCommit := env.createLayer(testKeyBase, "", "base.txt", "base")

	// Non-extract active snapshot: EROFS layer(s) + ext4 writable layer
	upperMounts, err := env.snapshotter.Prepare(env.ctx(), testKeyUpper, baseCommit)
	if err != nil {
		t.Fatal(err)
	}
	if !mountutils.HasActiveSnapshotMounts(upperMounts) {
		t.Fatalf("expected EROFS + ext4 active snapshot mounts, got: %#v", upperMounts)
	}

	// Write a file into the guest-style upper directory inside the ext4
	env.populateGuestUpper(upperMounts, func(upperDir string) error {
		return os.WriteFile(filepath.Join(upperDir, "upper.txt"), []byte("upper"), 0o644)
	})

	lowerMounts := env.createView(testKeyLower, baseCommit)

	mm := env.createMountManager()
	differ := differ.NewErofsDiffer(env.contentStore, differ.WithMountManager(mm))
	env.compareAndVerify(differ, lowerMounts, upperMounts, "upper.txt")
}

// TestErofsDifferCompareWithoutMountManager verifies that Compare resolves
// snapshotter mounts without a mount manager. Multi-layer views (merged fsmeta
// multi-device or stacked individual EROFS layer mounts) and extract bind
// uppers are mounted directly by the differ, with no manager activation.
func TestErofsDifferCompareWithoutMountManager(t *testing.T) {
	env := newDifferTestEnv(t)

	// Two layers so the lower view is either a merged fsmeta multi-device
	// mount or stacked individual EROFS layer mounts - both shapes the
	// differ handles directly without mount manager activation.
	baseCommit := env.createLayer(testKeyBase, "", "base.txt", "base")
	childCommit := env.createLayer("child", baseCommit, "child.txt", "child")

	upperMounts := env.prepareActiveLayer(testKeyUpper, childCommit, "upper.txt", "upper")
	lowerMounts := env.createView(testKeyLower, childCommit)

	// Direct mounts must not contain template syntax, which would require
	// mount manager resolution.
	if mountsHaveTemplate(lowerMounts) {
		t.Fatalf("view mounts should not have templates, got: %#v", lowerMounts)
	}
	if mountsHaveTemplate(upperMounts) {
		t.Fatalf("extract mounts should not have templates, got: %#v", upperMounts)
	}

	// No mount manager: Compare must still produce the diff.
	differ := differ.NewErofsDiffer(env.contentStore)
	env.compareAndVerify(differ, lowerMounts, upperMounts, "upper.txt")
}

// TestErofsDifferCompareMultipleStackedLayers tests Compare with 5+ stacked
// EROFS layers to verify that fsmeta consolidation works correctly
// with many layers.
func TestErofsDifferCompareMultipleStackedLayers(t *testing.T) {
	env := newDifferTestEnv(t)

	// Create 6 stacked layers
	var parentKey string
	for i := range 6 {
		key := fmt.Sprintf("layer-%d", i)
		filename := fmt.Sprintf("file-%d.txt", i)
		content := fmt.Sprintf("content-%d", i)
		parentKey = env.createLayer(key, parentKey, filename, content)
	}

	// Create upper layer and lower view
	upperMounts := env.prepareActiveLayer(testKeyUpper, parentKey, "upper.txt", "upper")
	lowerMounts := env.createView(testKeyLower, parentKey)

	// Should have at least one EROFS mount
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

	mm := env.createMountManager()
	differ := differ.NewErofsDiffer(env.contentStore, differ.WithMountManager(mm))
	env.compareAndVerify(differ, lowerMounts, upperMounts, "upper.txt")
}

// TestErofsDifferCompareEmptyLowerMounts tests Compare behavior when lower
// mounts slice is empty. This simulates creating a diff from scratch.
func TestErofsDifferCompareEmptyLowerMounts(t *testing.T) {
	env := newDifferTestEnv(t)

	// Create a single layer, then prepare an upper on top of it
	singleCommit := env.createLayer("single", "", "new.txt", "new")
	upperMounts := env.prepareActiveLayer(testKeyUpper, singleCommit, "upper.txt", "upper")

	mm := env.createMountManager()
	differ := differ.NewErofsDiffer(env.contentStore, differ.WithMountManager(mm))

	// Compare with empty lower mounts - tests the base case
	emptyLower := []mount.Mount{}
	env.compareAndVerify(differ, emptyLower, upperMounts, "upper.txt")
}

// TestErofsDifferCompareContextCancellation verifies that Compare fails with
// context.Canceled when the context is already cancelled. Empty lower mounts
// and a bind upper are used so neither side needs the mount manager: the
// failure must come from the cancelled context, not from mount resolution.
func TestErofsDifferCompareContextCancellation(t *testing.T) {
	env := newDifferTestEnv(t)

	// Create an upper layer (extract-style bind mount)
	baseCommit := env.createLayer(testKeyBase, "", "base.txt", "base")
	upperMounts := env.prepareActiveLayer(testKeyUpper, baseCommit, "upper.txt", "upper")
	lowerMounts := []mount.Mount{}

	differ := differ.NewErofsDiffer(env.contentStore)

	// Create a cancelled context
	ctx, cancel := context.WithCancel(env.ctx())
	cancel() // Cancel immediately

	_, err := differ.Compare(ctx, lowerMounts, upperMounts)
	if err == nil {
		t.Fatal("expected Compare with cancelled context to fail")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected error to wrap context.Canceled, got: %v", err)
	}
}

// TestErofsDifferCompareSingleLayerView tests Compare when lower is a single
// EROFS layer returned directly (KindView optimization path).
func TestErofsDifferCompareSingleLayerView(t *testing.T) {
	env := newDifferTestEnv(t)

	// Create single base layer
	baseCommit := env.createLayer(testKeyBase, "", "base.txt", "base")

	// Create a view of the single layer - this triggers the KindView optimization
	viewMounts := env.createView("view", baseCommit)

	// Verify it's a single EROFS mount (the optimization path)
	if len(viewMounts) != 1 || viewMounts[0].Type != testTypeErofs {
		t.Fatalf("expected single erofs mount, got: %#v", viewMounts)
	}

	// Create upper layer on top
	upperMounts := env.prepareActiveLayer(testKeyUpper, baseCommit, "new.txt", "new")

	mm := env.createMountManager()
	differ := differ.NewErofsDiffer(env.contentStore, differ.WithMountManager(mm))
	env.compareAndVerify(differ, viewMounts, upperMounts, "new.txt")
}

// TestErofsDifferCompareViewWithMultipleLayers tests Compare when lower is a
// view of multiple stacked layers with fsmeta consolidation.
func TestErofsDifferCompareViewWithMultipleLayers(t *testing.T) {
	env := newDifferTestEnv(t)

	// Create two stacked layers
	layer1Commit := env.createLayer("layer1", "", "layer1.txt", "layer1")
	layer2Commit := env.createLayer("layer2", layer1Commit, "layer2.txt", "layer2")

	// Create a view of the two layers
	viewMounts := env.createView("view", layer2Commit)
	t.Logf("view mounts: %#v", viewMounts)

	// Create upper layer
	upperMounts := env.prepareActiveLayer(testKeyUpper, layer2Commit, "upper.txt", "upper")

	mm := env.createMountManager()
	differ := differ.NewErofsDiffer(env.contentStore, differ.WithMountManager(mm))
	env.compareAndVerify(differ, viewMounts, upperMounts, "upper.txt")
}
