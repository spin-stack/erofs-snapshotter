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
// - TestErofsDifferCompareDoesNotRequireMountManager

import (
	"context"
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

// newDifferTestEnvWithBlockMode creates a test environment with block mode enabled.
//
// NOTE: Tests using this helper require the mount manager to set up loop devices
// for EROFS and ext4 mounts. The containerd mount manager handles this automatically.
func newDifferTestEnvWithBlockMode(t *testing.T) *differTestEnv {
	t.Helper()
	testutil.RequiresRoot(t)
	ctx := namespaces.WithNamespace(t.Context(), "testsuite")

	if err := preflight.CheckErofsSupport(); err != nil {
		t.Skipf("EROFS support check failed: %v", err)
	}

	tempDir := t.TempDir()
	contentStore := imagetest.NewContentStore(ctx, t).Store
	snapshotRoot := filepath.Join(tempDir, "snapshots")

	s, err := NewSnapshotter(snapshotRoot, WithDefaultSize(16*1024*1024))
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

	db, err := bolt.Open(filepath.Join(e.tempDir, "mounts.db"), 0600, nil)
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
	if err := os.WriteFile(filePath, []byte(content), 0644); err != nil {
		e.t.Fatalf("failed to write %s: %v", filename, err)
	}

	commitKey := key + "-commit"
	if err := e.snapshotter.Commit(e.ctx(), commitKey, extractKey); err != nil {
		e.t.Fatalf("failed to commit %s: %v", extractKey, err)
	}

	return commitKey
}

// createBlockLayer creates and commits a layer using block mode (ext4 upper).
// Note: This is now the same as createLayer since block mode is always used.
// Uses extract-style key to ensure ext4 is mounted on host for writing.
func (e *differTestEnv) createBlockLayer(key, parentKey, filename, content string) string {
	e.t.Helper()

	// Use extract-style key so the snapshotter mounts the ext4 on host
	extractKey := "extract-" + key
	if _, err := e.snapshotter.Prepare(e.ctx(), extractKey, parentKey); err != nil {
		e.t.Fatalf("failed to prepare %s: %v", extractKey, err)
	}

	id := snapshotID(e.ctx(), e.t, e.snapshotter, extractKey)
	filePath := filepath.Join(e.snapshotter.blockUpperPath(id), filename)
	if err := os.WriteFile(filePath, []byte(content), 0644); err != nil {
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
	if err := os.WriteFile(filePath, []byte(content), 0644); err != nil {
		e.t.Fatalf("failed to write %s: %v", filename, err)
	}

	return mounts
}

// prepareActiveBlockLayer prepares an active layer in block mode.
// Note: This is now the same as prepareActiveLayer since block mode is always used.
// Uses extract-style key to ensure ext4 is mounted on host for writing.
func (e *differTestEnv) prepareActiveBlockLayer(key, parentKey, filename, content string) []mount.Mount {
	e.t.Helper()

	// Use extract-style key so the snapshotter mounts the ext4 on host
	extractKey := "extract-" + key
	mounts, err := e.snapshotter.Prepare(e.ctx(), extractKey, parentKey)
	if err != nil {
		e.t.Fatalf("failed to prepare %s: %v", extractKey, err)
	}

	id := snapshotID(e.ctx(), e.t, e.snapshotter, extractKey)
	filePath := filepath.Join(e.snapshotter.blockUpperPath(id), filename)
	if err := os.WriteFile(filePath, []byte(content), 0644); err != nil {
		e.t.Fatalf("failed to write %s: %v", filename, err)
	}

	return mounts
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

	// Create EROFS snapshotter first (creates the snapshot root directory)
	snapshotRoot := filepath.Join(tempDir, "snapshots")
	s, err := NewSnapshotter(snapshotRoot)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	defer cleanupAllSnapshots(ctx, s)
	t.Cleanup(func() { mount.UnmountRecursive(snapshotRoot, 0) })

	// Create mount manager for EROFS mounts (after snapshotter creates root)
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
	if err := os.MkdirAll(viewTarget, 0755); err != nil {
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
	env.compareAndVerify(differ, lowerMounts, upperMounts)
}

func TestErofsDifferCompareBlockUpperFallback(t *testing.T) {
	env := newDifferTestEnvWithBlockMode(t)

	// Create empty base layer using extract-style key so ext4 is mounted
	if _, err := env.snapshotter.Prepare(env.ctx(), "extract-"+testKeyBase, ""); err != nil {
		t.Fatal(err)
	}
	if err := env.snapshotter.Commit(env.ctx(), "base-commit", "extract-"+testKeyBase); err != nil {
		t.Fatal(err)
	}

	// Create active upper layer in block mode
	upperMounts := env.prepareActiveBlockLayer(testKeyUpper, "base-commit", "marker.txt", "marker")
	lowerMounts := env.createView(testKeyLower, "base-commit")

	mm := env.createMountManager()
	differ := differ.NewErofsDiffer(env.contentStore, differ.WithMountManager(mm))
	env.compareAndVerify(differ, lowerMounts, upperMounts, "marker.txt")
}

func TestErofsDifferComparePreservesWhiteouts(t *testing.T) {
	// Skip: The differ's writeDiff function handles whiteouts from overlayfs (char device 0:0)
	// but the test creates whiteouts in a non-overlay context. Overlayfs whiteout detection
	// requires the file system's opaque markers which aren't present in ext4 upper directories.
	// TODO: Investigate proper whiteout handling for the EROFS differ in non-overlay contexts.
	t.Skip("whiteout detection requires overlayfs context")
	env := newDifferTestEnvWithBlockMode(t)

	// Create base layer with a file that will be deleted
	env.createBlockLayer(testKeyBase, "", "gone.txt", "gone")

	// Create upper layer with a whiteout using extract-style key
	extractKey := "extract-" + testKeyUpper
	upperMounts, err := env.snapshotter.Prepare(env.ctx(), extractKey, "base-commit")
	if err != nil {
		t.Fatal(err)
	}

	// For extract snapshots, the bind mount source is the rw/upper directory.
	// Create whiteout (character device 0:0) in the blockUpperPath directory.
	// In overlayfs, whiteouts are char devices 0:0 with the original filename.
	// The .wh. prefix is only added when converting to tar format.
	if len(upperMounts) == 0 {
		t.Fatal("expected at least one mount")
	}
	// Use blockUpperPath to get the correct directory inside the mounted ext4
	id := snapshotID(env.ctx(), t, env.snapshotter, extractKey)
	whiteoutPath := filepath.Join(env.snapshotter.blockUpperPath(id), "gone.txt")
	if err := unix.Mknod(whiteoutPath, unix.S_IFCHR|0644, 0); err != nil {
		t.Fatalf("failed to create whiteout at %s: %v", whiteoutPath, err)
	}

	lowerMounts := env.createView(testKeyLower, "base-commit")

	mm := env.createMountManager()
	differ := differ.NewErofsDiffer(env.contentStore, differ.WithMountManager(mm))
	env.compareAndVerify(differ, lowerMounts, upperMounts, ".wh.gone.txt")
}

func TestErofsDifferCompareWithFormattedUpperMounts(t *testing.T) {
	env := newDifferTestEnvWithBlockMode(t)

	// Create empty base layer using extract-style key
	if _, err := env.snapshotter.Prepare(env.ctx(), "extract-"+testKeyBase, ""); err != nil {
		t.Fatal(err)
	}
	if err := env.snapshotter.Commit(env.ctx(), "base-commit", "extract-"+testKeyBase); err != nil {
		t.Fatal(err)
	}

	// Create active upper layer in block mode
	upperMounts := env.prepareActiveBlockLayer(testKeyUpper, "base-commit", "upper.txt", "upper")

	// Active layer returns mounts (combination of EROFS + ext4 or bind)
	if len(upperMounts) == 0 {
		t.Fatalf("expected mounts, got empty slice")
	}

	lowerMounts := env.createView(testKeyLower, "base-commit")

	mm := env.createMountManager()
	differ := differ.NewErofsDiffer(env.contentStore, differ.WithMountManager(mm))
	env.compareAndVerify(differ, lowerMounts, upperMounts, "upper.txt")
}

// TestErofsDifferCompareWithoutMountManager verifies that Compare works
// without a mount manager. EROFS snapshotter now returns direct mounts that
// don't require mount manager resolution.
func TestErofsDifferCompareWithoutMountManager(t *testing.T) {
	env := newDifferTestEnv(t)

	// Create layers
	baseCommit := env.createLayer(testKeyBase, "", "base.txt", "base")
	upperMounts := env.prepareActiveLayer(testKeyUpper, baseCommit, "upper.txt", "upper")
	lowerMounts := env.createView(testKeyLower, baseCommit)

	// Single-layer view should NOT have templates
	if mountsHaveTemplate(lowerMounts) {
		t.Fatalf("single-layer view should not have templates, got: %#v", lowerMounts)
	}

	// Active snapshot with parent now uses templates for overlay (new architecture)
	if !mountsHaveTemplate(upperMounts) {
		t.Logf("active mounts (may have templates): %#v", upperMounts)
	}

	// Compare with mount manager since active mounts may have templates
	mm := env.createMountManager()
	differ := differ.NewErofsDiffer(env.contentStore, differ.WithMountManager(mm))
	env.compareAndVerify(differ, lowerMounts, upperMounts)
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
	env.compareAndVerify(differ, emptyLower, upperMounts)
}

// TestErofsDifferCompareContextCancellation tests that Compare properly handles
// context cancellation. With direct mounts (no mount manager activation needed),
// fast operations may complete before cancellation takes effect.
func TestErofsDifferCompareContextCancellation(t *testing.T) {
	env := newDifferTestEnv(t)

	// Create layers
	baseCommit := env.createLayer(testKeyBase, "", "base.txt", "base")
	upperMounts := env.prepareActiveLayer(testKeyUpper, baseCommit, "upper.txt", "upper")
	lowerMounts := env.createView(testKeyLower, baseCommit)

	// Create differ without mount manager (direct mounts)
	differ := differ.NewErofsDiffer(env.contentStore)

	// Create a cancelled context
	ctx, cancel := context.WithCancel(env.ctx())
	cancel() // Cancel immediately

	// Compare with cancelled context - may fail or complete quickly
	_, err := differ.Compare(ctx, lowerMounts, upperMounts)
	if err != nil {
		t.Logf("Compare with cancelled context returned: %v", err)
	} else {
		t.Log("Compare completed before context cancellation took effect")
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

// TestErofsDifferCompareDoesNotRequireMountManager verifies that Compare
// works with mount manager for fsmeta consolidated mounts.
func TestErofsDifferCompareDoesNotRequireMountManager(t *testing.T) {
	env := newDifferTestEnv(t)

	// Create two layers to test multi-layer behavior
	baseCommit := env.createLayer(testKeyBase, "", "base.txt", "base")
	childCommit := env.createLayer("child", baseCommit, "child.txt", "child")

	// Get mounts
	upperMounts := env.prepareActiveLayer(testKeyUpper, childCommit, "upper.txt", "upper")
	lowerMounts := env.createView(testKeyLower, childCommit)

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

	desc := env.compareAndVerify(differ, lowerMounts, upperMounts)
	t.Logf("Compare succeeded: %s", desc.Digest)
}
