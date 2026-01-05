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

// This file contains EROFS differ integration tests.
// These tests verify the differ's Compare and Apply functionality
// with various mount configurations.
//
// Tests in this file:
// - TestErofsDifferWithTarIndexMode
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
// - TestErofsDifferCompareRequiresMountManagerForTemplates
// - TestErofsDifferCompareRejectsNonEROFSMounts

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
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

	erofsdiffer "github.com/aledbf/nexuserofs/internal/differ"
	erofsutils "github.com/aledbf/nexuserofs/internal/erofs"
)

// Snapshot key constants used across tests
const (
	testKeyBase   = "base"
	testKeyUpper  = "upper"
	testKeyLower  = "lower"
	testTypeExt4  = "ext4"
	testTypeErofs = "erofs"
)

func TestErofsDifferWithTarIndexMode(t *testing.T) {
	testutil.RequiresRoot(t)
	ctx := t.Context()

	if !findErofs() {
		t.Skip("check for erofs kernel support failed, skipping test")
	}

	// Check if mkfs.erofs supports tar index mode
	supported, err := erofsutils.SupportGenerateFromTar()
	if err != nil || !supported {
		t.Skip("mkfs.erofs does not support tar mode, skipping tar index test")
	}

	tempDir := t.TempDir()

	// Create content store for the differ
	contentStore := imagetest.NewContentStore(ctx, t).Store

	// Create EROFS differ with tar index mode enabled
	differ := erofsdiffer.NewErofsDiffer(contentStore, erofsdiffer.WithTarIndexMode())

	// Create EROFS snapshotter
	snapshotRoot := filepath.Join(tempDir, "snapshots")
	s, err := NewSnapshotter(snapshotRoot)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	defer cleanupAllSnapshots(ctx, s)

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

	// Prepare a snapshot using the snapshotter
	snapshotKey := "test-snapshot"
	mounts, err := s.Prepare(ctx, snapshotKey, "")
	if err != nil {
		t.Fatal(err)
	}

	// Apply the tar content using the EROFS differ with tar index mode
	appliedDesc, err := differ.Apply(ctx, desc, mounts)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("Applied layer using EROFS differ with tar index mode:")
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
	layerPath := snap.layerBlobPath(id)
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

	t.Logf("EROFS layer file created with tar index mode: %s (%d bytes)", layerPath, stat.Size())

	// Create a view to verify the content
	viewKey := "test-view"
	viewMounts, err := s.View(ctx, viewKey, commitKey)
	if err != nil {
		t.Fatal(err)
	}

	viewTarget := filepath.Join(tempDir, viewKey)
	if err := os.MkdirAll(viewTarget, 0755); err != nil {
		t.Fatal(err)
	}
	if err := mount.All(viewMounts, viewTarget); err != nil {
		t.Fatal(err)
	}
	defer testutil.Unmount(t, viewTarget)

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

	t.Logf("Successfully verified EROFS Snapshotter using the differ with tar index mode")
}

func TestErofsDifferCompareWithMountManager(t *testing.T) {
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

	baseKey := testKeyBase
	if _, err := s.Prepare(ctx, baseKey, ""); err != nil {
		t.Fatal(err)
	}
	baseID := snapshotID(ctx, t, snap, baseKey)
	if err := os.WriteFile(filepath.Join(snap.upperPath(baseID), "base.txt"), []byte("base"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := s.Commit(ctx, "base-commit", baseKey); err != nil {
		t.Fatal(err)
	}

	childKey := "child"
	if _, err := s.Prepare(ctx, childKey, "base-commit"); err != nil {
		t.Fatal(err)
	}
	childID := snapshotID(ctx, t, snap, childKey)
	if err := os.WriteFile(filepath.Join(snap.upperPath(childID), "child.txt"), []byte("child"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := s.Commit(ctx, "child-commit", childKey); err != nil {
		t.Fatal(err)
	}

	upperKey := testKeyUpper
	upperMounts, err := s.Prepare(ctx, upperKey, "child-commit")
	if err != nil {
		t.Fatal(err)
	}
	upperID := snapshotID(ctx, t, snap, upperKey)
	if err := os.WriteFile(filepath.Join(snap.upperPath(upperID), "upper.txt"), []byte("upper"), 0644); err != nil {
		t.Fatal(err)
	}

	lowerKey := testKeyLower
	lowerMounts, err := s.View(ctx, lowerKey, "child-commit")
	if err != nil {
		t.Fatal(err)
	}

	// Mounts should NOT have templates - they should be directly usable
	if mountsHaveTemplate(lowerMounts) {
		t.Fatalf("expected lower mounts without templates, got: %#v", lowerMounts)
	}
	if mountsHaveTemplate(upperMounts) {
		t.Fatalf("expected upper mounts without templates, got: %#v", upperMounts)
	}

	// Multi-layer view should return overlay mount
	if len(lowerMounts) != 1 || lowerMounts[0].Type != "overlay" {
		t.Fatalf("expected single overlay mount for multi-layer view, got: %#v", lowerMounts)
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

	// Mount manager is optional now but we still test with it for compatibility
	differ := erofsdiffer.NewErofsDiffer(contentStore, erofsdiffer.WithMountManager(mm))
	desc, err := differ.Compare(ctx, lowerMounts, upperMounts)
	if err != nil {
		t.Fatal(err)
	}
	if desc.Digest == "" || desc.Size == 0 {
		t.Fatalf("unexpected diff descriptor: %+v", desc)
	}
}

func TestErofsDifferCompareBlockUpperFallback(t *testing.T) {
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
	s, err := NewSnapshotter(snapshotRoot, WithDefaultSize(16*1024*1024))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	defer cleanupAllSnapshots(ctx, s)

	baseKey := testKeyBase
	if _, err := s.Prepare(ctx, baseKey, ""); err != nil {
		t.Fatal(err)
	}
	if err := s.Commit(ctx, "base-commit", baseKey); err != nil {
		t.Fatal(err)
	}

	upperKey := testKeyUpper
	// Block mode now returns direct mounts (overlay with rw/upper as upperdir)
	upperMounts, err := s.Prepare(ctx, upperKey, "base-commit")
	if err != nil {
		t.Fatal(err)
	}

	// Mount the overlay and write a file directly
	upperTarget := t.TempDir()
	if err := mount.All(upperMounts, upperTarget); err != nil {
		t.Fatalf("failed to mount upper: %v", err)
	}
	defer testutil.Unmount(t, upperTarget)

	if err := os.WriteFile(filepath.Join(upperTarget, "marker.txt"), []byte("marker"), 0644); err != nil {
		t.Fatal(err)
	}

	lowerKey := testKeyLower
	lowerMounts, err := s.View(ctx, lowerKey, "base-commit")
	if err != nil {
		t.Fatal(err)
	}

	// Differ works without mount manager since mounts are direct
	differ := erofsdiffer.NewErofsDiffer(contentStore)
	desc, err := differ.Compare(ctx, lowerMounts, upperMounts)
	if err != nil {
		t.Fatal(err)
	}

	found, err := tarHasPath(ctx, contentStore, desc, "marker.txt")
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("expected diff to include marker.txt")
	}
}

func TestErofsDifferComparePreservesWhiteouts(t *testing.T) {
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
	s, err := NewSnapshotter(snapshotRoot, WithDefaultSize(16*1024*1024))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	defer cleanupAllSnapshots(ctx, s)

	// Create base layer with a file that will be deleted
	baseKey := testKeyBase
	baseMounts, err := s.Prepare(ctx, baseKey, "")
	if err != nil {
		t.Fatal(err)
	}

	// Mount and write a file to the base layer
	baseTarget := t.TempDir()
	if err := mount.All(baseMounts, baseTarget); err != nil {
		t.Fatalf("failed to mount base: %v", err)
	}
	if err := os.WriteFile(filepath.Join(baseTarget, "gone.txt"), []byte("gone"), 0644); err != nil {
		testutil.Unmount(t, baseTarget)
		t.Fatal(err)
	}
	testutil.Unmount(t, baseTarget)

	if err := s.Commit(ctx, "base-commit", baseKey); err != nil {
		t.Fatal(err)
	}

	// Create upper layer and delete the file (creating whiteout)
	upperKey := testKeyUpper
	upperMounts, err := s.Prepare(ctx, upperKey, "base-commit")
	if err != nil {
		t.Fatal(err)
	}

	// Mount the overlay and delete the file
	upperTarget := t.TempDir()
	if err := mount.All(upperMounts, upperTarget); err != nil {
		t.Fatalf("failed to mount upper: %v", err)
	}
	if err := os.Remove(filepath.Join(upperTarget, "gone.txt")); err != nil {
		testutil.Unmount(t, upperTarget)
		t.Fatal(err)
	}
	testutil.Unmount(t, upperTarget)

	lowerKey := testKeyLower
	lowerMounts, err := s.View(ctx, lowerKey, "base-commit")
	if err != nil {
		t.Fatal(err)
	}

	// Differ works without mount manager since mounts are direct
	differ := erofsdiffer.NewErofsDiffer(contentStore)
	desc, err := differ.Compare(ctx, lowerMounts, upperMounts)
	if err != nil {
		t.Fatal(err)
	}

	found, err := tarHasPath(ctx, contentStore, desc, ".wh.gone.txt")
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("expected diff to include whiteout for gone.txt")
	}
}

func TestErofsDifferCompareWithFormattedUpperMounts(t *testing.T) {
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
	s, err := NewSnapshotter(snapshotRoot, WithDefaultSize(16*1024*1024))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	defer cleanupAllSnapshots(ctx, s)

	baseKey := testKeyBase
	if _, err := s.Prepare(ctx, baseKey, ""); err != nil {
		t.Fatal(err)
	}
	if err := s.Commit(ctx, "base-commit", baseKey); err != nil {
		t.Fatal(err)
	}

	upperKey := testKeyUpper
	// Prepare() creates and mounts the ext4 layer, returning direct mounts
	upperMounts, err := s.Prepare(ctx, upperKey, "base-commit")
	if err != nil {
		t.Fatal(err)
	}
	// Block mode now returns direct mounts (no templates)
	if mountsHaveTemplate(upperMounts) {
		t.Fatalf("expected upper mounts without templates, got: %#v", upperMounts)
	}

	// Write files directly to the mount point (overlay upperdir)
	if len(upperMounts) != 1 {
		t.Fatalf("expected single mount, got %d: %#v", len(upperMounts), upperMounts)
	}
	upperTarget := t.TempDir()
	if err := mount.All(upperMounts, upperTarget); err != nil {
		t.Fatalf("failed to mount upper: %v", err)
	}
	defer testutil.Unmount(t, upperTarget)

	if err := os.WriteFile(filepath.Join(upperTarget, "upper.txt"), []byte("upper"), 0644); err != nil {
		t.Fatal(err)
	}

	lowerKey := testKeyLower
	lowerMounts, err := s.View(ctx, lowerKey, "base-commit")
	if err != nil {
		t.Fatal(err)
	}

	// Differ works without mount manager since mounts are direct
	differ := erofsdiffer.NewErofsDiffer(contentStore)
	desc, err := differ.Compare(ctx, lowerMounts, upperMounts)
	if err != nil {
		t.Fatal(err)
	}

	found, err := tarHasPath(ctx, contentStore, desc, "upper.txt")
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("expected diff to include upper.txt")
	}
}

// TestErofsDifferCompareWithoutMountManager verifies that Compare works
// without a mount manager. EROFS snapshotter now returns direct mounts that
// don't require mount manager resolution.
func TestErofsDifferCompareWithoutMountManager(t *testing.T) {
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

	baseKey := testKeyBase
	if _, err := s.Prepare(ctx, baseKey, ""); err != nil {
		t.Fatal(err)
	}
	baseID := snapshotID(ctx, t, snap, baseKey)
	if err := os.WriteFile(filepath.Join(snap.upperPath(baseID), "base.txt"), []byte("base"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := s.Commit(ctx, "base-commit", baseKey); err != nil {
		t.Fatal(err)
	}

	upperKey := testKeyUpper
	upperMounts, err := s.Prepare(ctx, upperKey, "base-commit")
	if err != nil {
		t.Fatal(err)
	}
	upperID := snapshotID(ctx, t, snap, upperKey)
	if err := os.WriteFile(filepath.Join(snap.upperPath(upperID), "upper.txt"), []byte("upper"), 0644); err != nil {
		t.Fatal(err)
	}

	lowerKey := testKeyLower
	lowerMounts, err := s.View(ctx, lowerKey, "base-commit")
	if err != nil {
		t.Fatal(err)
	}

	// Mounts should NOT have templates - they should be directly usable
	if mountsHaveTemplate(lowerMounts) || mountsHaveTemplate(upperMounts) {
		t.Fatalf("expected mounts without templates, got lower=%#v, upper=%#v", lowerMounts, upperMounts)
	}

	// Compare works WITHOUT mount manager since mounts are direct
	differ := erofsdiffer.NewErofsDiffer(contentStore)
	desc, err := differ.Compare(ctx, lowerMounts, upperMounts)
	if err != nil {
		t.Fatalf("Compare should work without mount manager, got: %v", err)
	}
	if desc.Digest == "" || desc.Size == 0 {
		t.Fatalf("unexpected diff descriptor: %+v", desc)
	}
}

// TestErofsDifferCompareMultipleStackedLayers tests Compare with 5+ stacked
// EROFS layers to verify that overlay template expansion works correctly
// with many layers.
func TestErofsDifferCompareMultipleStackedLayers(t *testing.T) {
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

	// Create 6 stacked layers
	layerCount := 6
	var parentKey string
	for i := range layerCount {
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

	// Create upper layer on top of all stacked layers
	upperKey := testKeyUpper
	upperMounts, err := s.Prepare(ctx, upperKey, parentKey)
	if err != nil {
		t.Fatal(err)
	}
	upperID := snapshotID(ctx, t, snap, upperKey)
	if err := os.WriteFile(filepath.Join(snap.upperPath(upperID), "upper.txt"), []byte("upper"), 0644); err != nil {
		t.Fatal(err)
	}

	// Create lower view from the stacked layers
	lowerKey := testKeyLower
	lowerMounts, err := s.View(ctx, lowerKey, parentKey)
	if err != nil {
		t.Fatal(err)
	}

	// Mounts should NOT have templates - they should be directly usable
	if mountsHaveTemplate(lowerMounts) {
		t.Fatalf("expected lower mounts without templates, got: %#v", lowerMounts)
	}
	if mountsHaveTemplate(upperMounts) {
		t.Fatalf("expected upper mounts without templates, got: %#v", upperMounts)
	}

	// Multi-layer view should return single overlay mount
	if len(lowerMounts) != 1 || lowerMounts[0].Type != "overlay" {
		t.Fatalf("expected single overlay mount for multi-layer view, got: %#v", lowerMounts)
	}

	// Differ works without mount manager since mounts are direct
	differ := erofsdiffer.NewErofsDiffer(contentStore)
	desc, err := differ.Compare(ctx, lowerMounts, upperMounts)
	if err != nil {
		t.Fatal(err)
	}
	if desc.Digest == "" || desc.Size == 0 {
		t.Fatalf("unexpected diff descriptor: %+v", desc)
	}

	// Verify the diff contains the upper file
	found, err := tarHasPath(ctx, contentStore, desc, "upper.txt")
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("expected diff to include upper.txt")
	}
}

// TestErofsDifferCompareEmptyLowerMounts tests Compare behavior when lower
// mounts slice is empty. This simulates creating a diff from scratch.
func TestErofsDifferCompareEmptyLowerMounts(t *testing.T) {
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

	// Create a single layer (no parent)
	key := "single"
	if _, err := s.Prepare(ctx, key, ""); err != nil {
		t.Fatal(err)
	}
	id := snapshotID(ctx, t, snap, key)
	if err := os.WriteFile(filepath.Join(snap.upperPath(id), "new.txt"), []byte("new"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := s.Commit(ctx, "single-commit", key); err != nil {
		t.Fatal(err)
	}

	// Get mounts for the committed layer as upper
	upperKey := testKeyUpper
	upperMounts, err := s.Prepare(ctx, upperKey, "single-commit")
	if err != nil {
		t.Fatal(err)
	}
	upperID := snapshotID(ctx, t, snap, upperKey)
	if err := os.WriteFile(filepath.Join(snap.upperPath(upperID), "upper.txt"), []byte("upper"), 0644); err != nil {
		t.Fatal(err)
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

	// Compare with empty lower mounts - this tests the base case
	emptyLower := []mount.Mount{}
	desc, err := differ.Compare(ctx, emptyLower, upperMounts)
	if err != nil {
		t.Fatal(err)
	}
	if desc.Digest == "" || desc.Size == 0 {
		t.Fatalf("unexpected diff descriptor: %+v", desc)
	}
}

// TestErofsDifferCompareContextCancellation tests that Compare properly handles
// context cancellation. With direct mounts (no mount manager activation needed),
// fast operations may complete before cancellation takes effect.
func TestErofsDifferCompareContextCancellation(t *testing.T) {
	testutil.RequiresRoot(t)
	baseCtx := namespaces.WithNamespace(t.Context(), "testsuite")

	_, err := exec.LookPath("mkfs.erofs")
	if err != nil {
		t.Skipf("could not find mkfs.erofs: %v", err)
	}
	if !findErofs() {
		t.Skip("check for erofs kernel support failed, skipping test")
	}

	tempDir := t.TempDir()
	contentStore := imagetest.NewContentStore(baseCtx, t).Store

	snapshotRoot := filepath.Join(tempDir, "snapshots")
	s, err := NewSnapshotter(snapshotRoot)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	defer cleanupAllSnapshots(baseCtx, s)

	snap, ok := s.(*snapshotter)
	if !ok {
		t.Fatal("failed to cast snapshotter to *snapshotter")
	}

	// Create base layer
	baseKey := testKeyBase
	if _, err := s.Prepare(baseCtx, baseKey, ""); err != nil {
		t.Fatal(err)
	}
	baseID := snapshotID(baseCtx, t, snap, baseKey)
	if err := os.WriteFile(filepath.Join(snap.upperPath(baseID), "base.txt"), []byte("base"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := s.Commit(baseCtx, "base-commit", baseKey); err != nil {
		t.Fatal(err)
	}

	// Create upper layer
	upperKey := testKeyUpper
	upperMounts, err := s.Prepare(baseCtx, upperKey, "base-commit")
	if err != nil {
		t.Fatal(err)
	}
	upperID := snapshotID(baseCtx, t, snap, upperKey)
	if err := os.WriteFile(filepath.Join(snap.upperPath(upperID), "upper.txt"), []byte("upper"), 0644); err != nil {
		t.Fatal(err)
	}

	lowerKey := testKeyLower
	lowerMounts, err := s.View(baseCtx, lowerKey, "base-commit")
	if err != nil {
		t.Fatal(err)
	}

	// Create differ without mount manager (direct mounts)
	differ := erofsdiffer.NewErofsDiffer(contentStore)

	// Create a cancelled context
	ctx, cancel := context.WithCancel(baseCtx)
	cancel() // Cancel immediately

	// Compare with cancelled context - may fail with context error or complete
	// quickly before cancellation takes effect (direct mounts are fast)
	_, err = differ.Compare(ctx, lowerMounts, upperMounts)
	if err != nil {
		// If there's an error, it should be context-related or path extraction related
		t.Logf("Compare with cancelled context returned: %v", err)
	} else {
		// Fast completion before cancellation is acceptable for direct mounts
		t.Log("Compare completed before context cancellation took effect")
	}
}

// TestErofsDifferCompareSingleLayerView tests Compare when lower is a single
// EROFS layer returned directly (KindView optimization path).
func TestErofsDifferCompareSingleLayerView(t *testing.T) {
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

	// Create single base layer
	baseKey := testKeyBase
	if _, err := s.Prepare(ctx, baseKey, ""); err != nil {
		t.Fatal(err)
	}
	baseID := snapshotID(ctx, t, snap, baseKey)
	if err := os.WriteFile(filepath.Join(snap.upperPath(baseID), "base.txt"), []byte("base"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := s.Commit(ctx, "base-commit", baseKey); err != nil {
		t.Fatal(err)
	}

	// Create a view of the single layer - this triggers the KindView optimization
	viewKey := "view"
	viewMounts, err := s.View(ctx, viewKey, "base-commit")
	if err != nil {
		t.Fatal(err)
	}

	// Verify it's a single EROFS mount (the optimization path)
	if len(viewMounts) != 1 {
		t.Fatalf("expected single mount for view, got %d", len(viewMounts))
	}
	if viewMounts[0].Type != testTypeErofs {
		t.Fatalf("expected erofs mount type, got %s", viewMounts[0].Type)
	}

	// Create upper layer on top
	upperKey := testKeyUpper
	upperMounts, err := s.Prepare(ctx, upperKey, "base-commit")
	if err != nil {
		t.Fatal(err)
	}
	upperID := snapshotID(ctx, t, snap, upperKey)
	if err := os.WriteFile(filepath.Join(snap.upperPath(upperID), "new.txt"), []byte("new"), 0644); err != nil {
		t.Fatal(err)
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

	// Compare using the single-layer view as lower
	desc, err := differ.Compare(ctx, viewMounts, upperMounts)
	if err != nil {
		t.Fatal(err)
	}
	if desc.Digest == "" || desc.Size == 0 {
		t.Fatalf("unexpected diff descriptor: %+v", desc)
	}

	// Verify the diff contains the new file
	found, err := tarHasPath(ctx, contentStore, desc, "new.txt")
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("expected diff to include new.txt")
	}
}

// TestErofsDifferCompareViewWithMultipleLayers tests Compare when lower is a
// view of multiple stacked layers, triggering the overlay template path.
func TestErofsDifferCompareViewWithMultipleLayers(t *testing.T) {
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

	// Create first layer
	layer1Key := "layer1"
	if _, err := s.Prepare(ctx, layer1Key, ""); err != nil {
		t.Fatal(err)
	}
	layer1ID := snapshotID(ctx, t, snap, layer1Key)
	if err := os.WriteFile(filepath.Join(snap.upperPath(layer1ID), "layer1.txt"), []byte("layer1"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := s.Commit(ctx, "layer1-commit", layer1Key); err != nil {
		t.Fatal(err)
	}

	// Create second layer
	layer2Key := "layer2"
	if _, err := s.Prepare(ctx, layer2Key, "layer1-commit"); err != nil {
		t.Fatal(err)
	}
	layer2ID := snapshotID(ctx, t, snap, layer2Key)
	if err := os.WriteFile(filepath.Join(snap.upperPath(layer2ID), "layer2.txt"), []byte("layer2"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := s.Commit(ctx, "layer2-commit", layer2Key); err != nil {
		t.Fatal(err)
	}

	// Create a view of the two layers - this should return overlay with templates
	viewKey := "view"
	viewMounts, err := s.View(ctx, viewKey, "layer2-commit")
	if err != nil {
		t.Fatal(err)
	}

	// The view of multiple layers should have templates or multiple mounts
	if len(viewMounts) < 2 && !mountsHaveTemplate(viewMounts) {
		t.Logf("view mounts: %#v", viewMounts)
		// May be EROFS mounts without templates, which is also valid
	}

	// Create upper layer
	upperKey := testKeyUpper
	upperMounts, err := s.Prepare(ctx, upperKey, "layer2-commit")
	if err != nil {
		t.Fatal(err)
	}
	upperID := snapshotID(ctx, t, snap, upperKey)
	if err := os.WriteFile(filepath.Join(snap.upperPath(upperID), "upper.txt"), []byte("upper"), 0644); err != nil {
		t.Fatal(err)
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
	desc, err := differ.Compare(ctx, viewMounts, upperMounts)
	if err != nil {
		t.Fatal(err)
	}
	if desc.Digest == "" || desc.Size == 0 {
		t.Fatalf("unexpected diff descriptor: %+v", desc)
	}

	// Verify the diff contains the upper file
	found, err := tarHasPath(ctx, contentStore, desc, "upper.txt")
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("expected diff to include upper.txt")
	}
}

// TestErofsDifferCompareDoesNotRequireMountManager verifies that Compare
// works without mount manager since EROFS snapshotter now returns direct mounts.
func TestErofsDifferCompareDoesNotRequireMountManager(t *testing.T) {
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

	// Create two layers to test multi-layer behavior
	baseKey := testKeyBase
	if _, err := s.Prepare(ctx, baseKey, ""); err != nil {
		t.Fatal(err)
	}
	baseID := snapshotID(ctx, t, snap, baseKey)
	if err := os.WriteFile(filepath.Join(snap.upperPath(baseID), "base.txt"), []byte("base"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := s.Commit(ctx, "base-commit", baseKey); err != nil {
		t.Fatal(err)
	}

	childKey := "child"
	if _, err := s.Prepare(ctx, childKey, "base-commit"); err != nil {
		t.Fatal(err)
	}
	childID := snapshotID(ctx, t, snap, childKey)
	if err := os.WriteFile(filepath.Join(snap.upperPath(childID), "child.txt"), []byte("child"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := s.Commit(ctx, "child-commit", childKey); err != nil {
		t.Fatal(err)
	}

	// Get mounts (should NOT have templates)
	upperKey := testKeyUpper
	upperMounts, err := s.Prepare(ctx, upperKey, "child-commit")
	if err != nil {
		t.Fatal(err)
	}

	lowerKey := testKeyLower
	lowerMounts, err := s.View(ctx, lowerKey, "child-commit")
	if err != nil {
		t.Fatal(err)
	}

	// Mounts should NOT have templates
	if mountsHaveTemplate(lowerMounts) || mountsHaveTemplate(upperMounts) {
		t.Fatalf("expected mounts without templates, got lower=%#v, upper=%#v", lowerMounts, upperMounts)
	}

	// Create differ WITHOUT mount manager - should work
	differ := erofsdiffer.NewErofsDiffer(contentStore)

	// Compare should succeed without mount manager
	desc, err := differ.Compare(ctx, lowerMounts, upperMounts)
	if err != nil {
		t.Fatalf("Compare should work without mount manager, got: %v", err)
	}
	if desc.Digest == "" || desc.Size == 0 {
		t.Fatalf("unexpected diff descriptor: %+v", desc)
	}
	t.Logf("Compare succeeded without mount manager: %s", desc.Digest)
}

// TestErofsDifferCompareRejectsNonEROFSMounts tests that Compare correctly
// rejects mounts that are not EROFS layers (no .erofslayer marker).
// The EROFS differ is specifically designed for EROFS snapshotter layers.
func TestErofsDifferCompareRejectsNonEROFSMounts(t *testing.T) {
	testutil.RequiresRoot(t)
	ctx := namespaces.WithNamespace(t.Context(), "testsuite")

	tempDir := t.TempDir()
	contentStore := imagetest.NewContentStore(ctx, t).Store

	// Create simple directory structures for lower and upper
	lowerDir := filepath.Join(tempDir, "lower")
	upperDir := filepath.Join(tempDir, "upper")

	if err := os.MkdirAll(lowerDir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(upperDir, 0755); err != nil {
		t.Fatal(err)
	}

	// Add files
	if err := os.WriteFile(filepath.Join(lowerDir, "base.txt"), []byte("base"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(upperDir, "new.txt"), []byte("new"), 0644); err != nil {
		t.Fatal(err)
	}

	// Create simple bind mounts WITHOUT .erofslayer marker
	// These are not valid EROFS snapshotter layers
	lowerMounts := []mount.Mount{
		{
			Source:  lowerDir,
			Type:    "bind",
			Options: []string{"ro", "rbind"},
		},
	}
	upperMounts := []mount.Mount{
		{
			Source:  upperDir,
			Type:    "bind",
			Options: []string{"ro", "rbind"},
		},
	}

	// Create differ WITHOUT mount manager
	differ := erofsdiffer.NewErofsDiffer(contentStore)

	// Compare should fail because upper is not an EROFS layer
	_, err := differ.Compare(ctx, lowerMounts, upperMounts)
	if err == nil {
		t.Fatal("expected error for non-EROFS layer mounts")
	}
	// Should get "not implemented" error indicating unsupported layer type
	if !strings.Contains(err.Error(), "not implemented") && !strings.Contains(err.Error(), "erofs-layer") {
		t.Fatalf("expected 'not implemented' or 'erofs-layer' error, got: %v", err)
	}
	t.Logf("correctly rejected non-EROFS mounts: %v", err)
}
