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

// This file contains the core EROFS snapshotter tests, test suite,
// and shared helper functions used by other test files.
//
// NOTE: Most tests in this file are SKIPPED because the EROFS snapshotter
// is designed exclusively for VM runtimes (like qemubox). The snapshotter
// returns raw file paths (EROFS blobs, ext4 images) that are passed to VMs
// as virtio-blk devices, NOT mounted on the host.
//
// Core tests in this file:
// - TestErofs (testsuite) - SKIPPED (VM-only)
// - TestErofsWithQuota - SKIPPED (VM-only)
//
// Helper functions shared with erofs_differ_linux_test.go and
// erofs_snapshot_linux_test.go:
// - newSnapshotter
// - createTestTarContent
// - tarHasPath
// - cloneMounts
// - mountsHaveTemplate
// - snapshotID
// - cleanupAllSnapshots

import (
	"archive/tar"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/containerd/containerd/v2/core/content"
	"github.com/containerd/containerd/v2/core/mount"
	"github.com/containerd/containerd/v2/core/snapshots"
	"github.com/containerd/containerd/v2/core/snapshots/storage"
	"github.com/containerd/containerd/v2/core/snapshots/testsuite"
	"github.com/containerd/containerd/v2/pkg/archive/compression"
	"github.com/containerd/containerd/v2/pkg/archive/tartest"
	"github.com/containerd/containerd/v2/pkg/testutil"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"github.com/spin-stack/erofs-snapshotter/internal/loop"
	"github.com/spin-stack/erofs-snapshotter/internal/mountutils"
	"github.com/spin-stack/erofs-snapshotter/internal/preflight"
)

// vmOnlySkipMessage is the skip message for tests that require host mounting.
// The EROFS snapshotter is designed for VM runtimes only - it returns raw file
// paths (EROFS blobs, ext4 images) that are passed to VMs as virtio-blk devices.
const vmOnlySkipMessage = "SKIPPED: EROFS snapshotter is VM-only; returns raw file paths for virtio-blk, not host-mountable filesystems"

// skipIfVMOnly skips the test with the VM-only message.
// Use this for tests that require host mounting of returned mounts.
func skipIfVMOnly(t *testing.T) {
	t.Helper()
	t.Skip(vmOnlySkipMessage)
}

// skipIfNoImmutableSupport skips the test if the filesystem doesn't support
// the FS_IOC_GETFLAGS ioctl (e.g., tmpfs doesn't support immutable flags).
func skipIfNoImmutableSupport(t *testing.T, dir string) {
	t.Helper()
	// Create a test file to check if FS_IOC_GETFLAGS is supported
	testFile := filepath.Join(dir, ".immutable-test")
	if err := os.WriteFile(testFile, []byte("test"), 0644); err != nil {
		t.Skipf("failed to create test file: %v", err)
	}
	defer os.Remove(testFile)

	// Try to get inode flags using lsattr (which uses FS_IOC_GETFLAGS)
	if _, err := exec.Command("lsattr", testFile).CombinedOutput(); err != nil {
		t.Skip("filesystem does not support immutable flags (FS_IOC_GETFLAGS)")
	}
}

const (
	testFileContent       = "Hello, this is content for testing the EROFS Snapshotter!"
	testNestedFileContent = "Nested file content"
)

// snapshotTestEnv encapsulates the common test environment for snapshot tests.
// It provides helpers for creating layers, views, and managing cleanup.
type snapshotTestEnv struct {
	t            *testing.T
	tempDir      string
	snapshotRoot string
	snapshotter  *snapshotter
}

// ctx returns the test context.
func (e *snapshotTestEnv) ctx() context.Context {
	return e.t.Context()
}

// newSnapshotTestEnv creates a new test environment with all prerequisites checked.
// It skips the test if EROFS support is not available.
//
// NOTE: Tests using this helper must use extract-style keys ("extract-" prefix)
// when calling Prepare() to write files. This ensures the ext4 writable layer
// is mounted on the host, allowing content to be written.
func newSnapshotTestEnv(t *testing.T, opts ...Opt) *snapshotTestEnv {
	t.Helper()
	testutil.RequiresRoot(t)

	if err := preflight.CheckErofsSupport(); err != nil {
		t.Skipf("EROFS support check failed: %v", err)
	}

	tempDir := t.TempDir()
	snapshotRoot := filepath.Join(tempDir, "snapshots")

	s, err := NewSnapshotter(snapshotRoot, opts...)
	if err != nil {
		t.Fatal(err)
	}

	snap, ok := s.(*snapshotter)
	if !ok {
		t.Fatal("failed to cast snapshotter to *snapshotter")
	}

	env := &snapshotTestEnv{
		t:            t,
		tempDir:      tempDir,
		snapshotRoot: snapshotRoot,
		snapshotter:  snap,
	}

	t.Cleanup(func() {
		cleanupAllSnapshots(t.Context(), s)
		s.Close()
		mount.UnmountRecursive(snapshotRoot, 0)
	})

	return env
}

// createLayer creates and commits a layer with a single file.
// Returns the commit key for use as a parent.
func (e *snapshotTestEnv) createLayer(key, parentKey, filename, content string) string {
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

// createLayerWithLabels creates and commits a layer with labels and a single file.
// Returns the commit key for use as a parent.
func (e *snapshotTestEnv) createLayerWithLabels(key, parentKey, filename, content string, labels map[string]string) string {
	e.t.Helper()

	// Use extract-style key so the snapshotter mounts the ext4 on host
	extractKey := "extract-" + key
	if _, err := e.snapshotter.Prepare(e.ctx(), extractKey, parentKey, snapshots.WithLabels(labels)); err != nil {
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

// createView creates a read-only view of a committed snapshot.
func (e *snapshotTestEnv) createView(key, parentKey string) []mount.Mount {
	e.t.Helper()

	mounts, err := e.snapshotter.View(e.ctx(), key, parentKey)
	if err != nil {
		e.t.Fatalf("failed to create view %s: %v", key, err)
	}

	return mounts
}

func newSnapshotter(t *testing.T, opts ...Opt) func(ctx context.Context, root string) (snapshots.Snapshotter, func() error, error) {
	if err := preflight.CheckErofsSupport(); err != nil {
		t.Skipf("EROFS support check failed: %v", err)
	}
	return func(ctx context.Context, root string) (snapshots.Snapshotter, func() error, error) {
		//nolint:contextcheck // NewSnapshotter follows containerd interface (no context param)
		snapshotter, err := NewSnapshotter(root, opts...)
		if err != nil {
			return nil, nil, err
		}

		return snapshotter, func() error { return snapshotter.Close() }, nil
	}
}

func TestErofs(t *testing.T) {
	skipIfVMOnly(t) // Testsuite requires host mounting
	testutil.RequiresRoot(t)
	testsuite.SnapshotterSuite(t, "erofs", newSnapshotter(t))
}

func TestErofsWithQuota(t *testing.T) {
	skipIfVMOnly(t) // Testsuite requires host mounting
	testutil.RequiresRoot(t)
	testsuite.SnapshotterSuite(t, "erofs", newSnapshotter(t, WithDefaultSize(16*1024*1024)))
}

// createTestTarContent creates test tar content using tartest.
func createTestTarContent() io.ReadCloser {
	// Create a tar context with current time for consistency
	tc := tartest.TarContext{}.WithModTime(time.Now())

	// Create the tar with our test files and directories
	tarWriter := tartest.TarAll(
		tc.File("test-file.txt", []byte(testFileContent), 0644),
		tc.Dir("testdir", 0755),
		tc.File("testdir/nested.txt", []byte(testNestedFileContent), 0644),
	)

	// Return the tar as a ReadCloser
	return tartest.TarFromWriterTo(tarWriter)
}

func tarHasPath(ctx context.Context, store content.Store, desc ocispec.Descriptor, target string) (bool, error) {
	ra, err := store.ReaderAt(ctx, desc)
	if err != nil {
		return false, err
	}
	defer ra.Close()

	rc := content.NewReader(ra)

	dr, err := compression.DecompressStream(rc)
	if err != nil {
		return false, err
	}
	defer dr.Close()

	tr := tar.NewReader(dr)
	for {
		h, err := tr.Next()
		if err == io.EOF {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		name := strings.TrimPrefix(h.Name, "./")
		if name == target {
			return true, nil
		}
	}
}

func mountsHaveTemplate(mounts []mount.Mount) bool {
	for _, m := range mounts {
		if strings.Contains(m.Source, "{{") || strings.Contains(m.Target, "{{") {
			return true
		}
		for _, opt := range m.Options {
			if strings.Contains(opt, "{{") {
				return true
			}
		}
	}
	return false
}

func snapshotID(ctx context.Context, t *testing.T, s *snapshotter, key string) string {
	t.Helper()
	var id string
	if err := s.ms.WithTransaction(ctx, false, func(ctx context.Context) error {
		var err error
		id, _, _, err = storage.GetInfo(ctx, key)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	return id
}

// cleanupAllSnapshots removes all snapshots using only the public Snapshotter interface.
// Snapshots are removed in reverse order (children first, then parents) to respect
// the snapshot dependency chain. After removing all snapshots, Cleanup() is called
// to unmount any remaining EROFS layers and release resources.
func cleanupAllSnapshots(ctx context.Context, s snapshots.Snapshotter) {
	var keys []string
	_ = s.Walk(ctx, func(ctx context.Context, info snapshots.Info) error {
		keys = append(keys, info.Name)
		return nil
	})
	// Remove in reverse order (children first, then parents)
	for i := len(keys) - 1; i >= 0; i-- {
		_ = s.Remove(ctx, keys[i])
	}
	// Call Cleanup to unmount EROFS layers and release resources
	if cleaner, ok := s.(interface{ Cleanup(context.Context) error }); ok {
		_ = cleaner.Cleanup(ctx)
	}
}

// mountErofsView mounts EROFS view mounts at the target directory for testing.
// Uses mountutils.MountAll which handles both single-device and multi-device EROFS mounts.
// Returns a cleanup function to unmount and detach loop devices.
func mountErofsView(t *testing.T, mounts []mount.Mount, target string) func() {
	t.Helper()

	if len(mounts) == 0 {
		t.Fatal("no mounts to mount")
	}

	cleanup, err := mountutils.MountAll(mounts, target)
	if err != nil {
		t.Fatalf("failed to mount EROFS: %v", err)
	}

	return func() {
		if err := cleanup(); err != nil {
			t.Logf("cleanup error: %v", err)
		}
	}
}

// createOverlayViewForCompareWithMountedLower creates an overlay mount at target that combines
// an already-mounted lower directory with the upper directory. This is needed because
// archive.WriteDiff expects to see the full desired state, not just the new files.
// Returns a cleanup function to unmount everything.
func createOverlayViewForCompareWithMountedLower(t *testing.T, lowerDir, upperDir, target string) func() {
	t.Helper()

	// Create workdir for overlay in the same filesystem as upperdir
	// (overlay requires upperdir and workdir to be on the same filesystem)
	// The upperDir is inside the mounted ext4 at /rw/upper, so workdir goes at /rw/work
	workdir := filepath.Join(filepath.Dir(upperDir), "work")
	if err := os.MkdirAll(workdir, 0755); err != nil {
		t.Fatalf("failed to create workdir: %v", err)
	}

	// Create overlay with lower as lowerdir and rw/upper as upperdir
	// Note: We use the parent's contents as lowerdir so the diff only sees new/modified files
	overlayOpts := fmt.Sprintf("lowerdir=%s,upperdir=%s,workdir=%s", lowerDir, upperDir, workdir)
	args := []string{"-t", "overlay", "-o", overlayOpts, "overlay", target}
	cmd := exec.Command("mount", args...)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("failed to mount overlay for compare: %v: %s", err, out)
	}

	return func() {
		exec.Command("umount", target).Run()
	}
}

// mountErofsLayersResult contains the result of mounting EROFS layers.
type mountErofsLayersResult struct {
	layerDirs      []string // Directories where each layer is mounted
	overlayMounted bool     // Whether overlay was successfully mounted at target
	cleanup        func()   // Cleanup function
}

// mountErofsLayersWithOverlay mounts multiple individual EROFS layers and creates
// an overlay to combine them. This is used when fsmeta merge fails and we fall back
// to individual layer mounts.
// Returns mountErofsLayersResult with layer directories and cleanup function.
func mountErofsLayersWithOverlay(t *testing.T, layers []mount.Mount, target string) mountErofsLayersResult {
	t.Helper()

	if len(layers) == 0 {
		t.Fatal("no layers to mount")
	}

	// Create a single base directory for all layer mounts
	// This ensures all lowerdirs are on the same filesystem (required by overlay)
	baseDir := t.TempDir()

	// Mount each EROFS layer at a separate subdirectory
	var layerDirs []string
	var loopDevices []*loop.Device

	for i, m := range layers {
		layerDir := filepath.Join(baseDir, fmt.Sprintf("layer%d", i))
		if err := os.MkdirAll(layerDir, 0755); err != nil {
			t.Fatalf("failed to create layer dir: %v", err)
		}

		// Set up loop device for this layer
		loopDev, err := loop.Setup(m.Source, loop.Config{ReadOnly: true})
		if err != nil {
			// Cleanup on failure
			for _, l := range loopDevices {
				l.Detach()
			}
			t.Fatalf("failed to setup loop device for %s: %v", m.Source, err)
		}
		loopDevices = append(loopDevices, loopDev)

		// Mount EROFS layer
		args := []string{"-t", "erofs", "-o", "ro", loopDev.Path, layerDir}
		cmd := exec.Command("mount", args...)
		if out, err := cmd.CombinedOutput(); err != nil {
			// Cleanup on failure
			for _, l := range loopDevices {
				l.Detach()
			}
			t.Fatalf("failed to mount EROFS layer %s: %v: %s", m.Source, err, out)
		}
		layerDirs = append(layerDirs, layerDir)
	}

	// Create overlay with all layers as lowerdirs
	// Layers are ordered newest to oldest (same as ParentIDs), which is the correct order for lowerdir
	lowerdir := strings.Join(layerDirs, ":")

	// Debug: list contents of each layer
	for i, dir := range layerDirs {
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Logf("layer %d (%s): error reading: %v", i, dir, err)
		} else {
			var names []string
			for _, e := range entries {
				names = append(names, e.Name())
			}
			t.Logf("layer %d (%s): %v", i, dir, names)
		}
	}

	// Ensure target directory exists
	if err := os.MkdirAll(target, 0755); err != nil {
		for _, dir := range layerDirs {
			exec.Command("umount", dir).Run()
		}
		for _, l := range loopDevices {
			l.Detach()
		}
		t.Fatalf("failed to create target dir: %v", err)
	}

	// Create a read-only overlay with all layers as lowerdirs
	// Use index=off and metacopy=off for compatibility with older kernels
	overlayOpts := fmt.Sprintf("lowerdir=%s,index=off,metacopy=off", lowerdir)
	args := []string{"-t", "overlay", "-o", overlayOpts, "overlay", target}
	cmd := exec.Command("mount", args...)
	if out, err := cmd.CombinedOutput(); err != nil {
		// If overlay mount fails, DON'T cleanup - keep layers mounted for individual verification
		t.Logf("overlay mount failed (may need kernel support): %v: %s", err, out)
		t.Logf("verifying files in individual layers instead of overlay")

		return mountErofsLayersResult{
			layerDirs:      layerDirs,
			overlayMounted: false,
			cleanup: func() {
				// Cleanup when test is done
				for _, dir := range layerDirs {
					exec.Command("umount", dir).Run()
				}
				for _, l := range loopDevices {
					l.Detach()
				}
			},
		}
	}

	return mountErofsLayersResult{
		layerDirs:      layerDirs,
		overlayMounted: true,
		cleanup: func() {
			// Unmount overlay first
			exec.Command("umount", target).Run()
			// Unmount each layer
			for _, dir := range layerDirs {
				exec.Command("umount", dir).Run()
			}
			// Detach loop devices
			for _, l := range loopDevices {
				l.Detach()
			}
		},
	}
}
