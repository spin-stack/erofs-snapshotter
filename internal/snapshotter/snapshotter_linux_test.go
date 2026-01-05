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

// This file contains the core EROFS snapshotter tests, test suite,
// and shared helper functions used by other test files.
//
// Core tests in this file:
// - TestErofs (testsuite)
// - TestErofsWithQuota
// - TestErofsFsverity
//
// Helper functions shared with erofs_differ_linux_test.go and
// erofs_snapshot_linux_test.go:
// - newSnapshotter
// - findErofs
// - createTestTarContent
// - tarHasPath
// - cloneMounts
// - mountsHaveTemplate
// - snapshotID
// - cleanupAllSnapshots

import (
	"archive/tar"
	"context"
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

	"github.com/aledbf/nexuserofs/internal/fsverity"
)

const (
	testFileContent       = "Hello, this is content for testing the EROFS Snapshotter!"
	testNestedFileContent = "Nested file content"
)

func newSnapshotter(t *testing.T, opts ...Opt) func(ctx context.Context, root string) (snapshots.Snapshotter, func() error, error) {
	_, err := exec.LookPath("mkfs.erofs")
	if err != nil {
		t.Skipf("could not find mkfs.erofs: %v", err)
	}

	if !findErofs() {
		t.Skip("check for erofs kernel support failed, skipping test")
	}
	return func(ctx context.Context, root string) (snapshots.Snapshotter, func() error, error) {
		snapshotter, err := NewSnapshotter(root, opts...)
		if err != nil {
			return nil, nil, err
		}

		return snapshotter, func() error { return snapshotter.Close() }, nil
	}
}

func TestErofs(t *testing.T) {
	testutil.RequiresRoot(t)
	testsuite.SnapshotterSuite(t, "erofs", newSnapshotter(t))
}

func TestErofsWithQuota(t *testing.T) {
	testutil.RequiresRoot(t)
	testsuite.SnapshotterSuite(t, "erofs", newSnapshotter(t, WithDefaultSize(16*1024*1024)))
}

func TestErofsFsverity(t *testing.T) {
	testutil.RequiresRoot(t)
	ctx := t.Context()

	root := t.TempDir()

	// Skip if fsverity is not supported
	supported, err := fsverity.IsSupported(root)
	if !supported || err != nil {
		t.Skip("fsverity not supported, skipping test")
	}

	// Create snapshotter with fsverity enabled
	s, err := NewSnapshotter(root, WithFsverity())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	defer cleanupAllSnapshots(ctx, s)

	// Create a test snapshot
	key := "test-snapshot"
	mounts, err := s.Prepare(ctx, key, "")
	if err != nil {
		t.Fatal(err)
	}

	target := filepath.Join(root, key)
	if err := os.MkdirAll(target, 0755); err != nil {
		t.Fatal(err)
	}
	if err := mount.All(mounts, target); err != nil {
		t.Fatal(err)
	}
	defer testutil.Unmount(t, target)

	// Write test data
	if err := os.WriteFile(filepath.Join(target, "foo"), []byte("test data"), 0777); err != nil {
		t.Fatal(err)
	}

	// Commit the snapshot
	commitKey := "test-commit"
	if err := s.Commit(ctx, commitKey, key); err != nil {
		t.Fatal(err)
	}

	snap, ok := s.(*snapshotter)
	if !ok {
		t.Fatal("failed to cast snapshotter to *snapshotter")
	}

	// Get the internal ID from the snapshotter
	var id string
	if err := snap.ms.WithTransaction(ctx, false, func(ctx context.Context) error {
		id, _, _, err = storage.GetInfo(ctx, commitKey)
		return err
	}); err != nil {
		t.Fatal(err)
	}

	// Verify fsverity is enabled on the EROFS layer

	layerPath := snap.layerBlobPath(id)

	enabled, err := fsverity.IsEnabled(layerPath)
	if err != nil {
		t.Fatalf("Failed to check fsverity status: %v", err)
	}
	if !enabled {
		t.Fatal("Expected fsverity to be enabled on committed layer")
	}

	// Try to modify the layer file directly (should fail)
	if err := os.WriteFile(layerPath, []byte("tampered data"), 0666); err == nil {
		t.Fatal("Expected direct write to fsverity-enabled layer to fail")
	}
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

func cloneMounts(in []mount.Mount) []mount.Mount {
	if in == nil {
		return nil
	}
	out := make([]mount.Mount, len(in))
	for i := range in {
		out[i] = in[i]
		if len(in[i].Options) > 0 {
			out[i].Options = append([]string(nil), in[i].Options...)
		}
	}
	return out
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
