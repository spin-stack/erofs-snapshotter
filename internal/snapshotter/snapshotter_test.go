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

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/containerd/containerd/v2/core/snapshots"
	"github.com/containerd/containerd/v2/core/snapshots/storage"

	// Import testutil to register the -test.root flag
	_ "github.com/spin-stack/erofs-snapshotter/internal/testutil"
)

func TestIsExtractKey(t *testing.T) {
	tests := []struct {
		name     string
		key      string
		expected bool
	}{
		{
			name:     "namespaced extract key",
			key:      "default/1/extract-12345",
			expected: true,
		},
		{
			name:     "extract key with digest",
			key:      "default/1/extract-sha256:abc123",
			expected: true,
		},
		{
			name:     "non-extract key",
			key:      "default/1/other-12345",
			expected: false,
		},
		{
			name:     "empty string",
			key:      "",
			expected: false,
		},
		{
			name:     "just extract prefix",
			key:      "extract-",
			expected: true,
		},
		{
			name:     "extract without hyphen",
			key:      "default/1/extract",
			expected: true, // HasPrefix matches "extract" prefix
		},
		{
			name:     "key without namespace",
			key:      "extract-12345",
			expected: true,
		},
		{
			name:     "deeply nested extract key",
			key:      "ns/a/b/c/extract-12345",
			expected: true,
		},
		{
			name:     "extract in middle of path",
			key:      "default/extract-123/snapshot",
			expected: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := isExtractKey(tc.key)
			if got != tc.expected {
				t.Errorf("isExtractKey(%q) = %v, want %v", tc.key, got, tc.expected)
			}
		})
	}
}

func TestIsExtractSnapshot(t *testing.T) {
	tests := []struct {
		name     string
		labels   map[string]string
		expected bool
	}{
		{
			name:     "no labels",
			labels:   nil,
			expected: false,
		},
		{
			name:     "empty labels",
			labels:   map[string]string{},
			expected: false,
		},
		{
			name:     "extract label set to true",
			labels:   map[string]string{extractLabel: "true"},
			expected: true,
		},
		{
			name:     "extract label set to false",
			labels:   map[string]string{extractLabel: "false"},
			expected: false,
		},
		{
			name:     "extract label set to empty string",
			labels:   map[string]string{extractLabel: ""},
			expected: false,
		},
		{
			name:     "other labels present",
			labels:   map[string]string{"other": "value"},
			expected: false,
		},
		{
			name:     "extract label with other labels",
			labels:   map[string]string{extractLabel: "true", "other": "value"},
			expected: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			info := snapshots.Info{Labels: tc.labels}
			got := isExtractSnapshot(info)
			if got != tc.expected {
				t.Errorf("isExtractSnapshot(%v) = %v, want %v", tc.labels, got, tc.expected)
			}
		})
	}
}

func TestEnsureMarkerFile(t *testing.T) {
	t.Run("creates new file", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "marker")

		err := ensureMarkerFile(path)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Verify file exists
		if _, err := os.Stat(path); err != nil {
			t.Errorf("marker file not created: %v", err)
		}
	})

	t.Run("is idempotent", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "marker")

		// Create first time
		if err := ensureMarkerFile(path); err != nil {
			t.Fatalf("first call failed: %v", err)
		}

		// Create second time - should not error
		if err := ensureMarkerFile(path); err != nil {
			t.Errorf("second call failed: %v", err)
		}
	})

	t.Run("fails if parent directory does not exist", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "nonexistent", "marker")

		err := ensureMarkerFile(path)
		if err == nil {
			t.Error("expected error for non-existent parent directory")
		}
	})

	t.Run("creates empty file", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "marker")

		if err := ensureMarkerFile(path); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		info, err := os.Stat(path)
		if err != nil {
			t.Fatalf("failed to stat marker: %v", err)
		}
		if info.Size() != 0 {
			t.Errorf("marker file should be empty, got size %d", info.Size())
		}
	})
}

func TestSnapshotterOptions(t *testing.T) {
	t.Run("WithImmutable", func(t *testing.T) {
		config := &SnapshotterConfig{}
		opt := WithImmutable()
		opt(config)

		if !config.setImmutable {
			t.Error("expected immutable to be enabled")
		}
	})

	t.Run("WithDefaultSize", func(t *testing.T) {
		config := &SnapshotterConfig{}
		opt := WithDefaultSize(1024 * 1024 * 100) // 100MB
		opt(config)

		if config.defaultSize != 100*1024*1024 {
			t.Errorf("expected defaultSize to be 100MB, got %d", config.defaultSize)
		}
	})

}

func TestMountFsMetaReturnsFormatErofs(t *testing.T) {
	// This test verifies that mountFsMeta returns "format/erofs" type for multi-device mounts.
	// The format/ prefix signals that containerd's standard mount manager cannot handle this type,
	// providing a clear "unsupported mount type" error instead of cryptic EINVAL.

	root := t.TempDir()
	s := &snapshotter{root: root}

	// Create fake snapshot directories with fsmeta and vmdk files
	snapshotDir := filepath.Join(root, "snapshots", "parent1")
	if err := os.MkdirAll(snapshotDir, 0755); err != nil {
		t.Fatal(err)
	}

	// Create required files for mountFsMeta to succeed
	// Use digest-based naming for layer file (as the differ now creates)
	vmdkPath := filepath.Join(snapshotDir, "merged.vmdk")
	fsmetaPath := filepath.Join(snapshotDir, "fsmeta.erofs")
	layerPath := filepath.Join(snapshotDir, "sha256-a3ed95caeb02ffe68cdd9fd84406680ae93d633cb16422d00e8a7c22955b46d4.erofs")

	for _, path := range []string{vmdkPath, fsmetaPath, layerPath} {
		if err := os.WriteFile(path, []byte("fake"), 0644); err != nil {
			t.Fatal(err)
		}
	}

	// Create a fake storage.Snapshot with ParentIDs
	snap := storage.Snapshot{
		ID:        "child",
		ParentIDs: []string{"parent1"},
	}

	mount, ok := s.mountFsMeta(snap)
	if !ok {
		t.Fatal("mountFsMeta should return true when fsmeta/vmdk exist")
	}

	// Verify mount type is "format/erofs" (not "erofs")
	if mount.Type != "format/erofs" {
		t.Errorf("mountFsMeta returned Type=%q, want %q", mount.Type, "format/erofs")
	}

	// Verify source points to fsmeta.erofs
	if mount.Source != fsmetaPath {
		t.Errorf("mountFsMeta returned Source=%q, want %q", mount.Source, fsmetaPath)
	}

	// Verify options include device= for parent layer
	hasDevice := false
	for _, opt := range mount.Options {
		if opt == "device="+layerPath {
			hasDevice = true
			break
		}
	}
	if !hasDevice {
		t.Errorf("mountFsMeta should include device= option for parent layer, got: %v", mount.Options)
	}
}

func TestMountFsMetaDeviceOrder(t *testing.T) {
	// This test verifies that mountFsMeta returns device= options in oldest-first order,
	// matching containerd's approach (backward iteration through ParentIDs).
	// See: https://github.com/containerd/containerd/pull/12374

	root := t.TempDir()
	s := &snapshotter{root: root}

	// Create 3 parent snapshot directories with layer blobs
	// ParentIDs order: [parent3, parent2, parent1] (newest to oldest, as containerd provides)
	// Expected device order after backward iteration: [parent1, parent2, parent3] (oldest to newest)
	parentIDs := []string{"parent3", "parent2", "parent1"}

	// Create layer blobs for each parent
	layerPaths := make(map[string]string)
	for _, pid := range parentIDs {
		snapshotDir := filepath.Join(root, "snapshots", pid)
		if err := os.MkdirAll(snapshotDir, 0755); err != nil {
			t.Fatal(err)
		}
		// Use digest-based layer names (64 hex chars required)
		layerPath := filepath.Join(snapshotDir, "sha256-"+pid+pid+pid+pid+pid+pid+pid+pid+".erofs")
		if err := os.WriteFile(layerPath, []byte("fake"), 0644); err != nil {
			t.Fatal(err)
		}
		layerPaths[pid] = layerPath
	}

	// Expected order: backward iteration through parentIDs gives oldest-first
	// parentIDs = [parent3, parent2, parent1], backward = [parent1, parent2, parent3]
	expectedOrder := []string{
		"device=" + layerPaths["parent1"],
		"device=" + layerPaths["parent2"],
		"device=" + layerPaths["parent3"],
	}

	// Create fsmeta and vmdk in the newest parent (parent3)
	newestDir := filepath.Join(root, "snapshots", "parent3")
	vmdkPath := filepath.Join(newestDir, "merged.vmdk")
	fsmetaPath := filepath.Join(newestDir, "fsmeta.erofs")
	for _, path := range []string{vmdkPath, fsmetaPath} {
		if err := os.WriteFile(path, []byte("fake"), 0644); err != nil {
			t.Fatal(err)
		}
	}

	// Create a snapshot with 3 parents (newest first in ParentIDs)
	snap := storage.Snapshot{
		ID:        "child",
		ParentIDs: parentIDs,
	}

	mount, ok := s.mountFsMeta(snap)
	if !ok {
		t.Fatal("mountFsMeta should return true when fsmeta/vmdk exist")
	}

	// Extract device= options from mount.Options
	var deviceOpts []string
	for _, opt := range mount.Options {
		if len(opt) > 7 && opt[:7] == "device=" {
			deviceOpts = append(deviceOpts, opt)
		}
	}

	// Verify we have 3 device options
	if len(deviceOpts) != 3 {
		t.Fatalf("expected 3 device options, got %d: %v", len(deviceOpts), deviceOpts)
	}

	// Verify order is oldest-first (parent1, parent2, parent3)
	for i, expected := range expectedOrder {
		if deviceOpts[i] != expected {
			t.Errorf("device option %d:\n  got:  %q\n  want: %q", i, deviceOpts[i], expected)
		}
	}
}

func TestSnapshotterPaths(t *testing.T) {
	root := "/var/lib/containerd/io.containerd.snapshotter.v1.erofs"
	s := &snapshotter{root: root}

	t.Run("viewLowerPath", func(t *testing.T) {
		got := s.viewLowerPath("123")
		want := filepath.Join(root, "snapshots", "123", "lower")
		if got != want {
			t.Errorf("viewLowerPath(123) = %q, want %q", got, want)
		}
	})

	t.Run("writablePath", func(t *testing.T) {
		got := s.writablePath("123")
		want := filepath.Join(root, "snapshots", "123", "rwlayer.img")
		if got != want {
			t.Errorf("writablePath(123) = %q, want %q", got, want)
		}
	})

	t.Run("fallbackLayerBlobPath", func(t *testing.T) {
		got := s.fallbackLayerBlobPath("123")
		want := filepath.Join(root, "snapshots", "123", "snapshot-123.erofs")
		if got != want {
			t.Errorf("fallbackLayerBlobPath(123) = %q, want %q", got, want)
		}
	})

	t.Run("findLayerBlob_notFound", func(t *testing.T) {
		_, err := s.findLayerBlob("nonexistent")
		if err == nil {
			t.Error("findLayerBlob(nonexistent) should return error")
		}
	})

	t.Run("fsMetaPath", func(t *testing.T) {
		got := s.fsMetaPath("123")
		want := filepath.Join(root, "snapshots", "123", "fsmeta.erofs")
		if got != want {
			t.Errorf("fsMetaPath(123) = %q, want %q", got, want)
		}
	})
}
