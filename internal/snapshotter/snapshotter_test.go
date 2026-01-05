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

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/containerd/containerd/v2/core/snapshots"

	// Import testutil to register the -test.root flag
	_ "github.com/aledbf/nexuserofs/internal/testutil"
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
	t.Run("WithOvlOptions", func(t *testing.T) {
		config := &SnapshotterConfig{}
		opt := WithOvlOptions([]string{"metacopy=on", "redirect_dir=on"})
		opt(config)

		if len(config.ovlOptions) != 2 {
			t.Errorf("expected 2 options, got %d", len(config.ovlOptions))
		}
		if config.ovlOptions[0] != "metacopy=on" {
			t.Errorf("expected first option to be 'metacopy=on', got %q", config.ovlOptions[0])
		}
	})

	t.Run("WithFsverity", func(t *testing.T) {
		config := &SnapshotterConfig{}
		opt := WithFsverity()
		opt(config)

		if !config.enableFsverity {
			t.Error("expected fsverity to be enabled")
		}
	})

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

	t.Run("WithFsMergeThreshold", func(t *testing.T) {
		config := &SnapshotterConfig{}
		opt := WithFsMergeThreshold(5)
		opt(config)

		if config.fsMergeThreshold != 5 {
			t.Errorf("expected fsMergeThreshold to be 5, got %d", config.fsMergeThreshold)
		}
	})

}

func TestSnapshotterIsBlockMode(t *testing.T) {
	tests := []struct {
		name            string
		defaultWritable int64
		want            bool
	}{
		{
			name:            "zero means directory mode",
			defaultWritable: 0,
			want:            false,
		},
		{
			name:            "positive means block mode",
			defaultWritable: 1024 * 1024,
			want:            true,
		},
		{
			name:            "negative treated as not block mode",
			defaultWritable: -1,
			want:            false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := &snapshotter{defaultWritable: tc.defaultWritable}
			got := s.isBlockMode()
			if got != tc.want {
				t.Errorf("isBlockMode() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestSnapshotterPaths(t *testing.T) {
	root := "/var/lib/containerd/io.containerd.snapshotter.v1.erofs"
	s := &snapshotter{root: root}

	t.Run("upperPath", func(t *testing.T) {
		got := s.upperPath("123")
		want := filepath.Join(root, "snapshots", "123", "fs")
		if got != want {
			t.Errorf("upperPath(123) = %q, want %q", got, want)
		}
	})

	t.Run("viewLowerPath", func(t *testing.T) {
		got := s.viewLowerPath("123")
		want := filepath.Join(root, "snapshots", "123", "lower")
		if got != want {
			t.Errorf("viewLowerPath(123) = %q, want %q", got, want)
		}
	})

	t.Run("workPath", func(t *testing.T) {
		got := s.workPath("123")
		want := filepath.Join(root, "snapshots", "123", "work")
		if got != want {
			t.Errorf("workPath(123) = %q, want %q", got, want)
		}
	})

	t.Run("writablePath", func(t *testing.T) {
		got := s.writablePath("123")
		want := filepath.Join(root, "snapshots", "123", "rwlayer.img")
		if got != want {
			t.Errorf("writablePath(123) = %q, want %q", got, want)
		}
	})

	t.Run("layerBlobPath", func(t *testing.T) {
		got := s.layerBlobPath("123")
		want := filepath.Join(root, "snapshots", "123", "layer.erofs")
		if got != want {
			t.Errorf("layerBlobPath(123) = %q, want %q", got, want)
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
