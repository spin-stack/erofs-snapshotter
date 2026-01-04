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

package erofsutils

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/containerd/containerd/v2/core/mount"

	// Import testutil to register the -test.root flag
	_ "github.com/aledbf/nexuserofs/internal/testutil"
)

func TestMountsToLayer(t *testing.T) {
	tests := []struct {
		name        string
		mounts      []mount.Mount
		expectError bool
	}{
		{
			name:        "empty mounts",
			mounts:      []mount.Mount{},
			expectError: true,
		},
		{
			name:        "nil mounts",
			mounts:      nil,
			expectError: true,
		},
		{
			name: "mkfs type mount",
			mounts: []mount.Mount{
				{Type: "mkfs/ext4", Source: "/some/path/layer.erofs"},
			},
			expectError: true, // No .erofslayer marker
		},
		{
			name: "bind mount without marker",
			mounts: []mount.Mount{
				{Type: "bind", Source: "/some/path/fs"},
			},
			expectError: true, // No .erofslayer marker
		},
		{
			name: "erofs mount without marker",
			mounts: []mount.Mount{
				{Type: "erofs", Source: "/some/path/layer.erofs"},
			},
			expectError: true, // No .erofslayer marker
		},
		{
			name: "overlay mount without marker",
			mounts: []mount.Mount{
				{Type: "overlay", Source: "overlay", Options: []string{"upperdir=/tmp/upper", "lowerdir=/tmp/lower"}},
			},
			expectError: true, // No .erofslayer marker
		},
		{
			name: "unsupported mount type",
			mounts: []mount.Mount{
				{Type: "tmpfs", Source: "tmpfs"},
			},
			expectError: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := MountsToLayer(tc.mounts)
			if tc.expectError && err == nil {
				t.Error("expected error, got nil")
			}
			if !tc.expectError && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

func TestMountsToLayerWithMarker(t *testing.T) {
	// Create a temp directory with the erofs layer marker
	dir := t.TempDir()
	markerPath := filepath.Join(dir, ErofsLayerMarker)
	if err := os.WriteFile(markerPath, nil, 0600); err != nil {
		t.Fatalf("failed to create marker file: %v", err)
	}

	// Create fs subdirectory for bind mount source
	fsDir := filepath.Join(dir, "fs")
	if err := os.Mkdir(fsDir, 0755); err != nil {
		t.Fatalf("failed to create fs dir: %v", err)
	}

	tests := []struct {
		name   string
		mounts []mount.Mount
		want   string
	}{
		{
			name: "bind mount with marker",
			mounts: []mount.Mount{
				{Type: "bind", Source: fsDir},
			},
			want: dir,
		},
		{
			name: "mkfs type with marker",
			mounts: []mount.Mount{
				{Type: "mkfs/ext4", Source: filepath.Join(dir, "layer.erofs")},
			},
			want: dir,
		},
		{
			name: "erofs mount with marker",
			mounts: []mount.Mount{
				{Type: "erofs", Source: filepath.Join(dir, "layer.erofs")},
			},
			want: dir,
		},
		{
			name: "format/mkdir/bind compound type",
			mounts: []mount.Mount{
				{Type: "format/mkdir/bind", Source: fsDir},
			},
			want: dir,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := MountsToLayer(tc.mounts)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			if got != tc.want {
				t.Errorf("MountsToLayer() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestMountsToLayerOverlay(t *testing.T) {
	// Create directories for overlay mount test
	baseDir := t.TempDir()

	// Create upperdir parent with marker
	upperParent := filepath.Join(baseDir, "upper-parent")
	if err := os.MkdirAll(filepath.Join(upperParent, "upper"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(upperParent, ErofsLayerMarker), nil, 0600); err != nil {
		t.Fatal(err)
	}

	// Create lowerdir parent with marker
	lowerParent := filepath.Join(baseDir, "lower-parent")
	lowerDir := filepath.Join(lowerParent, "lower")
	if err := os.MkdirAll(lowerDir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(lowerParent, ErofsLayerMarker), nil, 0600); err != nil {
		t.Fatal(err)
	}

	t.Run("overlay with upperdir", func(t *testing.T) {
		mounts := []mount.Mount{
			{Type: "erofs", Source: lowerDir}, // first mount for lowerdir resolution
			{
				Type:   "overlay",
				Source: "overlay",
				Options: []string{
					"upperdir=" + filepath.Join(upperParent, "upper"),
					"lowerdir=" + lowerDir,
					"workdir=" + filepath.Join(upperParent, "work"),
				},
			},
		}
		got, err := MountsToLayer(mounts)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
			return
		}
		if got != upperParent {
			t.Errorf("got %q, want %q", got, upperParent)
		}
	})

	t.Run("overlay with only lowerdir (view)", func(t *testing.T) {
		mounts := []mount.Mount{
			{Type: "erofs", Source: lowerDir},
			{
				Type:   "overlay",
				Source: "overlay",
				Options: []string{
					"lowerdir=" + lowerDir,
					"ro",
				},
			},
		}
		got, err := MountsToLayer(mounts)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
			return
		}
		if got != lowerParent {
			t.Errorf("got %q, want %q", got, lowerParent)
		}
	})

	t.Run("overlay without upperdir or lowerdir", func(t *testing.T) {
		mounts := []mount.Mount{
			{Type: "erofs", Source: lowerDir},
			{
				Type:    "overlay",
				Source:  "overlay",
				Options: []string{"ro"},
			},
		}
		_, err := MountsToLayer(mounts)
		if err == nil {
			t.Error("expected error for overlay without upperdir/lowerdir")
		}
	})
}

func TestMountBaseType(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"overlay", "overlay"},
		{"bind", "bind"},
		{"erofs", "erofs"},
		{"ext4", "ext4"},
		{"format/mkdir/overlay", "overlay"},
		{"mkfs/ext4", "ext4"},
		{"format/mkdir/bind", "bind"},
		{"a/b/c/d", "d"},
		{"", ""},
	}

	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			got := mountBaseType(tc.input)
			if got != tc.want {
				t.Errorf("mountBaseType(%q) = %q, want %q", tc.input, got, tc.want)
			}
		})
	}
}

func TestSupportGenerateFromTar(t *testing.T) {
	// This test just verifies the function doesn't panic
	// The actual result depends on whether mkfs.erofs is installed
	supported, err := SupportGenerateFromTar()
	if err != nil {
		t.Logf("mkfs.erofs not available: %v", err)
		return
	}
	t.Logf("mkfs.erofs tar support: %v", supported)
}

func TestConstants(t *testing.T) {
	// Verify constants have expected values
	if ErofsLayerMarker != ".erofslayer" {
		t.Errorf("ErofsLayerMarker = %q, want %q", ErofsLayerMarker, ".erofslayer")
	}
	if LayerBlobFilename != "layer.erofs" {
		t.Errorf("LayerBlobFilename = %q, want %q", LayerBlobFilename, "layer.erofs")
	}
}
