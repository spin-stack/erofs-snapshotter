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

package mountutils

import (
	"strings"
	"testing"

	"github.com/containerd/containerd/v2/core/mount"

	// Import testutil to register the -test.root flag
	_ "github.com/aledbf/nexus-erofs/internal/testutil"
)

func TestNeedsMountManager(t *testing.T) {
	tests := []struct {
		name     string
		mounts   []mount.Mount
		expected bool
	}{
		{
			name:     "empty mounts",
			mounts:   []mount.Mount{},
			expected: false,
		},
		{
			name: "simple bind mount",
			mounts: []mount.Mount{
				{Type: "bind", Source: "/src", Target: "/dst"},
			},
			expected: false,
		},
		{
			name: "overlay mount",
			mounts: []mount.Mount{
				{Type: "overlay", Source: "overlay", Options: []string{"lowerdir=/lower", "upperdir=/upper"}},
			},
			expected: false,
		},
		{
			name: "template in source",
			mounts: []mount.Mount{
				{Type: "bind", Source: "{{ mount 0 }}", Target: "/dst"},
			},
			expected: true,
		},
		{
			name: "template in options",
			mounts: []mount.Mount{
				{Type: "overlay", Source: "overlay", Options: []string{"lowerdir={{ mount 0 }}"}},
			},
			expected: true,
		},
		{
			name: "format mount type",
			mounts: []mount.Mount{
				{Type: "format/ext4", Source: "/dev/loop0"},
			},
			expected: true,
		},
		{
			name: "mkfs mount type",
			mounts: []mount.Mount{
				{Type: "mkfs/ext4", Source: "/dev/loop0"},
			},
			expected: true,
		},
		{
			name: "mkdir mount type",
			mounts: []mount.Mount{
				{Type: "mkdir/bind", Source: "/src"},
			},
			expected: true,
		},
		{
			name: "erofs mount with loop option requires mount manager",
			mounts: []mount.Mount{
				{Type: "erofs", Source: "/path/to/layer.erofs", Options: []string{"ro", "loop"}},
			},
			expected: true,
		},
		{
			name: "ext4 mount with loop option requires mount manager",
			mounts: []mount.Mount{
				{Type: "ext4", Source: "/path/to/upper.img", Options: []string{"rw", "loop"}},
			},
			expected: true,
		},
		{
			name: "erofs mount with loop and device options requires mount manager",
			mounts: []mount.Mount{
				{Type: "erofs", Source: "/path/to/layer.erofs", Options: []string{"ro", "loop", "device=/path/to/blob1"}},
			},
			expected: true,
		},
		{
			name: "erofs mount without loop option does not require mount manager",
			mounts: []mount.Mount{
				{Type: "erofs", Source: "/path/to/layer.erofs", Options: []string{"ro"}},
			},
			expected: false,
		},
		{
			name: "format with nested type",
			mounts: []mount.Mount{
				{Type: "format/mkdir/overlay", Source: "/dev/loop0"},
			},
			expected: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := NeedsMountManager(tc.mounts)
			if got != tc.expected {
				t.Errorf("NeedsMountManager() = %v, want %v", got, tc.expected)
			}
		})
	}
}

func TestNeedsNonTemporaryActivation(t *testing.T) {
	tests := []struct {
		name     string
		mounts   []mount.Mount
		expected bool
	}{
		{
			name:     "empty mounts",
			mounts:   []mount.Mount{},
			expected: false,
		},
		{
			name: "bind mount",
			mounts: []mount.Mount{
				{Type: "bind"},
			},
			expected: false,
		},
		{
			name: "format mount",
			mounts: []mount.Mount{
				{Type: "format/ext4"},
			},
			expected: true,
		},
		{
			name: "mkfs mount",
			mounts: []mount.Mount{
				{Type: "mkfs/ext4"},
			},
			expected: true,
		},
		{
			name: "mkdir mount",
			mounts: []mount.Mount{
				{Type: "mkdir/overlay"},
			},
			expected: true,
		},
		{
			name: "erofs mount - not non-temporary",
			mounts: []mount.Mount{
				{Type: "erofs"},
			},
			expected: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := NeedsNonTemporaryActivation(tc.mounts)
			if got != tc.expected {
				t.Errorf("NeedsNonTemporaryActivation() = %v, want %v", got, tc.expected)
			}
		})
	}
}

func TestHasTemplate(t *testing.T) {
	tests := []struct {
		name     string
		mount    mount.Mount
		expected bool
	}{
		{
			name:     "no template",
			mount:    mount.Mount{Type: "bind", Source: "/src", Target: "/dst"},
			expected: false,
		},
		{
			name:     "template in source",
			mount:    mount.Mount{Source: "{{ mount 0 }}"},
			expected: true,
		},
		{
			name:     "template in target",
			mount:    mount.Mount{Target: "{{ mount 1 }}"},
			expected: true,
		},
		{
			name:     "template in options",
			mount:    mount.Mount{Options: []string{"lowerdir={{ mount 0 }}"}},
			expected: true,
		},
		{
			name:     "partial template syntax - opening only",
			mount:    mount.Mount{Source: "{{"},
			expected: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := HasTemplate(tc.mount)
			if got != tc.expected {
				t.Errorf("HasTemplate() = %v, want %v", got, tc.expected)
			}
		})
	}
}

func TestTypeBase(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"", ""},
		{"bind", "bind"},
		{"overlay", "overlay"},
		{"format/ext4", "format"},
		{"mkfs/ext4", "mkfs"},
		{"format/mkdir/overlay", "format"},
	}

	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			got := TypeBase(tc.input)
			if got != tc.expected {
				t.Errorf("TypeBase(%q) = %q, want %q", tc.input, got, tc.expected)
			}
		})
	}
}

func TestTypeSuffix(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"", ""},
		{"bind", "bind"},
		{"overlay", "overlay"},
		{"format/ext4", "ext4"},
		{"mkfs/ext4", "ext4"},
		{"format/mkdir/overlay", "overlay"},
	}

	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			got := TypeSuffix(tc.input)
			if got != tc.expected {
				t.Errorf("TypeSuffix(%q) = %q, want %q", tc.input, got, tc.expected)
			}
		})
	}
}

func TestUniqueRef(t *testing.T) {
	// Generate multiple refs and ensure they're unique
	refs := make(map[string]bool)
	for range 100 {
		ref := UniqueRef()
		if refs[ref] {
			t.Errorf("UniqueRef() generated duplicate: %s", ref)
		}
		refs[ref] = true

		// Check format: should contain timestamp and base64
		if !strings.Contains(ref, "-") {
			t.Errorf("UniqueRef() should contain separator: %s", ref)
		}
	}
}

func TestHasErofsMultiDevice(t *testing.T) {
	tests := []struct {
		name   string
		mounts []mount.Mount
		want   bool
	}{
		{
			name:   "empty mounts",
			mounts: nil,
			want:   false,
		},
		{
			name: "single bind mount",
			mounts: []mount.Mount{
				{Type: "bind", Source: "/path"},
			},
			want: false,
		},
		{
			name: "erofs without device option",
			mounts: []mount.Mount{
				{Type: "erofs", Source: "/path/layer.erofs", Options: []string{"ro", "loop"}},
			},
			want: false,
		},
		{
			name: "erofs with device option",
			mounts: []mount.Mount{
				{Type: "erofs", Source: "/path/fsmeta.erofs", Options: []string{"ro", "loop", "device=/path/blob.erofs"}},
			},
			want: true,
		},
		{
			name: "format/erofs with device option",
			mounts: []mount.Mount{
				{Type: "format/erofs", Source: "/path/fsmeta.erofs", Options: []string{"ro", "device=/path/blob1.erofs", "device=/path/blob2.erofs"}},
			},
			want: true,
		},
		{
			name: "multiple mounts with erofs multi-device",
			mounts: []mount.Mount{
				{Type: "erofs", Source: "/path/fsmeta.erofs", Options: []string{"ro", "device=/path/blob.erofs"}},
				{Type: "ext4", Source: "/path/rwlayer.img", Options: []string{"rw"}},
			},
			want: true,
		},
		{
			name: "overlay mount (not erofs)",
			mounts: []mount.Mount{
				{Type: "overlay", Source: "overlay", Options: []string{"lowerdir=/lower", "upperdir=/upper"}},
			},
			want: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := HasErofsMultiDevice(tc.mounts)
			if got != tc.want {
				t.Errorf("HasErofsMultiDevice() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestHasActiveSnapshotMounts(t *testing.T) {
	tests := []struct {
		name   string
		mounts []mount.Mount
		want   bool
	}{
		{
			name:   "empty mounts",
			mounts: nil,
			want:   false,
		},
		{
			name: "erofs only",
			mounts: []mount.Mount{
				{Type: "erofs", Source: "/path/layer.erofs"},
			},
			want: false,
		},
		{
			name: "ext4 only",
			mounts: []mount.Mount{
				{Type: "ext4", Source: "/path/rwlayer.img"},
			},
			want: false,
		},
		{
			name: "erofs and ext4 (active snapshot)",
			mounts: []mount.Mount{
				{Type: "erofs", Source: "/path/layer.erofs", Options: []string{"ro", "loop"}},
				{Type: "ext4", Source: "/path/rwlayer.img", Options: []string{"rw", "loop"}},
			},
			want: true,
		},
		{
			name: "format/erofs and ext4 (active snapshot with fsmeta)",
			mounts: []mount.Mount{
				{Type: "format/erofs", Source: "/path/fsmeta.erofs", Options: []string{"ro", "device=/path/blob.erofs"}},
				{Type: "ext4", Source: "/path/rwlayer.img", Options: []string{"rw"}},
			},
			want: true,
		},
		{
			name: "multiple erofs and ext4",
			mounts: []mount.Mount{
				{Type: "erofs", Source: "/path/layer1.erofs"},
				{Type: "erofs", Source: "/path/layer2.erofs"},
				{Type: "ext4", Source: "/path/rwlayer.img"},
			},
			want: true,
		},
		{
			name: "bind and overlay (not active snapshot)",
			mounts: []mount.Mount{
				{Type: "bind", Source: "/path"},
				{Type: "overlay", Source: "overlay"},
			},
			want: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := HasActiveSnapshotMounts(tc.mounts)
			if got != tc.want {
				t.Errorf("HasActiveSnapshotMounts() = %v, want %v", got, tc.want)
			}
		})
	}
}
