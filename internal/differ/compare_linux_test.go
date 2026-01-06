//go:build linux

package differ

import (
	"testing"

	"github.com/containerd/containerd/v2/core/mount"

	// Import testutil to register the -test.root flag
	_ "github.com/aledbf/nexuserofs/internal/testutil"
)

func TestLowerOverlayOnly(t *testing.T) {
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
			name:   "single non-overlay mount",
			mounts: []mount.Mount{{Type: "bind", Source: "/path"}},
			want:   false,
		},
		{
			name:   "multiple mounts",
			mounts: []mount.Mount{{Type: "overlay"}, {Type: "bind"}},
			want:   false,
		},
		{
			name: "overlay with upperdir",
			mounts: []mount.Mount{{
				Type:    "overlay",
				Options: []string{"lowerdir=/lower", "upperdir=/upper", "workdir=/work"},
			}},
			want: false,
		},
		{
			name: "overlay with lowerdir only",
			mounts: []mount.Mount{{
				Type:    "overlay",
				Options: []string{"lowerdir=/lower1:/lower2"},
			}},
			want: true,
		},
		{
			name: "overlay without lowerdir",
			mounts: []mount.Mount{{
				Type:    "overlay",
				Options: []string{"upperdir=/upper", "workdir=/work"},
			}},
			want: false,
		},
		{
			name: "format/overlay with lowerdir only",
			mounts: []mount.Mount{{
				Type:    "format/overlay",
				Options: []string{"lowerdir=/lower"},
			}},
			want: true,
		},
		{
			name: "mkfs/overlay with lowerdir only",
			mounts: []mount.Mount{{
				Type:    "mkfs/overlay",
				Options: []string{"lowerdir=/lower"},
			}},
			want: true,
		},
		{
			name: "overlay with ro option and lowerdir only",
			mounts: []mount.Mount{{
				Type:    "overlay",
				Options: []string{"ro", "lowerdir=/lower1:/lower2"},
			}},
			want: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := lowerOverlayOnly(tc.mounts)
			if got != tc.want {
				t.Errorf("lowerOverlayOnly() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestMergedLowerFromActive(t *testing.T) {
	tests := []struct {
		name   string
		active []mount.ActiveMount
		want   string
		wantOK bool
	}{
		{
			name:   "empty active mounts",
			active: nil,
			want:   "",
			wantOK: false,
		},
		{
			name: "no erofs mounts",
			active: []mount.ActiveMount{
				{Mount: mount.Mount{Type: "bind", Source: "/path"}, MountPoint: "/mnt"},
				{Mount: mount.Mount{Type: "overlay", Source: "overlay"}, MountPoint: "/merged"},
			},
			want:   "",
			wantOK: false,
		},
		{
			name: "erofs without fsmeta suffix or device option",
			active: []mount.ActiveMount{
				{Mount: mount.Mount{Type: "erofs", Source: "/path/layer.erofs"}, MountPoint: "/mnt/layer"},
			},
			want:   "",
			wantOK: false,
		},
		{
			name: "erofs with fsmeta.erofs suffix",
			active: []mount.ActiveMount{
				{Mount: mount.Mount{Type: "bind", Source: "/path"}, MountPoint: "/mnt/bind"},
				{Mount: mount.Mount{Type: "erofs", Source: "/path/fsmeta.erofs"}, MountPoint: "/mnt/merged"},
			},
			want:   "/mnt/merged",
			wantOK: true,
		},
		{
			name: "erofs with device option",
			active: []mount.ActiveMount{
				{Mount: mount.Mount{Type: "erofs", Source: "/path/layer.erofs", Options: []string{"device=/path/blob.erofs"}}, MountPoint: "/mnt/layer"},
			},
			want:   "/mnt/layer",
			wantOK: true,
		},
		{
			name: "returns last matching erofs mount",
			active: []mount.ActiveMount{
				{Mount: mount.Mount{Type: "erofs", Source: "/path/first-fsmeta.erofs"}, MountPoint: "/mnt/first"},
				{Mount: mount.Mount{Type: "bind", Source: "/path"}, MountPoint: "/mnt/bind"},
				{Mount: mount.Mount{Type: "erofs", Source: "/path/second-fsmeta.erofs"}, MountPoint: "/mnt/second"},
			},
			want:   "/mnt/second",
			wantOK: true,
		},
		{
			name: "format/erofs with fsmeta suffix",
			active: []mount.ActiveMount{
				{Mount: mount.Mount{Type: "format/erofs", Source: "/path/fsmeta.erofs"}, MountPoint: "/mnt/merged"},
			},
			want:   "/mnt/merged",
			wantOK: true,
		},
		{
			name: "mkfs/erofs with device option",
			active: []mount.ActiveMount{
				{Mount: mount.Mount{Type: "mkfs/erofs", Source: "/path/layer.erofs", Options: []string{"ro", "device=/path/blob.erofs"}}, MountPoint: "/mnt/layer"},
			},
			want:   "/mnt/layer",
			wantOK: true,
		},
		{
			name: "mixed mounts returns correct erofs",
			active: []mount.ActiveMount{
				{Mount: mount.Mount{Type: "bind", Source: "/path1"}, MountPoint: "/mnt1"},
				{Mount: mount.Mount{Type: "erofs", Source: "/path/layer.erofs"}, MountPoint: "/mnt2"},
				{Mount: mount.Mount{Type: "overlay", Source: "overlay"}, MountPoint: "/mnt3"},
				{Mount: mount.Mount{Type: "erofs", Source: "/path/fsmeta.erofs"}, MountPoint: "/mnt4"},
				{Mount: mount.Mount{Type: "bind", Source: "/path5"}, MountPoint: "/mnt5"},
			},
			want:   "/mnt4",
			wantOK: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := mergedLowerFromActive(tc.active)
			if ok != tc.wantOK {
				t.Errorf("mergedLowerFromActive() ok = %v, want %v", ok, tc.wantOK)
			}
			if got != tc.want {
				t.Errorf("mergedLowerFromActive() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestMountManagerMethod(t *testing.T) {
	t.Run("returns nil when no resolver configured", func(t *testing.T) {
		d := NewErofsDiffer(nil)
		if d.mountManager() != nil {
			t.Error("expected nil mount manager")
		}
	})

	t.Run("returns mount manager from resolver", func(t *testing.T) {
		mm := &mockMountManager{}
		d := NewErofsDiffer(nil, WithMountManager(mm))
		if d.mountManager() != mm {
			t.Error("expected mount manager to be returned")
		}
	})

	t.Run("calls resolver lazily", func(t *testing.T) {
		callCount := 0
		mm := &mockMountManager{}
		resolver := func() mount.Manager {
			callCount++
			return mm
		}
		d := NewErofsDiffer(nil, WithMountManagerResolver(resolver))

		if callCount != 0 {
			t.Errorf("resolver called %d times during construction, want 0", callCount)
		}

		got := d.mountManager()
		if callCount != 1 {
			t.Errorf("resolver called %d times, want 1", callCount)
		}
		if got != mm {
			t.Error("expected mount manager to be returned")
		}

		// Call again to verify it's called each time
		d.mountManager()
		if callCount != 2 {
			t.Errorf("resolver called %d times, want 2", callCount)
		}
	})
}
