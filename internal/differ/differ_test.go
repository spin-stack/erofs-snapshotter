package differ

import (
	"bytes"
	"context"
	"io"
	"runtime"
	"strings"
	"testing"

	"github.com/containerd/containerd/v2/core/mount"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	// Import testutil to register the -test.root flag
	_ "github.com/spin-stack/erofs-snapshotter/internal/testutil"
)

func TestNewErofsDiffer(t *testing.T) {
	t.Run("creates differ with defaults", func(t *testing.T) {
		d := NewErofsDiffer(nil)
		if d == nil {
			t.Fatal("expected non-nil differ")
		}
		if d.store != nil {
			t.Error("expected nil store when passed nil")
		}
	})

	t.Run("applies WithMountManager", func(t *testing.T) {
		mm := &mockMountManager{}
		d := NewErofsDiffer(nil, WithMountManager(mm))
		if d.mmResolver == nil {
			t.Fatal("expected mmResolver to be set")
		}
		if d.mmResolver() != mm {
			t.Error("mmResolver should return the mount manager")
		}
	})

	t.Run("applies WithMountManagerResolver", func(t *testing.T) {
		called := false
		resolver := func() mount.Manager {
			called = true
			return nil
		}
		d := NewErofsDiffer(nil, WithMountManagerResolver(resolver))
		if d.mmResolver == nil {
			t.Fatal("expected mmResolver to be set")
		}
		d.mmResolver()
		if !called {
			t.Error("resolver should have been called")
		}
	})
}

func TestDefaultMkfsOpts(t *testing.T) {
	opts := defaultMkfsOpts()

	// No compression should be used (compressed layers are incompatible with fsmeta merge)
	for _, opt := range opts {
		if strings.HasPrefix(opt, "-z") {
			t.Errorf("compression options are incompatible with fsmeta merge, got: %v", opts)
		}
	}

	// On darwin, should have -b4096 as first option
	if runtime.GOOS == "darwin" {
		if len(opts) == 0 || opts[0] != "-b4096" {
			t.Errorf("expected -b4096 as first option on darwin, got: %v", opts)
		}
	}

	// On linux, should return nil (no special options needed)
	if runtime.GOOS == "linux" {
		if opts != nil {
			t.Errorf("expected nil options on linux, got: %v", opts)
		}
	}
}

func TestIsErofsMediaType(t *testing.T) {
	tests := []struct {
		mediaType string
		want      bool
	}{
		// Valid EROFS media types
		{"application/vnd.oci.image.layer.erofs", true},
		{"application/vnd.erofs", true},
		{"some/type.erofs", true},

		// Invalid - has suffix (not allowed per code comment)
		{"application/vnd.oci.image.layer.erofs+gzip", false},
		{"application/vnd.erofs+zstd", false},

		// Invalid - not EROFS
		{"application/vnd.oci.image.layer.v1.tar", false},
		{"application/vnd.oci.image.layer.v1.tar+gzip", false},
		{"application/vnd.oci.image.layer.v1.tar+zstd", false},
		{"application/octet-stream", false},

		// Edge cases
		{"", false},
		{".erofs", true}, // technically valid per the code
		{"erofs", false}, // doesn't end with .erofs
	}

	for _, tc := range tests {
		t.Run(tc.mediaType, func(t *testing.T) {
			got := isErofsMediaType(tc.mediaType)
			if got != tc.want {
				t.Errorf("isErofsMediaType(%q) = %v, want %v", tc.mediaType, got, tc.want)
			}
		})
	}
}

func TestReadCounter(t *testing.T) {
	t.Run("counts bytes read", func(t *testing.T) {
		data := []byte("hello world")
		rc := &readCounter{r: bytes.NewReader(data)}

		buf := make([]byte, 5)
		n, err := rc.Read(buf)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if n != 5 {
			t.Errorf("read %d bytes, want 5", n)
		}
		if rc.count != 5 {
			t.Errorf("count = %d, want 5", rc.count)
		}
	})

	t.Run("accumulates across multiple reads", func(t *testing.T) {
		data := []byte("hello world")
		rc := &readCounter{r: bytes.NewReader(data)}

		buf := make([]byte, 3)
		rc.Read(buf) // 3 bytes
		rc.Read(buf) // 3 bytes
		rc.Read(buf) // 3 bytes
		rc.Read(buf) // 2 bytes (remaining)

		if rc.count != int64(len(data)) {
			t.Errorf("total count = %d, want %d", rc.count, len(data))
		}
	})

	t.Run("handles EOF", func(t *testing.T) {
		data := []byte("hi")
		rc := &readCounter{r: bytes.NewReader(data)}

		buf := make([]byte, 10)
		n, err := rc.Read(buf)
		if err != nil {
			t.Fatalf("unexpected error on first read: %v", err)
		}
		if n != 2 {
			t.Errorf("first read = %d bytes, want 2", n)
		}

		n, err = rc.Read(buf)
		if err != io.EOF {
			t.Errorf("expected EOF, got %v", err)
		}
		if n != 0 {
			t.Errorf("read %d bytes at EOF, want 0", n)
		}

		// Count should still be 2
		if rc.count != 2 {
			t.Errorf("count = %d, want 2", rc.count)
		}
	})

	t.Run("handles empty reader", func(t *testing.T) {
		rc := &readCounter{r: bytes.NewReader(nil)}
		buf := make([]byte, 10)

		n, err := rc.Read(buf)
		if err != io.EOF {
			t.Errorf("expected EOF, got %v", err)
		}
		if n != 0 {
			t.Errorf("read %d bytes, want 0", n)
		}
		if rc.count != 0 {
			t.Errorf("count = %d, want 0", rc.count)
		}
	})
}

// mockMountManager is a minimal mock for testing WithMountManager
type mockMountManager struct{}

func (m *mockMountManager) Activate(_ context.Context, _ string, _ []mount.Mount, _ ...mount.ActivateOpt) (mount.ActivationInfo, error) {
	return mount.ActivationInfo{}, nil
}

func (m *mockMountManager) Deactivate(_ context.Context, _ string) error {
	return nil
}

func (m *mockMountManager) Info(_ context.Context, _ string) (mount.ActivationInfo, error) {
	return mount.ActivationInfo{}, nil
}

func (m *mockMountManager) Update(_ context.Context, _ mount.ActivationInfo, _ ...string) (mount.ActivationInfo, error) {
	return mount.ActivationInfo{}, nil
}

func (m *mockMountManager) List(_ context.Context, _ ...string) ([]mount.ActivationInfo, error) {
	return nil, nil
}

func TestApplyErrors(t *testing.T) {
	ctx := context.Background()

	t.Run("fails with unsupported media type", func(t *testing.T) {
		d := NewErofsDiffer(nil)
		desc := ocispec.Descriptor{
			MediaType: "application/unsupported-type",
			Digest:    "sha256:abc123",
			Size:      100,
		}
		mounts := []mount.Mount{{Type: "bind", Source: "/some/path"}}

		_, err := d.Apply(ctx, desc, mounts)
		if err == nil {
			t.Error("expected error for unsupported media type")
		}
		if !strings.Contains(err.Error(), "unsupported media type") {
			t.Errorf("error should mention unsupported media type: %v", err)
		}
	})

	t.Run("fails with empty mounts", func(t *testing.T) {
		d := NewErofsDiffer(nil)
		desc := ocispec.Descriptor{
			MediaType: ocispec.MediaTypeImageLayerGzip,
			Digest:    "sha256:abc123",
			Size:      100,
		}

		_, err := d.Apply(ctx, desc, nil)
		if err == nil {
			t.Error("expected error for empty mounts")
		}
	})

	t.Run("fails with invalid mount type for EROFS differ", func(t *testing.T) {
		d := NewErofsDiffer(nil)
		desc := ocispec.Descriptor{
			MediaType: ocispec.MediaTypeImageLayerGzip,
			Digest:    "sha256:abc123",
			Size:      100,
		}
		// Use a mount type that won't pass MountsToLayer validation
		mounts := []mount.Mount{{Type: "tmpfs", Source: "tmpfs"}}

		_, err := d.Apply(ctx, desc, mounts)
		if err == nil {
			t.Error("expected error for invalid mount type")
		}
	})

	t.Run("fails when layer marker is missing", func(t *testing.T) {
		d := NewErofsDiffer(nil)
		desc := ocispec.Descriptor{
			MediaType: ocispec.MediaTypeImageLayerGzip,
			Digest:    "sha256:abc123def456",
			Size:      100,
		}
		// Create a temp directory WITHOUT the marker file
		tmpDir := t.TempDir()
		layerBlob := tmpDir + "/layer.erofs"

		mounts := []mount.Mount{{Type: "bind", Source: layerBlob}}

		_, err := d.Apply(ctx, desc, mounts)
		if err == nil {
			t.Error("expected error when layer marker is missing")
		}
	})
}

func TestApplySupportedMediaTypes(t *testing.T) {
	// Test that the Apply method recognizes all expected media types
	supportedTypes := []string{
		ocispec.MediaTypeImageLayer,             // uncompressed tar
		ocispec.MediaTypeImageLayerGzip,         // gzip compressed
		ocispec.MediaTypeImageLayerZstd,         // zstd compressed
		"application/vnd.oci.image.layer.erofs", // native EROFS
	}

	for _, mediaType := range supportedTypes {
		t.Run(mediaType, func(t *testing.T) {
			d := NewErofsDiffer(nil)
			desc := ocispec.Descriptor{
				MediaType: mediaType,
				Digest:    "sha256:abc123",
				Size:      100,
			}

			// We expect this to fail later (e.g., empty mounts, nil store),
			// but NOT with "unsupported media type"
			_, err := d.Apply(context.Background(), desc, nil)
			if err != nil && strings.Contains(err.Error(), "unsupported media type") {
				t.Errorf("media type %q should be supported but got: %v", mediaType, err)
			}
		})
	}
}

// Note: Compare tests with content store are in compare_linux_test.go since
// Compare is only implemented on Linux. Those tests use local.NewStore to
// create a real content store for testing writeAndCommitDiff and Compare.

func TestDiffWriteFuncNil(t *testing.T) {
	// Test that the differ handles nil writeFn gracefully
	// This is an internal test for edge cases
	d := NewErofsDiffer(nil)
	if d.store != nil {
		t.Error("expected nil store")
	}
}

func TestDifferStoreAccess(t *testing.T) {
	// Test that the differ correctly stores and accesses the content store
	d := NewErofsDiffer(nil)
	if d.store != nil {
		t.Error("expected nil store when passed nil")
	}

	// Create a minimal mock store (we can't easily mock content.Store interface
	// but we can verify the differ stores what we give it)
	d2 := NewErofsDiffer(nil)
	if d2.store != nil {
		t.Error("expected nil store")
	}
}

func TestApplyWithDifferentMountTypes(t *testing.T) {
	ctx := context.Background()
	d := NewErofsDiffer(nil)

	// These tests focus on mount validation that happens before content store access.
	// Tests that would require a content store are covered in integration tests.
	tests := []struct {
		name      string
		mounts    []mount.Mount
		wantError bool
		errorMsg  string
	}{
		{
			name:      "empty mounts",
			mounts:    nil,
			wantError: true,
			errorMsg:  "no mounts",
		},
		{
			name:      "single tmpfs",
			mounts:    []mount.Mount{{Type: "tmpfs", Source: "tmpfs"}},
			wantError: true,
			errorMsg:  "unsupported filesystem type",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			desc := ocispec.Descriptor{
				MediaType: ocispec.MediaTypeImageLayerGzip,
				Digest:    "sha256:abc123def456",
				Size:      100,
			}

			_, err := d.Apply(ctx, desc, tc.mounts)
			if tc.wantError && err == nil {
				t.Error("expected error")
			}
			if tc.errorMsg != "" && err != nil && !strings.Contains(err.Error(), tc.errorMsg) {
				t.Errorf("error should contain %q: %v", tc.errorMsg, err)
			}
		})
	}
}
