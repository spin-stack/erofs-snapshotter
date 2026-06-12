package differ

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"
	"testing/iotest"

	"github.com/containerd/containerd/v2/core/content"
	"github.com/containerd/containerd/v2/core/images"
	"github.com/containerd/containerd/v2/core/mount"
	"github.com/containerd/containerd/v2/plugins/content/local"
	digest "github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"github.com/spin-stack/erofs-snapshotter/internal/erofs"
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
		// Valid EROFS media types (legacy ".erofs" suffix)
		{"application/vnd.oci.image.layer.erofs", true},
		{"application/vnd.erofs", true},
		{"some/type.erofs", true},

		// Valid - official containerd EROFS media type (no ".erofs" suffix)
		{images.MediaTypeErofsLayer, true},                 // application/vnd.erofs.layer.v1
		{"application/vnd.erofs.layer.v1", true},           // literal form
		{"application/vnd.erofs.layer.experimental", true}, // any vendor variant under the prefix

		// Invalid - has suffix (reserved for images.DiffCompression)
		{"application/vnd.oci.image.layer.erofs+gzip", false},
		{"application/vnd.erofs+zstd", false},
		{"application/vnd.erofs.layer.v1+zstd", false},

		// Invalid - not EROFS
		{"application/vnd.oci.image.layer.v1.tar", false},
		{"application/vnd.oci.image.layer.v1.tar+gzip", false},
		{"application/vnd.oci.image.layer.v1.tar+zstd", false},
		{"application/octet-stream", false},

		// Edge cases
		{"", false},
		{".erofs", true}, // technically valid per the code
		{"erofs", false}, // doesn't end with .erofs and missing vnd prefix
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

func TestDifferStoreAccess(t *testing.T) {
	// The differ must retain the content store it is constructed with
	// (the nil case is covered by TestNewErofsDiffer).
	store, err := local.NewStore(t.TempDir())
	if err != nil {
		t.Fatalf("failed to create content store: %v", err)
	}

	d := NewErofsDiffer(store)
	if d.store != store {
		t.Error("differ should retain the store passed to NewErofsDiffer")
	}
}

// requireMkfsErofs skips the test when mkfs.erofs is not installed.
func requireMkfsErofs(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("mkfs.erofs"); err != nil {
		t.Skip("mkfs.erofs not installed")
	}
}

// requireTarConversion skips the test when mkfs.erofs lacks tar mode (--tar).
func requireTarConversion(t *testing.T) {
	t.Helper()
	requireMkfsErofs(t)
	ok, err := erofs.SupportGenerateFromTar()
	if err != nil || !ok {
		t.Skip("mkfs.erofs does not support tar conversion mode")
	}
}

// newLocalStore creates a plain local content store for Apply tests.
func newLocalStore(t *testing.T) content.Store {
	t.Helper()
	store, err := local.NewStore(t.TempDir())
	if err != nil {
		t.Fatalf("failed to create content store: %v", err)
	}
	return store
}

// makeTar returns an uncompressed tar archive containing the given files,
// written in sorted name order for a deterministic digest.
func makeTar(t *testing.T, files map[string]string) []byte {
	t.Helper()
	names := make([]string, 0, len(files))
	for name := range files {
		names = append(names, name)
	}
	sort.Strings(names)

	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	for _, name := range names {
		contents := files[name]
		hdr := &tar.Header{
			Name: name,
			Mode: 0o644,
			Size: int64(len(contents)),
		}
		if err := tw.WriteHeader(hdr); err != nil {
			t.Fatalf("failed to write tar header for %s: %v", name, err)
		}
		if _, err := tw.Write([]byte(contents)); err != nil {
			t.Fatalf("failed to write tar contents for %s: %v", name, err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("failed to close tar writer: %v", err)
	}
	return buf.Bytes()
}

// gzipBytes compresses data with gzip.
func gzipBytes(t *testing.T, data []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	if _, err := gz.Write(data); err != nil {
		t.Fatalf("failed to gzip data: %v", err)
	}
	if err := gz.Close(); err != nil {
		t.Fatalf("failed to close gzip writer: %v", err)
	}
	return buf.Bytes()
}

// ingestBlob writes data into the content store and returns its descriptor.
func ingestBlob(ctx context.Context, t *testing.T, store content.Store, mediaType string, data []byte) ocispec.Descriptor {
	t.Helper()
	desc := ocispec.Descriptor{
		MediaType: mediaType,
		Digest:    digest.FromBytes(data),
		Size:      int64(len(data)),
	}
	if err := content.WriteBlob(ctx, store, "ingest-"+desc.Digest.String(), bytes.NewReader(data), desc); err != nil {
		t.Fatalf("failed to write blob to content store: %v", err)
	}
	return desc
}

// newApplyTarget creates a snapshot-like layer directory (with the
// .erofslayer marker) and the extract-style bind mounts the snapshotter hands
// to the differ: a bind mount of the snapshot's fs/ directory, whose parent
// must contain the marker for MountsToLayer validation.
func newApplyTarget(t *testing.T) (string, []mount.Mount) {
	t.Helper()
	layerDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(layerDir, erofs.ErofsLayerMarker), nil, 0o644); err != nil {
		t.Fatalf("failed to write layer marker: %v", err)
	}
	fsDir := filepath.Join(layerDir, "fs")
	if err := os.MkdirAll(fsDir, 0o755); err != nil {
		t.Fatalf("failed to create fs dir: %v", err)
	}
	mounts := []mount.Mount{{Type: "bind", Source: fsDir, Options: []string{"rw", "rbind"}}}
	return layerDir, mounts
}

func TestApplyConvertsTarLayer(t *testing.T) {
	requireTarConversion(t)
	ctx := context.Background()

	tarData := makeTar(t, map[string]string{
		"hello.txt":      "hello world",
		"dir/nested.txt": "nested contents",
	})
	tarDigest := digest.FromBytes(tarData)

	tests := []struct {
		name      string
		mediaType string
		blob      []byte
	}{
		{"uncompressed tar", ocispec.MediaTypeImageLayer, tarData},
		{"gzip compressed tar", ocispec.MediaTypeImageLayerGzip, gzipBytes(t, tarData)},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := newLocalStore(t)
			d := NewErofsDiffer(store)
			desc := ingestBlob(ctx, t, store, tc.mediaType, tc.blob)
			layerDir, mounts := newApplyTarget(t)

			got, err := d.Apply(ctx, desc, mounts)
			if err != nil {
				t.Fatalf("Apply failed: %v", err)
			}

			// Apply must return the descriptor of the UNCOMPRESSED tar
			// stream regardless of the input compression.
			if got.MediaType != ocispec.MediaTypeImageLayer {
				t.Errorf("media type = %q, want %q", got.MediaType, ocispec.MediaTypeImageLayer)
			}
			if got.Digest != tarDigest {
				t.Errorf("digest = %s, want uncompressed tar digest %s", got.Digest, tarDigest)
			}
			if got.Size != int64(len(tarData)) {
				t.Errorf("size = %d, want %d", got.Size, len(tarData))
			}

			// The EROFS blob must exist under the digest-based name and be a
			// valid EROFS image with fsmeta-compatible block size.
			blobPath := filepath.Join(layerDir, erofs.LayerBlobFilename(desc.Digest.String()))
			blockSize, err := erofs.GetBlockSize(blobPath)
			if err != nil {
				t.Fatalf("layer blob is not a valid EROFS image: %v", err)
			}
			if blockSize < 4096 {
				t.Errorf("block size = %d, want >= 4096 (fsmeta merge compatibility)", blockSize)
			}

			// No partially-written temp file may remain.
			if _, err := os.Stat(blobPath + ".tmp"); !os.IsNotExist(err) {
				t.Errorf("temporary blob file still present: %v", err)
			}
		})
	}
}

func TestApplyNativeErofsLayer(t *testing.T) {
	requireMkfsErofs(t)
	ctx := context.Background()

	// Build a small native EROFS blob from a directory.
	srcDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(srcDir, "native.txt"), []byte("native erofs contents"), 0o644); err != nil {
		t.Fatalf("failed to write source file: %v", err)
	}
	erofsPath := filepath.Join(t.TempDir(), "native.erofs")
	if err := erofs.ConvertErofs(ctx, erofsPath, srcDir, nil); err != nil {
		t.Fatalf("mkfs.erofs failed: %v", err)
	}
	blob, err := os.ReadFile(erofsPath)
	if err != nil {
		t.Fatalf("failed to read EROFS blob: %v", err)
	}

	t.Run("copies blob and returns descriptor unchanged", func(t *testing.T) {
		store := newLocalStore(t)
		d := NewErofsDiffer(store)
		desc := ingestBlob(ctx, t, store, images.MediaTypeErofsLayer, blob)
		layerDir, mounts := newApplyTarget(t)

		got, err := d.Apply(ctx, desc, mounts)
		if err != nil {
			t.Fatalf("Apply failed: %v", err)
		}

		// Native EROFS layers pass through: the descriptor is unchanged.
		if got.MediaType != desc.MediaType || got.Digest != desc.Digest || got.Size != desc.Size {
			t.Errorf("descriptor changed: got %+v, want %+v", got, desc)
		}

		blobPath := filepath.Join(layerDir, erofs.LayerBlobFilename(desc.Digest.String()))
		copied, err := os.ReadFile(blobPath)
		if err != nil {
			t.Fatalf("failed to read copied blob: %v", err)
		}
		if !bytes.Equal(copied, blob) {
			t.Error("copied blob differs from original EROFS blob")
		}
		if _, err := erofs.GetBlockSize(blobPath); err != nil {
			t.Errorf("copied blob is not a valid EROFS image: %v", err)
		}
		if _, err := os.Stat(blobPath + ".tmp"); !os.IsNotExist(err) {
			t.Errorf("temporary blob file still present: %v", err)
		}
	})

	t.Run("rejects native media type with compression suffix", func(t *testing.T) {
		store := newLocalStore(t)
		d := NewErofsDiffer(store)
		desc := ingestBlob(ctx, t, store, images.MediaTypeErofsLayer+"+zstd", blob)
		_, mounts := newApplyTarget(t)

		_, err := d.Apply(ctx, desc, mounts)
		if err == nil || !strings.Contains(err.Error(), "unsupported media type") {
			t.Fatalf("expected unsupported media type error, got: %v", err)
		}
	})
}

func TestCopyBlobAtomic(t *testing.T) {
	t.Run("writes file atomically", func(t *testing.T) {
		dst := filepath.Join(t.TempDir(), "blob.erofs")
		data := []byte("blob contents")

		if err := copyBlobAtomic(dst, bytes.NewReader(data)); err != nil {
			t.Fatalf("copyBlobAtomic failed: %v", err)
		}

		got, err := os.ReadFile(dst)
		if err != nil {
			t.Fatalf("failed to read destination: %v", err)
		}
		if !bytes.Equal(got, data) {
			t.Errorf("content = %q, want %q", got, data)
		}
		if _, err := os.Stat(dst + ".tmp"); !os.IsNotExist(err) {
			t.Errorf("temporary file still present: %v", err)
		}
	})

	t.Run("read error removes temp file and final name never appears", func(t *testing.T) {
		dst := filepath.Join(t.TempDir(), "blob.erofs")
		readErr := errors.New("read failed")

		err := copyBlobAtomic(dst, iotest.ErrReader(readErr))
		if !errors.Is(err, readErr) {
			t.Fatalf("expected read error, got: %v", err)
		}
		if _, err := os.Stat(dst); !os.IsNotExist(err) {
			t.Errorf("destination file must not exist after failure: %v", err)
		}
		if _, err := os.Stat(dst + ".tmp"); !os.IsNotExist(err) {
			t.Errorf("temporary file must be removed after failure: %v", err)
		}
	})
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
