//go:build linux

package differ

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/containerd/containerd/v2/core/content"
	"github.com/containerd/containerd/v2/core/diff"
	"github.com/containerd/containerd/v2/core/mount"
	"github.com/containerd/containerd/v2/plugins/content/local"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	// Import testutil to register the -test.root flag
	_ "github.com/spin-stack/erofs-snapshotter/internal/testutil"
)

// newTestContentStore creates a content store for testing with label support.
func newTestContentStore(t *testing.T) content.Store {
	t.Helper()
	store, err := local.NewLabeledStore(t.TempDir(), &memoryLabelStore{
		labels: make(map[digest.Digest]map[string]string),
	})
	if err != nil {
		t.Fatalf("failed to create content store: %v", err)
	}
	return store
}

// memoryLabelStore is an in-memory label store for testing.
type memoryLabelStore struct {
	mu     sync.Mutex
	labels map[digest.Digest]map[string]string
}

func (m *memoryLabelStore) Get(d digest.Digest) (map[string]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.labels[d], nil
}

func (m *memoryLabelStore) Set(d digest.Digest, labels map[string]string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.labels[d] = labels
	return nil
}

func (m *memoryLabelStore) Update(d digest.Digest, update map[string]string) (map[string]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	labels, ok := m.labels[d]
	if !ok {
		labels = make(map[string]string)
	}
	for k, v := range update {
		if v == "" {
			delete(labels, k)
		} else {
			labels[k] = v
		}
	}
	m.labels[d] = labels
	return labels, nil
}

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

// TestCompareWithContentStore tests the Compare function with a real content store.
func TestCompareWithContentStore(t *testing.T) {
	ctx := context.Background()

	t.Run("fails with unsupported media type", func(t *testing.T) {
		store := newTestContentStore(t)
		d := NewErofsDiffer(store)

		// Use unsupported media type
		_, err := d.Compare(ctx, nil, nil, diff.WithMediaType("unsupported/type"))
		if err == nil {
			t.Error("expected error for unsupported media type")
		}
	})

	t.Run("succeeds with bind mounts", func(t *testing.T) {
		// Skip if not running as root (bind mounts require privileges)
		if os.Getuid() != 0 {
			t.Skip("skipping test that requires root privileges")
		}

		store := newTestContentStore(t)
		d := NewErofsDiffer(store)

		// Create lower directory with some content
		lowerDir := t.TempDir()
		if err := os.WriteFile(filepath.Join(lowerDir, "file.txt"), []byte("lower content"), 0644); err != nil {
			t.Fatalf("failed to write lower file: %v", err)
		}

		// Create upper directory with modified content
		upperDir := t.TempDir()
		if err := os.WriteFile(filepath.Join(upperDir, "file.txt"), []byte("upper content"), 0644); err != nil {
			t.Fatalf("failed to write upper file: %v", err)
		}
		if err := os.WriteFile(filepath.Join(upperDir, "newfile.txt"), []byte("new content"), 0644); err != nil {
			t.Fatalf("failed to write new file: %v", err)
		}

		lower := []mount.Mount{{Type: "bind", Source: lowerDir, Options: []string{"rbind", "ro"}}}
		upper := []mount.Mount{{Type: "bind", Source: upperDir, Options: []string{"rbind", "ro"}}}

		desc, err := d.Compare(ctx, lower, upper)
		if err != nil {
			t.Fatalf("Compare failed: %v", err)
		}

		// Verify descriptor
		if desc.MediaType != ocispec.MediaTypeImageLayerGzip {
			t.Errorf("unexpected media type: %s", desc.MediaType)
		}
		if desc.Size == 0 {
			t.Error("expected non-zero size")
		}
		if desc.Digest == "" {
			t.Error("expected non-empty digest")
		}

		// Verify content was stored
		info, err := store.Info(ctx, desc.Digest)
		if err != nil {
			t.Fatalf("failed to get content info: %v", err)
		}
		if info.Size != desc.Size {
			t.Errorf("size mismatch: info=%d, desc=%d", info.Size, desc.Size)
		}
	})

	t.Run("succeeds with identical directories", func(t *testing.T) {
		// Skip if not running as root (bind mounts require privileges)
		if os.Getuid() != 0 {
			t.Skip("skipping test that requires root privileges")
		}

		store := newTestContentStore(t)
		d := NewErofsDiffer(store)

		// Create identical directories
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, "file.txt"), []byte("content"), 0644); err != nil {
			t.Fatalf("failed to write file: %v", err)
		}

		mounts := []mount.Mount{{Type: "bind", Source: dir, Options: []string{"rbind", "ro"}}}

		desc, err := d.Compare(ctx, mounts, mounts)
		if err != nil {
			t.Fatalf("Compare failed: %v", err)
		}

		// For identical directories, we should get an empty diff (minimal size)
		if desc.Digest == "" {
			t.Error("expected non-empty digest")
		}
	})
}

// TestWriteAndCommitDiff tests the internal writeAndCommitDiff function.
func TestWriteAndCommitDiff(t *testing.T) {
	ctx := context.Background()

	t.Run("writes uncompressed content", func(t *testing.T) {
		store := newTestContentStore(t)
		d := NewErofsDiffer(store)
		testData := []byte("test content for uncompressed diff")

		writeFn := func(ctx context.Context, w io.Writer) error {
			_, err := w.Write(testData)
			return err
		}

		config := diff.Config{
			MediaType: ocispec.MediaTypeImageLayer,
		}

		desc, err := d.writeAndCommitDiff(ctx, config, writeFn)
		if err != nil {
			t.Fatalf("writeAndCommitDiff failed: %v", err)
		}

		if desc.MediaType != ocispec.MediaTypeImageLayer {
			t.Errorf("unexpected media type: %s", desc.MediaType)
		}
		if desc.Size != int64(len(testData)) {
			t.Errorf("unexpected size: got %d, want %d", desc.Size, len(testData))
		}

		// Verify we can read the content back
		ra, err := store.ReaderAt(ctx, ocispec.Descriptor{Digest: desc.Digest})
		if err != nil {
			t.Fatalf("failed to get reader: %v", err)
		}
		defer ra.Close()

		readBack := make([]byte, desc.Size)
		if _, err := ra.ReadAt(readBack, 0); err != nil {
			t.Fatalf("failed to read content: %v", err)
		}
		if !bytes.Equal(readBack, testData) {
			t.Error("content mismatch")
		}
	})

	t.Run("writes gzip compressed content", func(t *testing.T) {
		store := newTestContentStore(t)
		d := NewErofsDiffer(store)
		testData := []byte("test content for gzip compressed diff - make it long enough to compress")

		writeFn := func(ctx context.Context, w io.Writer) error {
			_, err := w.Write(testData)
			return err
		}

		config := diff.Config{
			MediaType: ocispec.MediaTypeImageLayerGzip,
		}

		desc, err := d.writeAndCommitDiff(ctx, config, writeFn)
		if err != nil {
			t.Fatalf("writeAndCommitDiff failed: %v", err)
		}

		if desc.MediaType != ocispec.MediaTypeImageLayerGzip {
			t.Errorf("unexpected media type: %s", desc.MediaType)
		}

		// Compressed size should be non-zero
		if desc.Size == 0 {
			t.Error("expected non-zero compressed size")
		}
	})

	t.Run("writes zstd compressed content", func(t *testing.T) {
		store := newTestContentStore(t)
		d := NewErofsDiffer(store)
		testData := []byte("test content for zstd compressed diff - make it long enough to compress well")

		writeFn := func(ctx context.Context, w io.Writer) error {
			_, err := w.Write(testData)
			return err
		}

		config := diff.Config{
			MediaType: ocispec.MediaTypeImageLayerZstd,
		}

		desc, err := d.writeAndCommitDiff(ctx, config, writeFn)
		if err != nil {
			t.Fatalf("writeAndCommitDiff failed: %v", err)
		}

		if desc.MediaType != ocispec.MediaTypeImageLayerZstd {
			t.Errorf("unexpected media type: %s", desc.MediaType)
		}

		if desc.Size == 0 {
			t.Error("expected non-zero compressed size")
		}
	})

	t.Run("fails with write error", func(t *testing.T) {
		store := newTestContentStore(t)
		d := NewErofsDiffer(store)

		writeFn := func(ctx context.Context, w io.Writer) error {
			return io.ErrUnexpectedEOF
		}

		config := diff.Config{
			MediaType: ocispec.MediaTypeImageLayer,
		}

		_, err := d.writeAndCommitDiff(ctx, config, writeFn)
		if err == nil {
			t.Error("expected error from writeFn")
		}
	})

	t.Run("handles duplicate content", func(t *testing.T) {
		store := newTestContentStore(t)
		d := NewErofsDiffer(store)
		testData := []byte("duplicate content test")

		writeFn := func(ctx context.Context, w io.Writer) error {
			_, err := w.Write(testData)
			return err
		}

		config := diff.Config{
			MediaType: ocispec.MediaTypeImageLayer,
		}

		// Write first time
		desc1, err := d.writeAndCommitDiff(ctx, config, writeFn)
		if err != nil {
			t.Fatalf("first writeAndCommitDiff failed: %v", err)
		}

		// Write same content again - should succeed (already exists is handled)
		desc2, err := d.writeAndCommitDiff(ctx, config, writeFn)
		if err != nil {
			t.Fatalf("second writeAndCommitDiff failed: %v", err)
		}

		// Should have same digest
		if desc1.Digest != desc2.Digest {
			t.Errorf("digest mismatch: %s != %s", desc1.Digest, desc2.Digest)
		}
	})
}
