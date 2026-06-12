//go:build linux

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
	"strings"
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

	"github.com/containerd/containerd/v2/core/images"
	"github.com/containerd/containerd/v2/pkg/archive/compression"
	"github.com/spin-stack/erofs-snapshotter/internal/erofs"
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
		if err := os.WriteFile(filepath.Join(lowerDir, "file.txt"), []byte("lower content"), 0o644); err != nil {
			t.Fatalf("failed to write lower file: %v", err)
		}

		// Create upper directory with modified content
		upperDir := t.TempDir()
		if err := os.WriteFile(filepath.Join(upperDir, "file.txt"), []byte("upper content"), 0o644); err != nil {
			t.Fatalf("failed to write upper file: %v", err)
		}
		if err := os.WriteFile(filepath.Join(upperDir, "newfile.txt"), []byte("new content"), 0o644); err != nil {
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
		if err := os.WriteFile(filepath.Join(dir, "file.txt"), []byte("content"), 0o644); err != nil {
			t.Fatalf("failed to write file: %v", err)
		}

		mounts := []mount.Mount{{Type: "bind", Source: dir, Options: []string{"rbind", "ro"}}}

		desc, err := d.Compare(ctx, mounts, mounts)
		if err != nil {
			t.Fatalf("Compare failed: %v", err)
		}

		if desc.Digest == "" {
			t.Error("expected non-empty digest")
		}

		// The diff of identical directories must be an empty tar: any entry
		// here means Compare produced a full diff instead of an empty one.
		ra, err := store.ReaderAt(ctx, desc)
		if err != nil {
			t.Fatalf("failed to get reader: %v", err)
		}
		defer ra.Close()

		gz, err := gzip.NewReader(content.NewReader(ra))
		if err != nil {
			t.Fatalf("failed to create gzip reader: %v", err)
		}
		defer gz.Close()

		hdr, err := tar.NewReader(gz).Next()
		if err == nil {
			t.Fatalf("expected empty diff for identical directories, got entry %q", hdr.Name)
		}
		if err != io.EOF {
			t.Fatalf("failed to read diff tar: %v", err)
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

func TestStackedErofsLayers(t *testing.T) {
	erofsMount := func(src string, opts ...string) mount.Mount {
		return mount.Mount{Type: "erofs", Source: src, Options: append([]string{"ro", "loop"}, opts...)}
	}

	tests := []struct {
		name   string
		mounts []mount.Mount
		want   bool
	}{
		{
			name:   "empty mounts",
			mounts: nil,
		},
		{
			name:   "single erofs layer",
			mounts: []mount.Mount{erofsMount("/s/1/layer.erofs")},
		},
		{
			name: "fsmeta multi-device mount",
			mounts: []mount.Mount{{
				Type:    "format/erofs",
				Source:  "/s/1/fsmeta.erofs",
				Options: []string{"ro", "loop", "device=/s/1/layer.erofs", "device=/s/2/layer.erofs"},
			}},
		},
		{
			name: "active snapshot: erofs plus ext4",
			mounts: []mount.Mount{
				erofsMount("/s/1/layer.erofs"),
				{Type: "ext4", Source: "/s/2/rwlayer.img", Options: []string{"rw", "loop"}},
			},
		},
		{
			name: "two individual erofs layers without fsmeta",
			mounts: []mount.Mount{
				erofsMount("/s/2/layer.erofs"),
				erofsMount("/s/1/layer.erofs"),
			},
			want: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := stackedErofsLayers(tc.mounts); got != tc.want {
				t.Fatalf("stackedErofsLayers = %v, want %v", got, tc.want)
			}
		})
	}
}

// recordingMountManager is a configurable mount.Manager that records
// Activate/Deactivate calls so the mount-manager activation paths of
// withLowerMount/withUpperMount can be tested without root or real mounts.
type recordingMountManager struct {
	info        mount.ActivationInfo
	activateErr error
	activated   []string
	deactivated []string
}

func (m *recordingMountManager) Activate(_ context.Context, name string, _ []mount.Mount, _ ...mount.ActivateOpt) (mount.ActivationInfo, error) {
	if m.activateErr != nil {
		return mount.ActivationInfo{}, m.activateErr
	}
	m.activated = append(m.activated, name)
	info := m.info
	info.Name = name
	return info, nil
}

func (m *recordingMountManager) Deactivate(_ context.Context, name string) error {
	m.deactivated = append(m.deactivated, name)
	return nil
}

func (m *recordingMountManager) Info(_ context.Context, _ string) (mount.ActivationInfo, error) {
	return mount.ActivationInfo{}, nil
}

func (m *recordingMountManager) Update(_ context.Context, _ mount.ActivationInfo, _ ...string) (mount.ActivationInfo, error) {
	return mount.ActivationInfo{}, nil
}

func (m *recordingMountManager) List(_ context.Context, _ ...string) ([]mount.ActivationInfo, error) {
	return nil, nil
}

// checkActivationBalanced asserts exactly one Activate call with a matching
// Deactivate call (no leaked activations).
func checkActivationBalanced(t *testing.T, mm *recordingMountManager) {
	t.Helper()
	if len(mm.activated) != 1 {
		t.Fatalf("Activate called %d times, want 1", len(mm.activated))
	}
	if len(mm.deactivated) != 1 || mm.deactivated[0] != mm.activated[0] {
		t.Fatalf("Deactivate calls = %v, want [%s]", mm.deactivated, mm.activated[0])
	}
}

// formattedErofsMount returns a single formatted EROFS mount without device=
// options: not a stacked layer set and not multi-device, so it exercises the
// mount-manager activation branch of withLowerMount/withUpperMount.
func formattedErofsMount() []mount.Mount {
	return []mount.Mount{{
		Type:    "format/erofs",
		Source:  "/snapshots/1/fsmeta.erofs",
		Options: []string{"ro", "loop"},
	}}
}

func TestWithLowerMountManagerActivation(t *testing.T) {
	ctx := context.Background()
	lower := formattedErofsMount()

	t.Run("fails without mount manager", func(t *testing.T) {
		err := withLowerMount(ctx, lower, nil, func(string) error { return nil })
		if err == nil || !strings.Contains(err.Error(), "mount manager is required") {
			t.Fatalf("expected mount manager required error, got: %v", err)
		}
	})

	t.Run("single bind shortcut uses source directly", func(t *testing.T) {
		bindDir := t.TempDir()
		mm := &recordingMountManager{info: mount.ActivationInfo{
			System: []mount.Mount{{Type: "bind", Source: bindDir}},
		}}

		var gotRoot string
		if err := withLowerMount(ctx, lower, mm, func(root string) error {
			gotRoot = root
			return nil
		}); err != nil {
			t.Fatalf("withLowerMount failed: %v", err)
		}
		if gotRoot != bindDir {
			t.Errorf("root = %q, want bind source %q", gotRoot, bindDir)
		}
		checkActivationBalanced(t, mm)
	})

	t.Run("merged fsmeta shortcut uses erofs mount point", func(t *testing.T) {
		mergedDir := t.TempDir()
		mm := &recordingMountManager{info: mount.ActivationInfo{
			// Lower-only overlay as the system mount plus an active merged
			// fsmeta mount: withLowerMount must use the EROFS mount point
			// directly instead of re-mounting the overlay.
			System: []mount.Mount{{
				Type:    "overlay",
				Options: []string{"lowerdir=/lower1:/lower2"},
			}},
			Active: []mount.ActiveMount{{
				Mount:      mount.Mount{Type: "erofs", Source: "/snapshots/1/fsmeta.erofs"},
				MountPoint: mergedDir,
			}},
		}}

		var gotRoot string
		if err := withLowerMount(ctx, lower, mm, func(root string) error {
			gotRoot = root
			return nil
		}); err != nil {
			t.Fatalf("withLowerMount failed: %v", err)
		}
		if gotRoot != mergedDir {
			t.Errorf("root = %q, want fsmeta mount point %q", gotRoot, mergedDir)
		}
		checkActivationBalanced(t, mm)
	})

	t.Run("activation error propagates without deactivation", func(t *testing.T) {
		mm := &recordingMountManager{activateErr: errors.New("activation failed")}
		called := false
		err := withLowerMount(ctx, lower, mm, func(string) error {
			called = true
			return nil
		})
		if err == nil || !strings.Contains(err.Error(), "activation failed") {
			t.Fatalf("expected activation error, got: %v", err)
		}
		if called {
			t.Error("callback should not run when activation fails")
		}
		if len(mm.deactivated) != 0 {
			t.Errorf("Deactivate called %d times after failed activation, want 0", len(mm.deactivated))
		}
	})
}

func TestWithUpperMountManagerActivation(t *testing.T) {
	ctx := context.Background()
	upper := formattedErofsMount()

	t.Run("fails without mount manager", func(t *testing.T) {
		err := withUpperMount(ctx, upper, nil, func(string) error { return nil })
		if err == nil || !strings.Contains(err.Error(), "mount manager is required") {
			t.Fatalf("expected mount manager required error, got: %v", err)
		}
	})

	t.Run("single bind shortcut uses source directly", func(t *testing.T) {
		bindDir := t.TempDir()
		mm := &recordingMountManager{info: mount.ActivationInfo{
			System: []mount.Mount{{Type: "bind", Source: bindDir}},
		}}

		var gotRoot string
		if err := withUpperMount(ctx, upper, mm, func(root string) error {
			gotRoot = root
			return nil
		}); err != nil {
			t.Fatalf("withUpperMount failed: %v", err)
		}
		if gotRoot != bindDir {
			t.Errorf("root = %q, want bind source %q", gotRoot, bindDir)
		}
		checkActivationBalanced(t, mm)
	})
}

func TestWithActiveSnapshotMountMissingExt4(t *testing.T) {
	// Active snapshot handling must fail loud when the ext4 writable layer
	// is missing instead of silently diffing only the EROFS layers.
	err := withActiveSnapshotMount(context.Background(), []mount.Mount{
		{Type: "erofs", Source: "/snapshots/1/layer.erofs", Options: []string{"ro", "loop"}},
	}, func(string) error {
		t.Fatal("callback should not be called")
		return nil
	})
	if err == nil || !strings.Contains(err.Error(), "missing ext4 writable layer") {
		t.Fatalf("expected missing ext4 error, got: %v", err)
	}
}

// TestWriteAndCommitDiffNativeErofs verifies the native EROFS layer path:
// the tar diff is converted through mkfs.erofs and the staged image is
// committed as-is, with the uncompressed label set to the blob digest (the
// diff ID convention for native EROFS layers).
func TestWriteAndCommitDiffNativeErofs(t *testing.T) {
	if _, err := exec.LookPath("mkfs.erofs"); err != nil {
		t.Skip("mkfs.erofs not installed")
	}
	supported, err := erofs.SupportGenerateFromTar()
	if err != nil || !supported {
		t.Skipf("mkfs.erofs lacks --tar support (err=%v)", err)
	}

	ctx := context.Background()
	store := newTestContentStore(t)
	d := NewErofsDiffer(store)

	writeFn := func(ctx context.Context, w io.Writer) error {
		tw := tar.NewWriter(w)
		content := []byte("hello from a native erofs layer")
		if err := tw.WriteHeader(&tar.Header{
			Name: "hello.txt",
			Mode: 0o644,
			Size: int64(len(content)),
		}); err != nil {
			return err
		}
		if _, err := tw.Write(content); err != nil {
			return err
		}
		return tw.Close()
	}

	config := diff.Config{MediaType: images.MediaTypeErofsLayer}
	// Mirror Compare's wiring for EROFS media types.
	desc, err := d.writeAndCommitDiff(ctx, config, erofsLayerWriteFn(writeFn))
	if err != nil {
		t.Fatalf("writeAndCommitDiff failed: %v", err)
	}

	if desc.MediaType != images.MediaTypeErofsLayer {
		t.Errorf("media type = %s, want %s", desc.MediaType, images.MediaTypeErofsLayer)
	}

	// The committed blob must be a valid EROFS image with 4KiB blocks
	// (fsmeta-merge compatible, like layers produced by the pull path).
	blob, err := content.ReadBlob(ctx, store, desc)
	if err != nil {
		t.Fatalf("read committed blob: %v", err)
	}
	blobPath := filepath.Join(t.TempDir(), "layer.erofs")
	if err := os.WriteFile(blobPath, blob, 0o644); err != nil {
		t.Fatal(err)
	}
	blockSize, err := erofs.GetBlockSize(blobPath)
	if err != nil {
		t.Fatalf("committed blob is not a valid EROFS image: %v", err)
	}
	if blockSize != 4096 {
		t.Errorf("block size = %d, want 4096 (fsmeta-merge compatibility)", blockSize)
	}

	// For native EROFS layers the diff ID is the blob digest itself.
	info, err := store.Info(ctx, desc.Digest)
	if err != nil {
		t.Fatalf("blob info: %v", err)
	}
	if got := info.Labels["containerd.io/uncompressed"]; got != desc.Digest.String() {
		t.Errorf("uncompressed label = %q, want blob digest %q", got, desc.Digest)
	}
}

// TestCompressionTypeFromMediaTypeErofs verifies EROFS media types are
// accepted as uncompressed while +suffix forms remain rejected.
func TestCompressionTypeFromMediaTypeErofs(t *testing.T) {
	for _, mt := range []string{images.MediaTypeErofsLayer, "application/vnd.oci.image.layer.v1.erofs"} {
		ct, err := compressionTypeFromMediaType(mt)
		if err != nil {
			t.Errorf("compressionTypeFromMediaType(%q): %v", mt, err)
		}
		if ct != compression.Uncompressed {
			t.Errorf("compressionTypeFromMediaType(%q) = %v, want Uncompressed", mt, ct)
		}
	}
	if _, err := compressionTypeFromMediaType(images.MediaTypeErofsLayer + "+gzip"); err == nil {
		t.Error("expected +suffix erofs media type to be rejected")
	}
}
