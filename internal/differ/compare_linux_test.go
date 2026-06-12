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
	"github.com/containerd/containerd/v2/pkg/archive/compression"
	"github.com/containerd/containerd/v2/pkg/labels"
	"github.com/containerd/containerd/v2/pkg/testutil"
	"github.com/containerd/containerd/v2/plugins/content/local"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"golang.org/x/sys/unix"

	"github.com/spin-stack/erofs-snapshotter/internal/erofs"
	"github.com/spin-stack/erofs-snapshotter/internal/loop"

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

// requireRootAndTools skips the test unless it runs as root (via -test.root)
// and all the given binaries are installed.
func requireRootAndTools(t *testing.T, bins ...string) {
	t.Helper()
	for _, bin := range bins {
		if _, err := exec.LookPath(bin); err != nil {
			t.Skipf("%s not installed", bin)
		}
	}
	testutil.RequiresRoot(t)
}

// buildErofsLayerBlob creates an EROFS layer blob containing the given files.
func buildErofsLayerBlob(t *testing.T, files map[string]string) string {
	t.Helper()
	src := t.TempDir()
	for name, contents := range files {
		p := filepath.Join(src, name)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatalf("failed to create dir for %s: %v", name, err)
		}
		if err := os.WriteFile(p, []byte(contents), 0o644); err != nil {
			t.Fatalf("failed to write %s: %v", name, err)
		}
	}
	blob := filepath.Join(t.TempDir(), "layer.erofs")
	if err := erofs.ConvertErofs(context.Background(), blob, src, nil); err != nil {
		t.Fatalf("mkfs.erofs failed: %v", err)
	}
	return blob
}

// buildFsmeta merges the given layer blobs (oldest-first) into a fsmeta.erofs
// using the same mkfs.erofs invocation shape as the snapshotter's
// generateFsMeta (--quiet --vmdk-desc=<vmdk> <fsmeta> <blobs...>).
func buildFsmeta(t *testing.T, blobs ...string) string {
	t.Helper()
	dir := t.TempDir()
	fsmeta := filepath.Join(dir, "fsmeta.erofs")
	vmdk := filepath.Join(dir, "merged.vmdk")
	args := append([]string{"--quiet", "--vmdk-desc=" + vmdk, fsmeta}, blobs...)
	if out, err := exec.Command("mkfs.erofs", args...).CombinedOutput(); err != nil {
		t.Skipf("mkfs.erofs does not support fsmeta merge: %v: %s", err, out)
	}
	return fsmeta
}

// erofsLayerMount returns the plain EROFS layer mount the snapshotter emits
// while fsmeta generation has not completed.
func erofsLayerMount(blob string) mount.Mount {
	return mount.Mount{Type: "erofs", Source: blob, Options: []string{"ro", "loop"}}
}

// createExt4Image creates an empty ext4 image of the given size.
func createExt4Image(t *testing.T, sizeMB int64) string {
	t.Helper()
	img := filepath.Join(t.TempDir(), "rwlayer.img")
	f, err := os.Create(img)
	if err != nil {
		t.Fatalf("failed to create ext4 image file: %v", err)
	}
	if err := f.Truncate(sizeMB << 20); err != nil {
		f.Close()
		t.Fatalf("failed to truncate ext4 image: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("failed to close ext4 image: %v", err)
	}
	if out, err := exec.Command("mkfs.ext4", "-q", "-F", img).CombinedOutput(); err != nil {
		t.Fatalf("mkfs.ext4 failed: %v: %s", err, out)
	}
	return img
}

// populateExt4 temporarily mounts img read-write and runs fill to seed its
// contents. The image is unmounted (and its loop device released) before
// returning, so MountExt4 can later take the image locks.
func populateExt4(t *testing.T, img string, fill func(root string)) {
	t.Helper()
	dir := t.TempDir()
	if out, err := exec.Command("mount", "-o", "loop,rw", img, dir).CombinedOutput(); err != nil {
		t.Fatalf("failed to mount ext4 read-write: %v: %s", err, out)
	}
	defer func() {
		if out, err := exec.Command("umount", dir).CombinedOutput(); err != nil {
			t.Fatalf("failed to unmount ext4: %v: %s", err, out)
		}
	}()
	fill(dir)
}

// checkNoLoopDevice asserts that no loop device remains attached to any of
// the given backing files after the with*Mount helpers returned.
func checkNoLoopDevice(t *testing.T, backingFiles ...string) {
	t.Helper()
	for _, f := range backingFiles {
		dev, err := loop.FindByBackingFile(f)
		if err != nil {
			t.Fatalf("failed to look up loop device for %s: %v", f, err)
		}
		if dev != nil {
			t.Errorf("loop device %s still attached to %s", dev.Path, f)
		}
	}
}

// readFileString reads a file and fails the test on error.
func readFileString(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("failed to read %s: %v", path, err)
	}
	return string(data)
}

func TestWithStackedErofsTempMount(t *testing.T) {
	requireRootAndTools(t, "mkfs.erofs")
	ctx := context.Background()

	older := buildErofsLayerBlob(t, map[string]string{
		"base.txt":     "from older layer",
		"conflict.txt": "older wins is a bug",
	})
	newer := buildErofsLayerBlob(t, map[string]string{
		"new.txt":      "from newer layer",
		"conflict.txt": "newer wins",
	})

	// Snapshotter fallback mount order: newest first.
	mounts := []mount.Mount{erofsLayerMount(newer), erofsLayerMount(older)}

	called := false
	err := withStackedErofsTempMount(ctx, mounts, func(root string) error {
		called = true
		// Both layers' files must be visible in the merged view.
		if got := readFileString(t, filepath.Join(root, "base.txt")); got != "from older layer" {
			t.Errorf("base.txt = %q, want %q", got, "from older layer")
		}
		if got := readFileString(t, filepath.Join(root, "new.txt")); got != "from newer layer" {
			t.Errorf("new.txt = %q, want %q", got, "from newer layer")
		}
		// The newest layer must win for conflicting paths.
		if got := readFileString(t, filepath.Join(root, "conflict.txt")); got != "newer wins" {
			t.Errorf("conflict.txt = %q, want %q", got, "newer wins")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("withStackedErofsTempMount failed: %v", err)
	}
	if !called {
		t.Fatal("callback was not called")
	}

	checkNoLoopDevice(t, newer, older)
}

func TestWithErofsTempMountMultiDevice(t *testing.T) {
	requireRootAndTools(t, "mkfs.erofs")
	ctx := context.Background()

	layer1 := buildErofsLayerBlob(t, map[string]string{"a.txt": "from layer one"})
	layer2 := buildErofsLayerBlob(t, map[string]string{"b.txt": "from layer two"})
	fsmeta := buildFsmeta(t, layer1, layer2)

	// Merged fsmeta multi-device mount: device= options in oldest-first order,
	// matching the blob order passed to mkfs.erofs.
	mounts := []mount.Mount{{
		Type:    "erofs",
		Source:  fsmeta,
		Options: []string{"ro", "loop", "device=" + layer1, "device=" + layer2},
	}}

	called := false
	err := withErofsTempMount(ctx, mounts, func(root string) error {
		called = true
		if got := readFileString(t, filepath.Join(root, "a.txt")); got != "from layer one" {
			t.Errorf("a.txt = %q, want %q", got, "from layer one")
		}
		if got := readFileString(t, filepath.Join(root, "b.txt")); got != "from layer two" {
			t.Errorf("b.txt = %q, want %q", got, "from layer two")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("withErofsTempMount failed: %v", err)
	}
	if !called {
		t.Fatal("callback was not called")
	}

	checkNoLoopDevice(t, fsmeta, layer1, layer2)
}

func TestWithExt4UpperMount(t *testing.T) {
	requireRootAndTools(t, "mkfs.ext4")
	ctx := context.Background()

	t.Run("upper directory is the diff root", func(t *testing.T) {
		img := createExt4Image(t, 8)
		populateExt4(t, img, func(root string) {
			for _, dir := range []string{"upper", "work", "rogue-dir"} {
				if err := os.MkdirAll(filepath.Join(root, dir), 0o755); err != nil {
					t.Fatalf("failed to create %s: %v", dir, err)
				}
			}
			if err := os.WriteFile(filepath.Join(root, "upper", "hello.txt"), []byte("hello from upper"), 0o644); err != nil {
				t.Fatalf("failed to write upper file: %v", err)
			}
			// rogue-dir exercises the warnUnexpectedExt4Entries warning branch.
			if err := os.WriteFile(filepath.Join(root, "rogue-dir", "stray.txt"), []byte("stray"), 0o644); err != nil {
				t.Fatalf("failed to write stray file: %v", err)
			}
		})

		mounts := []mount.Mount{{Type: "ext4", Source: img, Options: []string{"rw", "loop"}}}

		var gotRoot string
		err := withUpperMount(ctx, mounts, nil, func(root string) error {
			gotRoot = root
			if got := readFileString(t, filepath.Join(root, "hello.txt")); got != "hello from upper" {
				t.Errorf("hello.txt = %q, want %q", got, "hello from upper")
			}
			// The diff root is upper/ itself: ext4 root entries such as
			// lost+found, work/ and the stray rogue-dir/ must not leak in.
			entries, err := os.ReadDir(root)
			if err != nil {
				return err
			}
			if len(entries) != 1 || entries[0].Name() != "hello.txt" {
				names := make([]string, 0, len(entries))
				for _, e := range entries {
					names = append(names, e.Name())
				}
				t.Errorf("diff root entries = %v, want [hello.txt]", names)
			}
			return nil
		})
		if err != nil {
			t.Fatalf("withUpperMount failed: %v", err)
		}
		if filepath.Base(gotRoot) != "upper" {
			t.Errorf("diff root = %q, want the ext4 upper/ directory", gotRoot)
		}

		checkNoLoopDevice(t, img)
	})

	t.Run("missing upper yields empty diff root", func(t *testing.T) {
		img := createExt4Image(t, 8)

		mounts := []mount.Mount{{Type: "ext4", Source: img, Options: []string{"rw", "loop"}}}

		called := false
		err := withUpperMount(ctx, mounts, nil, func(root string) error {
			called = true
			entries, err := os.ReadDir(root)
			if err != nil {
				return err
			}
			if len(entries) != 0 {
				t.Errorf("expected empty diff root, got %d entries", len(entries))
			}
			return nil
		})
		if err != nil {
			t.Fatalf("withUpperMount failed: %v", err)
		}
		if !called {
			t.Fatal("callback was not called")
		}

		checkNoLoopDevice(t, img)
	})
}

func TestWithActiveSnapshotMountOverlay(t *testing.T) {
	requireRootAndTools(t, "mkfs.erofs", "mkfs.ext4")
	ctx := context.Background()

	layer := buildErofsLayerBlob(t, map[string]string{
		"keep.txt":    "keep me",
		"removed.txt": "remove me",
	})

	img := createExt4Image(t, 8)
	populateExt4(t, img, func(root string) {
		for _, dir := range []string{"upper", "work"} {
			if err := os.MkdirAll(filepath.Join(root, dir), 0o755); err != nil {
				t.Fatalf("failed to create %s: %v", dir, err)
			}
		}
		if err := os.WriteFile(filepath.Join(root, "upper", "added.txt"), []byte("added in container"), 0o644); err != nil {
			t.Fatalf("failed to write added file: %v", err)
		}
		// Overlay whiteout: a 0:0 character device in upper/ hides the
		// matching file from the EROFS layer below.
		if err := unix.Mknod(filepath.Join(root, "upper", "removed.txt"), unix.S_IFCHR, 0); err != nil {
			t.Fatalf("failed to create whiteout: %v", err)
		}
	})

	// EROFS layer + ext4 writable layer: HasActiveSnapshotMounts routes this
	// through withActiveSnapshotMount.
	mounts := []mount.Mount{
		erofsLayerMount(layer),
		{Type: "ext4", Source: img, Options: []string{"rw", "loop"}},
	}

	called := false
	err := withUpperMount(ctx, mounts, nil, func(root string) error {
		called = true
		// File added by the guest is visible.
		if got := readFileString(t, filepath.Join(root, "added.txt")); got != "added in container" {
			t.Errorf("added.txt = %q, want %q", got, "added in container")
		}
		// Base layer files remain visible.
		if got := readFileString(t, filepath.Join(root, "keep.txt")); got != "keep me" {
			t.Errorf("keep.txt = %q, want %q", got, "keep me")
		}
		// The whiteout target must NOT be visible in the merged view.
		if _, err := os.Stat(filepath.Join(root, "removed.txt")); !os.IsNotExist(err) {
			t.Errorf("removed.txt should be hidden by the whiteout, stat err = %v", err)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("withUpperMount failed: %v", err)
	}
	if !called {
		t.Fatal("callback was not called")
	}

	checkNoLoopDevice(t, layer, img)
}

func TestEnsureUncompressedLabel(t *testing.T) {
	ctx := context.Background()

	ingest := func(t *testing.T, store content.Store, data []byte) content.Info {
		t.Helper()
		desc := ocispec.Descriptor{
			MediaType: ocispec.MediaTypeImageLayer,
			Digest:    digest.FromBytes(data),
			Size:      int64(len(data)),
		}
		if err := content.WriteBlob(ctx, store, "label-"+desc.Digest.String(), bytes.NewReader(data), desc); err != nil {
			t.Fatalf("failed to write blob: %v", err)
		}
		info, err := store.Info(ctx, desc.Digest)
		if err != nil {
			t.Fatalf("failed to get info: %v", err)
		}
		if info.Labels == nil {
			info.Labels = make(map[string]string)
		}
		return info
	}

	t.Run("existing label is left untouched", func(t *testing.T) {
		store := newTestContentStore(t)
		d := NewErofsDiffer(store)
		info := ingest(t, store, []byte("blob with existing label"))

		// Simulate a label already present on the info: no store update must
		// happen, so the store keeps NO label for this blob.
		info.Labels[labels.LabelUncompressed] = "sha256:existing"
		if err := d.ensureUncompressedLabel(ctx, info, "sha256:replacement"); err != nil {
			t.Fatalf("ensureUncompressedLabel failed: %v", err)
		}

		stored, err := store.Info(ctx, info.Digest)
		if err != nil {
			t.Fatalf("failed to get info: %v", err)
		}
		if got := stored.Labels[labels.LabelUncompressed]; got != "" {
			t.Errorf("store label = %q, want no update when the label already exists", got)
		}
	})

	t.Run("missing label is set on the store", func(t *testing.T) {
		store := newTestContentStore(t)
		d := NewErofsDiffer(store)
		info := ingest(t, store, []byte("blob without label"))

		const uncompressed = "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
		if err := d.ensureUncompressedLabel(ctx, info, uncompressed); err != nil {
			t.Fatalf("ensureUncompressedLabel failed: %v", err)
		}

		stored, err := store.Info(ctx, info.Digest)
		if err != nil {
			t.Fatalf("failed to get info: %v", err)
		}
		if got := stored.Labels[labels.LabelUncompressed]; got != uncompressed {
			t.Errorf("store label = %q, want %q", got, uncompressed)
		}
	})
}

// errWriteCloser is an io.WriteCloser that fails on demand.
type errWriteCloser struct {
	writeErr error
	closeErr error
}

func (w *errWriteCloser) Write(p []byte) (int, error) {
	if w.writeErr != nil {
		return 0, w.writeErr
	}
	return len(p), nil
}

func (w *errWriteCloser) Close() error {
	return w.closeErr
}

func TestWriteCompressedDiffCompressorErrors(t *testing.T) {
	ctx := context.Background()
	writePayload := func(_ context.Context, w io.Writer) error {
		_, err := w.Write([]byte("diff payload"))
		return err
	}

	t.Run("compressor constructor error", func(t *testing.T) {
		cfg := diff.Config{
			MediaType: ocispec.MediaTypeImageLayerGzip,
			Compressor: func(io.Writer, string) (io.WriteCloser, error) {
				return nil, errors.New("no compressor available")
			},
		}
		_, err := writeCompressedDiff(ctx, nil, cfg, compression.Gzip, writePayload)
		if err == nil || !strings.Contains(err.Error(), "failed to get compressed stream") {
			t.Fatalf("expected compressed stream error, got: %v", err)
		}
	})

	t.Run("compressor write error propagates", func(t *testing.T) {
		writeErr := errors.New("compressor write failed")
		cfg := diff.Config{
			MediaType: ocispec.MediaTypeImageLayerGzip,
			Compressor: func(io.Writer, string) (io.WriteCloser, error) {
				return &errWriteCloser{writeErr: writeErr}, nil
			},
		}
		_, err := writeCompressedDiff(ctx, nil, cfg, compression.Gzip, writePayload)
		if err == nil || !errors.Is(err, writeErr) {
			t.Fatalf("expected write error, got: %v", err)
		}
	})

	t.Run("compressor close error propagates", func(t *testing.T) {
		cfg := diff.Config{
			MediaType: ocispec.MediaTypeImageLayerGzip,
			Compressor: func(io.Writer, string) (io.WriteCloser, error) {
				return &errWriteCloser{closeErr: errors.New("trailer flush failed")}, nil
			},
		}
		_, err := writeCompressedDiff(ctx, nil, cfg, compression.Gzip, writePayload)
		if err == nil || !strings.Contains(err.Error(), "failed to close compressed stream") {
			t.Fatalf("expected close error, got: %v", err)
		}
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
