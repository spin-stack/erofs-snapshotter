package snapshotter

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestWriteLayerManifest(t *testing.T) {
	const (
		digestA = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		digestB = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	)
	blobA := "/snapshots/1/" + strings.Replace(digestA, ":", "-", 1) + ".erofs"
	blobB := "/snapshots/2/" + strings.Replace(digestB, ":", "-", 1) + ".erofs"
	fallback := "/snapshots/3/snapshot-3.erofs"

	tests := []struct {
		name  string
		blobs []string
		want  string // empty means: file must not be created
	}{
		{
			name:  "no blobs writes nothing",
			blobs: nil,
			want:  "",
		},
		{
			name:  "digest blobs only",
			blobs: []string{blobA, blobB},
			want:  digestA + "\n" + digestB + "\n",
		},
		{
			// A fallback blob (no content digest) must still produce a line so
			// the manifest stays 1:1 with the device= options. Order preserved.
			name:  "fallback blob keeps positional alignment",
			blobs: []string{blobA, fallback, blobB},
			want:  digestA + "\n" + "blob:snapshot-3.erofs\n" + digestB + "\n",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			s := newTestSnapshotterWithRoot(t, root) //nolint:contextcheck // startup cleanup uses background context
			manifestFile := filepath.Join(root, "layers.manifest")

			if err := s.writeLayerManifest(manifestFile, tc.blobs); err != nil {
				t.Fatalf("writeLayerManifest: %v", err)
			}

			data, err := os.ReadFile(manifestFile)
			switch {
			case tc.want == "":
				if err == nil {
					t.Fatalf("expected no manifest file, got content %q", data)
				}
				if !os.IsNotExist(err) {
					t.Fatalf("unexpected error reading manifest: %v", err)
				}
				return
			case err != nil:
				t.Fatalf("read manifest: %v", err)
			}

			if string(data) != tc.want {
				t.Errorf("manifest content = %q, want %q", data, tc.want)
			}

			// One line per blob: the manifest must be positionally 1:1 with
			// the layers (and thus the device= options).
			gotLines := strings.Count(string(data), "\n")
			if gotLines != len(tc.blobs) {
				t.Errorf("manifest line count = %d, want %d (one per layer)", gotLines, len(tc.blobs))
			}

			// No leftover temp files from the atomic write.
			entries, err := os.ReadDir(root)
			if err != nil {
				t.Fatal(err)
			}
			for _, e := range entries {
				if strings.HasPrefix(e.Name(), ".tmp-") {
					t.Errorf("leftover temp file after atomic write: %s", e.Name())
				}
			}
		})
	}
}

func TestAtomicWriteFile(t *testing.T) {
	t.Run("writes content and applies perm", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "out.txt")

		if err := atomicWriteFile(path, []byte("hello"), 0o600); err != nil {
			t.Fatalf("atomicWriteFile: %v", err)
		}

		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		if string(data) != "hello" {
			t.Errorf("content = %q, want %q", data, "hello")
		}

		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode().Perm() != 0o600 {
			t.Errorf("perm = %o, want 600", info.Mode().Perm())
		}
	})

	t.Run("overwrites existing file atomically and leaves no temp", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "out.txt")
		if err := os.WriteFile(path, []byte("old"), 0o644); err != nil {
			t.Fatal(err)
		}

		if err := atomicWriteFile(path, []byte("new content"), 0o644); err != nil {
			t.Fatalf("atomicWriteFile: %v", err)
		}

		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if string(data) != "new content" {
			t.Errorf("content = %q, want %q", data, "new content")
		}

		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) != 1 {
			t.Errorf("expected only the target file, got %d entries: %v", len(entries), entries)
		}
	})

	t.Run("error when directory does not exist", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "missing-subdir", "out.txt")
		if err := atomicWriteFile(path, []byte("x"), 0o644); err == nil {
			t.Fatal("expected error for nonexistent directory")
		}
	})
}

func TestGetCommitUpperDir(t *testing.T) {
	// This test verifies that getCommitUpperDir correctly determines
	// block vs overlay mode based on rwlayer.img existence and mount state.
	ctx := context.Background()

	t.Run("overlay mode when no rwlayer.img", func(t *testing.T) {
		root := t.TempDir()
		s := newTestSnapshotterWithRoot(t, root) //nolint:contextcheck // NewSnapshotter startup cleanup intentionally uses a background context

		// Create snapshot directory without rwlayer.img
		snapshotDir := filepath.Join(root, "snapshots", "test-id")
		fsDir := filepath.Join(snapshotDir, "fs")
		if err := os.MkdirAll(fsDir, 0o755); err != nil {
			t.Fatal(err)
		}

		upperDir, cleanup, prune, err := s.getCommitUpperDir(ctx, "test-id", false)
		if err != nil {
			t.Fatalf("getCommitUpperDir: %v", err)
		}
		defer cleanup()

		// Should return overlay upper dir (fs/)
		expectedUpper := filepath.Join(snapshotDir, "fs")
		if upperDir != expectedUpper {
			t.Errorf("upperDir = %q, want %q", upperDir, expectedUpper)
		}
		if !prune {
			t.Error("prune = false, want true for overlay mode")
		}
	})

	t.Run("block mode when rwlayer.img exists and upper dir exists", func(t *testing.T) {
		root := t.TempDir()
		s := newTestSnapshotterWithRoot(t, root) //nolint:contextcheck // NewSnapshotter startup cleanup intentionally uses a background context

		// Create snapshot directory with rwlayer.img and rw/upper/
		snapshotDir := filepath.Join(root, "snapshots", "test-id")
		rwDir := filepath.Join(snapshotDir, "rw")
		upperDir := filepath.Join(rwDir, "upper")
		if err := os.MkdirAll(upperDir, 0o755); err != nil {
			t.Fatal(err)
		}

		rwLayer := filepath.Join(snapshotDir, "rwlayer.img")
		if err := os.WriteFile(rwLayer, []byte("fake ext4"), 0o644); err != nil {
			t.Fatal(err)
		}

		result, cleanup, prune, err := s.getCommitUpperDir(ctx, "test-id", false)
		if err != nil {
			t.Fatalf("getCommitUpperDir: %v", err)
		}
		defer cleanup()

		// Should return block upper dir (rw/upper/)
		if result != upperDir {
			t.Errorf("upperDir = %q, want %q", result, upperDir)
		}
		if !prune {
			t.Error("prune = false, want true for mounted extract upper")
		}
	})

	// In block mode without an accessible rw/upper, getCommitUpperDir must
	// NOT fall back to the (empty) fs/ directory: doing so would silently
	// commit an empty layer and lose the container's changes. It mounts the
	// ext4 instead, which fails loudly for an invalid image. The same
	// scenarios used to return fs/ (or rw/) before this was fixed.
	for _, tc := range []struct {
		name  string
		setup func(t *testing.T, snapshotDir string)
	}{
		{
			name: "errors when rw has content but no upper subdir",
			setup: func(t *testing.T, snapshotDir string) {
				rwDir := filepath.Join(snapshotDir, "rw")
				if err := os.MkdirAll(rwDir, 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(rwDir, "somefile"), []byte("data"), 0o644); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "errors when rw is empty (ext4 not mounted)",
			setup: func(t *testing.T, snapshotDir string) {
				if err := os.MkdirAll(filepath.Join(snapshotDir, "rw"), 0o755); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name:  "errors when rw does not exist",
			setup: func(t *testing.T, snapshotDir string) {},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			s := newTestSnapshotterWithRoot(t, root) //nolint:contextcheck // NewSnapshotter startup cleanup intentionally uses a background context

			snapshotDir := filepath.Join(root, "snapshots", "test-id")
			fsDir := filepath.Join(snapshotDir, "fs")
			if err := os.MkdirAll(fsDir, 0o755); err != nil {
				t.Fatal(err)
			}
			tc.setup(t, snapshotDir)

			// rwlayer.img is not a valid ext4 image, so the read-only mount
			// attempt must fail; what matters is that no fs/ fallback happens.
			rwLayer := filepath.Join(snapshotDir, "rwlayer.img")
			if err := os.WriteFile(rwLayer, []byte("fake ext4"), 0o644); err != nil {
				t.Fatal(err)
			}

			result, cleanup, _, err := s.getCommitUpperDir(ctx, "test-id", false)
			if err == nil {
				cleanup()
				t.Fatalf("getCommitUpperDir = %q, want error (no silent fallback to fs/)", result)
			}
		})
	}
}
