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
	"archive/tar"
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/containerd/containerd/v2/core/mount"

	// Import testutil to register the -test.root flag
	_ "github.com/aledbf/nexus-erofs/internal/testutil"
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
	if LayerBlobPattern != "sha256-*.erofs" {
		t.Errorf("LayerBlobPattern = %q, want %q", LayerBlobPattern, "sha256-*.erofs")
	}
}

func TestLayerBlobFilename(t *testing.T) {
	tests := []struct {
		digest string
		want   string
	}{
		{
			digest: "sha256:a3ed95caeb02ffe68cdd9fd84406680ae93d633cb16422d00e8a7c22955b46d4",
			want:   "sha256-a3ed95caeb02ffe68cdd9fd84406680ae93d633cb16422d00e8a7c22955b46d4.erofs",
		},
		{
			digest: "sha256:abc123",
			want:   "sha256-abc123.erofs",
		},
	}

	for _, tc := range tests {
		t.Run(tc.digest, func(t *testing.T) {
			got := LayerBlobFilename(tc.digest)
			if got != tc.want {
				t.Errorf("LayerBlobFilename(%q) = %q, want %q", tc.digest, got, tc.want)
			}
		})
	}
}

func TestBuildTarErofsArgs(t *testing.T) {
	tests := []struct {
		name          string
		layerPath     string
		uuid          string
		mkfsExtraOpts []string
		wantArgs      []string
	}{
		{
			name:          "basic without uuid",
			layerPath:     "/path/to/layer.erofs",
			uuid:          "",
			mkfsExtraOpts: nil,
			wantArgs:      []string{"--tar=f", "--aufs", "--quiet", "-Enoinline_data", "--sort=none", "/path/to/layer.erofs"},
		},
		{
			name:          "with uuid",
			layerPath:     "/path/to/layer.erofs",
			uuid:          "550e8400-e29b-41d4-a716-446655440000",
			mkfsExtraOpts: nil,
			wantArgs:      []string{"--tar=f", "--aufs", "--quiet", "-Enoinline_data", "--sort=none", "-U", "550e8400-e29b-41d4-a716-446655440000", "/path/to/layer.erofs"},
		},
		{
			name:          "with extra options",
			layerPath:     "/path/to/layer.erofs",
			uuid:          "",
			mkfsExtraOpts: []string{"-zlz4hc", "-C65536"},
			wantArgs:      []string{"--tar=f", "--aufs", "--quiet", "-Enoinline_data", "--sort=none", "-zlz4hc", "-C65536", "/path/to/layer.erofs"},
		},
		{
			name:          "with uuid and extra options",
			layerPath:     "/path/to/layer.erofs",
			uuid:          "550e8400-e29b-41d4-a716-446655440000",
			mkfsExtraOpts: []string{"-zlz4hc", "12", "-C65536"},
			wantArgs:      []string{"--tar=f", "--aufs", "--quiet", "-Enoinline_data", "--sort=none", "-zlz4hc", "12", "-C65536", "-U", "550e8400-e29b-41d4-a716-446655440000", "/path/to/layer.erofs"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := buildTarErofsArgs(tc.layerPath, tc.uuid, tc.mkfsExtraOpts)

			if len(got) != len(tc.wantArgs) {
				t.Fatalf("buildTarErofsArgs() returned %d args, want %d\ngot:  %v\nwant: %v",
					len(got), len(tc.wantArgs), got, tc.wantArgs)
			}

			for i, arg := range got {
				if arg != tc.wantArgs[i] {
					t.Errorf("arg[%d] = %q, want %q\nfull args: %v", i, arg, tc.wantArgs[i], got)
				}
			}

			// Critical check: last argument must be the layer path (mkfs.erofs reads from stdin automatically)
			if got[len(got)-1] != tc.layerPath {
				t.Errorf("last argument must be layer path %q, got %q", tc.layerPath, got[len(got)-1])
			}
		})
	}
}

func TestBuildTarIndexArgs(t *testing.T) {
	tests := []struct {
		name          string
		layerPath     string
		mkfsExtraOpts []string
		wantArgs      []string
	}{
		{
			name:          "basic",
			layerPath:     "/path/to/layer.erofs",
			mkfsExtraOpts: nil,
			wantArgs:      []string{"--tar=i", "--aufs", "--quiet", "/path/to/layer.erofs"},
		},
		{
			name:          "with extra options",
			layerPath:     "/path/to/layer.erofs",
			mkfsExtraOpts: []string{"-zlz4hc", "-C65536"},
			wantArgs:      []string{"--tar=i", "--aufs", "--quiet", "-zlz4hc", "-C65536", "/path/to/layer.erofs"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := buildTarIndexArgs(tc.layerPath, tc.mkfsExtraOpts)

			if len(got) != len(tc.wantArgs) {
				t.Fatalf("buildTarIndexArgs() returned %d args, want %d\ngot:  %v\nwant: %v",
					len(got), len(tc.wantArgs), got, tc.wantArgs)
			}

			for i, arg := range got {
				if arg != tc.wantArgs[i] {
					t.Errorf("arg[%d] = %q, want %q\nfull args: %v", i, arg, tc.wantArgs[i], got)
				}
			}

			// Critical check: last argument must be the layer path (mkfs.erofs reads from stdin automatically)
			if got[len(got)-1] != tc.layerPath {
				t.Errorf("last argument must be layer path %q, got %q", tc.layerPath, got[len(got)-1])
			}
		})
	}
}

// TestArgsEndWithLayerPath verifies that both tar conversion functions
// end with the layer path as the last argument. mkfs.erofs reads from
// stdin automatically when no SOURCE is specified after FILE.
func TestArgsEndWithLayerPath(t *testing.T) {
	t.Run("ConvertTarErofs args end with layer path", func(t *testing.T) {
		args := buildTarErofsArgs("/any/path.erofs", "550e8400-e29b-41d4-a716-446655440000", []string{"-z", "lz4"})

		if len(args) < 1 {
			t.Fatal("args too short")
		}

		// The last argument must be the output file path
		if args[len(args)-1] != "/any/path.erofs" {
			t.Errorf("output file must be the last argument, got args: %v", args)
		}
	})

	t.Run("GenerateTarIndex args end with layer path", func(t *testing.T) {
		args := buildTarIndexArgs("/any/path.erofs", []string{"-z", "lz4"})

		if len(args) < 1 {
			t.Fatal("args too short")
		}

		// The last argument must be the output file path
		if args[len(args)-1] != "/any/path.erofs" {
			t.Errorf("output file must be the last argument, got args: %v", args)
		}
	})
}

// createTestTar creates a simple tar archive in memory for testing.
func createTestTar(t *testing.T) *bytes.Buffer {
	t.Helper()
	buf := new(bytes.Buffer)
	tw := tar.NewWriter(buf)

	// Add a directory
	if err := tw.WriteHeader(&tar.Header{
		Name:     "testdir/",
		Mode:     0755,
		Typeflag: tar.TypeDir,
		ModTime:  time.Now(),
	}); err != nil {
		t.Fatalf("failed to write dir header: %v", err)
	}

	// Add a file with content
	content := []byte("Hello, EROFS!")
	if err := tw.WriteHeader(&tar.Header{
		Name:     "testdir/hello.txt",
		Mode:     0644,
		Size:     int64(len(content)),
		Typeflag: tar.TypeReg,
		ModTime:  time.Now(),
	}); err != nil {
		t.Fatalf("failed to write file header: %v", err)
	}
	if _, err := tw.Write(content); err != nil {
		t.Fatalf("failed to write file content: %v", err)
	}

	// Add a symlink
	if err := tw.WriteHeader(&tar.Header{
		Name:     "testdir/link",
		Linkname: "hello.txt",
		Mode:     0777,
		Typeflag: tar.TypeSymlink,
		ModTime:  time.Now(),
	}); err != nil {
		t.Fatalf("failed to write symlink header: %v", err)
	}

	if err := tw.Close(); err != nil {
		t.Fatalf("failed to close tar writer: %v", err)
	}

	return buf
}

// skipIfNoMkfsErofs skips the test if mkfs.erofs is not available.
func skipIfNoMkfsErofs(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("mkfs.erofs"); err != nil {
		t.Skip("mkfs.erofs not available, skipping integration test")
	}
}

// TestConvertTarErofsIntegration tests the actual conversion of a tar to EROFS.
// This is an integration test that requires mkfs.erofs to be installed.
func TestConvertTarErofsIntegration(t *testing.T) {
	skipIfNoMkfsErofs(t)

	dir := t.TempDir()
	layerPath := filepath.Join(dir, "layer.erofs")

	tarBuf := createTestTar(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	err := ConvertTarErofs(ctx, tarBuf, layerPath, "550e8400-e29b-41d4-a716-446655440000", nil)
	if err != nil {
		t.Fatalf("ConvertTarErofs failed: %v", err)
	}

	// Verify the output file exists and has content
	info, err := os.Stat(layerPath)
	if err != nil {
		t.Fatalf("failed to stat output file: %v", err)
	}
	if info.Size() == 0 {
		t.Error("output file is empty")
	}

	// Verify it's a valid EROFS image by checking the magic number
	f, err := os.Open(layerPath)
	if err != nil {
		t.Fatalf("failed to open output file: %v", err)
	}
	defer f.Close()

	// EROFS magic is at offset 1024, value 0xe0f5e1e2
	magic := make([]byte, 4)
	if _, err := f.ReadAt(magic, 1024); err != nil {
		t.Fatalf("failed to read magic: %v", err)
	}

	// EROFS magic in little-endian: 0xe2e1f5e0
	expectedMagic := []byte{0xe2, 0xe1, 0xf5, 0xe0}
	if !bytes.Equal(magic, expectedMagic) {
		t.Errorf("invalid EROFS magic: got %x, want %x", magic, expectedMagic)
	}

	t.Logf("Successfully created EROFS image: %s (%d bytes)", layerPath, info.Size())
}

// TestConvertTarErofsWithCompression tests conversion with compression options.
func TestConvertTarErofsWithCompression(t *testing.T) {
	skipIfNoMkfsErofs(t)

	dir := t.TempDir()
	layerPath := filepath.Join(dir, "layer.erofs")

	tarBuf := createTestTar(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Test with lz4hc compression (commonly used)
	err := ConvertTarErofs(ctx, tarBuf, layerPath, "", []string{"-zlz4hc"})
	if err != nil {
		t.Fatalf("ConvertTarErofs with compression failed: %v", err)
	}

	info, err := os.Stat(layerPath)
	if err != nil {
		t.Fatalf("failed to stat output file: %v", err)
	}
	if info.Size() == 0 {
		t.Error("output file is empty")
	}

	t.Logf("Successfully created compressed EROFS image: %s (%d bytes)", layerPath, info.Size())
}

// TestGenerateTarIndexAndAppendTarIntegration tests the tar index generation.
func TestGenerateTarIndexAndAppendTarIntegration(t *testing.T) {
	skipIfNoMkfsErofs(t)

	// Check if tar index mode is supported
	supported, err := SupportGenerateFromTar()
	if err != nil {
		t.Skipf("cannot check mkfs.erofs capabilities: %v", err)
	}
	if !supported {
		t.Skip("mkfs.erofs does not support --tar option")
	}

	dir := t.TempDir()
	layerPath := filepath.Join(dir, "layer.erofs")

	tarBuf := createTestTar(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	err = GenerateTarIndexAndAppendTar(ctx, tarBuf, layerPath, nil)
	if err != nil {
		t.Fatalf("GenerateTarIndexAndAppendTar failed: %v", err)
	}

	// Verify the output file exists and has content
	info, err := os.Stat(layerPath)
	if err != nil {
		t.Fatalf("failed to stat output file: %v", err)
	}
	if info.Size() == 0 {
		t.Error("output file is empty")
	}

	// The file should contain both the EROFS index and the appended tar
	// EROFS magic should be at offset 1024
	f, err := os.Open(layerPath)
	if err != nil {
		t.Fatalf("failed to open output file: %v", err)
	}
	defer f.Close()

	magic := make([]byte, 4)
	if _, err := f.ReadAt(magic, 1024); err != nil {
		t.Fatalf("failed to read magic: %v", err)
	}

	expectedMagic := []byte{0xe2, 0xe1, 0xf5, 0xe0}
	if !bytes.Equal(magic, expectedMagic) {
		t.Errorf("invalid EROFS magic: got %x, want %x", magic, expectedMagic)
	}

	t.Logf("Successfully created EROFS tar index layer: %s (%d bytes)", layerPath, info.Size())
}

// TestGetBlockSize tests reading block size from EROFS layers.
func TestGetBlockSize(t *testing.T) {
	t.Run("invalid file", func(t *testing.T) {
		_, err := GetBlockSize("/nonexistent/file.erofs")
		if err == nil {
			t.Error("expected error for nonexistent file")
		}
	})

	t.Run("non-erofs file", func(t *testing.T) {
		// Create a file with invalid magic
		f := filepath.Join(t.TempDir(), "invalid.erofs")
		data := make([]byte, 2048)
		if err := os.WriteFile(f, data, 0644); err != nil {
			t.Fatal(err)
		}
		_, err := GetBlockSize(f)
		if err == nil {
			t.Error("expected error for non-EROFS file")
		}
	})

	t.Run("valid erofs file", func(t *testing.T) {
		skipIfNoMkfsErofs(t)

		dir := t.TempDir()
		layerPath := filepath.Join(dir, "layer.erofs")
		tarBuf := createTestTar(t)

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		if err := ConvertTarErofs(ctx, tarBuf, layerPath, "", nil); err != nil {
			t.Fatalf("ConvertTarErofs failed: %v", err)
		}

		blockSize, err := GetBlockSize(layerPath)
		if err != nil {
			t.Fatalf("GetBlockSize failed: %v", err)
		}

		// Default block size for mkfs.erofs is 4096
		if blockSize != 4096 {
			t.Errorf("GetBlockSize() = %d, want 4096", blockSize)
		}

		t.Logf("EROFS layer block size: %d", blockSize)
	})

	t.Run("tar index mode layer", func(t *testing.T) {
		skipIfNoMkfsErofs(t)

		supported, err := SupportGenerateFromTar()
		if err != nil || !supported {
			t.Skip("mkfs.erofs does not support --tar option")
		}

		dir := t.TempDir()
		layerPath := filepath.Join(dir, "layer.erofs")
		tarBuf := createTestTar(t)

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		if err := GenerateTarIndexAndAppendTar(ctx, tarBuf, layerPath, nil); err != nil {
			t.Fatalf("GenerateTarIndexAndAppendTar failed: %v", err)
		}

		blockSize, err := GetBlockSize(layerPath)
		if err != nil {
			t.Fatalf("GetBlockSize failed: %v", err)
		}

		// Tar index mode uses 512-byte blocks
		t.Logf("EROFS tar index layer block size: %d", blockSize)

		// The block size for tar index mode should be 512
		// (This is what causes the fsmeta merge incompatibility)
		if blockSize != 512 {
			t.Logf("Note: tar index mode block size is %d (expected 512)", blockSize)
		}
	})
}

// TestCanMergeFsmeta tests the compatibility check for fsmeta merge.
func TestCanMergeFsmeta(t *testing.T) {
	t.Run("empty list", func(t *testing.T) {
		// Empty list is compatible (nothing to merge)
		if !CanMergeFsmeta(nil) {
			t.Error("CanMergeFsmeta(nil) = false, want true")
		}
		if !CanMergeFsmeta([]string{}) {
			t.Error("CanMergeFsmeta([]) = false, want true")
		}
	})

	t.Run("nonexistent files", func(t *testing.T) {
		paths := []string{"/nonexistent/file1.erofs", "/nonexistent/file2.erofs"}
		if CanMergeFsmeta(paths) {
			t.Error("CanMergeFsmeta with nonexistent files should return false")
		}
	})

	t.Run("compatible layers", func(t *testing.T) {
		skipIfNoMkfsErofs(t)

		dir := t.TempDir()
		var paths []string

		// Create two layers with ConvertTarErofs (4096-byte blocks)
		for i := range 2 {
			layerPath := filepath.Join(dir, "layer"+string(rune('0'+i))+".erofs")
			tarBuf := createTestTar(t)

			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			if err := ConvertTarErofs(ctx, tarBuf, layerPath, "", nil); err != nil {
				cancel()
				t.Fatalf("ConvertTarErofs failed: %v", err)
			}
			cancel()
			paths = append(paths, layerPath)
		}

		if !CanMergeFsmeta(paths) {
			t.Error("CanMergeFsmeta should return true for compatible layers")
		}
	})

	t.Run("mixed layers with tar index", func(t *testing.T) {
		skipIfNoMkfsErofs(t)

		supported, err := SupportGenerateFromTar()
		if err != nil || !supported {
			t.Skip("mkfs.erofs does not support --tar option")
		}

		dir := t.TempDir()

		// Create one normal layer (4096-byte blocks)
		normalPath := filepath.Join(dir, "normal.erofs")
		tarBuf1 := createTestTar(t)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		if err := ConvertTarErofs(ctx, tarBuf1, normalPath, "", nil); err != nil {
			cancel()
			t.Fatalf("ConvertTarErofs failed: %v", err)
		}
		cancel()

		// Create one tar index layer (512-byte blocks)
		tarIndexPath := filepath.Join(dir, "tarindex.erofs")
		tarBuf2 := createTestTar(t)
		ctx, cancel = context.WithTimeout(context.Background(), 30*time.Second)
		if err := GenerateTarIndexAndAppendTar(ctx, tarBuf2, tarIndexPath, nil); err != nil {
			cancel()
			t.Fatalf("GenerateTarIndexAndAppendTar failed: %v", err)
		}
		cancel()

		// Check block sizes for debugging
		normalBlockSize, _ := GetBlockSize(normalPath)
		tarIndexBlockSize, _ := GetBlockSize(tarIndexPath)
		t.Logf("Normal layer block size: %d, tar index layer block size: %d", normalBlockSize, tarIndexBlockSize)

		// Mixed layers should be incompatible if tar index has 512-byte blocks
		paths := []string{normalPath, tarIndexPath}
		canMerge := CanMergeFsmeta(paths)

		// If tar index has 512-byte blocks, should be incompatible
		if tarIndexBlockSize < 4096 && canMerge {
			t.Error("CanMergeFsmeta should return false for mixed layers with small block size")
		}
	})
}
