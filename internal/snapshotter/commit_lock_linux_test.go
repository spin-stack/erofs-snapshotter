//go:build linux

package snapshotter

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/containerd/containerd/v2/core/snapshots"
	"github.com/containerd/containerd/v2/pkg/testutil"
	"github.com/containerd/errdefs"
	"golang.org/x/sys/unix"

	"github.com/spin-stack/erofs-snapshotter/internal/erofs"
)

// holdOFDWriteLock takes a whole-file OFD write lock on path (the same lock
// family QEMU uses for its images) and returns the open file; closing it
// releases the lock. This simulates a VM holding the rwlayer image.
func holdOFDWriteLock(t *testing.T, path string) *os.File {
	t.Helper()
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("open %s for locking: %v", path, err)
	}
	flk := unix.Flock_t{Type: unix.F_WRLCK, Whence: 0, Start: 0, Len: 0}
	if err := unix.FcntlFlock(f.Fd(), unix.F_OFD_SETLK, &flk); err != nil {
		_ = f.Close()
		t.Fatalf("take OFD write lock on %s: %v", path, err)
	}
	t.Cleanup(func() { _ = f.Close() })
	return f
}

// createExt4WithUpper builds an ext4 image at path and seeds its root with the
// guest overlay layout (upper/ containing the given files, plus work/),
// mirroring what a VM runtime writes into rwlayer.img. Requires root and
// mkfs.ext4. The image is left unmounted with no lock held.
func createExt4WithUpper(t *testing.T, path string, files map[string]string) {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(32 * 1024 * 1024); err != nil {
		f.Close()
		t.Fatal(err)
	}
	f.Close()

	if out, err := exec.Command("mkfs.ext4", "-q", "-F", path).CombinedOutput(); err != nil {
		t.Fatalf("mkfs.ext4 %s: %v: %s", path, err, out)
	}

	mnt := t.TempDir()
	if out, err := exec.Command("mount", "-o", "loop", path, mnt).CombinedOutput(); err != nil {
		t.Skipf("loop mount unavailable: %v: %s", err, out)
	}
	mounted := true
	defer func() {
		if mounted {
			_ = exec.Command("umount", mnt).Run()
		}
	}()

	for _, d := range []string{"upper", "work"} {
		if err := os.MkdirAll(filepath.Join(mnt, d), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	for name, content := range files {
		p := filepath.Join(mnt, "upper", name)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	if out, err := exec.Command("umount", mnt).CombinedOutput(); err != nil {
		t.Fatalf("umount %s: %v: %s", mnt, err, out)
	}
	mounted = false
}

// snapshotWithRwLayer creates a snapshot directory with an empty rwlayer.img
// (no upper inside) and an fs/ directory, the shape getCommitUpperDir sees for
// a runtime active snapshot whose ext4 is not mounted on the host.
func snapshotWithRwLayer(t *testing.T, root, id string) (rwLayer string) {
	t.Helper()
	s := &snapshotter{root: root}
	snapshotDir := s.snapshotDir(id)
	if err := os.MkdirAll(filepath.Join(snapshotDir, fsDirName), 0o755); err != nil {
		t.Fatal(err)
	}
	rwLayer = s.writablePath(id)
	if err := os.WriteFile(rwLayer, []byte("placeholder"), 0o644); err != nil {
		t.Fatal(err)
	}
	return rwLayer
}

// Test 1: a runtime active snapshot whose rwlayer.img is held by the VM (OFD
// lock) must NOT be committable - getCommitUpperDir mounts the ext4 itself,
// hits the lock gate and surfaces ErrFailedPrecondition. Critically it must
// not silently fall back to the empty fs/ and commit an empty layer. No root
// needed: the lock conflict is detected before any mount syscall.
func TestGetCommitUpperDirRejectsLockedImage(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	s := &snapshotter{root: root}
	rwLayer := snapshotWithRwLayer(t, root, "locked")

	holdOFDWriteLock(t, rwLayer)

	upperDir, cleanup, _, err := s.getCommitUpperDir(ctx, "locked", false)
	if err == nil {
		cleanup()
		t.Fatalf("getCommitUpperDir = %q, want ErrFailedPrecondition (image is locked)", upperDir)
	}
	if !errors.Is(err, errdefs.ErrFailedPrecondition) {
		t.Fatalf("error = %v, want ErrFailedPrecondition", err)
	}
	// Must not have fallen back to fs/.
	if upperDir == s.upperPath("locked") {
		t.Error("getCommitUpperDir fell back to fs/ instead of failing on the locked image")
	}
}

// Test 2: a runtime active snapshot whose container is stopped (no lock held)
// commits successfully - getCommitUpperDir mounts the ext4 read-only and
// returns the guest's upper/, and commitBlock produces a valid, non-empty
// EROFS layer.
func TestRuntimeActiveCommitSucceeds(t *testing.T) {
	testutil.RequiresRoot(t)
	requireBinary(t, "mkfs.ext4")
	requireBinary(t, "mkfs.erofs")

	ctx := context.Background()
	root := t.TempDir()
	s := &snapshotter{root: root}
	id := "runtime"
	if err := os.MkdirAll(filepath.Join(s.snapshotDir(id), fsDirName), 0o755); err != nil {
		t.Fatal(err)
	}
	createExt4WithUpper(t, s.writablePath(id), map[string]string{
		"committed.txt": "data written by the guest",
	})

	upperDir, cleanup, prune, err := s.getCommitUpperDir(ctx, id, false)
	if err != nil {
		t.Fatalf("getCommitUpperDir: %v", err)
	}
	defer cleanup()

	if prune {
		t.Error("prune = true, want false for a read-only mounted ext4")
	}
	if upperDir != s.blockUpperPath(id) {
		t.Errorf("upperDir = %q, want %q", upperDir, s.blockUpperPath(id))
	}
	got, err := os.ReadFile(filepath.Join(upperDir, "committed.txt"))
	if err != nil {
		t.Fatalf("read guest upper file: %v", err)
	}
	if string(got) != "data written by the guest" {
		t.Errorf("upper content = %q, want the guest's data", got)
	}

	// End to end: the fallback conversion must yield a valid EROFS blob.
	layerBlob := s.fallbackLayerBlobPath(id)
	if err := s.commitBlock(ctx, layerBlob, id, false); err != nil {
		t.Fatalf("commitBlock: %v", err)
	}
	if _, err := erofs.GetBlockSize(layerBlob); err != nil {
		t.Fatalf("committed blob is not a valid EROFS image: %v", err)
	}
	fi, err := os.Stat(layerBlob)
	if err != nil || fi.Size() == 0 {
		t.Fatalf("committed layer is empty or missing: size=%v err=%v", fiSize(fi), err)
	}
}

// Test 3: the quiesced label lets a hot commit read the rwlayer of a paused
// VM. With quiesced=true the lock gate is skipped, so a commit succeeds even
// while the image is OFD-locked (as a paused QEMU holds it); with
// quiesced=false the same lock is rejected.
func TestQuiescedCommitSkipsLock(t *testing.T) {
	testutil.RequiresRoot(t)
	requireBinary(t, "mkfs.ext4")

	ctx := context.Background()
	root := t.TempDir()
	s := &snapshotter{root: root}
	id := "quiesced"
	if err := os.MkdirAll(filepath.Join(s.snapshotDir(id), fsDirName), 0o755); err != nil {
		t.Fatal(err)
	}
	createExt4WithUpper(t, s.writablePath(id), map[string]string{
		"hot.txt": "committed while paused",
	})

	// Simulate the paused VM still holding the image lock.
	holdOFDWriteLock(t, s.writablePath(id))

	// Without quiesced, the held lock blocks the commit.
	if _, cleanup, _, err := s.getCommitUpperDir(ctx, id, false); !errors.Is(err, errdefs.ErrFailedPrecondition) {
		cleanup()
		t.Fatalf("getCommitUpperDir(quiesced=false) error = %v, want ErrFailedPrecondition", err)
	}

	// With quiesced, the lock gate is skipped and the upper is readable.
	upperDir, cleanup, prune, err := s.getCommitUpperDir(ctx, id, true)
	if err != nil {
		t.Fatalf("getCommitUpperDir(quiesced=true): %v", err)
	}
	defer cleanup()

	if prune {
		t.Error("prune = true, want false for a read-only mounted ext4")
	}
	got, err := os.ReadFile(filepath.Join(upperDir, "hot.txt"))
	if err != nil {
		t.Fatalf("read guest upper file under quiesced commit: %v", err)
	}
	if string(got) != "committed while paused" {
		t.Errorf("upper content = %q, want the guest's data", got)
	}
}

// TestCommitOptsQuiesced verifies the label is read from commit opts.
func TestCommitOptsQuiesced(t *testing.T) {
	cases := []struct {
		name string
		opts []snapshots.Opt
		want bool
	}{
		{"no opts", nil, false},
		{"unrelated label", []snapshots.Opt{snapshots.WithLabels(map[string]string{"foo": "bar"})}, false},
		{"quiesced false value", []snapshots.Opt{snapshots.WithLabels(map[string]string{quiescedLabel: "false"})}, false},
		{"quiesced true", []snapshots.Opt{snapshots.WithLabels(map[string]string{quiescedLabel: "true"})}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := commitOptsQuiesced(tc.opts)
			if err != nil {
				t.Fatalf("commitOptsQuiesced: %v", err)
			}
			if got != tc.want {
				t.Errorf("commitOptsQuiesced = %v, want %v", got, tc.want)
			}
		})
	}
}

func fiSize(fi os.FileInfo) int64 {
	if fi == nil {
		return -1
	}
	return fi.Size()
}

func requireBinary(t *testing.T, name string) {
	t.Helper()
	if _, err := exec.LookPath(name); err != nil {
		t.Skipf("%s not installed", name)
	}
}
