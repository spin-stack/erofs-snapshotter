//go:build linux

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
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/containerd/containerd/v2/core/mount"
	"github.com/containerd/containerd/v2/pkg/testutil"
	"github.com/containerd/errdefs"
	"golang.org/x/sys/unix"

	"github.com/spin-stack/erofs-snapshotter/internal/loop"
)

// requireBinary skips the test if the named binary is not in PATH.
func requireBinary(t *testing.T, name string) {
	t.Helper()
	if _, err := exec.LookPath(name); err != nil {
		t.Skipf("%s not found in PATH: %v", name, err)
	}
}

// erofsKernelSupported reports whether the running kernel knows the erofs
// filesystem. Checked after a failed mount attempt so that module autoloading
// triggered by the mount has already happened.
func erofsKernelSupported() bool {
	data, err := os.ReadFile("/proc/filesystems")
	if err != nil {
		return false
	}
	return strings.Contains(string(data), "erofs")
}

// isMountPoint reports whether target appears as a mount point in
// /proc/self/mountinfo.
func isMountPoint(t *testing.T, target string) bool {
	t.Helper()
	data, err := os.ReadFile("/proc/self/mountinfo")
	if err != nil {
		t.Fatalf("failed to read mountinfo: %v", err)
	}
	for line := range strings.SplitSeq(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) >= 5 && fields[4] == target {
			return true
		}
	}
	return false
}

// verifyNoLoopDevice asserts that no loop device is backed by the given file.
// The kernel may release loop devices asynchronously, so poll briefly.
func verifyNoLoopDevice(t *testing.T, backingFile string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		dev, err := loop.FindByBackingFile(backingFile)
		if err != nil {
			t.Fatalf("FindByBackingFile(%s) failed: %v", backingFile, err)
		}
		if dev == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("loop device %s still attached to %s", dev.Path, backingFile)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// detachLoopBestEffort detaches any loop device backed by the given file.
// Used in t.Cleanup so failed tests never leak loop devices.
func detachLoopBestEffort(backingFile string) {
	if dev, err := loop.FindByBackingFile(backingFile); err == nil && dev != nil {
		_ = dev.Detach()
	}
}

// makeErofsBlob creates an EROFS layer blob containing the given files.
func makeErofsBlob(t *testing.T, workDir, name string, files map[string]string) string {
	t.Helper()
	srcDir := filepath.Join(workDir, name+"-src")
	if err := os.MkdirAll(srcDir, 0o755); err != nil {
		t.Fatalf("failed to create source dir: %v", err)
	}
	for fname, content := range files {
		if err := os.WriteFile(filepath.Join(srcDir, fname), []byte(content), 0o644); err != nil {
			t.Fatalf("failed to write %s: %v", fname, err)
		}
	}
	blob := filepath.Join(workDir, name+".erofs")
	out, err := exec.Command("mkfs.erofs", "--quiet", "-Enoinline_data", blob, srcDir).CombinedOutput()
	if err != nil {
		t.Fatalf("mkfs.erofs failed for %s: %v: %s", blob, err, out)
	}
	return blob
}

// makeFsmeta merges the given EROFS blobs into a fsmeta image, matching the
// invocation used by the snapshotter's generateFsMeta (minus the VMDK).
// Skips the test if mkfs.erofs does not support merged fsmeta generation.
func makeFsmeta(t *testing.T, workDir string, blobs ...string) string {
	t.Helper()
	fsmeta := filepath.Join(workDir, "fsmeta.erofs")
	args := append([]string{"--quiet", fsmeta}, blobs...)
	out, err := exec.Command("mkfs.erofs", args...).CombinedOutput()
	if err != nil {
		t.Skipf("mkfs.erofs does not support merged fsmeta: %v: %s", err, out)
	}
	return fsmeta
}

// makePlainFile creates a file of the given size filled with zeros.
func makePlainFile(t *testing.T, path string, size int64) {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("failed to create %s: %v", path, err)
	}
	if err := f.Truncate(size); err != nil {
		f.Close()
		t.Fatalf("failed to truncate %s: %v", path, err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("failed to close %s: %v", path, err)
	}
}

// makeExt4Image creates an ext4 image file of the given size.
func makeExt4Image(t *testing.T, path string, size int64) {
	t.Helper()
	makePlainFile(t, path, size)
	out, err := exec.Command("mkfs.ext4", "-q", "-F", path).CombinedOutput()
	if err != nil {
		t.Fatalf("mkfs.ext4 failed for %s: %v: %s", path, err, out)
	}
}

func TestMountAllPlainBind(t *testing.T) {
	testutil.RequiresRoot(t)

	srcDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(srcDir, "hello.txt"), []byte("hello"), 0o644); err != nil {
		t.Fatalf("failed to write source file: %v", err)
	}
	target := t.TempDir()

	mounts := []mount.Mount{{
		Type:    "bind",
		Source:  srcDir,
		Options: []string{"rbind", "ro"},
	}}

	cleanup, err := MountAll(mounts, target)
	if err != nil {
		t.Fatalf("MountAll failed: %v", err)
	}
	cleaned := false
	t.Cleanup(func() {
		if !cleaned {
			_ = cleanup()
		}
	})

	data, err := os.ReadFile(filepath.Join(target, "hello.txt"))
	if err != nil {
		t.Fatalf("failed to read file through bind mount: %v", err)
	}
	if string(data) != "hello" {
		t.Errorf("unexpected content: got %q, want %q", data, "hello")
	}

	if err := cleanup(); err != nil {
		t.Fatalf("cleanup failed: %v", err)
	}
	cleaned = true

	if isMountPoint(t, target) {
		t.Errorf("target %s still mounted after cleanup", target)
	}
	if _, err := os.Stat(filepath.Join(target, "hello.txt")); !os.IsNotExist(err) {
		t.Errorf("expected file to be gone after unmount, got err=%v", err)
	}
}

func TestMountAllPlainErrorUnwindsPartialMounts(t *testing.T) {
	testutil.RequiresRoot(t)

	srcDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(srcDir, "ok.txt"), []byte("ok"), 0o644); err != nil {
		t.Fatalf("failed to write source file: %v", err)
	}
	target := t.TempDir()
	t.Cleanup(func() {
		_ = mount.UnmountAll(target, 0)
	})

	// First mount succeeds, second fails (nonexistent source). MountAll must
	// unwind the partial first mount.
	mounts := []mount.Mount{
		{Type: "bind", Source: srcDir, Options: []string{"rbind", "ro"}},
		{Type: "bind", Source: filepath.Join(srcDir, "nonexistent"), Options: []string{"rbind", "ro"}},
	}

	cleanup, err := MountAll(mounts, target)
	if err == nil {
		_ = cleanup()
		t.Fatal("expected MountAll to fail for nonexistent source")
	}
	// On error MountAll returns a nop cleanup that must be safe to call.
	if cerr := cleanup(); cerr != nil {
		t.Errorf("nop cleanup returned error: %v", cerr)
	}

	if isMountPoint(t, target) {
		t.Errorf("partial mount left behind at %s", target)
	}
}

func TestMountAllErofsMultiDevice(t *testing.T) {
	testutil.RequiresRoot(t)
	requireBinary(t, "mkfs.erofs")

	workDir := t.TempDir()
	blob1 := makeErofsBlob(t, workDir, "layer1", map[string]string{"a.txt": "from layer one"})
	blob2 := makeErofsBlob(t, workDir, "layer2", map[string]string{"b.txt": "from layer two"})
	fsmeta := makeFsmeta(t, workDir, blob1, blob2)

	target := t.TempDir()
	t.Cleanup(func() {
		_ = mount.UnmountAll(target, 0)
		for _, p := range []string{fsmeta, blob1, blob2} {
			detachLoopBestEffort(p)
		}
	})

	mounts := []mount.Mount{{
		Type:    "erofs",
		Source:  fsmeta,
		Options: []string{"ro", "loop", "device=" + blob1, "device=" + blob2},
	}}

	cleanup, err := MountAll(mounts, target)
	if err != nil {
		if !erofsKernelSupported() {
			t.Skipf("kernel lacks erofs support: %v", err)
		}
		t.Fatalf("MountAll failed: %v", err)
	}
	cleaned := false
	t.Cleanup(func() {
		if !cleaned {
			_ = cleanup()
		}
	})

	// Files from BOTH layers must be visible through the merged fsmeta.
	for file, want := range map[string]string{
		"a.txt": "from layer one",
		"b.txt": "from layer two",
	} {
		data, err := os.ReadFile(filepath.Join(target, file))
		if err != nil {
			t.Fatalf("failed to read %s from merged mount: %v", file, err)
		}
		if string(data) != want {
			t.Errorf("%s: got %q, want %q", file, data, want)
		}
	}

	if err := cleanup(); err != nil {
		t.Fatalf("cleanup failed: %v", err)
	}
	cleaned = true

	if isMountPoint(t, target) {
		t.Errorf("target %s still mounted after cleanup", target)
	}
	for _, p := range []string{fsmeta, blob1, blob2} {
		verifyNoLoopDevice(t, p)
	}
}

func TestMountAllErofsMultiDeviceSiblingGuard(t *testing.T) {
	target := t.TempDir()

	// A multi-device EROFS mount with a sibling mount must be rejected before
	// any loop device is touched, so this needs neither root nor real files.
	mounts := []mount.Mount{
		{
			Type:    "format/erofs",
			Source:  "/nonexistent/fsmeta.erofs",
			Options: []string{"ro", "loop", "device=/nonexistent/layer.erofs"},
		},
		{
			Type:    "ext4",
			Source:  "/nonexistent/rwlayer.img",
			Options: []string{"rw", "loop"},
		},
	}

	cleanup, err := MountAll(mounts, target)
	if err == nil {
		_ = cleanup()
		t.Fatal("expected MountAll to reject multi-device mount with sibling")
	}
	if !strings.Contains(err.Error(), "must be the only mount") {
		t.Errorf("unexpected error: %v", err)
	}
	if cerr := cleanup(); cerr != nil {
		t.Errorf("nop cleanup returned error: %v", cerr)
	}
}

func TestMountAllErofsMultiDeviceLoopSetupFailure(t *testing.T) {
	testutil.RequiresRoot(t)

	workDir := t.TempDir()
	fsmeta := filepath.Join(workDir, "fsmeta.erofs")
	// Content is irrelevant: the failure happens before any mount attempt.
	makePlainFile(t, fsmeta, 1024*1024)
	t.Cleanup(func() {
		detachLoopBestEffort(fsmeta)
	})

	target := t.TempDir()
	mounts := []mount.Mount{{
		Type:    "erofs",
		Source:  fsmeta,
		Options: []string{"ro", "loop", "device=" + filepath.Join(workDir, "missing.erofs")},
	}}

	cleanup, err := MountAll(mounts, target)
	if err == nil {
		_ = cleanup()
		t.Fatal("expected MountAll to fail for nonexistent device blob")
	}
	if !strings.Contains(err.Error(), "failed to setup loop device") {
		t.Errorf("unexpected error: %v", err)
	}
	if cerr := cleanup(); cerr != nil {
		t.Errorf("nop cleanup returned error: %v", cerr)
	}

	// The fsmeta loop device acquired before the failure must be released.
	verifyNoLoopDevice(t, fsmeta)
}

func TestMountExt4(t *testing.T) {
	testutil.RequiresRoot(t)
	requireBinary(t, "mkfs.ext4")

	workDir := t.TempDir()
	img := filepath.Join(workDir, "rwlayer.img")
	makeExt4Image(t, img, 8*1024*1024)

	target := t.TempDir()
	t.Cleanup(func() {
		_ = mount.UnmountAll(target, 0)
		detachLoopBestEffort(img)
	})

	cleanup, err := MountExt4(img, target)
	if err != nil {
		t.Fatalf("MountExt4 failed: %v", err)
	}
	cleaned := false
	t.Cleanup(func() {
		if !cleaned {
			_ = cleanup()
		}
	})

	// The filesystem must be readable.
	if _, err := os.Stat(filepath.Join(target, "lost+found")); err != nil {
		t.Errorf("expected lost+found in mounted ext4: %v", err)
	}

	// The mount must be read-only even for root.
	if werr := os.WriteFile(filepath.Join(target, "should-fail.txt"), []byte("x"), 0o644); werr == nil {
		t.Error("expected write to read-only ext4 mount to fail")
	}

	// The image lock must be held while mounted.
	if f, lerr := lockImageFile(img); lerr == nil {
		f.Close()
		t.Error("expected lockImageFile to fail while image is mounted")
	} else if !errdefs.IsFailedPrecondition(lerr) {
		t.Errorf("expected FailedPrecondition while mounted, got: %v", lerr)
	}

	if err := cleanup(); err != nil {
		t.Fatalf("cleanup failed: %v", err)
	}
	cleaned = true

	if isMountPoint(t, target) {
		t.Errorf("target %s still mounted after cleanup", target)
	}
	verifyNoLoopDevice(t, img)

	// The image lock must be released after cleanup.
	f, err := lockImageFile(img)
	if err != nil {
		t.Fatalf("image lock not released after cleanup: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Errorf("failed to close lock file: %v", err)
	}
}

func TestMountExt4MountFailureReleasesResources(t *testing.T) {
	testutil.RequiresRoot(t)

	workDir := t.TempDir()
	img := filepath.Join(workDir, "rwlayer.img")
	// A plain zero-filled file: locking and loop setup succeed, but the ext4
	// mount fails, exercising the unwind path.
	makePlainFile(t, img, 1024*1024)
	t.Cleanup(func() {
		detachLoopBestEffort(img)
	})

	cleanup, err := MountExt4(img, t.TempDir())
	if err == nil {
		_ = cleanup()
		t.Fatal("expected MountExt4 to fail for non-ext4 image")
	}
	if !strings.Contains(err.Error(), "failed to mount ext4") {
		t.Errorf("unexpected error: %v", err)
	}
	if cerr := cleanup(); cerr != nil {
		t.Errorf("nop cleanup returned error: %v", cerr)
	}

	// The loop device set up before the failed mount must be detached.
	verifyNoLoopDevice(t, img)

	// The image lock must be released on failure.
	f, err := lockImageFile(img)
	if err != nil {
		t.Fatalf("image lock not released after failed mount: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Errorf("failed to close lock file: %v", err)
	}
}

func TestMountExt4InUseFlock(t *testing.T) {
	workDir := t.TempDir()
	img := filepath.Join(workDir, "rwlayer.img")
	// MountExt4 fails at the locking stage, before any loop device or mount
	// is attempted, so a plain file and no root are sufficient.
	makePlainFile(t, img, 1024*1024)

	f, err := os.OpenFile(img, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("failed to open image: %v", err)
	}
	defer f.Close()
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		t.Fatalf("failed to flock image: %v", err)
	}

	cleanup, err := MountExt4(img, t.TempDir())
	if err == nil {
		_ = cleanup()
		t.Fatal("expected MountExt4 to fail while image is flocked")
	}
	if !errdefs.IsFailedPrecondition(err) {
		t.Errorf("expected FailedPrecondition, got: %v", err)
	}
	if !strings.Contains(err.Error(), "container is still running") {
		t.Errorf("unexpected error message: %v", err)
	}
	if cerr := cleanup(); cerr != nil {
		t.Errorf("nop cleanup returned error: %v", cerr)
	}
}

func TestMountExt4InUseOFDLock(t *testing.T) {
	workDir := t.TempDir()
	img := filepath.Join(workDir, "rwlayer.img")
	makePlainFile(t, img, 1024*1024)

	// Hold an OFD write lock on a separate open file description, the way
	// QEMU protects its disk images. flock does not see it, so this drives
	// MountExt4 through the OFD conflict branch.
	f, err := os.OpenFile(img, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("failed to open image: %v", err)
	}
	defer f.Close()
	flk := unix.Flock_t{Type: unix.F_WRLCK, Whence: 0, Start: 0, Len: 0}
	if err := unix.FcntlFlock(f.Fd(), unix.F_OFD_SETLK, &flk); err != nil {
		t.Fatalf("failed to take OFD lock: %v", err)
	}

	cleanup, err := MountExt4(img, t.TempDir())
	if err == nil {
		_ = cleanup()
		t.Fatal("expected MountExt4 to fail while image holds an OFD lock")
	}
	if !errdefs.IsFailedPrecondition(err) {
		t.Errorf("expected FailedPrecondition, got: %v", err)
	}
	if !strings.Contains(err.Error(), "locked by the VM") {
		t.Errorf("unexpected error message: %v", err)
	}
	if cerr := cleanup(); cerr != nil {
		t.Errorf("nop cleanup returned error: %v", cerr)
	}
}

func TestLockImageFileReleasesOnClose(t *testing.T) {
	workDir := t.TempDir()
	img := filepath.Join(workDir, "rwlayer.img")
	makePlainFile(t, img, 1024*1024)

	f1, err := lockImageFile(img)
	if err != nil {
		t.Fatalf("lockImageFile failed: %v", err)
	}

	// While held, a second acquisition must fail.
	if f, err := lockImageFile(img); err == nil {
		f.Close()
		f1.Close()
		t.Fatal("expected second lockImageFile to fail while lock is held")
	} else if !errdefs.IsFailedPrecondition(err) {
		t.Errorf("expected FailedPrecondition, got: %v", err)
	}

	if err := f1.Close(); err != nil {
		t.Fatalf("failed to close lock file: %v", err)
	}

	// After Close, the locks must be released.
	f2, err := lockImageFile(img)
	if err != nil {
		t.Fatalf("lockImageFile failed after release: %v", err)
	}
	if err := f2.Close(); err != nil {
		t.Errorf("failed to close lock file: %v", err)
	}
}

func TestLockImageFileFlockConflict(t *testing.T) {
	workDir := t.TempDir()
	img := filepath.Join(workDir, "rwlayer.img")
	makePlainFile(t, img, 1024*1024)

	f, err := os.OpenFile(img, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("failed to open image: %v", err)
	}
	defer f.Close()
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		t.Fatalf("failed to flock image: %v", err)
	}

	if lf, err := lockImageFile(img); err == nil {
		lf.Close()
		t.Fatal("expected lockImageFile to fail while flock is held")
	} else if !errdefs.IsFailedPrecondition(err) {
		t.Errorf("expected FailedPrecondition, got: %v", err)
	} else if !strings.Contains(err.Error(), "is in use") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestLockImageFileNonexistent(t *testing.T) {
	_, err := lockImageFile(filepath.Join(t.TempDir(), "missing.img"))
	if err == nil {
		t.Fatal("expected lockImageFile to fail for nonexistent path")
	}
	if errdefs.IsFailedPrecondition(err) {
		t.Errorf("open failure must not be FailedPrecondition: %v", err)
	}
}
