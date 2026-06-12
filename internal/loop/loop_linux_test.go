package loop

import (
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/containerd/containerd/v2/pkg/testutil"
)

// testSerialPrefix is the prefix used for all test loop device serials.
// This allows cleanup of orphaned devices from interrupted tests.
const testSerialPrefix = "erofs-test-"

// readSysfsBackingFile returns the full (untruncated) backing file path of a
// loop device from /sys/block/loopN/loop/backing_file.
func readSysfsBackingFile(t *testing.T, devNumber int) string {
	t.Helper()
	data, err := os.ReadFile(fmt.Sprintf("/sys/block/loop%d/loop/backing_file", devNumber))
	if err != nil {
		t.Fatalf("failed to read sysfs backing_file for loop%d: %v", devNumber, err)
	}
	return strings.TrimSpace(string(data))
}

// verifyDetached asserts that the loop device released its backing file.
// After LOOP_CLR_FD the kernel removes /sys/block/loopN/loop/backing_file
// (the whole loop attribute group disappears for unconfigured devices), but
// it may do so asynchronously, so poll with a short deadline.
func verifyDetached(t *testing.T, dev *Device) {
	t.Helper()
	sysfsPath := fmt.Sprintf("/sys/block/loop%d/loop/backing_file", dev.Number)
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := os.Stat(sysfsPath); os.IsNotExist(err) {
			return
		}
		if time.Now().After(deadline) {
			data, _ := os.ReadFile(sysfsPath)
			t.Fatalf("loop device %s still attached after detach: backing_file=%q",
				dev.Path, strings.TrimSpace(string(data)))
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestMain handles test setup and cleanup, including signal handling
// to clean up loop devices if tests are interrupted.
func TestMain(m *testing.M) {
	// Clean up any orphaned loop devices from previous test runs
	if n, err := CleanupBySerialPrefix(testSerialPrefix); err == nil && n > 0 {
		fmt.Fprintf(os.Stderr, "cleaned up %d orphaned test loop devices\n", n)
	}

	// Set up signal handler for cleanup on interruption
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Run cleanup in background goroutine if interrupted
	done := make(chan struct{})
	go func() {
		select {
		case <-sigChan:
			fmt.Fprintf(os.Stderr, "\ninterrupted, cleaning up test loop devices...\n")
			if n, err := CleanupBySerialPrefix(testSerialPrefix); err != nil {
				fmt.Fprintf(os.Stderr, "cleanup error: %v\n", err)
			} else if n > 0 {
				fmt.Fprintf(os.Stderr, "cleaned up %d test loop devices\n", n)
			}
			os.Exit(1)
		case <-done:
			return
		}
	}()

	// Run tests
	code := m.Run()

	// Stop signal handler
	close(done)
	signal.Stop(sigChan)

	// Final cleanup
	if n, _ := CleanupBySerialPrefix(testSerialPrefix); n > 0 {
		fmt.Fprintf(os.Stderr, "cleaned up %d test loop devices after tests\n", n)
	}

	os.Exit(code)
}

func TestSetupAndDetach(t *testing.T) {
	testutil.RequiresRoot(t)
	// Don't run loop device tests in parallel - they share system resources

	// Create a temporary file to use as backing file
	tmpDir := t.TempDir()
	backingFile := filepath.Join(tmpDir, "backing.img")

	// Create a 1MB file
	f, err := os.Create(backingFile)
	if err != nil {
		t.Fatalf("failed to create backing file: %v", err)
	}
	if err := f.Truncate(1024 * 1024); err != nil {
		f.Close()
		t.Fatalf("failed to truncate backing file: %v", err)
	}
	f.Close()

	// Setup loop device with serial to match udev rules (erofs-* prefix)
	dev, err := Setup(backingFile, Config{
		ReadOnly: true,
		Serial:   "erofs-test-setup-detach",
	})
	if err != nil {
		t.Fatalf("Setup failed: %v", err)
	}

	// Verify device was created
	if dev.Path == "" {
		t.Fatal("expected non-empty device path")
	}
	if !strings.HasPrefix(dev.Path, "/dev/loop") {
		t.Errorf("expected device path to start with /dev/loop, got %s", dev.Path)
	}

	t.Logf("created loop device: %s (number: %d)", dev.Path, dev.Number)

	// Verify device exists
	if _, err := os.Stat(dev.Path); err != nil {
		t.Errorf("device %s does not exist: %v", dev.Path, err)
	}

	// Get device info
	info, err := dev.GetInfo()
	if err != nil {
		t.Fatalf("GetInfo failed: %v", err)
	}

	// Verify flags
	if info.Flags&LoFlagsReadOnly == 0 {
		t.Error("expected read-only flag to be set")
	}

	// Verify backing file.
	// LoopInfo64.FileName is truncated to LO_NAME_SIZE (64 bytes including
	// the NUL terminator), so a strict equality check would fail with long
	// TMPDIR paths. The ioctl value must be a prefix of the real path; the
	// full path is verified via sysfs, which is not truncated.
	gotBackingFile := info.BackingFile()
	if !strings.HasPrefix(backingFile, gotBackingFile) {
		t.Errorf("backing file mismatch: got %s, want prefix of %s", gotBackingFile, backingFile)
	}
	if sysfsBacking := readSysfsBackingFile(t, dev.Number); sysfsBacking != backingFile {
		t.Errorf("sysfs backing file mismatch: got %s, want %s", sysfsBacking, backingFile)
	}

	// Detach
	if err := dev.Detach(); err != nil {
		t.Fatalf("Detach failed: %v", err)
	}

	// Verify the device actually released its backing file (the kernel may
	// clear the association asynchronously, so this retries briefly).
	verifyDetached(t, dev)
}

func TestSetupWithOffset(t *testing.T) {
	testutil.RequiresRoot(t)

	tmpDir := t.TempDir()
	backingFile := filepath.Join(tmpDir, "backing.img")

	// Create a 2MB file
	f, err := os.Create(backingFile)
	if err != nil {
		t.Fatalf("failed to create backing file: %v", err)
	}
	if err := f.Truncate(2 * 1024 * 1024); err != nil {
		f.Close()
		t.Fatalf("failed to truncate backing file: %v", err)
	}
	f.Close()

	offset := uint64(512 * 1024)     // 512KB offset
	sizeLimit := uint64(1024 * 1024) // 1MB size limit

	dev, err := Setup(backingFile, Config{
		ReadOnly:  true,
		Offset:    offset,
		SizeLimit: sizeLimit,
		Serial:    "erofs-test-offset",
	})
	if err != nil {
		t.Fatalf("Setup failed: %v", err)
	}
	defer dev.Detach()

	info, err := dev.GetInfo()
	if err != nil {
		t.Fatalf("GetInfo failed: %v", err)
	}

	if info.Offset != offset {
		t.Errorf("offset mismatch: got %d, want %d", info.Offset, offset)
	}
	if info.SizeLimit != sizeLimit {
		t.Errorf("size limit mismatch: got %d, want %d", info.SizeLimit, sizeLimit)
	}
}

func TestSetupReadWrite(t *testing.T) {
	testutil.RequiresRoot(t)

	tmpDir := t.TempDir()
	backingFile := filepath.Join(tmpDir, "backing.img")

	f, err := os.Create(backingFile)
	if err != nil {
		t.Fatalf("failed to create backing file: %v", err)
	}
	if err := f.Truncate(1024 * 1024); err != nil {
		f.Close()
		t.Fatalf("failed to truncate backing file: %v", err)
	}
	f.Close()

	// Setup read-write loop device
	dev, err := Setup(backingFile, Config{
		ReadOnly: false,
		Serial:   "erofs-test-readwrite",
	})
	if err != nil {
		t.Fatalf("Setup failed: %v", err)
	}
	defer dev.Detach()

	info, err := dev.GetInfo()
	if err != nil {
		t.Fatalf("GetInfo failed: %v", err)
	}

	if info.Flags&LoFlagsReadOnly != 0 {
		t.Error("expected read-only flag to NOT be set")
	}
}

func TestSerial(t *testing.T) {
	testutil.RequiresRoot(t)

	tmpDir := t.TempDir()
	backingFile := filepath.Join(tmpDir, "backing.img")

	f, err := os.Create(backingFile)
	if err != nil {
		t.Fatalf("failed to create backing file: %v", err)
	}
	if err := f.Truncate(1024 * 1024); err != nil {
		f.Close()
		t.Fatalf("failed to truncate backing file: %v", err)
	}
	f.Close()

	serial := "erofs-test-serial-12345"

	dev, err := Setup(backingFile, Config{
		ReadOnly: true,
		Serial:   serial,
	})
	if err != nil {
		t.Fatalf("Setup failed: %v", err)
	}
	defer dev.Detach()

	// Check if serial support is available (Linux 5.17+)
	gotSerial := dev.GetSerial()
	if gotSerial == "" {
		t.Skip("serial support not available (requires Linux 5.17+)")
	}

	// Trim any newline
	gotSerial = strings.TrimSpace(gotSerial)
	if gotSerial != serial {
		t.Errorf("serial mismatch: got %q, want %q", gotSerial, serial)
	}

	t.Logf("serial set successfully: %s", gotSerial)
}

func TestFindByBackingFile(t *testing.T) {
	testutil.RequiresRoot(t)

	tmpDir := t.TempDir()
	backingFile := filepath.Join(tmpDir, "backing.img")

	f, err := os.Create(backingFile)
	if err != nil {
		t.Fatalf("failed to create backing file: %v", err)
	}
	if err := f.Truncate(1024 * 1024); err != nil {
		f.Close()
		t.Fatalf("failed to truncate backing file: %v", err)
	}
	f.Close()

	dev, err := Setup(backingFile, Config{
		ReadOnly: true,
		Serial:   "erofs-test-find-backing",
	})
	if err != nil {
		t.Fatalf("Setup failed: %v", err)
	}
	defer dev.Detach()

	// Find by backing file
	found, err := FindByBackingFile(backingFile)
	if err != nil {
		t.Fatalf("FindByBackingFile failed: %v", err)
	}
	if found == nil {
		t.Fatal("expected to find loop device")
	}
	if found.Path != dev.Path {
		t.Errorf("path mismatch: got %s, want %s", found.Path, dev.Path)
	}
	if found.Number != dev.Number {
		t.Errorf("number mismatch: got %d, want %d", found.Number, dev.Number)
	}
}

func TestFindBySerial(t *testing.T) {
	testutil.RequiresRoot(t)

	tmpDir := t.TempDir()
	backingFile := filepath.Join(tmpDir, "backing.img")

	f, err := os.Create(backingFile)
	if err != nil {
		t.Fatalf("failed to create backing file: %v", err)
	}
	if err := f.Truncate(1024 * 1024); err != nil {
		f.Close()
		t.Fatalf("failed to truncate backing file: %v", err)
	}
	f.Close()

	serial := "erofs-find-test-serial"

	dev, err := Setup(backingFile, Config{
		ReadOnly: true,
		Serial:   serial,
	})
	if err != nil {
		t.Fatalf("Setup failed: %v", err)
	}
	defer dev.Detach()

	// Check if serial was set (Linux 5.17+)
	if dev.GetSerial() == "" {
		t.Skip("serial support not available (requires Linux 5.17+)")
	}

	// Find by serial
	found, err := FindBySerial(serial)
	if err != nil {
		t.Fatalf("FindBySerial failed: %v", err)
	}
	if found == nil {
		t.Fatal("expected to find loop device by serial")
	}
	if found.Path != dev.Path {
		t.Errorf("path mismatch: got %s, want %s", found.Path, dev.Path)
	}
}

func TestDetachPath(t *testing.T) {
	testutil.RequiresRoot(t)

	tmpDir := t.TempDir()
	backingFile := filepath.Join(tmpDir, "backing.img")

	f, err := os.Create(backingFile)
	if err != nil {
		t.Fatalf("failed to create backing file: %v", err)
	}
	if err := f.Truncate(1024 * 1024); err != nil {
		f.Close()
		t.Fatalf("failed to truncate backing file: %v", err)
	}
	f.Close()

	dev, err := Setup(backingFile, Config{
		ReadOnly: true,
		Serial:   "erofs-test-detach-path",
	})
	if err != nil {
		t.Fatalf("Setup failed: %v", err)
	}

	// Detach using path
	if err := DetachPath(dev.Path); err != nil {
		t.Fatalf("DetachPath failed: %v", err)
	}

	// Verify the device actually released its backing file (the kernel may
	// clear the association asynchronously, so this retries briefly).
	verifyDetached(t, dev)
}

func TestDetachNonexistent(t *testing.T) {
	// Should not error on non-existent device
	if err := DetachPath("/dev/loop99999"); err != nil {
		t.Errorf("DetachPath should not error on non-existent device: %v", err)
	}

	if err := DetachPath(""); err != nil {
		t.Errorf("DetachPath should not error on empty path: %v", err)
	}
}

func TestMultipleDevices(t *testing.T) {
	testutil.RequiresRoot(t)

	tmpDir := t.TempDir()

	var devices []*Device
	defer func() {
		for _, dev := range devices {
			dev.Detach()
		}
	}()

	// Create multiple loop devices
	for i := range 3 {
		backingFile := filepath.Join(tmpDir, fmt.Sprintf("backing%d.img", i))
		f, err := os.Create(backingFile)
		if err != nil {
			t.Fatalf("failed to create backing file %d: %v", i, err)
		}
		if err := f.Truncate(1024 * 1024); err != nil {
			f.Close()
			t.Fatalf("failed to truncate backing file %d: %v", i, err)
		}
		f.Close()

		dev, err := Setup(backingFile, Config{
			ReadOnly: true,
			Serial:   fmt.Sprintf("erofs-test-multi-%d", i),
		})
		if err != nil {
			t.Fatalf("Setup %d failed: %v", i, err)
		}
		devices = append(devices, dev)
		t.Logf("created device %d: %s", i, dev.Path)
	}

	// Verify all devices are different
	paths := make(map[string]bool)
	for _, dev := range devices {
		if paths[dev.Path] {
			t.Errorf("duplicate device path: %s", dev.Path)
		}
		paths[dev.Path] = true
	}
}

func TestBackingFileNotFound(t *testing.T) {
	// No root required: Setup fails opening the backing file before it
	// touches /dev/loop-control.
	const missing = "/nonexistent/backing.img"
	_, err := Setup(missing, Config{ReadOnly: true})
	if err == nil {
		t.Fatal("expected error for non-existent backing file")
	}
	if !strings.Contains(err.Error(), missing) {
		t.Errorf("error should mention backing file path %q, got: %v", missing, err)
	}
}

// createBackingFile creates a sparse backing file of the given size in a
// per-test temporary directory and returns its path.
func createBackingFile(t *testing.T, size int64) string {
	t.Helper()
	backingFile := filepath.Join(t.TempDir(), "backing.img")
	f, err := os.Create(backingFile)
	if err != nil {
		t.Fatalf("failed to create backing file: %v", err)
	}
	if err := f.Truncate(size); err != nil {
		f.Close()
		t.Fatalf("failed to truncate backing file: %v", err)
	}
	f.Close()
	return backingFile
}

func TestFindBySerialNotFound(t *testing.T) {
	// Reading /sys/block does not require root.
	serial := fmt.Sprintf("erofs-test-no-such-serial-%d", os.Getpid())
	found, err := FindBySerial(serial)
	if err != nil {
		t.Fatalf("FindBySerial failed: %v", err)
	}
	if found != nil {
		t.Errorf("expected nil for unknown serial, got %s", found.Path)
	}
}

func TestFindByBackingFileNotFound(t *testing.T) {
	// File exists but has no loop device attached.
	backingFile := createBackingFile(t, 1024)
	found, err := FindByBackingFile(backingFile)
	if err != nil {
		t.Fatalf("FindByBackingFile failed: %v", err)
	}
	if found != nil {
		t.Errorf("expected nil for file without loop device, got %s", found.Path)
	}
}

func TestCleanupBySerialPrefix(t *testing.T) {
	testutil.RequiresRoot(t)

	// Unique prefix per run; still under testSerialPrefix so TestMain
	// cleanup catches orphans from interrupted runs.
	prefix := fmt.Sprintf("%scleanup-%d-", testSerialPrefix, os.Getpid())

	var devices []*Device
	for i := range 2 {
		backingFile := createBackingFile(t, 1024*1024)
		dev, err := Setup(backingFile, Config{
			ReadOnly: true,
			Serial:   fmt.Sprintf("%s%d", prefix, i),
		})
		if err != nil {
			t.Fatalf("Setup %d failed: %v", i, err)
		}
		t.Cleanup(func() { _ = dev.Detach() })
		devices = append(devices, dev)
	}

	// Serial support requires Linux 5.17+.
	if devices[0].GetSerial() == "" {
		t.Skip("serial support not available (requires Linux 5.17+)")
	}

	n, err := CleanupBySerialPrefix(prefix)
	if err != nil {
		t.Fatalf("CleanupBySerialPrefix failed: %v", err)
	}
	if n != 2 {
		t.Errorf("expected 2 devices detached, got %d", n)
	}
	for _, dev := range devices {
		verifyDetached(t, dev)
	}
}

func TestDetachIdempotent(t *testing.T) {
	testutil.RequiresRoot(t)

	backingFile := createBackingFile(t, 1024*1024)
	dev, err := Setup(backingFile, Config{
		ReadOnly: true,
		Serial:   fmt.Sprintf("erofs-test-idempotent-%d", os.Getpid()),
	})
	if err != nil {
		t.Fatalf("Setup failed: %v", err)
	}
	t.Cleanup(func() { _ = dev.Detach() })

	if err := dev.Detach(); err != nil {
		t.Fatalf("first Detach failed: %v", err)
	}
	verifyDetached(t, dev)

	// Second Detach hits the ENXIO (device not configured) path and must
	// return nil.
	if err := dev.Detach(); err != nil {
		t.Errorf("second Detach on already-detached device should return nil, got: %v", err)
	}
}

func TestDetachNonexistentDevice(t *testing.T) {
	dev := &Device{Path: "/dev/loop-does-not-exist-99999", Number: 99999}
	if err := dev.Detach(); err != nil {
		t.Errorf("Detach on nonexistent device path should return nil, got: %v", err)
	}
}

func TestDetachNonLoopFile(t *testing.T) {
	// Opening a regular file succeeds but LOOP_CLR_FD fails with a
	// non-ENXIO errno, exercising Detach's error return.
	path := filepath.Join(t.TempDir(), "not-a-loop")
	if err := os.WriteFile(path, []byte("x"), 0o600); err != nil {
		t.Fatalf("failed to create file: %v", err)
	}
	dev := &Device{Path: path}
	if err := dev.Detach(); err == nil {
		t.Error("expected error detaching a regular file")
	}
}

func TestDetachPathNonLoopFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "not-a-loop")
	if err := os.WriteFile(path, []byte("x"), 0o600); err != nil {
		t.Fatalf("failed to create file: %v", err)
	}
	if err := DetachPath(path); err == nil {
		t.Error("expected error detaching a regular file by path")
	}
}

func TestGetInfoErrors(t *testing.T) {
	t.Run("nonexistent device", func(t *testing.T) {
		dev := &Device{Path: "/dev/loop-does-not-exist-99999", Number: 99999}
		if _, err := dev.GetInfo(); err == nil {
			t.Error("expected error for nonexistent device path")
		}
	})

	t.Run("non-loop file", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "not-a-loop")
		if err := os.WriteFile(path, []byte("x"), 0o600); err != nil {
			t.Fatalf("failed to create file: %v", err)
		}
		dev := &Device{Path: path}
		if _, err := dev.GetInfo(); err == nil {
			t.Error("expected error for LOOP_GET_STATUS64 on regular file")
		}
	})
}

func TestGetInfoDetached(t *testing.T) {
	testutil.RequiresRoot(t)

	backingFile := createBackingFile(t, 1024*1024)
	dev, err := Setup(backingFile, Config{
		ReadOnly: true,
		Serial:   fmt.Sprintf("erofs-test-info-detached-%d", os.Getpid()),
	})
	if err != nil {
		t.Fatalf("Setup failed: %v", err)
	}
	t.Cleanup(func() { _ = dev.Detach() })

	if err := dev.Detach(); err != nil {
		t.Fatalf("Detach failed: %v", err)
	}
	verifyDetached(t, dev)

	// LOOP_GET_STATUS64 on an unconfigured device fails (ENXIO). The device
	// node may also have been removed entirely; both result in an error.
	if _, err := dev.GetInfo(); err == nil {
		t.Error("expected error from GetInfo on detached device")
	}
}

func TestGetSerialNoDevice(t *testing.T) {
	// Sysfs read fails for a device number that does not exist; GetSerial
	// must return the empty string rather than an error.
	dev := &Device{Path: "/dev/loop1048576", Number: 1 << 20}
	if s := dev.GetSerial(); s != "" {
		t.Errorf("expected empty serial for nonexistent device, got %q", s)
	}
}

func TestSetupReadOnlyBackingFile(t *testing.T) {
	testutil.RequiresRoot(t)

	backingFile := createBackingFile(t, 1024*1024)
	if err := os.Chmod(backingFile, 0o400); err != nil {
		t.Fatalf("failed to chmod backing file: %v", err)
	}

	dev, err := Setup(backingFile, Config{
		ReadOnly: true,
		Serial:   fmt.Sprintf("erofs-test-rofile-%d", os.Getpid()),
	})
	if err != nil {
		t.Fatalf("Setup with ReadOnly on read-only file failed: %v", err)
	}
	t.Cleanup(func() { _ = dev.Detach() })

	info, err := dev.GetInfo()
	if err != nil {
		t.Fatalf("GetInfo failed: %v", err)
	}
	if info.Flags&LoFlagsReadOnly == 0 {
		t.Error("expected read-only flag to be set")
	}
}

func TestSetupDirectIO(t *testing.T) {
	testutil.RequiresRoot(t)

	backingFile := createBackingFile(t, 1024*1024)
	dev, err := Setup(backingFile, Config{
		ReadOnly: true,
		DirectIO: true,
		Serial:   fmt.Sprintf("erofs-test-dio-%d", os.Getpid()),
	})
	if err != nil {
		// Direct I/O support depends on the backing filesystem (e.g.
		// overlayfs/tmpfs may reject it); the failure path through
		// LOOP_SET_STATUS64 cleanup is still exercised.
		t.Logf("Setup with DirectIO not supported on this filesystem: %v", err)
		return
	}
	t.Cleanup(func() { _ = dev.Detach() })

	info, err := dev.GetInfo()
	if err != nil {
		t.Fatalf("GetInfo failed: %v", err)
	}
	// The kernel may accept the request but silently fall back to buffered
	// I/O depending on the backing filesystem, so the flag is informational.
	t.Logf("direct I/O flag set: %v", info.Flags&LoFlagsDirectIO != 0)
}

// brokenSymlink returns a path whose open(2) fails with ELOOP (a symlink
// pointing to itself). Unlike a missing path, os.IsNotExist is false for
// ELOOP, which exercises the open-error returns in Detach and DetachPath.
func brokenSymlink(t *testing.T) string {
	t.Helper()
	link := filepath.Join(t.TempDir(), "self-loop")
	if err := os.Symlink(link, link); err != nil {
		t.Fatalf("failed to create self-referencing symlink: %v", err)
	}
	return link
}

func TestDetachOpenError(t *testing.T) {
	dev := &Device{Path: brokenSymlink(t)}
	if err := dev.Detach(); err == nil {
		t.Error("expected error from Detach on unopenable path")
	}
}

func TestDetachPathOpenError(t *testing.T) {
	if err := DetachPath(brokenSymlink(t)); err == nil {
		t.Error("expected error from DetachPath on unopenable path")
	}
}

func TestFindByBackingFileRelativeWithDeletedCwd(t *testing.T) {
	// filepath.Abs fails when the working directory has been removed,
	// exercising the fallback to the raw backing file path.
	dir := t.TempDir()
	t.Chdir(dir)
	if err := os.Remove(dir); err != nil {
		t.Fatalf("failed to remove temp dir: %v", err)
	}

	found, err := FindByBackingFile("relative-backing.img")
	if err != nil {
		t.Fatalf("FindByBackingFile failed: %v", err)
	}
	if found != nil {
		t.Errorf("expected nil for unmatched relative path, got %s", found.Path)
	}
}

func TestBackingFileNoNullTerminator(t *testing.T) {
	// When FileName fills all 64 bytes the kernel omits the NUL terminator;
	// BackingFile must return the full buffer.
	var info LoopInfo64
	for i := range info.FileName {
		info.FileName[i] = 'a'
	}
	want := strings.Repeat("a", len(info.FileName))
	if got := info.BackingFile(); got != want {
		t.Errorf("BackingFile mismatch: got %q, want %q", got, want)
	}
}
