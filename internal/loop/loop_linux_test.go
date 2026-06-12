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
	testutil.RequiresRoot(t)

	_, err := Setup("/nonexistent/backing.img", Config{ReadOnly: true})
	if err == nil {
		t.Error("expected error for non-existent backing file")
	}
}
