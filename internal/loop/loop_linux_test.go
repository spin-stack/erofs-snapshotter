package loop

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/containerd/containerd/v2/pkg/testutil"
)

func TestSetupAndDetach(t *testing.T) {
	testutil.RequiresRoot(t)

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

	// Setup loop device
	dev, err := Setup(backingFile, Config{
		ReadOnly:  true,
		Autoclear: true,
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
	if info.Flags&LoFlagsAutoclear == 0 {
		t.Error("expected autoclear flag to be set")
	}

	// Verify backing file
	gotBackingFile := info.BackingFile()
	if gotBackingFile != backingFile {
		t.Errorf("backing file mismatch: got %s, want %s", gotBackingFile, backingFile)
	}

	// Detach
	if err := dev.Detach(); err != nil {
		t.Fatalf("Detach failed: %v", err)
	}

	// Verify device is detached (GetInfo should fail)
	_, err = dev.GetInfo()
	if err == nil {
		t.Error("expected GetInfo to fail after detach")
	}
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
		Autoclear: true,
		Offset:    offset,
		SizeLimit: sizeLimit,
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
		ReadOnly:  false,
		Autoclear: true,
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

	serial := "test-serial-12345"

	dev, err := Setup(backingFile, Config{
		ReadOnly:  true,
		Autoclear: true,
		Serial:    serial,
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
		ReadOnly:  true,
		Autoclear: true,
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

	serial := "find-test-serial"

	dev, err := Setup(backingFile, Config{
		ReadOnly:  true,
		Autoclear: true,
		Serial:    serial,
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
		// Note: not using Autoclear to test explicit detach
	})
	if err != nil {
		t.Fatalf("Setup failed: %v", err)
	}

	// Detach using path
	if err := DetachPath(dev.Path); err != nil {
		t.Fatalf("DetachPath failed: %v", err)
	}

	// Verify detached
	_, err = dev.GetInfo()
	if err == nil {
		t.Error("expected GetInfo to fail after DetachPath")
	}
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
	for i := 0; i < 3; i++ {
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
			ReadOnly:  true,
			Autoclear: true,
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
