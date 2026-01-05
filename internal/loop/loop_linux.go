// Package loop provides functions for managing Linux loop devices.
package loop

import (
	"fmt"
	"os"
	"path/filepath"
	"unsafe"

	"golang.org/x/sys/unix"
)

// Loop device ioctl constants from <linux/loop.h>
const (
	loopSetFd       = 0x4C00
	loopClrFd       = 0x4C01
	loopSetStatus64 = 0x4C04
	loopGetStatus64 = 0x4C05
	loopCtlGetFree  = 0x4C82
)

// Setup creates and configures a loop device for the given backing file.
// Returns the loop device path (e.g., "/dev/loop0").
func Setup(backingFile string, cfg Config) (*Device, error) {
	// Open the backing file
	flags := unix.O_CLOEXEC
	if cfg.ReadOnly {
		flags |= unix.O_RDONLY
	} else {
		flags |= unix.O_RDWR
	}
	backingFd, err := unix.Open(backingFile, flags, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to open backing file %s: %w", backingFile, err)
	}
	defer unix.Close(backingFd)

	// Get a free loop device from /dev/loop-control
	ctlFd, err := unix.Open("/dev/loop-control", unix.O_RDWR|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to open /dev/loop-control: %w", err)
	}
	defer unix.Close(ctlFd)

	devNum, _, errno := unix.Syscall(unix.SYS_IOCTL, uintptr(ctlFd), loopCtlGetFree, 0)
	if errno != 0 {
		return nil, fmt.Errorf("LOOP_CTL_GET_FREE failed: %w", errno)
	}

	loopPath := fmt.Sprintf("/dev/loop%d", devNum)

	// Open the loop device
	loopFd, err := unix.Open(loopPath, unix.O_RDWR|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to open loop device %s: %w", loopPath, err)
	}
	defer unix.Close(loopFd)

	// Associate the loop device with the backing file
	_, _, errno = unix.Syscall(unix.SYS_IOCTL, uintptr(loopFd), loopSetFd, uintptr(backingFd))
	if errno != 0 {
		return nil, fmt.Errorf("LOOP_SET_FD failed for %s: %w", loopPath, errno)
	}

	// Build flags
	var info LoopInfo64
	if cfg.ReadOnly {
		info.Flags |= LoFlagsReadOnly
	}
	if cfg.Autoclear {
		info.Flags |= LoFlagsAutoclear
	}
	if cfg.DirectIO {
		info.Flags |= LoFlagsDirectIO
	}
	info.Offset = cfg.Offset
	info.SizeLimit = cfg.SizeLimit

	// Copy backing file name (truncated to 64 bytes)
	copy(info.FileName[:], backingFile)

	// Set loop device status
	_, _, errno = unix.Syscall(unix.SYS_IOCTL, uintptr(loopFd), loopSetStatus64, uintptr(unsafe.Pointer(&info)))
	if errno != 0 {
		// Clean up on failure
		unix.Syscall(unix.SYS_IOCTL, uintptr(loopFd), loopClrFd, 0)
		return nil, fmt.Errorf("LOOP_SET_STATUS64 failed for %s: %w", loopPath, errno)
	}

	dev := &Device{
		Path:   loopPath,
		Number: int(devNum),
	}

	// Try to set serial via sysfs (Linux 5.17+)
	// This is best-effort; if it fails, the loop device still works
	if cfg.Serial != "" {
		_ = dev.SetSerial(cfg.Serial)
	}

	return dev, nil
}

// SetSerial sets the serial number on a loop device via sysfs.
// Requires Linux 5.17+ where /sys/block/loopN/loop/serial is writable.
// Returns an error if the sysfs attribute doesn't exist or isn't writable.
func (d *Device) SetSerial(serial string) error {
	sysfsPath := fmt.Sprintf("/sys/block/loop%d/loop/serial", d.Number)
	return os.WriteFile(sysfsPath, []byte(serial), 0644)
}

// GetSerial reads the serial number from a loop device via sysfs.
// Returns empty string if the serial is not set or sysfs attribute doesn't exist.
func (d *Device) GetSerial() string {
	sysfsPath := fmt.Sprintf("/sys/block/loop%d/loop/serial", d.Number)
	data, err := os.ReadFile(sysfsPath)
	if err != nil {
		return ""
	}
	return string(data)
}

// GetInfo retrieves the current status of the loop device.
func (d *Device) GetInfo() (*LoopInfo64, error) {
	loopFd, err := unix.Open(d.Path, unix.O_RDONLY|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to open loop device %s: %w", d.Path, err)
	}
	defer unix.Close(loopFd)

	var info LoopInfo64
	_, _, errno := unix.Syscall(unix.SYS_IOCTL, uintptr(loopFd), loopGetStatus64, uintptr(unsafe.Pointer(&info)))
	if errno != 0 {
		return nil, fmt.Errorf("LOOP_GET_STATUS64 failed for %s: %w", d.Path, errno)
	}

	return &info, nil
}

// Detach detaches the loop device.
// Returns nil if the device is already detached.
func (d *Device) Detach() error {
	loopFd, err := unix.Open(d.Path, unix.O_RDONLY|unix.O_CLOEXEC, 0)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("failed to open loop device %s: %w", d.Path, err)
	}
	defer unix.Close(loopFd)

	_, _, errno := unix.Syscall(unix.SYS_IOCTL, uintptr(loopFd), loopClrFd, 0)
	if errno != 0 && errno != unix.ENXIO {
		// ENXIO means device not configured, which is fine
		return fmt.Errorf("LOOP_CLR_FD failed for %s: %w", d.Path, errno)
	}

	return nil
}

// DetachPath detaches a loop device by its path.
// Returns nil if the device doesn't exist or is already detached.
func DetachPath(loopPath string) error {
	if loopPath == "" {
		return nil
	}

	if _, err := os.Stat(loopPath); os.IsNotExist(err) {
		return nil
	}

	loopFd, err := unix.Open(loopPath, unix.O_RDONLY|unix.O_CLOEXEC, 0)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("failed to open loop device %s: %w", loopPath, err)
	}
	defer unix.Close(loopFd)

	_, _, errno := unix.Syscall(unix.SYS_IOCTL, uintptr(loopFd), loopClrFd, 0)
	if errno != 0 && errno != unix.ENXIO {
		return fmt.Errorf("LOOP_CLR_FD failed for %s: %w", loopPath, errno)
	}

	return nil
}

// FindByBackingFile finds a loop device associated with the given backing file.
// Returns nil if no loop device is found.
func FindByBackingFile(backingFile string) (*Device, error) {
	// Get absolute path for comparison
	absPath, err := filepath.Abs(backingFile)
	if err != nil {
		absPath = backingFile
	}

	// Iterate through possible loop devices
	entries, err := os.ReadDir("/sys/block")
	if err != nil {
		return nil, fmt.Errorf("failed to read /sys/block: %w", err)
	}

	for _, entry := range entries {
		name := entry.Name()
		if len(name) < 4 || name[:4] != "loop" {
			continue
		}

		// Read backing_file from sysfs
		backingPath := filepath.Join("/sys/block", name, "loop", "backing_file")
		data, err := os.ReadFile(backingPath)
		if err != nil {
			continue // Device may not be configured
		}

		// Compare paths (strip newline from sysfs output)
		sysfsBackingFile := string(data)
		if len(sysfsBackingFile) > 0 && sysfsBackingFile[len(sysfsBackingFile)-1] == '\n' {
			sysfsBackingFile = sysfsBackingFile[:len(sysfsBackingFile)-1]
		}

		if sysfsBackingFile == absPath || sysfsBackingFile == backingFile {
			var devNum int
			fmt.Sscanf(name, "loop%d", &devNum)
			return &Device{
				Path:   "/dev/" + name,
				Number: devNum,
			}, nil
		}
	}

	return nil, nil
}

// FindBySerial finds a loop device with the given serial number.
// Returns nil if no loop device is found.
func FindBySerial(serial string) (*Device, error) {
	entries, err := os.ReadDir("/sys/block")
	if err != nil {
		return nil, fmt.Errorf("failed to read /sys/block: %w", err)
	}

	for _, entry := range entries {
		name := entry.Name()
		if len(name) < 4 || name[:4] != "loop" {
			continue
		}

		serialPath := filepath.Join("/sys/block", name, "loop", "serial")
		data, err := os.ReadFile(serialPath)
		if err != nil {
			continue
		}

		devSerial := string(data)
		if len(devSerial) > 0 && devSerial[len(devSerial)-1] == '\n' {
			devSerial = devSerial[:len(devSerial)-1]
		}

		if devSerial == serial {
			var devNum int
			fmt.Sscanf(name, "loop%d", &devNum)
			return &Device{
				Path:   "/dev/" + name,
				Number: devNum,
			}, nil
		}
	}

	return nil, nil
}
