// Package preflight provides system requirement checks for nexuserofs.
package preflight

import (
	"bytes"
	"fmt"
	"os"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"
)

// MinKernelVersion is the minimum required kernel version.
const MinKernelVersion = "6.10"

// Check runs all preflight checks and returns an error if any fail.
// This should be called early in main() to fail fast.
func Check() error {
	if err := CheckKernelVersion(MinKernelVersion); err != nil {
		return err
	}
	if err := CheckErofsSupport(); err != nil {
		return err
	}
	return nil
}

// KernelVersion returns the current kernel version as a string (e.g., "6.16.0").
func KernelVersion() (string, error) {
	var uname unix.Utsname
	if err := unix.Uname(&uname); err != nil {
		return "", fmt.Errorf("uname failed: %w", err)
	}

	// Convert release field to string (null-terminated)
	release := unix.ByteSliceToString(uname.Release[:])
	return release, nil
}

// parseVersion parses a kernel version string into major, minor, patch components.
// Handles versions like "6.16.0", "6.16.0-rc1", "6.16.0-generic", etc.
func parseVersion(version string) (major, minor, patch int, err error) {
	// Remove any suffix after the version numbers (e.g., "-rc1", "-generic")
	parts := strings.Split(version, "-")
	version = parts[0]

	// Split by dots
	nums := strings.Split(version, ".")
	if len(nums) < 2 {
		return 0, 0, 0, fmt.Errorf("invalid version format: %s", version)
	}

	major, err = strconv.Atoi(nums[0])
	if err != nil {
		return 0, 0, 0, fmt.Errorf("invalid major version: %s", nums[0])
	}

	minor, err = strconv.Atoi(nums[1])
	if err != nil {
		return 0, 0, 0, fmt.Errorf("invalid minor version: %s", nums[1])
	}

	if len(nums) >= 3 {
		patch, err = strconv.Atoi(nums[2])
		if err != nil {
			// Patch might have additional suffix, try to parse just the number
			patchStr := nums[2]
			for i, c := range patchStr {
				if c < '0' || c > '9' {
					patchStr = patchStr[:i]
					break
				}
			}
			if patchStr != "" {
				patch, _ = strconv.Atoi(patchStr)
			}
		}
	}

	return major, minor, patch, nil
}

// CompareVersions compares two version strings.
// Returns -1 if v1 < v2, 0 if v1 == v2, 1 if v1 > v2.
func CompareVersions(v1, v2 string) (int, error) {
	maj1, min1, pat1, err := parseVersion(v1)
	if err != nil {
		return 0, err
	}

	maj2, min2, pat2, err := parseVersion(v2)
	if err != nil {
		return 0, err
	}

	if maj1 != maj2 {
		if maj1 < maj2 {
			return -1, nil
		}
		return 1, nil
	}

	if min1 != min2 {
		if min1 < min2 {
			return -1, nil
		}
		return 1, nil
	}

	if pat1 != pat2 {
		if pat1 < pat2 {
			return -1, nil
		}
		return 1, nil
	}

	return 0, nil
}

// CheckKernelVersion checks if the running kernel meets the minimum version requirement.
// Returns nil if the kernel version is >= minVersion, otherwise returns an error.
func CheckKernelVersion(minVersion string) error {
	current, err := KernelVersion()
	if err != nil {
		return err
	}

	cmp, err := CompareVersions(current, minVersion)
	if err != nil {
		return fmt.Errorf("failed to compare versions: %w", err)
	}

	if cmp < 0 {
		return fmt.Errorf("kernel version %s is less than required %s", current, minVersion)
	}

	return nil
}

// CheckErofsSupport checks if the EROFS filesystem is available.
// Returns nil if EROFS is supported, otherwise returns an error with instructions.
func CheckErofsSupport() error {
	if !isErofsRegistered() {
		return fmt.Errorf("EROFS filesystem not available, please run: modprobe erofs")
	}
	return nil
}

// isErofsRegistered checks if EROFS is registered in /proc/filesystems.
func isErofsRegistered() bool {
	data, err := os.ReadFile("/proc/filesystems")
	if err != nil {
		return false
	}
	return bytes.Contains(data, []byte("\terofs\n"))
}
