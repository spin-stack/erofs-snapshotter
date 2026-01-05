//go:build !linux

// Package preflight provides system requirement checks for nexuserofs.
package preflight

import "github.com/containerd/errdefs"

// MinKernelVersion is the minimum required kernel version.
const MinKernelVersion = "6.10"

// Check runs all preflight checks.
// On non-Linux platforms, this returns ErrNotImplemented.
func Check() error {
	return errdefs.ErrNotImplemented
}

// KernelVersion returns the current kernel version.
func KernelVersion() (string, error) {
	return "", errdefs.ErrNotImplemented
}

// CompareVersions compares two version strings.
func CompareVersions(v1, v2 string) (int, error) {
	return 0, errdefs.ErrNotImplemented
}

// CheckKernelVersion checks if the running kernel meets the minimum version requirement.
func CheckKernelVersion(minVersion string) error {
	return errdefs.ErrNotImplemented
}

// CheckErofsSupport checks if the EROFS filesystem is available.
func CheckErofsSupport() error {
	return errdefs.ErrNotImplemented
}
