//go:build !linux

// Package loop provides functions for managing Linux loop devices.
package loop

import "github.com/containerd/errdefs"

// Setup creates and configures a loop device for the given backing file.
func Setup(backingFile string, cfg Config) (*Device, error) {
	return nil, errdefs.ErrNotImplemented
}

// SetSerial sets the serial number on a loop device.
func (d *Device) SetSerial(serial string) error {
	return errdefs.ErrNotImplemented
}

// GetSerial reads the serial number from a loop device.
func (d *Device) GetSerial() string {
	return ""
}

// GetInfo retrieves the current status of the loop device.
func (d *Device) GetInfo() (*LoopInfo64, error) {
	return nil, errdefs.ErrNotImplemented
}

// Detach detaches the loop device.
func (d *Device) Detach() error {
	return nil
}

// DetachPath detaches a loop device by its path.
func DetachPath(loopPath string) error {
	return nil
}

// FindByBackingFile finds a loop device associated with the given backing file.
func FindByBackingFile(backingFile string) (*Device, error) {
	return nil, errdefs.ErrNotImplemented
}

// FindBySerial finds a loop device with the given serial number.
func FindBySerial(serial string) (*Device, error) {
	return nil, errdefs.ErrNotImplemented
}
