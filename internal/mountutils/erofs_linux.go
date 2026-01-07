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
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"syscall"

	"github.com/aledbf/nexus-erofs/internal/loop"
	"github.com/containerd/containerd/v2/core/mount"
)

// MountAll mounts all provided mounts to the target directory.
// It extends the standard mount.All by adding support for EROFS multi-device mounts.
//
// EROFS multi-device mounts (fsmeta with device= options) require special handling:
// - The containerd mount manager cannot handle device= options directly
// - Loop devices must be set up for both the main fsmeta and each blob
// - The mount options must be rewritten to use loop device paths
//
// Returns a cleanup function that must be called to release resources (loop devices).
// The cleanup function is always non-nil, even on error.
func MountAll(mounts []mount.Mount, target string) (cleanup func() error, err error) {
	// Find EROFS mounts with device= options
	erofsIdx := -1
	for i, m := range mounts {
		if TypeSuffix(m.Type) == fsTypeErofs && hasDeviceOption(m.Options) {
			erofsIdx = i
			break
		}
	}

	// No EROFS multi-device mount - use standard mount.All
	if erofsIdx == -1 {
		if err := mount.All(mounts, target); err != nil {
			return nopCleanup, err
		}
		return func() error {
			return mount.UnmountMounts(mounts, target, 0)
		}, nil
	}

	// Handle EROFS multi-device mount
	erofsMount := mounts[erofsIdx]

	// Separate device= options from other options
	var devices []string
	var otherOpts []string
	for _, opt := range erofsMount.Options {
		if strings.HasPrefix(opt, "device=") {
			devices = append(devices, strings.TrimPrefix(opt, "device="))
		} else if opt != "loop" {
			otherOpts = append(otherOpts, opt)
		}
	}

	// Set up loop devices
	var loopDevices []*loop.Device
	cleanupLoops := func() error {
		var errs []error
		for _, l := range loopDevices {
			if err := l.Detach(); err != nil {
				errs = append(errs, err)
			}
		}
		if len(errs) > 0 {
			return fmt.Errorf("failed to detach loop devices: %v", errs)
		}
		return nil
	}

	// Set up loop device for the main fsmeta
	mainDev, err := loop.Setup(erofsMount.Source, loop.Config{ReadOnly: true})
	if err != nil {
		return cleanupLoops, fmt.Errorf("failed to setup loop device for %s: %w", erofsMount.Source, err)
	}
	loopDevices = append(loopDevices, mainDev)

	// Set up loop devices for each device= blob
	var deviceOpts []string
	for _, dev := range devices {
		loopDev, err := loop.Setup(dev, loop.Config{ReadOnly: true})
		if err != nil {
			return cleanupLoops, fmt.Errorf("failed to setup loop device for %s: %w", dev, err)
		}
		loopDevices = append(loopDevices, loopDev)
		deviceOpts = append(deviceOpts, fmt.Sprintf("device=%s", loopDev.Path))
	}

	// Mount with device= options pointing to loop devices
	otherOpts = append(otherOpts, deviceOpts...)
	args := []string{"-t", "erofs", "-o", strings.Join(otherOpts, ",")}
	args = append(args, mainDev.Path, target)
	cmd := exec.Command("mount", args...)
	if out, err := cmd.CombinedOutput(); err != nil {
		return cleanupLoops, fmt.Errorf("failed to mount multi-device EROFS: %w: %s", err, out)
	}

	return func() error {
		// Unmount first
		if out, err := exec.Command("umount", target).CombinedOutput(); err != nil {
			return fmt.Errorf("failed to unmount %s: %w: %s", target, err, out)
		}
		// Then detach loop devices
		return cleanupLoops()
	}, nil
}

// MountExt4 mounts an ext4 filesystem image to the target directory using a loop device.
// Returns a cleanup function that unmounts and detaches the loop device.
//
// This function checks if the file is in use (e.g., by a running VM) before mounting.
// If the file is in use, it returns an error indicating the container must be stopped first.
func MountExt4(source, target string) (cleanup func() error, err error) {
	// Check if the file is in use by trying to get an exclusive lock.
	// If a VM is using it via virtio-blk, we won't be able to get the lock.
	if err := checkFileNotInUse(source); err != nil {
		return nopCleanup, err
	}

	// Set up loop device for the ext4 image
	loopDev, err := loop.Setup(source, loop.Config{ReadOnly: false})
	if err != nil {
		return nopCleanup, fmt.Errorf("failed to setup loop device for ext4 %s: %w", source, err)
	}

	// Mount the loop device
	cmd := exec.Command("mount", "-t", "ext4", loopDev.Path, target)
	if out, err := cmd.CombinedOutput(); err != nil {
		_ = loopDev.Detach()
		return nopCleanup, fmt.Errorf("failed to mount ext4: %w: %s", err, out)
	}

	return func() error {
		// Unmount first
		if out, err := exec.Command("umount", target).CombinedOutput(); err != nil {
			return fmt.Errorf("failed to unmount ext4 %s: %w: %s", target, err, out)
		}
		// Then detach loop device
		if err := loopDev.Detach(); err != nil {
			return fmt.Errorf("failed to detach loop device: %w", err)
		}
		return nil
	}, nil
}

// checkFileNotInUse verifies that the file is not being used by another process
// (e.g., a running VM). It attempts to get an exclusive lock on the file.
// If the lock cannot be acquired, the file is in use and commit cannot proceed.
func checkFileNotInUse(path string) error {
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return fmt.Errorf("failed to open %s: %w", path, err)
	}
	defer f.Close()

	// Try to get an exclusive lock (non-blocking)
	err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if err != nil {
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return fmt.Errorf("container is still running: stop the container before committing (ext4 %s is in use)", path)
		}
		return fmt.Errorf("failed to check if file is in use: %w", err)
	}

	// Release the lock immediately - we just wanted to check
	_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
	return nil
}

func nopCleanup() error { return nil }
