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

	"github.com/containerd/containerd/v2/core/mount"
	"github.com/containerd/errdefs"
	"golang.org/x/sys/unix"

	"github.com/spin-stack/erofs-snapshotter/internal/loop"
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
// On error, MountAll releases everything it acquired itself and returns a nop
// cleanup function, so callers never need to invoke cleanup on failure
// (matching MountExt4).
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
			// mount.All does not unwind partially-applied mounts on failure.
			if uerr := mount.UnmountMounts(mounts, target, 0); uerr != nil {
				err = fmt.Errorf("%w (cleanup of partial mounts failed: %v)", err, uerr)
			}
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
	// failMount releases already-attached loop devices before returning err,
	// folding any detach failure into the returned error.
	failMount := func(err error) error {
		if cerr := cleanupLoops(); cerr != nil {
			return fmt.Errorf("%w (loop device cleanup also failed: %v)", err, cerr)
		}
		return err
	}

	// Set up loop device for the main fsmeta
	mainDev, err := loop.Setup(erofsMount.Source, loop.Config{ReadOnly: true})
	if err != nil {
		return nopCleanup, fmt.Errorf("failed to setup loop device for %s: %w", erofsMount.Source, err)
	}
	loopDevices = append(loopDevices, mainDev)

	// Set up loop devices for each device= blob
	var deviceOpts []string
	for _, dev := range devices {
		loopDev, err := loop.Setup(dev, loop.Config{ReadOnly: true})
		if err != nil {
			return nopCleanup, failMount(fmt.Errorf("failed to setup loop device for %s: %w", dev, err))
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
		return nopCleanup, failMount(fmt.Errorf("failed to mount multi-device EROFS: %w: %s", err, out))
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

// MountExt4 mounts an ext4 filesystem image read-only on the target directory
// using a loop device. Returns a cleanup function that unmounts, detaches the
// loop device and releases the image lock.
//
// The image is locked (flock + OFD fcntl lock) before mounting and the lock is
// held until cleanup runs. QEMU protects its disk images with OFD locks, so
// this both detects a running VM and prevents one from starting while the
// image is mounted on the host - releasing the lock before mounting would
// leave a window for the guest to attach the image concurrently.
//
// The mount is read-only: committing/diffing must never mutate the snapshot.
// The loop device stays writable so ext4 can replay a dirty journal (e.g.
// after a guest crash) and present a consistent view.
func MountExt4(source, target string) (cleanup func() error, err error) {
	lockFile, err := lockImageFile(source)
	if err != nil {
		return nopCleanup, err
	}
	defer func() {
		if err != nil {
			_ = lockFile.Close()
		}
	}()

	// Set up loop device for the ext4 image
	loopDev, err := loop.Setup(source, loop.Config{ReadOnly: false})
	if err != nil {
		return nopCleanup, fmt.Errorf("failed to setup loop device for ext4 %s: %w", source, err)
	}

	// Mount the loop device
	cmd := exec.Command("mount", "-t", "ext4", "-o", "ro", loopDev.Path, target)
	if out, merr := cmd.CombinedOutput(); merr != nil {
		_ = loopDev.Detach()
		err = fmt.Errorf("failed to mount ext4: %w: %s", merr, out)
		return nopCleanup, err
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
		return lockFile.Close()
	}, nil
}

// lockImageFile opens the image file and acquires both a flock and an OFD
// (fcntl) write lock without blocking. Both are needed because the two lock
// families are independent on Linux: QEMU locks its images with OFD locks
// (which flock cannot see), while flock guards against other host-side users
// of this package. Closing the returned file releases both locks, so it must
// stay open for as long as exclusive access to the image is required.
func lockImageFile(path string) (*os.File, error) {
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to open %s: %w", path, err)
	}

	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, fmt.Errorf("container is still running: stop the container before committing (ext4 %s is in use): %w", path, errdefs.ErrFailedPrecondition)
		}
		return nil, fmt.Errorf("failed to check if file is in use: %w", err)
	}

	// OFD write lock over the whole file. This conflicts with the byte-range
	// OFD locks QEMU takes on its images, unlike flock.
	flk := unix.Flock_t{Type: unix.F_WRLCK, Whence: 0, Start: 0, Len: 0}
	if err := unix.FcntlFlock(f.Fd(), unix.F_OFD_SETLK, &flk); err != nil {
		_ = f.Close()
		if errors.Is(err, unix.EAGAIN) || errors.Is(err, unix.EACCES) {
			return nil, fmt.Errorf("container is still running: stop the container before committing (ext4 %s is locked by the VM): %w", path, errdefs.ErrFailedPrecondition)
		}
		return nil, fmt.Errorf("failed to check OFD lock on %s: %w", path, err)
	}

	return f, nil
}

func nopCleanup() error { return nil }
