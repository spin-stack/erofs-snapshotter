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

package erofs

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"

	"github.com/containerd/containerd/v2/core/mount"
	"github.com/containerd/continuity/fs"
	"github.com/containerd/log"
	"golang.org/x/sys/unix"

	erofsutils "github.com/aledbf/nexuserofs/internal/erofs"
	"github.com/aledbf/nexuserofs/internal/preflight"
)

// defaultWritableSize controls the default writable layer mode.
//
// On Linux, this is set to 0 which enables "directory mode" - the writable
// layer uses a plain directory on the host filesystem. This matches the
// default behavior of other Linux snapshotters and works with any filesystem
// that supports overlayfs (ext4, xfs with d_type, etc.).
//
// When set to a non-zero value, "block mode" is used instead: an ext4 image
// file of the specified size is created and loop-mounted for the writable
// layer. Block mode is required on non-Linux platforms and optional on Linux.
//
// See snapshotter_other.go for the non-Linux default (64 MiB block mode).
const defaultWritableSize = 0

func checkCompatibility(root string) error {
	// Check kernel version and EROFS support via preflight
	if err := preflight.Check(); err != nil {
		return fmt.Errorf("preflight check failed: %w", err)
	}

	supportsDType, err := fs.SupportsDType(root)
	if err != nil {
		return err
	}
	if !supportsDType {
		return fmt.Errorf("%s does not support d_type. If the backing filesystem is xfs, please reformat with ftype=1 to enable d_type support", root)
	}

	return nil
}

func setImmutable(path string, enable bool) error {
	//nolint:revive,staticcheck	// silence "don't use ALL_CAPS in Go names; use CamelCase"
	const (
		FS_IMMUTABLE_FL = 0x10
	)
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("failed to open: %w", err)
	}
	defer f.Close()

	oldattr, err := unix.IoctlGetInt(int(f.Fd()), unix.FS_IOC_GETFLAGS)
	if err != nil {
		return fmt.Errorf("error getting inode flags: %w", err)
	}
	newattr := oldattr | FS_IMMUTABLE_FL
	if !enable {
		newattr ^= FS_IMMUTABLE_FL
	}
	if newattr == oldattr {
		return nil
	}
	return unix.IoctlSetPointerInt(int(f.Fd()), unix.FS_IOC_SETFLAGS, newattr)
}

func cleanupUpper(upper string) error {
	return unmountAll(upper)
}

// isNotMountError returns true if the error indicates the target was not mounted.
// These errors are expected during cleanup when the path was never mounted.
func isNotMountError(err error) bool {
	if err == nil {
		return false
	}
	// EINVAL: target is not a mount point
	// ENOENT: path doesn't exist (already cleaned up)
	return errors.Is(err, unix.EINVAL) || errors.Is(err, unix.ENOENT) || os.IsNotExist(err)
}

// unmountAll attempts to unmount the target. If normal unmount fails (e.g., due
// to EBUSY), it falls back to lazy unmount (MNT_DETACH) which detaches the mount
// immediately but may leave the mount lingering until all references are closed.
//
// Returns nil if the path was not mounted (EINVAL) or doesn't exist (ENOENT),
// as these are expected during cleanup. Returns an error only for unexpected
// failures like EBUSY that lazy unmount also can't resolve.
func unmountAll(target string) error {
	if err := mount.UnmountAll(target, 0); err != nil {
		// If the target wasn't a mount point, that's fine - nothing to unmount
		if isNotMountError(err) {
			return nil
		}
		// Normal unmount failed, try lazy unmount as fallback.
		// This detaches the mount immediately but resources may linger.
		if derr := mount.UnmountAll(target, unix.MNT_DETACH); derr != nil {
			// If lazy unmount also says "not mounted", that's fine
			if isNotMountError(derr) {
				return nil
			}
			// Both normal and lazy unmount failed - wrap the original error
			return fmt.Errorf("unmount %s failed (lazy unmount also failed): %w", target, err)
		}
		// Lazy unmount succeeded - mount is detached but may linger.
		return nil
	}
	return nil
}

func convertDirToErofs(ctx context.Context, layerBlob, upperDir string) error {
	err := erofsutils.ConvertErofs(ctx, layerBlob, upperDir, nil)
	if err != nil {
		return err
	}

	// Remove all sub-directories in the overlayfs upperdir.  Leave the
	// overlayfs upperdir itself since it's used for Lchown.
	fd, err := os.Open(upperDir)
	if err != nil {
		return err
	}
	defer fd.Close()

	dirs, err := fd.Readdirnames(0)
	if err != nil {
		return err
	}

	for _, d := range dirs {
		dir := filepath.Join(upperDir, d)
		if err := os.RemoveAll(dir); err != nil {
			log.G(ctx).WithError(err).WithField("path", dir).Warn("failed to remove directory")
		}
	}
	return nil
}

func upperDirectoryPermission(p, parent string) error {
	st, err := os.Stat(parent)
	if err != nil {
		return fmt.Errorf("failed to stat parent: %w", err)
	}

	stat, ok := st.Sys().(*syscall.Stat_t)
	if !ok {
		return fmt.Errorf("failed to get syscall.Stat_t from file info")
	}
	if err := os.Lchown(p, int(stat.Uid), int(stat.Gid)); err != nil {
		return fmt.Errorf("failed to chown: %w", err)
	}

	return nil
}
