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

package snapshotter

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/containerd/containerd/v2/core/mount"
	"github.com/containerd/containerd/v2/core/snapshots"
	"github.com/containerd/containerd/v2/core/snapshots/storage"
	"github.com/containerd/continuity/fs"
	"github.com/containerd/log"
	"golang.org/x/sys/unix"

	"github.com/spin-stack/erofs-snapshotter/internal/erofs"
	"github.com/spin-stack/erofs-snapshotter/internal/preflight"
)

// defaultWritableSize is the default size for the ext4 writable layer.
// An ext4 image file of this size is created and loop-mounted for each
// active snapshot's writable layer.
const defaultWritableSize = 64 * 1024 * 1024 // 64 MiB

// orphanCleanupTimeout is the maximum time allowed for orphan cleanup at startup.
// If cleanup takes longer than this, it will be aborted to prevent blocking forever.
const orphanCleanupTimeout = 30 * time.Second

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

// syncFile opens a file and calls fsync to ensure its data is flushed to disk.
// This is important for durability - without fsync, data may remain in the
// kernel's buffer cache and be lost if the system crashes.
func syncFile(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

// snapshotIDExists checks if a snapshot with the given internal ID exists in metadata.
// This is used for TOCTOU protection during orphan cleanup.
func (s *snapshotter) snapshotIDExists(ctx context.Context, targetID string) bool {
	var found bool
	_ = s.ms.WithTransaction(ctx, false, func(ctx context.Context) error {
		return storage.WalkInfo(ctx, func(ctx context.Context, info snapshots.Info) error {
			id, _, _, err := storage.GetInfo(ctx, info.Name)
			if err != nil {
				return nil // Continue on error
			}
			if id == targetID {
				found = true
				return fmt.Errorf("found") // Stop walking
			}
			return nil
		})
	})
	return found
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

// cleanupOrphanedMounts detects and cleans up mount leaks on startup.
// This handles two cases:
// 1. Orphaned snapshot directories (on disk but not in metadata) - unmount and remove
// 2. Stale mounts for existing snapshots (mounts left behind from previous runs)
// Errors are logged but not returned since this is best-effort cleanup.
// The function respects a timeout to avoid blocking indefinitely.
func (s *snapshotter) cleanupOrphanedMounts() {
	ctx, cancel := context.WithTimeout(context.Background(), orphanCleanupTimeout)
	defer cancel()

	snapshotsDir := filepath.Join(s.root, "snapshots")
	entries, err := os.ReadDir(snapshotsDir)
	if err != nil {
		// If the directory doesn't exist, there's nothing to clean up
		return
	}

	// Get all valid snapshot IDs from metadata
	validIDs := make(map[string]bool)
	if err := s.ms.WithTransaction(ctx, false, func(ctx context.Context) error {
		return storage.WalkInfo(ctx, func(ctx context.Context, info snapshots.Info) error {
			// Get the snapshot ID from its key
			id, _, _, err := storage.GetInfo(ctx, info.Name)
			if err != nil {
				// Log and continue walking even if one fails
				log.L.WithError(err).WithField("key", info.Name).Debug("failed to get snapshot info during orphan cleanup")
				return nil //nolint:nilerr // intentionally continue on error
			}
			validIDs[id] = true
			return nil
		})
	}); err != nil {
		log.L.WithError(err).Warn("failed to enumerate snapshots during orphan cleanup")
		return
	}

	for _, entry := range entries {
		// Check for timeout/cancellation
		if ctx.Err() != nil {
			log.L.Warn("orphan cleanup timed out, aborting")
			return
		}

		if !entry.IsDir() {
			continue
		}
		id := entry.Name()
		snapshotDir := filepath.Join(snapshotsDir, id)

		if !validIDs[id] {
			// Orphaned directory - not in metadata
			log.L.WithField("id", id).Info("cleaning up orphaned snapshot directory")

			// Unmount rw mount if it exists (from interrupted commit)
			rwDir := filepath.Join(snapshotDir, "rw")
			if err := unmountAll(rwDir); err != nil && !isNotMountError(err) {
				log.L.WithError(err).WithField("path", rwDir).Debug("failed to unmount orphan rw")
			}

			// Clear immutable flag if present
			layerBlob := filepath.Join(snapshotDir, "layer.erofs")
			if err := setImmutable(layerBlob, false); err != nil && !os.IsNotExist(err) {
				log.L.WithError(err).WithField("path", layerBlob).Debug("failed to clear immutable flag during orphan cleanup")
			}

			// TOCTOU protection: double-check this snapshot wasn't just created
			// between our initial scan and now. This prevents a race where:
			// 1. We scan metadata and don't see snapshot X
			// 2. Commit() creates snapshot X and adds to metadata
			// 3. We delete X, causing data loss
			if s.snapshotIDExists(ctx, id) {
				log.L.WithField("id", id).Debug("snapshot appeared in metadata during cleanup, skipping removal")
				continue
			}

			// Remove the entire directory
			if err := os.RemoveAll(snapshotDir); err != nil {
				log.L.WithError(err).WithField("path", snapshotDir).Warn("failed to remove orphaned snapshot directory")
			}
			continue
		}

		// Valid snapshot - clean up stale rw mount that might have been left behind
		// from an interrupted commit operation
		rwDir := filepath.Join(snapshotDir, "rw")
		if err := unmountAll(rwDir); err != nil && !isNotMountError(err) {
			log.L.WithError(err).WithField("path", rwDir).Debug("failed to cleanup stale rw mount")
		}
	}
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
	err := erofs.ConvertErofs(ctx, layerBlob, upperDir, nil)
	if err != nil {
		return err
	}

	// Sync the layer blob to disk to ensure durability.
	// This prevents data loss if the system crashes before the OS flushes the buffer cache.
	if err := syncFile(layerBlob); err != nil {
		return fmt.Errorf("failed to sync layer blob: %w", err)
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

// mountBlockRwLayer mounts the ext4 writable layer for extract snapshots.
// This allows the differ to write content to the mounted filesystem.
// The mount is cleaned up during Commit() after converting to EROFS.
func (s *snapshotter) mountBlockRwLayer(ctx context.Context, id string) error {
	rwLayerPath := s.writablePath(id)
	rwMountPath := s.blockRwMountPath(id)

	// Create mount point
	if err := os.MkdirAll(rwMountPath, 0755); err != nil {
		return fmt.Errorf("failed to create rw mount point: %w", err)
	}

	// Mount the ext4 file
	m := mount.Mount{
		Source:  rwLayerPath,
		Type:    "ext4",
		Options: []string{"rw", "loop"},
	}
	if err := m.Mount(rwMountPath); err != nil {
		return fmt.Errorf("failed to mount ext4 layer: %w", err)
	}

	// Create upper and work directories inside the mounted ext4
	upperDir := s.blockUpperPath(id)
	workDir := filepath.Join(s.blockRwMountPath(id), "work")

	if err := os.MkdirAll(upperDir, 0755); err != nil {
		// Cleanup mount on failure - log if unmount also fails
		if derr := unmountAll(rwMountPath); derr != nil && !isNotMountError(derr) {
			log.G(ctx).WithError(derr).WithField("path", rwMountPath).Debug("cleanup unmount failed after upper dir creation error")
		}
		return fmt.Errorf("failed to create upper directory: %w", err)
	}
	if err := os.MkdirAll(workDir, 0755); err != nil {
		if derr := unmountAll(rwMountPath); derr != nil && !isNotMountError(derr) {
			log.G(ctx).WithError(derr).WithField("path", rwMountPath).Debug("cleanup unmount failed after work dir creation error")
		}
		return fmt.Errorf("failed to create work directory: %w", err)
	}

	log.G(ctx).WithFields(log.Fields{
		"id":     id,
		"target": rwMountPath,
	}).Debug("mounted ext4 writable layer for extraction")

	return nil
}
