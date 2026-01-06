package snapshotter

import (
	"context"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/containerd/containerd/v2/core/mount"
	"github.com/containerd/containerd/v2/core/snapshots"
	"github.com/containerd/containerd/v2/core/snapshots/storage"
	"github.com/containerd/continuity/fs"
	"github.com/containerd/errdefs"
	"github.com/containerd/log"

	"github.com/aledbf/nexus-erofs/internal/erofs"
)

// isExtractKey returns true if the key indicates an extract/unpack operation.
// Snapshot keys use forward slashes as separators (e.g., "default/1/extract-12345"),
// so we use path.Base (POSIX paths) rather than filepath.Base (OS-specific).
func isExtractKey(key string) bool {
	return strings.HasPrefix(path.Base(key), snapshots.UnpackKeyPrefix)
}

// ensureMarkerFile creates the EROFS layer marker file at the given path if
// it doesn't already exist. This is idempotent - calling it multiple times
// with the same path is safe and will not return an error.
//
// The marker file is checked by erofs.MountsToLayer() in the EROFS differ
// to validate that a directory is a genuine EROFS snapshotter layer.
func ensureMarkerFile(path string) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		if os.IsExist(err) {
			return nil // File already exists
		}
		return fmt.Errorf("create marker file %q: %w", path, err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close marker file %q: %w", path, err)
	}
	return nil
}

// checkContext returns an error if the context is cancelled.
func checkContext(ctx context.Context, operation string) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("context cancelled %s: %w", operation, err)
	}
	return nil
}

func (s *snapshotter) createSnapshot(ctx context.Context, kind snapshots.Kind, key, parent string, opts []snapshots.Opt) (_ []mount.Mount, err error) {
	var (
		snap     storage.Snapshot
		td, path string
		info     snapshots.Info
	)

	defer func() {
		if err != nil {
			s.cleanupFailedSnapshot(ctx, td, path)
		}
	}()

	if err := checkContext(ctx, "before snapshot creation"); err != nil {
		return nil, err
	}

	snapshotDir := s.snapshotsDir()
	td, err = s.prepareDirectory(snapshotDir, kind)
	if err != nil {
		return nil, fmt.Errorf("create prepare snapshot dir: %w", err)
	}

	// Mark extract snapshots with a label for TOCTOU-safe detection.
	if isExtractKey(key) {
		opts = append(opts, snapshots.WithLabels(map[string]string{
			extractLabel: "true",
		}))
	}

	if err := s.ms.WithTransaction(ctx, true, func(ctx context.Context) (err error) {
		snap, err = storage.CreateSnapshot(ctx, kind, key, parent, opts...)
		if err != nil {
			return fmt.Errorf("create snapshot: %w", err)
		}

		_, info, _, err = storage.GetInfo(ctx, key)
		if err != nil {
			return fmt.Errorf("get snapshot info: %w", err)
		}

		if len(snap.ParentIDs) > 0 {
			if err := upperDirectoryPermission(filepath.Join(td, fsDirName), s.upperPath(snap.ParentIDs[0])); err != nil {
				return fmt.Errorf("set upper directory permissions: %w", err)
			}
		}

		path = filepath.Join(snapshotDir, snap.ID)
		if err = os.Rename(td, path); err != nil {
			return fmt.Errorf("rename: %w", err)
		}
		td = ""
		return nil
	}); err != nil {
		return nil, err
	}

	if err := checkContext(ctx, "after transaction"); err != nil {
		return nil, err
	}

	// Generate VMDK for VM runtimes - always generate when there are parent layers.
	if !isExtractKey(key) && len(snap.ParentIDs) > 0 {
		s.generateFsMeta(ctx, snap.ParentIDs)
	}

	// For active snapshots, create the writable ext4 layer file.
	if kind == snapshots.KindActive {
		if err := checkContext(ctx, "before writable layer creation"); err != nil {
			return nil, err
		}
		if err := s.createWritableLayer(ctx, snap.ID); err != nil {
			return nil, fmt.Errorf("create writable layer: %w", err)
		}

		// For extract snapshots, mount the ext4 on the host so the differ can write to it.
		if isExtractKey(key) {
			if err := s.mountBlockRwLayer(ctx, snap.ID); err != nil {
				return nil, fmt.Errorf("mount writable layer for extraction: %w", err)
			}
		}
	}

	return s.mounts(snap, info)
}

// cleanupFailedSnapshot removes temporary and final directories on failure.
func (s *snapshotter) cleanupFailedSnapshot(ctx context.Context, td, path string) {
	if td != "" {
		if err := os.RemoveAll(td); err != nil {
			log.G(ctx).WithError(err).Warn("failed to cleanup temp snapshot directory")
		}
	}
	if path != "" {
		if err := os.RemoveAll(path); err != nil {
			log.G(ctx).WithError(err).WithField("path", path).Error("failed to reclaim snapshot directory")
		}
	}
}

// Prepare creates an active snapshot for writing.
func (s *snapshotter) Prepare(ctx context.Context, key, parent string, opts ...snapshots.Opt) ([]mount.Mount, error) {
	return s.createSnapshot(ctx, snapshots.KindActive, key, parent, opts)
}

// View creates a view snapshot for reading.
func (s *snapshotter) View(ctx context.Context, key, parent string, opts ...snapshots.Opt) ([]mount.Mount, error) {
	return s.createSnapshot(ctx, snapshots.KindView, key, parent, opts)
}

// Mounts returns the mounts for a snapshot.
func (s *snapshotter) Mounts(ctx context.Context, key string) (_ []mount.Mount, err error) {
	var snap storage.Snapshot
	var info snapshots.Info
	if err := s.ms.WithTransaction(ctx, false, func(ctx context.Context) error {
		snap, err = storage.GetSnapshot(ctx, key)
		if err != nil {
			return fmt.Errorf("get active mount: %w", err)
		}

		_, info, _, err = storage.GetInfo(ctx, key)
		if err != nil {
			return fmt.Errorf("get snapshot info: %w", err)
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return s.mounts(snap, info)
}

func (s *snapshotter) getCleanupDirectories(ctx context.Context) ([]string, error) {
	ids, err := storage.IDMap(ctx)
	if err != nil {
		return nil, fmt.Errorf("get snapshot ID map: %w", err)
	}

	snapshotDir := s.snapshotsDir()
	fd, err := os.Open(snapshotDir)
	if err != nil {
		return nil, fmt.Errorf("open snapshots directory: %w", err)
	}
	defer fd.Close()

	dirs, err := fd.Readdirnames(0)
	if err != nil {
		return nil, fmt.Errorf("read snapshots directory: %w", err)
	}

	var cleanup []string
	for _, d := range dirs {
		if _, ok := ids[d]; ok {
			continue
		}
		cleanup = append(cleanup, filepath.Join(snapshotDir, d))
	}

	return cleanup, nil
}

// Remove abandons the snapshot identified by key.
func (s *snapshotter) Remove(ctx context.Context, key string) (err error) {
	var removals []string
	var id string

	defer func() {
		if err == nil {
			s.cleanupAfterRemove(ctx, id, removals)
		}
	}()

	return s.ms.WithTransaction(ctx, true, func(ctx context.Context) error {
		var k snapshots.Kind

		id, k, err = storage.Remove(ctx, key)
		if err != nil {
			return fmt.Errorf("remove snapshot %s: %w", key, err)
		}

		removals, err = s.getCleanupDirectories(ctx)
		if err != nil {
			return fmt.Errorf("get directories for removal: %w", err)
		}

		// The layer blob is only persisted for committed snapshots.
		if k == snapshots.KindCommitted {
			if layerBlob, ferr := s.findLayerBlob(id); ferr == nil {
				err = setImmutable(layerBlob, false)
				if err != nil && !errdefs.IsNotImplemented(err) {
					return fmt.Errorf("clear IMMUTABLE_FL: %w", err)
				}
			}
		}
		return nil
	})
}

// cleanupAfterRemove handles post-removal cleanup.
func (s *snapshotter) cleanupAfterRemove(ctx context.Context, id string, removals []string) {
	// Cleanup block rw mount (only exists if commit was in progress)
	if err := unmountAll(s.blockRwMountPath(id)); err != nil {
		log.G(ctx).WithError(err).WithField("id", id).Warnf("failed to cleanup block rw mount")
	}

	for _, dir := range removals {
		if err := os.RemoveAll(dir); err != nil {
			log.G(ctx).WithError(err).WithField("path", dir).Warn("failed to remove directory")
		}
	}
}

// Cleanup removes unreferenced snapshot directories.
func (s *snapshotter) Cleanup(ctx context.Context) (err error) {
	var removals []string
	if err := s.ms.WithTransaction(ctx, false, func(ctx context.Context) error {
		var err error
		removals, err = s.getCleanupDirectories(ctx)
		return err
	}); err != nil {
		return err
	}

	var cleanupErrs []error
	for _, dir := range removals {
		// Cleanup block rw mount
		if err := unmountAll(filepath.Join(dir, rwDirName)); err != nil {
			log.G(ctx).WithError(err).WithField("path", dir).Debug("failed to cleanup block rw mount")
			cleanupErrs = append(cleanupErrs, fmt.Errorf("cleanup rw %s: %w", dir, err))
		}
		// Clear immutable flag on any layer blobs (digest-based or fallback naming)
		if matches, err := filepath.Glob(filepath.Join(dir, erofs.LayerBlobPattern)); err == nil {
			for _, match := range matches {
				if err := setImmutable(match, false); err != nil && !errdefs.IsNotImplemented(err) {
					log.G(ctx).WithError(err).WithField("path", match).Debug("failed to clear immutable flag")
				}
			}
		}
		// Also try fallback naming pattern
		if matches, err := filepath.Glob(filepath.Join(dir, "snapshot-*.erofs")); err == nil {
			for _, match := range matches {
				if err := setImmutable(match, false); err != nil && !errdefs.IsNotImplemented(err) {
					log.G(ctx).WithError(err).WithField("path", match).Debug("failed to clear immutable flag")
				}
			}
		}
		if err := os.RemoveAll(dir); err != nil {
			log.G(ctx).WithError(err).WithField("path", dir).Warn("failed to remove directory")
			cleanupErrs = append(cleanupErrs, fmt.Errorf("remove %s: %w", dir, err))
		}
	}

	if len(cleanupErrs) > 0 {
		log.G(ctx).WithField("error_count", len(cleanupErrs)).Warn("cleanup completed with errors")
		return fmt.Errorf("cleanup had %d errors, first: %w", len(cleanupErrs), cleanupErrs[0])
	}
	return nil
}

// Stat returns information about a snapshot.
func (s *snapshotter) Stat(ctx context.Context, key string) (info snapshots.Info, err error) {
	err = s.ms.WithTransaction(ctx, false, func(ctx context.Context) error {
		_, info, _, err = storage.GetInfo(ctx, key)
		return err
	})
	if err != nil {
		return snapshots.Info{}, err
	}
	return info, nil
}

// Update modifies snapshot metadata.
func (s *snapshotter) Update(ctx context.Context, info snapshots.Info, fieldpaths ...string) (_ snapshots.Info, err error) {
	err = s.ms.WithTransaction(ctx, true, func(ctx context.Context) error {
		info, err = storage.UpdateInfo(ctx, info, fieldpaths...)
		return err
	})
	if err != nil {
		return snapshots.Info{}, err
	}
	return info, nil
}

// Walk iterates over all snapshots.
func (s *snapshotter) Walk(ctx context.Context, fn snapshots.WalkFunc, fs ...string) error {
	return s.ms.WithTransaction(ctx, false, func(ctx context.Context) error {
		return storage.WalkInfo(ctx, fn, fs...)
	})
}

// Usage returns the resources taken by the snapshot.
func (s *snapshotter) Usage(ctx context.Context, key string) (_ snapshots.Usage, err error) {
	var (
		usage snapshots.Usage
		info  snapshots.Info
		id    string
	)
	if err := s.ms.WithTransaction(ctx, false, func(ctx context.Context) error {
		id, info, usage, err = storage.GetInfo(ctx, key)
		return err
	}); err != nil {
		return usage, err
	}

	if info.Kind == snapshots.KindActive {
		upperPath := s.upperPath(id)
		du, err := fs.DiskUsage(ctx, upperPath)
		if err != nil {
			return snapshots.Usage{}, err
		}
		usage = snapshots.Usage(du)
	}
	return usage, nil
}
