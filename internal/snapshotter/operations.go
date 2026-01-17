package snapshotter

import (
	"context"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/containerd/containerd/v2/core/mount"
	"github.com/containerd/containerd/v2/core/snapshots"
	"github.com/containerd/containerd/v2/core/snapshots/storage"
	"github.com/containerd/continuity/fs"
	"github.com/containerd/errdefs"
	"github.com/containerd/log"

	"github.com/spin-stack/erofs-snapshotter/internal/erofs"
)

// ParentNotCommittedError is returned when a parent snapshot is referenced
// but hasn't been committed yet. This can happen during parallel layer unpacking
// when a child layer's Prepare is called before the parent layer's Commit completes.
// Containerd's unpack logic handles this by retrying with proper ordering.
type ParentNotCommittedError struct {
	Parent string
}

func (e *ParentNotCommittedError) Error() string {
	return fmt.Sprintf("parent snapshot %q not committed yet", e.Parent)
}

// Is implements errors.Is for ParentNotCommittedError.
// Returns true for errdefs.ErrNotFound so containerd's retry logic handles it.
func (e *ParentNotCommittedError) Is(target error) bool {
	return errdefs.IsNotFound(target)
}

// fsmetaTimeout is the maximum time allowed for fsmeta generation.
// This includes reading layer blobs and running mkfs.erofs.
const fsmetaTimeout = 5 * time.Minute

const (
	// parentWaitTimeout is the maximum time to wait for a parent snapshot
	// to be committed during parallel layer unpacking.
	parentWaitTimeout = 30 * time.Second
	// parentWaitInterval is the initial interval between checks for parent existence.
	// Start with a very short interval since most parents are ready quickly.
	parentWaitInterval = 10 * time.Millisecond
	// parentWaitMaxInterval is the maximum interval between checks.
	parentWaitMaxInterval = 200 * time.Millisecond
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
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
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

// startFsmetaGeneration spawns a background goroutine to generate fsmeta for multi-layer images.
// The goroutine acquires a semaphore to limit concurrent generations and uses an independent
// context with timeout to allow completion even if the original request is cancelled.
func (s *snapshotter) startFsmetaGeneration(parentIDs []string) {
	s.bgWg.Add(1)
	//nolint:contextcheck // intentionally using fresh context with timeout for background work
	go func(ids []string) {
		defer s.bgWg.Done()
		// Panic recovery prevents Close() from hanging forever if goroutine panics.
		defer func() {
			if r := recover(); r != nil {
				log.L.WithField("panic", r).Error("fsmeta generation panic recovered")
			}
		}()

		bgCtx, cancel := context.WithTimeout(context.Background(), fsmetaTimeout)
		defer cancel()

		// Acquire semaphore to limit concurrent fsmeta generations.
		if err := s.fsmetaSem.Acquire(bgCtx, 1); err != nil {
			return // Context cancelled or timed out waiting for semaphore
		}
		defer s.fsmetaSem.Release(1)

		s.generateFsMeta(bgCtx, ids)
	}(parentIDs)
}

// waitForParent waits for a parent snapshot to exist in metadata.
// This handles the race condition during parallel layer unpacking where a child
// layer's Prepare is called before the parent layer's Commit completes.
//
// Uses exponential backoff starting from a short interval (10ms) since most
// parents are ready quickly. This is more efficient than containerd's retry
// mechanism which would require a full round-trip through the unpack stack.
func (s *snapshotter) waitForParent(ctx context.Context, parent string) error {
	// Fast path: check if parent already exists
	if s.snapshotExists(ctx, parent) {
		return nil
	}

	log.G(ctx).WithField("parent", parent).Debug("waiting for parent snapshot to be committed")

	deadline := time.Now().Add(parentWaitTimeout)
	interval := parentWaitInterval

	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(interval):
			if s.snapshotExists(ctx, parent) {
				log.G(ctx).WithField("parent", parent).Debug("parent snapshot now available")
				return nil
			}
			// Exponential backoff with cap
			interval = min(interval*2, parentWaitMaxInterval)
		}
	}

	return &ParentNotCommittedError{Parent: parent}
}

// snapshotExists checks if a snapshot with the given key exists in metadata.
// The key may be in proxy format (namespace/txID/name) where the txID varies
// between gRPC calls. We extract the name part and search for any snapshot
// that matches it, regardless of the transaction ID prefix.
func (s *snapshotter) snapshotExists(ctx context.Context, key string) bool {
	// Extract the actual snapshot name from proxy key format.
	// Proxy keys have format: namespace/txID/name (e.g., "spinbox-ci/11/sha256:abc...")
	// The txID changes between calls, so we need to match by name only.
	targetName := extractSnapshotName(key)

	var exists bool
	_ = s.ms.WithTransaction(ctx, false, func(ctx context.Context) error {
		// First try exact match (fast path)
		_, _, _, err := storage.GetInfo(ctx, key)
		if err == nil {
			exists = true
			return nil
		}

		// If exact match failed and key has proxy format, scan for matching name
		if targetName != key {
			return storage.WalkInfo(ctx, func(_ context.Context, info snapshots.Info) error {
				if extractSnapshotName(info.Name) == targetName {
					exists = true
					return errStopWalk
				}
				return nil
			})
		}
		return nil
	})
	return exists
}

// errStopWalk is used to stop walking when we find a match.
var errStopWalk = fmt.Errorf("stop walk")

// extractSnapshotName extracts the snapshot name from a proxy-formatted key.
// Proxy keys from containerd's metadata layer have format: namespace/txID/name
// where txID is a transaction ID that changes between gRPC calls.
// Returns the name part, or the original key if not in proxy format.
func extractSnapshotName(key string) string {
	// Format: namespace/txID/name where name may contain slashes (e.g., sha256:abc)
	parts := strings.SplitN(key, "/", 3)
	if len(parts) == 3 {
		// Check if second part looks like a numeric transaction ID
		if _, err := fmt.Sscanf(parts[1], "%d", new(int)); err == nil {
			return parts[2]
		}
	}
	// Not in proxy format, return as-is
	return key
}

//nolint:cyclop // complexity needed for parallel unpack wait logic and snapshot lifecycle
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

	// For parallel layer unpacking, wait for the parent snapshot to be committed.
	// When containerd unpacks layers in parallel, a child layer's Prepare may be
	// called before the parent layer's Commit completes. We wait with exponential
	// backoff to allow the parent commit to finish.
	if parent != "" && isExtractKey(key) {
		if err := s.waitForParent(ctx, parent); err != nil {
			return nil, err
		}
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
	// Run async to avoid blocking Prepare/View.
	if !isExtractKey(key) && len(snap.ParentIDs) > 0 {
		//nolint:contextcheck // uses fresh context to complete even if request is cancelled
		s.startFsmetaGeneration(snap.ParentIDs)
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
//
// CONCURRENCY: Remove and Commit are serialized per-key using keyLocks to prevent
// race conditions where Remove deletes metadata while Commit is processing.
func (s *snapshotter) Remove(ctx context.Context, key string) (err error) {
	log.G(ctx).WithField("key", key).Debug("remove: entered function, acquiring lock")

	// Acquire per-key lock to serialize with Commit operations.
	// This prevents removing a snapshot while it's being committed.
	unlock := s.keyLocks.lock(key)
	defer unlock()

	log.G(ctx).WithField("key", key).Debug("remove: lock acquired, starting transaction")

	var removals []string
	var id string

	defer func() {
		if err == nil {
			log.G(ctx).WithFields(log.Fields{
				"key": key,
				"id":  id,
			}).Debug("remove: cleanup after successful remove")
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
				// Use local variable to avoid polluting the named return 'err'.
				// If err is set here and is errdefs.IsNotImplemented, the defer
				// would skip cleanupAfterRemove because err != nil.
				if immErr := setImmutable(layerBlob, false); immErr != nil && !errdefs.IsNotImplemented(immErr) {
					return fmt.Errorf("clear IMMUTABLE_FL: %w", immErr)
				}
			}
		}
		return nil
	})
}

// cleanupAfterRemove handles post-removal cleanup.
func (s *snapshotter) cleanupAfterRemove(ctx context.Context, id string, removals []string) {
	// Invalidate layer cache for removed snapshot
	s.invalidateLayerCache(id)

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
// Errors are logged but don't stop cleanup (best-effort).
func (s *snapshotter) Cleanup(ctx context.Context) error {
	var removals []string
	if err := s.ms.WithTransaction(ctx, false, func(ctx context.Context) error {
		var err error
		removals, err = s.getCleanupDirectories(ctx)
		return err
	}); err != nil {
		return err
	}

	for _, dir := range removals {
		// Cleanup block rw mount
		if err := unmountAll(filepath.Join(dir, rwDirName)); err != nil {
			log.G(ctx).WithError(err).WithField("path", dir).Debug("failed to cleanup block rw mount")
		}

		// Clear immutable flag on any EROFS blobs before removal
		clearImmutableFlags(ctx, dir)

		if err := os.RemoveAll(dir); err != nil {
			log.G(ctx).WithError(err).WithField("path", dir).Warn("failed to remove directory")
		}
	}

	return nil
}

// clearImmutableFlags clears the immutable flag on all EROFS blobs in a directory.
// Searches both digest-based (sha256-*.erofs) and fallback (snapshot-*.erofs) patterns.
func clearImmutableFlags(ctx context.Context, dir string) {
	patterns := []string{erofs.LayerBlobPattern, "snapshot-*.erofs"}
	for _, pattern := range patterns {
		matches, err := filepath.Glob(filepath.Join(dir, pattern))
		if err != nil {
			continue
		}
		for _, match := range matches {
			if err := setImmutable(match, false); err != nil && !errdefs.IsNotImplemented(err) {
				log.G(ctx).WithError(err).WithField("path", match).Debug("failed to clear immutable flag")
			}
		}
	}
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
