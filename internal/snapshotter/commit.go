package snapshotter

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/containerd/containerd/v2/core/snapshots"
	"github.com/containerd/containerd/v2/core/snapshots/storage"
	"github.com/containerd/continuity/fs"
	"github.com/containerd/log"
	"github.com/opencontainers/go-digest"
	"golang.org/x/sync/errgroup"

	"github.com/spin-stack/erofs-snapshotter/internal/erofs"
)

// fsmeta lock retry configuration
const (
	// fsmetaLockMaxRetries is the maximum number of times to retry waiting
	// for another goroutine's fsmeta generation to complete.
	fsmetaLockMaxRetries = 10

	// fsmetaLockBaseDelay is the base delay between retries (exponential backoff).
	fsmetaLockBaseDelay = 100 * time.Millisecond

	// fsmetaLockMaxDelay caps the maximum delay between retries.
	fsmetaLockMaxDelay = 2 * time.Second

	// fsmetaStaleLockAge is how old a lock file must be to be considered stale.
	// If a lock file is older than this and the final file doesn't exist,
	// we assume the lock holder crashed and remove the stale lock.
	fsmetaStaleLockAge = 5 * time.Minute
)

// acquireFsmetaLock attempts to acquire an exclusive lock for fsmeta generation.
// Returns (true, nil) if lock was acquired and caller should proceed.
// Returns (false, nil) if another goroutine completed generation (no work needed).
// Returns (false, error) on unexpected errors.
//
// This function handles:
// 1. Atomic lock file creation (O_EXCL)
// 2. Retry with exponential backoff when lock is held by another goroutine
// 3. Stale lock detection and cleanup (from crashed processes)
func (s *snapshotter) acquireFsmetaLock(ctx context.Context, lockFile, finalFile string) (bool, error) {
	for attempt := 0; attempt <= fsmetaLockMaxRetries; attempt++ {
		// Check if final file exists (fast path - another goroutine completed)
		if _, err := os.Stat(finalFile); err == nil {
			return false, nil
		}

		// Try to create lock file atomically
		lockFd, err := os.OpenFile(lockFile, os.O_CREATE|os.O_EXCL, 0o600)
		if err == nil {
			// We acquired the lock
			lockFd.Close()
			return true, nil
		}

		if !os.IsExist(err) {
			// Unexpected error (not "file exists")
			return false, fmt.Errorf("create lock file: %w", err)
		}

		// Lock file exists - check if it's stale
		if s.tryCleanStaleLock(ctx, lockFile, finalFile) {
			// Stale lock was cleaned, retry immediately
			continue
		}

		// Lock is held by another goroutine, wait with backoff
		if attempt < fsmetaLockMaxRetries {
			delay := s.backoffDelay(attempt)
			select {
			case <-ctx.Done():
				return false, ctx.Err()
			case <-time.After(delay):
				// Continue to next attempt
			}
		}
	}

	// Exhausted retries - log and give up
	log.G(ctx).WithField("lockFile", lockFile).Warn("fsmeta generation: timed out waiting for lock")
	return false, nil
}

// tryCleanStaleLock checks if a lock file is stale and removes it if so.
// A lock is considered stale if it's older than fsmetaStaleLockAge and
// the final file doesn't exist (indicating the lock holder crashed).
func (s *snapshotter) tryCleanStaleLock(ctx context.Context, lockFile, finalFile string) bool {
	info, err := os.Stat(lockFile)
	if err != nil {
		return false // Lock file doesn't exist or other error
	}

	age := time.Since(info.ModTime())
	if age < fsmetaStaleLockAge {
		return false // Lock is fresh, let holder complete
	}

	// Check if final file exists (lock holder may have just finished)
	if _, err := os.Stat(finalFile); err == nil {
		// Final file exists - just remove the orphaned lock
		if rmErr := os.Remove(lockFile); rmErr != nil {
			log.G(ctx).WithError(rmErr).Debug("failed to remove orphaned lock file")
		}
		return false
	}

	// Lock is stale - try to remove it
	if err := os.Remove(lockFile); err != nil {
		log.G(ctx).WithError(err).WithField("age", age).Debug("failed to remove stale lock")
		return false
	}

	log.G(ctx).WithField("age", age).Info("removed stale fsmeta lock file")
	return true
}

// backoffDelay calculates exponential backoff delay for the given attempt.
func (s *snapshotter) backoffDelay(attempt int) time.Duration {
	delay := fsmetaLockBaseDelay * (1 << attempt) // 2^attempt * base
	return min(delay, fsmetaLockMaxDelay)
}

// getCommitUpperDir returns the upper directory path for EROFS conversion.
//
// WHY TWO MODES EXIST:
//   - Block mode (extract snapshots): ext4 image mounted on host so differs can write.
//     The upper directory is inside the mounted ext4 at rw/upper/.
//   - Directory mode (regular snapshots): VM handles overlay, no host mount needed.
//     The upper directory is directly at fs/.
//
// For block mode, the ext4 must already be mounted by Prepare() for extract snapshots.
// If the block mount isn't available, falls back to overlay mode.
func (s *snapshotter) getCommitUpperDir(id string) string {
	rwLayer := s.writablePath(id)

	// Check if block layer exists (rwlayer.img)
	if _, err := os.Stat(rwLayer); err != nil {
		// No block layer - use overlay upper directly
		return s.upperPath(id)
	}

	// Block mode: check if ext4 upper directory is accessible
	upperDir := s.blockUpperPath(id)
	if _, err := os.Stat(upperDir); err == nil {
		return upperDir
	}

	// rw/upper/ doesn't exist - check if rw/ mount point has content
	rwMount := s.blockRwMountPath(id)
	if entries, err := os.ReadDir(rwMount); err == nil && len(entries) > 0 {
		// Mounted but no upper/ subdirectory (empty overlay case)
		return rwMount
	}

	// Block mount not available - fall back to overlay mode
	return s.upperPath(id)
}

// commitBlock handles the conversion of a writable layer to EROFS.
// It determines the appropriate source (block or overlay) and performs conversion.
func (s *snapshotter) commitBlock(ctx context.Context, layerBlob string, id string) error {
	upperDir := s.getCommitUpperDir(id)

	if err := convertDirToErofs(ctx, layerBlob, upperDir); err != nil {
		return &CommitConversionError{
			ID:       id,
			UpperDir: upperDir,
			Cause:    err,
		}
	}

	return nil
}

// generateFsMeta creates a merged fsmeta.erofs and VMDK descriptor for VM runtimes.
// The VMDK allows QEMU to present all EROFS layers as a single concatenated block device.
//
// CALLER CONTRACT: parentIDs must be in snapshot chain order (newest-first).
// This is the order returned by containerd's snapshot storage. We convert to
// OCI manifest order (oldest-first) internally for mkfs.erofs.
//
// CONCURRENCY: Multiple goroutines may try to generate fsmeta for the same parent
// chain. A lock file (O_EXCL) ensures only one wins. Others exit silently.
//
// CRASH SAFETY: Generation uses temporary files (.tmp suffix) with atomic rename
// on success. If the process crashes mid-generation, only .tmp files remain,
// allowing retry on next access. The lock file is removed on completion/failure.
//
// SILENT FAILURE: If fsmeta generation fails, callers fall back to individual
// layer mounts. This is slightly slower but functionally correct.
func (s *snapshotter) generateFsMeta(ctx context.Context, parentIDs []string) {
	if len(parentIDs) == 0 {
		return
	}

	t1 := time.Now()

	// parentIDs[0] is the newest snapshot in chain order
	newestID := parentIDs[0]
	mergedMeta := s.fsMetaPath(newestID)
	vmdkFile := s.vmdkPath(newestID)
	lockFile := mergedMeta + ".lock"

	// Check if already generated (fast path)
	if _, err := os.Stat(mergedMeta); err == nil {
		return
	}

	// Try to acquire lock with retry logic for robustness
	acquired, err := s.acquireFsmetaLock(ctx, lockFile, mergedMeta)
	if err != nil {
		log.G(ctx).WithError(err).Warn("fsmeta generation: failed to acquire lock")
		return
	}
	if !acquired {
		// Another goroutine completed generation or we timed out waiting
		return
	}

	// Always remove lock file when done
	defer os.Remove(lockFile)

	// Temporary file paths for atomic generation
	tmpMeta := mergedMeta + ".tmp"
	tmpVmdk := vmdkFile + ".tmp"

	// Cleanup temp files on failure
	success := false
	defer func() {
		if !success {
			if err := os.Remove(tmpMeta); err != nil && !os.IsNotExist(err) {
				log.L.WithError(err).WithField("path", tmpMeta).Debug("failed to cleanup temp fsmeta file")
			}
			if err := os.Remove(tmpVmdk); err != nil && !os.IsNotExist(err) {
				log.L.WithError(err).WithField("path", tmpVmdk).Debug("failed to cleanup temp vmdk file")
			}
		}
	}()

	// Convert to oldest-first order for mkfs.erofs (OCI manifest order)
	ociOrder := reverseStrings(parentIDs)

	// Collect layer blob paths in parallel (OCI order: oldest-first)
	blobs := make([]string, len(ociOrder))
	g, gctx := errgroup.WithContext(ctx)
	for i, snapID := range ociOrder {
		g.Go(func() error {
			blob, err := s.findLayerBlob(snapID)
			if err != nil {
				return err
			}
			blobs[i] = blob
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		log.G(gctx).WithError(err).WithFields(log.Fields{
			"layerCount": len(parentIDs),
			"stage":      "collect_blobs",
		}).Warn("fsmeta generation skipped: layer blob not found")
		return
	}

	// Check block size compatibility for fsmeta merge
	if !erofs.CanMergeFsmeta(blobs) {
		log.G(ctx).WithFields(log.Fields{
			"layerCount": len(blobs),
			"stage":      "check_compat",
		}).Debug("fsmeta generation skipped: incompatible block sizes")
		return
	}

	// Generate fsmeta and VMDK to temp files.
	// mkfs.erofs embeds the fsmeta path in the VMDK, so we generate to temp
	// and then fix up the VMDK paths before the final rename.
	args := append([]string{"--quiet", "--vmdk-desc=" + tmpVmdk, tmpMeta}, blobs...)

	cmd := exec.CommandContext(ctx, "mkfs.erofs", args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		log.G(ctx).WithError(err).WithFields(log.Fields{
			"layerCount": len(blobs),
			"stage":      "mkfs_erofs",
			"output":     string(out),
		}).Warn("fsmeta generation failed: mkfs.erofs error")
		return
	}

	// Fix VMDK to reference final fsmeta path instead of temp path.
	// The VMDK is a simple text file with embedded paths.
	if err := fixVmdkPaths(tmpVmdk, tmpMeta, mergedMeta); err != nil {
		log.G(ctx).WithError(err).WithFields(log.Fields{
			"layerCount": len(blobs),
			"stage":      "fix_vmdk_paths",
		}).Warn("fsmeta generation failed: cannot fix VMDK paths")
		return
	}

	// Atomic rename: first fsmeta, then VMDK (VMDK references fsmeta)
	if err := os.Rename(tmpMeta, mergedMeta); err != nil {
		log.G(ctx).WithError(err).WithFields(log.Fields{
			"layerCount": len(blobs),
			"stage":      "rename_fsmeta",
			"from":       tmpMeta,
			"to":         mergedMeta,
		}).Warn("fsmeta generation failed: cannot rename fsmeta file")
		return
	}
	if err := os.Rename(tmpVmdk, vmdkFile); err != nil {
		log.G(ctx).WithError(err).WithFields(log.Fields{
			"layerCount": len(blobs),
			"stage":      "rename_vmdk",
			"from":       tmpVmdk,
			"to":         vmdkFile,
		}).Warn("fsmeta generation failed: cannot rename VMDK file")
		_ = os.Remove(mergedMeta) // Clean up the renamed fsmeta
		return
	}

	success = true

	// Write layer manifest for external verification
	manifestFile := s.manifestPath(newestID)
	if err := s.writeLayerManifest(manifestFile, blobs); err != nil {
		log.G(ctx).WithError(err).Warn("failed to write layer manifest (non-fatal)")
	}

	log.G(ctx).WithFields(log.Fields{
		"duration": time.Since(t1),
		"layers":   len(blobs),
	}).Debug("fsmeta and VMDK generated")
}

// fixVmdkPaths replaces oldPath with newPath in a VMDK descriptor file.
// VMDK is a simple text format where paths appear in FLAT extent lines.
// Uses atomic write (write to temp + rename) to prevent corruption on crash.
func fixVmdkPaths(vmdkFile, oldPath, newPath string) error {
	content, err := os.ReadFile(vmdkFile)
	if err != nil {
		return fmt.Errorf("read vmdk: %w", err)
	}

	// Simple string replacement - the VMDK format uses quoted paths
	fixed := strings.ReplaceAll(string(content), oldPath, newPath)

	// Atomic write: write to temp file, then rename
	if err := atomicWriteFile(vmdkFile, []byte(fixed), 0o644); err != nil {
		return fmt.Errorf("write vmdk: %w", err)
	}

	return nil
}

// atomicWriteFile writes data to a file atomically by writing to a temp file
// and then renaming. This prevents corruption if the process crashes mid-write.
func atomicWriteFile(path string, data []byte, perm os.FileMode) error {
	tmpPath := path + ".atomic"

	// Write to temp file
	if err := os.WriteFile(tmpPath, data, perm); err != nil {
		return err
	}

	// Sync to ensure data is on disk before rename
	f, err := os.Open(tmpPath)
	if err != nil {
		os.Remove(tmpPath)
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmpPath)
		return err
	}
	f.Close()

	// Atomic rename
	if err := os.Rename(tmpPath, path); err != nil {
		os.Remove(tmpPath)
		return err
	}

	return nil
}

// writeLayerManifest writes layer digests to a manifest file in VMDK/OCI order.
// Format: one digest per line (sha256:hex...), oldest/base layer first.
// This is the authoritative source for VMDK layer order verification.
// Uses atomic write to prevent corruption on crash.
func (s *snapshotter) writeLayerManifest(manifestFile string, blobs []string) error {
	var digests []digest.Digest
	for _, blob := range blobs {
		d := erofs.DigestFromLayerBlobPath(blob)
		if d != "" {
			digests = append(digests, d)
		}
		// Skip non-digest-based blobs (e.g., snapshot-xxx.erofs fallback)
	}

	if len(digests) == 0 {
		return nil // No digests to write
	}

	var lines []string
	for _, d := range digests {
		lines = append(lines, d.String())
	}

	content := strings.Join(lines, "\n") + "\n"
	return atomicWriteFile(manifestFile, []byte(content), 0o644)
}

// Commit finalizes an active snapshot, converting it to EROFS format.
//
// The commit process:
// 1. Find or create the EROFS layer blob
// 2. Enable fs-verity if configured (integrity protection)
// 3. Set immutable flag if configured (accidental deletion protection)
// 4. Update metadata to mark snapshot as committed
//
// If no layer blob exists (EROFS differ hasn't processed it), we fall back
// to converting the upper directory ourselves using the fallback naming scheme.
//
// CONCURRENCY: Commit and Remove are serialized per-key using keyLocks to prevent
// race conditions where Remove deletes metadata while Commit is processing.
func (s *snapshotter) Commit(ctx context.Context, name, key string, opts ...snapshots.Opt) error {
	log.G(ctx).WithFields(log.Fields{
		"name": name,
		"key":  key,
	}).Debug("commit: entered function, acquiring lock")

	// Acquire per-key lock to serialize with Remove operations.
	// This prevents Remove from deleting the snapshot while we're committing.
	unlock := s.keyLocks.lock(key)
	defer unlock()

	log.G(ctx).WithFields(log.Fields{
		"name": name,
		"key":  key,
	}).Debug("commit: lock acquired, starting transaction")

	var layerBlob string
	var id string

	// Get snapshot ID in a read transaction (conversion can be slow)
	err := s.ms.WithTransaction(ctx, false, func(ctx context.Context) error {
		sid, _, _, err := storage.GetInfo(ctx, key)
		if err != nil {
			return fmt.Errorf("get snapshot info for %q: %w", key, err)
		}
		id = sid
		return nil
	})
	if err != nil {
		log.G(ctx).WithError(err).WithFields(log.Fields{
			"name": name,
			"key":  key,
		}).Debug("commit: failed to get snapshot info")
		return err
	}

	log.G(ctx).WithFields(log.Fields{
		"name": name,
		"key":  key,
		"id":   id,
	}).Debug("commit: got snapshot ID, finding layer blob")

	// Find existing layer blob or create via fallback
	layerBlob, err = s.findLayerBlob(id)
	if err != nil {
		// Layer doesn't exist - EROFS differ hasn't processed this layer.
		// Fall back to converting the upper directory ourselves.
		log.G(ctx).WithField("id", id).Debug("layer blob not found, using fallback conversion")

		layerBlob = s.fallbackLayerBlobPath(id)
		if cerr := s.commitBlock(ctx, layerBlob, id); cerr != nil {
			return fmt.Errorf("fallback conversion failed: %w", cerr)
		}
	}

	// Set immutable flag to prevent accidental deletion
	if s.setImmutable {
		if err := setImmutable(layerBlob, true); err != nil {
			log.G(ctx).WithError(err).Warn("failed to set immutable flag (non-fatal)")
		}
	}

	// Commit to metadata in a write transaction
	err = s.ms.WithTransaction(ctx, true, func(ctx context.Context) error {
		if _, err := os.Stat(layerBlob); err != nil {
			return fmt.Errorf("verify layer blob: %w", err)
		}

		usage, err := fs.DiskUsage(ctx, layerBlob)
		if err != nil {
			return fmt.Errorf("calculate disk usage: %w", err)
		}

		if _, err = storage.CommitActive(ctx, key, name, snapshots.Usage(usage), opts...); err != nil {
			return fmt.Errorf("commit snapshot: %w", err)
		}

		log.G(ctx).WithFields(log.Fields{
			"name":  name,
			"blob":  layerBlob,
			"bytes": usage.Size,
		}).Info("snapshot committed")

		return nil
	})
	if err != nil {
		return err
	}

	// Cleanup the ext4 mount from Prepare (for extract snapshots).
	// The EROFS blob now contains the layer data, so the ext4 is no longer needed.
	rwMount := s.blockRwMountPath(id)
	if isMounted(rwMount) {
		if unmountErr := unmountAll(rwMount); unmountErr != nil {
			log.G(ctx).WithError(unmountErr).WithField("id", id).Warn("failed to cleanup ext4 mount after commit")
		}
	}

	return nil
}
