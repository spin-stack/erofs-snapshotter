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

	"github.com/spin-stack/erofs-snapshotter/internal/erofs"
)

const fsmetaLockStaleAge = fsmetaTimeout + time.Minute

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
			SnapshotID: id,
			UpperDir:   upperDir,
			Cause:      err,
		}
	}

	return nil
}

func removeFsmetaArtifacts(paths ...string) error {
	for _, path := range paths {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}

func isStaleFsmetaLock(lockFile string, maxAge time.Duration) (bool, error) {
	info, err := os.Stat(lockFile)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}

	return time.Since(info.ModTime()) > maxAge, nil
}

func (s *snapshotter) cleanupFsmetaArtifacts() {
	entries, err := os.ReadDir(s.snapshotsDir())
	if err != nil {
		return
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		snapshotID := entry.Name()
		lockFile := s.fsMetaPath(snapshotID) + ".lock"
		tmpMeta := s.fsMetaPath(snapshotID) + ".tmp"
		tmpVmdk := s.vmdkPath(snapshotID) + ".tmp"

		if err := removeFsmetaArtifacts(lockFile, tmpMeta, tmpVmdk); err != nil {
			log.L.WithError(err).WithField("snapshot", snapshotID).Debug("failed to cleanup fsmeta startup artifacts")
		}
	}
}

func (s *snapshotter) tryAcquireFsmetaLock(ctx context.Context, snapshotID string) (bool, error) {
	mergedMeta := s.fsMetaPath(snapshotID)
	lockFile := mergedMeta + ".lock"
	tmpMeta := mergedMeta + ".tmp"
	tmpVmdk := s.vmdkPath(snapshotID) + ".tmp"

	if _, err := os.Stat(mergedMeta); err == nil {
		return false, nil
	}

	for range 2 {
		lockFd, err := os.OpenFile(lockFile, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if err == nil {
			if cerr := lockFd.Close(); cerr != nil {
				_ = os.Remove(lockFile)
				return false, fmt.Errorf("close fsmeta lock: %w", cerr)
			}
			return true, nil
		}
		if !os.IsExist(err) {
			return false, fmt.Errorf("create fsmeta lock: %w", err)
		}

		if _, statErr := os.Stat(mergedMeta); statErr == nil {
			return false, nil
		}

		stale, staleErr := isStaleFsmetaLock(lockFile, fsmetaLockStaleAge)
		if staleErr != nil {
			return false, fmt.Errorf("stat fsmeta lock: %w", staleErr)
		}
		if !stale {
			return false, nil
		}

		if err := removeFsmetaArtifacts(lockFile, tmpMeta, tmpVmdk); err != nil {
			return false, fmt.Errorf("cleanup stale fsmeta lock: %w", err)
		}

		log.G(ctx).WithFields(log.Fields{
			"snapshot": snapshotID,
			"lock":     lockFile,
		}).Warn("removed stale fsmeta generation lock")
	}

	return false, nil
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

	locked, err := s.tryAcquireFsmetaLock(ctx, newestID)
	if err != nil {
		log.G(ctx).WithError(err).WithField("snapshot", newestID).Warn("fsmeta generation skipped: cannot acquire lock")
		return
	}
	if !locked {
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
			_ = os.Remove(tmpMeta)
			_ = os.Remove(tmpVmdk)
		}
	}()

	// Convert to oldest-first order for mkfs.erofs (OCI manifest order)
	ociOrder := reverseStrings(parentIDs)

	// Collect layer blob paths in OCI order (oldest-first)
	var blobs []string
	for _, snapID := range ociOrder {
		blob, err := s.findLayerBlob(snapID)
		if err != nil {
			log.G(ctx).WithError(err).WithFields(log.Fields{
				"snapshot":       snapID,
				"layerCount":     len(parentIDs),
				"stage":          "collect_blobs",
				"collectedSoFar": len(blobs),
			}).Warn("fsmeta generation skipped: layer blob not found")
			return
		}
		blobs = append(blobs, blob)
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
func fixVmdkPaths(vmdkFile, oldPath, newPath string) error {
	content, err := os.ReadFile(vmdkFile)
	if err != nil {
		return fmt.Errorf("read vmdk: %w", err)
	}

	// Simple string replacement - the VMDK format uses quoted paths
	fixed := strings.ReplaceAll(string(content), oldPath, newPath)

	if err := os.WriteFile(vmdkFile, []byte(fixed), 0o644); err != nil { //nolint:gosec // G703: vmdkFile is constructed internally via path helpers, not user input
		return fmt.Errorf("write vmdk: %w", err)
	}

	return nil
}

// writeLayerManifest writes layer digests to a manifest file in VMDK/OCI order.
// Format: one digest per line (sha256:hex...), oldest/base layer first.
// This is the authoritative source for VMDK layer order verification.
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
	return os.WriteFile(manifestFile, []byte(content), 0o644)
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
func (s *snapshotter) Commit(ctx context.Context, name, key string, opts ...snapshots.Opt) error {
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
		return err
	}

	log.G(ctx).WithFields(log.Fields{
		"name": name,
		"key":  key,
		"id":   id,
	}).Debug("starting commit")

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
