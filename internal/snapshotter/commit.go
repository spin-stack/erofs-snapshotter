package snapshotter

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/containerd/containerd/v2/core/mount"
	"github.com/containerd/containerd/v2/core/snapshots"
	"github.com/containerd/containerd/v2/core/snapshots/storage"
	"github.com/containerd/continuity/fs"
	"github.com/containerd/log"
	"github.com/opencontainers/go-digest"

	"github.com/aledbf/nexus-erofs/internal/erofs"
	"github.com/aledbf/nexus-erofs/internal/fsverity"
)

// commitSource represents the source directory for EROFS conversion.
// It abstracts the difference between block mode (ext4 mounted) and
// directory mode (overlay upper).
type commitSource struct {
	// upperDir is the path containing files to convert to EROFS.
	upperDir string

	// cleanup is called after conversion to release resources (e.g., unmount).
	// Returns an error if cleanup fails (e.g., unmount failure).
	cleanup func() error
}

// resolveCommitSource determines where to read upper directory contents from.
//
// WHY TWO MODES EXIST:
//   - Block mode (extract snapshots): ext4 image mounted on host so differs can write.
//     The upper directory is inside the mounted ext4 at rw/upper/.
//   - Directory mode (regular snapshots): VM handles overlay, no host mount needed.
//     The upper directory is directly at fs/.
//
// This function returns the appropriate path and a cleanup function.
func (s *snapshotter) resolveCommitSource(ctx context.Context, id string) (*commitSource, error) {
	rwLayer := s.writablePath(id)

	// Check if block layer exists (rwlayer.img)
	if _, err := os.Stat(rwLayer); err != nil {
		if os.IsNotExist(err) {
			// Directory mode: no block layer, use overlay upper directly
			return s.commitSourceFromOverlay(ctx, id)
		}
		return nil, fmt.Errorf("access writable layer %s: %w", rwLayer, err)
	}

	// Block mode: need to mount/access the ext4 image
	return s.commitSourceFromBlock(ctx, id, rwLayer)
}

// commitSourceFromOverlay returns the commit source for directory mode.
// This is the simple case where the overlay upper is directly accessible.
func (s *snapshotter) commitSourceFromOverlay(_ context.Context, id string) (*commitSource, error) {
	return &commitSource{
		upperDir: s.upperPath(id),
		cleanup:  func() error { return nil }, // No cleanup needed
	}, nil
}

// commitSourceFromBlock returns the commit source for block mode.
// This handles the case where an ext4 image needs to be mounted.
//
// If the ext4 is already mounted (from Prepare for extract snapshots),
// we use it directly. Otherwise, we mount read-only for commit.
func (s *snapshotter) commitSourceFromBlock(ctx context.Context, id, rwLayer string) (*commitSource, error) {
	rwMount := s.blockRwMountPath(id)

	// Check if already mounted (e.g., from Prepare for extract snapshots)
	alreadyMounted := isMounted(rwMount)

	needsUnmount := false
	if !alreadyMounted {
		// Mount read-only for commit (we're just reading the upper contents)
		if err := s.mountBlockReadOnly(ctx, rwLayer, rwMount); err != nil {
			return nil, err
		}
		needsUnmount = true
		log.G(ctx).WithField("target", rwMount).Debug("mounted block layer read-only for commit")
	}

	// Determine upper directory inside the mounted ext4
	upperDir := s.blockUpperPath(id)
	if _, err := os.Stat(upperDir); os.IsNotExist(err) {
		// Upper directory doesn't exist inside ext4 - use mount root
		// This happens when the overlay had no changes
		upperDir = rwMount
	}

	cleanup := func() error {
		if needsUnmount {
			if err := unmountAll(rwMount); err != nil {
				return fmt.Errorf("cleanup block mount for %s: %w", id, err)
			}
		}
		return nil
	}

	return &commitSource{
		upperDir: upperDir,
		cleanup:  cleanup,
	}, nil
}

// mountBlockReadOnly mounts an ext4 image read-only for reading upper contents.
func (s *snapshotter) mountBlockReadOnly(ctx context.Context, source, target string) error {
	if err := os.MkdirAll(target, 0755); err != nil {
		return fmt.Errorf("create mount point: %w", err)
	}

	m := mount.Mount{
		Source:  source,
		Type:    "ext4",
		Options: []string{"ro", "loop", "noload"},
	}

	if err := m.Mount(target); err != nil {
		return &BlockMountError{
			Source: source,
			Target: target,
			Cause:  err,
		}
	}

	return nil
}

// commitBlock handles the conversion of a writable layer to EROFS.
// It determines the appropriate source (block or overlay) and performs conversion.
func (s *snapshotter) commitBlock(ctx context.Context, layerBlob string, id string) error {
	source, err := s.resolveCommitSource(ctx, id)
	if err != nil {
		return err
	}

	convErr := convertDirToErofs(ctx, layerBlob, source.upperDir)

	// Always attempt cleanup, even if conversion failed
	cleanupErr := source.cleanup()

	if convErr != nil {
		return &CommitConversionError{
			SnapshotID: id,
			UpperDir:   source.upperDir,
			Cause:      convErr,
		}
	}

	return cleanupErr
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

	// Atomic lock file creation - only one goroutine wins
	lockFd, err := os.OpenFile(lockFile, os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		// Another goroutine is generating or lock file is stale
		// Check if final file exists now (generation completed while we waited)
		if _, statErr := os.Stat(mergedMeta); statErr == nil {
			return
		}
		// Lock file exists but no final file - could be stale from crash.
		// Let the other holder finish or fail; don't compete.
		return
	}
	lockFd.Close()

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
			log.G(ctx).WithError(err).WithField("snapshot", snapID).Debug("layer blob not found, skipping fsmeta")
			return
		}
		blobs = append(blobs, blob)
	}

	// Check block size compatibility for fsmeta merge
	if !erofs.CanMergeFsmeta(blobs) {
		log.G(ctx).Debug("skipping fsmeta generation: incompatible block sizes")
		return
	}

	// Generate fsmeta and VMDK to temp files.
	// mkfs.erofs embeds the fsmeta path in the VMDK, so we generate to temp
	// and then fix up the VMDK paths before the final rename.
	args := append([]string{"--quiet", "--vmdk-desc=" + tmpVmdk, tmpMeta}, blobs...)

	cmd := exec.CommandContext(ctx, "mkfs.erofs", args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		log.G(ctx).WithError(err).Warnf("fsmeta generation failed: %s", string(out))
		return
	}

	// Fix VMDK to reference final fsmeta path instead of temp path.
	// The VMDK is a simple text file with embedded paths.
	if err := fixVmdkPaths(tmpVmdk, tmpMeta, mergedMeta); err != nil {
		log.G(ctx).WithError(err).Warn("failed to fix VMDK paths")
		return
	}

	// Atomic rename: first fsmeta, then VMDK (VMDK references fsmeta)
	if err := os.Rename(tmpMeta, mergedMeta); err != nil {
		log.G(ctx).WithError(err).Warn("failed to rename fsmeta file")
		return
	}
	if err := os.Rename(tmpVmdk, vmdkFile); err != nil {
		log.G(ctx).WithError(err).Warn("failed to rename VMDK file")
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

	if err := os.WriteFile(vmdkFile, []byte(fixed), 0644); err != nil {
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
	return os.WriteFile(manifestFile, []byte(content), 0644)
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

	// Enable fs-verity for integrity protection
	if s.enableFsverity {
		if err := fsverity.Enable(layerBlob); err != nil {
			return fmt.Errorf("enable fsverity: %w", err)
		}
		log.G(ctx).WithField("blob", layerBlob).Debug("fs-verity enabled")
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
