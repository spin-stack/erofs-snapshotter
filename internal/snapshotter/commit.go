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
	"github.com/spin-stack/erofs-snapshotter/internal/mountutils"
)

// getCommitUpperDir returns the upper directory to convert to EROFS, a
// cleanup function (releases any mount this function created) and whether the
// upper directory may be pruned after conversion.
//
// WHY TWO MODES EXIST:
//   - Block mode (rwlayer.img exists): the container's changes live inside the
//     ext4 image. For extract snapshots Prepare() already mounted it at rw/, so
//     the upper directory is rw/upper/. For runtime snapshots (direct commit of
//     a container, the "nerdctl commit" flow) the ext4 is NOT mounted: it is
//     mounted read-only here to read the upper the guest wrote.
//   - Overlay mode (no rwlayer.img): VM handles overlay, the upper is fs/.
//
// Block mode NEVER falls back to fs/: in block mode fs/ is always empty, so
// converting it would silently commit an empty layer and lose the container's
// changes. If the ext4 cannot be mounted (e.g. the VM is still running and
// holds the image lock), an error is returned instead.
func (s *snapshotter) getCommitUpperDir(ctx context.Context, id string) (upperDir string, cleanup func(), prune bool, err error) {
	nop := func() {}
	rwLayer := s.writablePath(id)

	// Check if block layer exists (rwlayer.img)
	if _, serr := os.Stat(rwLayer); serr != nil {
		if !os.IsNotExist(serr) {
			return "", nop, false, fmt.Errorf("stat writable layer: %w", serr)
		}
		// No block layer - use overlay upper directly
		return s.upperPath(id), nop, true, nil //nolint:nilerr // absence of rwlayer.img selects overlay mode, not an error
	}

	// Block mode: use the ext4 upper directory if Prepare() mounted it
	// (extract snapshots).
	upperDir = s.blockUpperPath(id)
	if _, err := os.Stat(upperDir); err == nil {
		return upperDir, nop, true, nil
	}

	// The ext4 is not mounted (runtime snapshot commit, or the mount was lost
	// across a restart). Mount it read-only to read the guest's upper. This
	// also acts as the "container must be stopped" gate: MountExt4 fails if
	// the VM still holds the image.
	rwMount := s.blockRwMountPath(id)
	if err := os.MkdirAll(rwMount, 0o755); err != nil {
		return "", nop, false, fmt.Errorf("create rw mount point: %w", err)
	}
	unmount, err := mountutils.MountExt4(rwLayer, rwMount)
	if err != nil {
		return "", nop, false, fmt.Errorf("mount writable layer for commit: %w", err)
	}
	cleanup = func() {
		if cerr := unmount(); cerr != nil {
			log.G(ctx).WithError(cerr).WithField("id", id).Warn("failed to unmount writable layer after commit")
		}
	}

	if _, serr := os.Stat(upperDir); serr != nil {
		// No upper/ layout inside the ext4: the guest made no changes.
		// Commit an empty layer from the (empty) fs/ directory rather than
		// the ext4 root, which contains lost+found/ and work/.
		return s.upperPath(id), cleanup, true, nil //nolint:nilerr // missing upper/ means an empty layer, not an error
	}
	// The ext4 is mounted read-only: the upper cannot (and must not) be pruned.
	return upperDir, cleanup, false, nil
}

// commitBlock handles the conversion of a writable layer to EROFS.
// It determines the appropriate source (block or overlay) and performs conversion.
func (s *snapshotter) commitBlock(ctx context.Context, layerBlob string, id string) error {
	upperDir, cleanup, prune, err := s.getCommitUpperDir(ctx, id)
	if err != nil {
		return err
	}
	defer cleanup()

	if err := convertDirToErofs(ctx, layerBlob, upperDir, prune); err != nil {
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

// generateFsMeta creates a merged fsmeta.erofs and VMDK descriptor for VM runtimes.
// The VMDK allows QEMU to present all EROFS layers as a single concatenated block device.
//
// CALLER CONTRACT: parentIDs must be in snapshot chain order (newest-first).
// This is the order returned by containerd's snapshot storage. We convert to
// OCI manifest order (oldest-first) internally for mkfs.erofs.
//
// CONCURRENCY: the caller (spawnFsmetaGeneration) holds the fsmeta flock for
// the chain while this runs; concurrent generation attempts for the same
// chain are deduplicated at acquisition time.
//
// CRASH SAFETY: Generation uses temporary files (.tmp suffix) with atomic rename
// on success. If the process crashes mid-generation, only .tmp files remain
// (the kernel releases the flock), allowing retry on next access.
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

	// Check if already generated (fast path)
	if _, err := os.Stat(mergedMeta); err == nil {
		return
	}

	// The parent snapshot may have been removed while this generation waited
	// for a semaphore slot; don't write artifacts into a directory that is
	// being deleted.
	if _, err := os.Stat(s.snapshotDir(newestID)); err != nil {
		return
	}

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

	// Check block size compatibility for fsmeta merge. This is a permanent
	// failure (block sizes never change), so future mount calls will keep
	// falling back to per-layer mounts until the layer blobs are regenerated.
	// A marker file records the verdict so the check (and a Warn per Prepare)
	// isn't repeated for every snapshot of the same chain.
	// Logged at Warn so operators can spot the problematic layer.
	if err := erofs.CheckFsmetaCompat(blobs); err != nil {
		log.G(ctx).WithError(err).WithFields(log.Fields{
			"layerCount": len(blobs),
			"stage":      "check_compat",
		}).Warn("fsmeta generation permanently disabled: incompatible layer")
		if werr := os.WriteFile(s.fsmetaIncompatPath(newestID), []byte(err.Error()+"\n"), 0o644); werr != nil {
			log.G(ctx).WithError(werr).Debug("failed to write fsmeta incompat marker")
		}
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
		s.logFsmetaWriteFailure(ctx, newestID, err, log.Fields{
			"layerCount": len(blobs),
			"stage":      "rename_fsmeta",
			"from":       tmpMeta,
			"to":         mergedMeta,
		})
		return
	}
	if err := os.Rename(tmpVmdk, vmdkFile); err != nil {
		s.logFsmetaWriteFailure(ctx, newestID, err, log.Fields{
			"layerCount": len(blobs),
			"stage":      "rename_vmdk",
			"from":       tmpVmdk,
			"to":         vmdkFile,
		})
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

// logFsmetaWriteFailure logs a failed fsmeta/VMDK write. When the parent
// snapshot directory no longer exists the failure is expected - the snapshot
// was removed while the background generation ran - so it is logged at Debug
// instead of alarming operators with a Warn.
func (s *snapshotter) logFsmetaWriteFailure(ctx context.Context, parentID string, err error, fields log.Fields) {
	logger := log.G(ctx).WithError(err).WithFields(fields)
	if _, serr := os.Stat(s.snapshotDir(parentID)); os.IsNotExist(serr) {
		logger.Debug("fsmeta generation aborted: snapshot removed during generation")
		return
	}
	logger.Warn("fsmeta generation failed")
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
	} else if _, verr := erofs.GetBlockSize(layerBlob); verr != nil {
		// Defense in depth: blob writers use tmp+rename, but never commit a
		// blob whose superblock doesn't parse - a corrupt layer would only
		// surface when the guest tries to mount it.
		return fmt.Errorf("existing layer blob %s is not a valid EROFS image: %w", layerBlob, verr)
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
			// Leave rwlayer.img in place: it may still back the live mount.
			return nil
		}
	}

	// Reclaim the ext4 writable layer: the committed EROFS blob holds the
	// layer content, and a sparse ~64MiB image plus journal would otherwise
	// linger for every committed layer.
	if rerr := os.Remove(s.writablePath(id)); rerr != nil && !os.IsNotExist(rerr) {
		log.G(ctx).WithError(rerr).WithField("id", id).Warn("failed to remove writable layer after commit")
	}

	return nil
}
