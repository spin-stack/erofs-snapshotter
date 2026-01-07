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
	cleanup func()

	// mode describes the source for logging/debugging.
	mode string
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
func (s *snapshotter) commitSourceFromOverlay(ctx context.Context, id string) (*commitSource, error) {
	upperDir := s.upperPath(id)

	log.G(ctx).WithField("upper", upperDir).Debug("using overlay directory mode for commit")

	return &commitSource{
		upperDir: upperDir,
		cleanup:  func() {}, // No cleanup needed
		mode:     "overlay",
	}, nil
}

// commitSourceFromBlock returns the commit source for block mode.
// This handles the case where an ext4 image needs to be mounted.
//
// WHY EXPLICIT STATE CHECK:
// We use the mount tracker instead of mountinfo.Mounted() because:
//   - mountinfo.Mounted() can fail on non-existent paths
//   - Race conditions between check and use
//   - Explicit state is easier to reason about and test
//
// Extract snapshots mount ext4 during Prepare() and track it in mounts.
// For crash recovery, the tracker will be empty so we mount read-only.
func (s *snapshotter) commitSourceFromBlock(ctx context.Context, id, rwLayer string) (*commitSource, error) {
	rwMount := s.blockRwMountPath(id)

	// Check mount tracker for explicit state (preferred over filesystem check)
	alreadyMounted := s.mountTracker.IsMounted(id)

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

	cleanup := func() {
		if needsUnmount {
			if err := unmountAll(rwMount); err != nil {
				log.G(ctx).WithError(err).WithField("id", id).Warn("failed to cleanup block mount after commit")
			}
		}
		// Clear mount state regardless of who mounted it - commit is done
		s.mountTracker.SetUnmounted(id)
	}

	return &commitSource{
		upperDir: upperDir,
		cleanup:  cleanup,
		mode:     "block",
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
	defer source.cleanup()

	log.G(ctx).WithFields(log.Fields{
		"id":    id,
		"mode":  source.mode,
		"upper": source.upperDir,
	}).Debug("converting upper directory to EROFS")

	if err := convertDirToErofs(ctx, layerBlob, source.upperDir); err != nil {
		return &CommitConversionError{
			SnapshotID: id,
			UpperDir:   source.upperDir,
			Cause:      err,
		}
	}

	return nil
}

// generateFsMeta creates a merged fsmeta.erofs and VMDK descriptor for VM runtimes.
// The VMDK allows QEMU to present all EROFS layers as a single concatenated block device.
//
// WHY ASYNC: fsmeta generation is expensive (mkfs.erofs subprocess) but not required
// for basic snapshot operations. Running async keeps Prepare/View fast.
//
// WHY PLACEHOLDER: Multiple goroutines may try to generate fsmeta for the same parent
// chain. The placeholder file (O_EXCL) ensures only one wins. Others exit silently -
// the winner's result will be used.
//
// WHY SILENT FAILURE: If fsmeta generation fails, callers fall back to individual
// layer mounts. This is slightly slower but functionally correct. The placeholder
// is removed on failure, allowing retry on next access.
//
// LAYER ORDERING:
// The parentIDs parameter uses snapshot chain order (newest-first).
// We convert to OCI manifest order (oldest-first) for mkfs.erofs.
// See layer_order.go for the LayerSequence type that makes this explicit.
func (s *snapshotter) generateFsMeta(ctx context.Context, parentIDs LayerSequence) {
	if parentIDs.IsEmpty() {
		return
	}

	t1 := time.Now()

	// Use the newest snapshot's directory for output files
	newestID := parentIDs.IDs[0]
	mergedMeta := s.fsMetaPath(newestID)
	vmdkFile := s.vmdkPath(newestID)

	// Atomic placeholder creation - only one goroutine wins
	if _, err := os.OpenFile(mergedMeta, os.O_CREATE|os.O_EXCL, 0600); err != nil {
		// Another goroutine is generating or already generated - exit silently
		return
	}

	// Track success for cleanup
	var blobs []string

	// Cleanup placeholder on failure
	defer func() {
		if blobs == nil {
			_ = os.Remove(mergedMeta)
			_ = os.Remove(vmdkFile)
		}
	}()

	// Convert to oldest-first order for mkfs.erofs
	ociOrder := parentIDs.Reverse()

	log.G(ctx).WithField("layers", ociOrder.Len()).Debug("collecting layer blobs for fsmeta generation")

	// Collect layer blob paths in OCI order (oldest-first)
	for _, snapID := range ociOrder.IDs {
		blob, err := s.findLayerBlob(snapID)
		if err != nil {
			log.G(ctx).WithError(err).WithField("snapshot", snapID).Debug("layer blob not found, skipping fsmeta")
			blobs = nil // Signal failure
			return
		}
		blobs = append(blobs, blob)
	}

	// Check block size compatibility for fsmeta merge
	if !erofs.CanMergeFsmeta(blobs) {
		log.G(ctx).Debug("skipping fsmeta generation: incompatible block sizes")
		blobs = nil
		return
	}

	// Generate fsmeta and VMDK in one mkfs.erofs call
	// IMPORTANT: Use final paths because mkfs.erofs embeds them in the VMDK
	args := append([]string{"--quiet", "--vmdk-desc=" + vmdkFile, mergedMeta}, blobs...)
	log.G(ctx).Debugf("running mkfs.erofs with args: %v", args)

	cmd := exec.CommandContext(ctx, "mkfs.erofs", args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		log.G(ctx).WithError(err).Warnf("fsmeta generation failed: %s", string(out))
		blobs = nil
		return
	}

	// Write layer manifest for external verification
	manifestFile := s.manifestPath(newestID)
	if err := s.writeLayerManifest(manifestFile, blobs); err != nil {
		log.G(ctx).WithError(err).Warn("failed to write layer manifest (non-fatal)")
	}

	log.G(ctx).WithFields(log.Fields{
		"duration": time.Since(t1),
		"layers":   len(blobs),
	}).Info("fsmeta and VMDK generated successfully")
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
	if s.mountTracker.IsMounted(id) {
		rwMount := s.blockRwMountPath(id)
		if unmountErr := unmountAll(rwMount); unmountErr != nil {
			log.G(ctx).WithError(unmountErr).WithField("id", id).Warn("failed to cleanup ext4 mount after commit")
		}
		s.mountTracker.SetUnmounted(id)
	}

	return nil
}
