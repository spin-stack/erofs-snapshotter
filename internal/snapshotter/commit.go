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
	"github.com/containerd/errdefs"
	"github.com/containerd/log"
	"github.com/moby/sys/mountinfo"
	"github.com/opencontainers/go-digest"

	"github.com/aledbf/nexus-erofs/internal/erofs"
	"github.com/aledbf/nexus-erofs/internal/fsverity"
)

// commitBlock handles the conversion of a writable layer to EROFS.
// It supports both block mode (ext4 image) and directory mode (overlay upper).
func (s *snapshotter) commitBlock(ctx context.Context, layerBlob string, id string) error {
	layer := s.writablePath(id)
	if _, err := os.Stat(layer); err != nil {
		if os.IsNotExist(err) {
			// No block layer - convert directory mode upper
			upperDir := s.upperPath(id)
			if cerr := convertDirToErofs(ctx, layerBlob, upperDir); cerr != nil {
				return fmt.Errorf("convert upper to erofs layer: %w", cerr)
			}
			return nil
		}
		return fmt.Errorf("access writable layer %s: %w", layer, err)
	}

	// Block mode: use the rw mount path
	rwMount := s.blockRwMountPath(id)

	// Check if already mounted (from Prepare/Mounts) before trying to mount again.
	// For VM-only snapshots, the /rw directory may not exist (VM mounts ext4, not host).
	alreadyMounted := false
	if _, err := os.Stat(rwMount); err == nil {
		alreadyMounted, err = mountinfo.Mounted(rwMount)
		if err != nil {
			return fmt.Errorf("check mount status: %w", err)
		}
	}
	if !alreadyMounted {
		// Mount read-only for commit (we're just reading the upper contents)
		if err := os.MkdirAll(rwMount, 0755); err != nil {
			return fmt.Errorf("create rw mount point: %w", err)
		}
		m := mount.Mount{
			Source:  layer,
			Type:    "ext4",
			Options: []string{"ro", "loop", "noload"},
		}
		if err := m.Mount(rwMount); err != nil {
			return fmt.Errorf("mount writable layer %s: %w", layer, err)
		}
		log.G(ctx).WithField("target", rwMount).Debug("Mounted writable layer for conversion")
	}

	// Cleanup the block rw mount after conversion
	defer func() {
		if err := unmountAll(rwMount); err != nil {
			log.G(ctx).WithError(err).WithField("id", id).Warn("failed to cleanup block rw mount after conversion")
		}
	}()

	// Convert the upper directory inside the mounted ext4
	upperDir := s.blockUpperPath(id)
	if _, err := os.Stat(upperDir); os.IsNotExist(err) {
		// upper is empty, convert empty directory
		upperDir = rwMount
	}
	if cerr := convertDirToErofs(ctx, layerBlob, upperDir); cerr != nil {
		return fmt.Errorf("convert upper block to erofs layer: %w", cerr)
	}
	return nil
}

// generateFsMeta creates a merged fsmeta.erofs and VMDK descriptor for VM runtimes.
// The VMDK allows QEMU to present all EROFS layers as a single concatenated block device.
// This is always called when there are parent layers (no threshold - VM runtimes need this).
//
// Layer ordering:
//
// OCI Image Manifest specifies layers in bottom-to-top order (base layer at index 0):
// https://github.com/opencontainers/image-spec/blob/main/manifest.md
// > "The array MUST have the base layer at index 0. Subsequent layers MUST then
// > follow in stack order (i.e. from layers[0] to layers[len(layers)-1])."
//
// mkfs.erofs rebuild mode expects layers in OCI manifest order (base/oldest first):
// https://man.archlinux.org/man/extra/erofs-utils/mkfs.erofs.1.en
// > "To merge these layers: mkfs.erofs merged.erofs layer0.erofs ... layerN-1.erofs"
//
// The snapIDs slice comes in newest-first order (from snapshot chain walk).
// We must REVERSE it to get oldest-first order for mkfs.erofs.
//
// VMDK layer order = OCI manifest order (after fsmeta):
// - VMDK: [fsmeta, layer_0, layer_1, ..., layer_n] (oldest to newest)
// - OCI:  [layer_0, layer_1, ..., layer_n]         (oldest to newest)
func (s *snapshotter) generateFsMeta(ctx context.Context, snapIDs []string) {
	var blobs []string

	if len(snapIDs) == 0 {
		return
	}

	t1 := time.Now()
	mergedMeta := s.fsMetaPath(snapIDs[0])
	vmdkFile := s.vmdkPath(snapIDs[0])

	// If the empty placeholder cannot be created (mainly due to os.IsExist), just return
	if _, err := os.OpenFile(mergedMeta, os.O_CREATE|os.O_EXCL, 0600); err != nil {
		return
	}

	// Cleanup on failure
	defer func() {
		if blobs == nil {
			// Generation failed, remove placeholder
			_ = os.Remove(mergedMeta)
			_ = os.Remove(vmdkFile)
		}
	}()

	// Collect blobs by iterating backwards through snapIDs (newest-first input).
	// This produces oldest-first order matching containerd's approach.
	// See: https://github.com/containerd/containerd/pull/12374
	for i := len(snapIDs) - 1; i >= 0; i-- {
		blob, err := s.findLayerBlob(snapIDs[i])
		if err != nil {
			blobs = nil // Signal failure
			return
		}
		blobs = append(blobs, blob)
	}

	// Check if all layers have block sizes compatible with fsmeta merge.
	if !erofs.CanMergeFsmeta(blobs) {
		log.G(ctx).Debugf("skipping fsmeta generation: one or more layers have incompatible block size")
		blobs = nil // Signal skip (cleanup placeholder)
		return
	}

	// Use rebuild mode to generate flatdev fsmeta with mapped_blkaddr.
	// The --vmdk-desc option generates both fsmeta and VMDK descriptor in one step.
	// IMPORTANT: We use final paths (not temp) because mkfs.erofs embeds the fsmeta
	// path into the VMDK descriptor. Using temp paths would cause QEMU to fail.
	args := append([]string{"--quiet", "--vmdk-desc=" + vmdkFile, mergedMeta}, blobs...)
	log.G(ctx).Infof("merging layers with mkfs.erofs %v", args)
	cmd := exec.CommandContext(ctx, "mkfs.erofs", args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		log.G(ctx).Warnf("failed to generate merged fsmeta for %v: %q: %v", snapIDs[0], string(out), err)
		blobs = nil // Signal failure
		return
	}

	// Write layer manifest file with digests in VMDK/OCI order (oldest-to-newest).
	// This allows external tools to verify VMDK layer order without parsing the snapshot chain.
	manifestFile := s.manifestPath(snapIDs[0])
	if err := s.writeLayerManifest(manifestFile, blobs); err != nil {
		log.G(ctx).WithError(err).Warn("failed to write layer manifest")
		// Non-fatal - continue even if manifest fails
	}

	log.G(ctx).WithFields(log.Fields{
		"d": time.Since(t1),
	}).Infof("merged fsmeta and vmdk for %v generated", snapIDs[0])
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
func (s *snapshotter) Commit(ctx context.Context, name, key string, opts ...snapshots.Opt) error {
	var layerBlob string
	var id string

	// Apply the overlayfs upperdir (generated by non-EROFS differs) into a EROFS blob
	// in a read transaction first since conversion could be slow.
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

	// If the layer blob doesn't exist, which means this layer wasn't applied by
	// the EROFS differ (possibly the walking differ), convert the upperdir instead.
	// Use fallback naming since we don't have the original layer digest.
	layerBlob, err = s.findLayerBlob(id)
	if err != nil {
		// Layer doesn't exist - create it using fallback path
		layerBlob = s.fallbackLayerBlobPath(id)
		if cerr := s.commitBlock(ctx, layerBlob, id); cerr != nil {
			if errdefs.IsNotImplemented(cerr) {
				return fmt.Errorf("layer blob not found and fallback failed: %w", err)
			}
			return cerr
		}
	}

	// Enable fsverity on the EROFS layer if configured
	if s.enableFsverity {
		if err := fsverity.Enable(layerBlob); err != nil {
			return fmt.Errorf("enable fsverity: %w", err)
		}
	}

	// Set IMMUTABLE_FL on the EROFS layer to avoid artificial data loss
	if s.setImmutable {
		if err := setImmutable(layerBlob, true); err != nil {
			log.G(ctx).WithError(err).Warnf("failed to set IMMUTABLE_FL for %s", layerBlob)
		}
	}

	return s.ms.WithTransaction(ctx, true, func(ctx context.Context) error {
		if _, err := os.Stat(layerBlob); err != nil {
			return fmt.Errorf("get converted erofs blob: %w", err)
		}

		usage, err := fs.DiskUsage(ctx, layerBlob)
		if err != nil {
			return fmt.Errorf("calculate disk usage for %q: %w", layerBlob, err)
		}
		if _, err = storage.CommitActive(ctx, key, name, snapshots.Usage(usage), opts...); err != nil {
			return fmt.Errorf("commit snapshot %s: %w", key, err)
		}
		return nil
	})
}
