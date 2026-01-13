package snapshotter

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/containerd/containerd/v2/core/mount"
	"github.com/containerd/containerd/v2/core/snapshots"
	"github.com/containerd/containerd/v2/core/snapshots/storage"

	"github.com/spin-stack/erofs-snapshotter/internal/erofs"
)

// mounts returns mount specifications for a snapshot.
//
// DECISION TREE:
//
//	Extract snapshot? → diffMounts() (bind to rw/upper/)
//	View/Active → layerMounts() (handles 0/1/N parents)
//	             + Active only: append ext4 writable layer
//
// Mounts use raw file paths for VM consumers. The "loop" option signals
// that host mounting requires loop device setup. VM runtimes convert
// these paths to virtio-blk devices directly.
func (s *snapshotter) mounts(snap storage.Snapshot, info snapshots.Info) ([]mount.Mount, error) {
	// Guard: extract snapshots use direct bind mount
	if isExtractSnapshot(info) {
		return s.diffMounts(snap)
	}

	// Get read-only layer mounts (handles 0, 1, N parents)
	mounts, err := s.layerMounts(snap)
	if err != nil {
		return nil, err
	}

	// Views: return read-only mounts only
	if snap.Kind == snapshots.KindView {
		return mounts, nil
	}

	// Active: append writable ext4 layer
	if snap.Kind == snapshots.KindActive {
		mounts = append(mounts, s.writableMount(snap.ID))
		return mounts, nil
	}

	return nil, fmt.Errorf("unsupported snapshot kind: %v", snap.Kind)
}

// layerMounts returns read-only mounts based on parent count.
// Uses switch for clarity (Dave Cheney: explicit over clever).
func (s *snapshotter) layerMounts(snap storage.Snapshot) ([]mount.Mount, error) {
	switch len(snap.ParentIDs) {
	case 0:
		return s.emptyLayerMount(snap.ID)
	case 1:
		return s.singleLayerMount(snap.ParentIDs[0])
	default:
		return s.multiLayerMounts(snap)
	}
}

// emptyLayerMount returns a bind mount to an empty directory.
// This is rare but valid for empty base images with no parents.
func (s *snapshotter) emptyLayerMount(id string) ([]mount.Mount, error) {
	fsPath := s.viewLowerPath(id)
	if err := os.MkdirAll(fsPath, 0755); err != nil {
		return nil, fmt.Errorf("create view fs directory: %w", err)
	}
	return []mount.Mount{{
		Source:  fsPath,
		Type:    "bind",
		Options: []string{"ro", "rbind"},
	}}, nil
}

// singleLayerMount returns a single EROFS mount for one parent.
// No fsmeta needed for single layer - returns the EROFS directly.
func (s *snapshotter) singleLayerMount(parentID string) ([]mount.Mount, error) {
	blob, err := s.lowerPath(parentID)
	if err != nil {
		return nil, fmt.Errorf("get layer blob for parent %s: %w", parentID, err)
	}
	return []mount.Mount{{
		Source:  blob,
		Type:    "erofs",
		Options: []string{"ro", "loop"},
	}}, nil
}

// multiLayerMounts returns mounts for multiple parent layers.
// Prefers fsmeta (single mount with VMDK) when available for efficiency,
// falls back to individual EROFS mounts if fsmeta not ready.
func (s *snapshotter) multiLayerMounts(snap storage.Snapshot) ([]mount.Mount, error) {
	// Try fsmeta first (single efficient mount)
	if mounts, err := s.fsmetaMount(snap); err == nil {
		return mounts, nil
	}

	// Fallback: individual EROFS mounts
	return s.individualLayerMounts(snap)
}

// fsmetaMount returns a fsmeta mount if VMDK exists.
//
// When VMDK exists, the consumer can use a single virtio-blk device for all layers.
// The mount type is "format/erofs" (not plain "erofs") to signal that this is a
// VM-only mount requiring special handling. Containerd's standard mount manager
// will reject "format/erofs" with a clear "unsupported mount type" error, rather
// than the cryptic EINVAL that occurs when it tries to mount EROFS with file paths
// in device= options.
//
// Returns error if fsmeta not available (files missing or layer lookup fails).
func (s *snapshotter) fsmetaMount(snap storage.Snapshot) ([]mount.Mount, error) {
	// fsmeta is stored under the immediate parent's snapshot ID
	parentID := snap.ParentIDs[0]
	vmdkFile := s.vmdkPath(parentID)
	fsmetaFile := s.fsMetaPath(parentID)

	// Both files required for fsmeta mode
	if _, err := os.Stat(vmdkFile); err != nil {
		return nil, err
	}
	if _, err := os.Stat(fsmetaFile); err != nil {
		return nil, err
	}

	// Build device= options by iterating backwards through ParentIDs (newest-first input).
	// This produces oldest-first order matching containerd's approach and the order
	// used when generating fsmeta with mkfs.erofs.
	// See: https://github.com/containerd/containerd/pull/12374
	var deviceOptions []string
	for i := len(snap.ParentIDs) - 1; i >= 0; i-- {
		blob, err := s.findLayerBlob(snap.ParentIDs[i])
		if err != nil {
			return nil, err
		}
		deviceOptions = append(deviceOptions, "device="+blob)
	}

	return []mount.Mount{{
		Source:  fsmetaFile,
		Type:    "format/erofs",
		Options: append([]string{"ro", "loop"}, deviceOptions...),
	}}, nil
}

// individualLayerMounts returns one EROFS mount per layer.
// Order matches ParentIDs: newest (immediate parent) to oldest (root).
// VM runtime handles layer stacking.
func (s *snapshotter) individualLayerMounts(snap storage.Snapshot) ([]mount.Mount, error) {
	var mounts []mount.Mount
	for _, parentID := range snap.ParentIDs {
		blob, err := s.lowerPath(parentID)
		if err != nil {
			return nil, err
		}
		mounts = append(mounts, mount.Mount{
			Source:  blob,
			Type:    "erofs",
			Options: []string{"ro", "loop"},
		})
	}
	return mounts, nil
}

// writableMount returns the ext4 writable layer mount.
// VM runtime (the consumer) passes this as a virtio-blk device to the guest.
func (s *snapshotter) writableMount(id string) mount.Mount {
	return mount.Mount{
		Source:  s.writablePath(id),
		Type:    "ext4",
		Options: []string{"rw", "loop"},
	}
}

// diffMounts returns mounts for extract snapshots.
// The ext4 is mounted at blockRwMountPath, and we return a bind mount to upper.
func (s *snapshotter) diffMounts(snap storage.Snapshot) ([]mount.Mount, error) {
	upperRoot := s.blockUpperPath(snap.ID)
	snapshotDir := s.snapshotDir(snap.ID)

	// Ensure EROFS layer marker exists at the snapshot root for diff operations.
	if err := ensureMarkerFile(filepath.Join(snapshotDir, erofs.ErofsLayerMarker)); err != nil {
		return nil, fmt.Errorf("create erofs marker: %w", err)
	}

	return []mount.Mount{{
		Type:    "bind",
		Source:  upperRoot,
		Options: []string{"rw", "rbind"},
	}}, nil
}

// isExtractSnapshot returns true if the snapshot is marked for layer extraction.
// This is determined by the extractLabel in the snapshot metadata, which is set
// atomically during snapshot creation for TOCTOU safety.
func isExtractSnapshot(info snapshots.Info) bool {
	return info.Labels[extractLabel] == "true"
}
