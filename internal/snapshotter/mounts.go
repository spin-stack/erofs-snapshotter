package snapshotter

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/containerd/containerd/v2/core/mount"
	"github.com/containerd/containerd/v2/core/snapshots"
	"github.com/containerd/containerd/v2/core/snapshots/storage"

	"github.com/aledbf/nexus-erofs/internal/erofs"
)

// mountFsMeta returns a mount for merged fsmeta.erofs if VMDK exists.
// When VMDK exists, the consumer can use a single virtio-blk device for all layers.
//
// The mount type is "format/erofs" (not plain "erofs") to signal that this is a
// VM-only mount requiring special handling. Containerd's standard mount manager
// will reject "format/erofs" with a clear "unsupported mount type" error, rather
// than the cryptic EINVAL that occurs when it tries to mount EROFS with file paths
// in device= options. VM runtimes (like qemubox) and the custom mountutils.MountAll()
// understand this type and handle it correctly.
func (s *snapshotter) mountFsMeta(snap storage.Snapshot) (mount.Mount, bool) {
	if len(snap.ParentIDs) == 0 {
		return mount.Mount{}, false
	}

	// fsmeta is stored under the immediate parent's snapshot ID
	parentID := snap.ParentIDs[0]
	vmdkFile := s.vmdkPath(parentID)
	fsmetaFile := s.fsMetaPath(parentID)

	// Both files must exist for VMDK mode
	if _, err := os.Stat(vmdkFile); err != nil {
		return mount.Mount{}, false
	}
	if _, err := os.Stat(fsmetaFile); err != nil {
		return mount.Mount{}, false
	}

	// Collect device options in newest-to-oldest order (same as ParentIDs order).
	// This matches the layer order expected by EROFS multidev mode.
	var deviceOptions []string
	for _, parentID := range snap.ParentIDs {
		blob, err := s.findLayerBlob(parentID)
		if err != nil {
			return mount.Mount{}, false
		}
		deviceOptions = append(deviceOptions, "device="+blob)
	}

	return mount.Mount{
		Source:  fsmetaFile,
		Type:    "format/erofs",
		Options: append([]string{"ro", "loop"}, deviceOptions...),
	}, true
}

// mounts returns mount specifications for a snapshot.
// Mounts use raw file paths for VM consumers; host mounting may require
// the mount manager to set up loop devices when "loop" is present.
func (s *snapshotter) mounts(snap storage.Snapshot, info snapshots.Info) ([]mount.Mount, error) {
	// Extract snapshots always use bind mount to fs/ directory
	if isExtractSnapshot(info) {
		return s.diffMounts(snap)
	}

	// View snapshots - read-only access to committed layers
	if snap.Kind == snapshots.KindView {
		return s.viewMountsForKind(snap)
	}

	// Active snapshots
	if snap.Kind == snapshots.KindActive {
		return s.activeMountsForKind(snap)
	}

	return nil, fmt.Errorf("unsupported snapshot kind: %v", snap.Kind)
}

// viewMountsForKind returns mounts for KindView snapshots.
func (s *snapshotter) viewMountsForKind(snap storage.Snapshot) ([]mount.Mount, error) {
	// View with no parent: return read-only bind mount to empty fs directory
	if len(snap.ParentIDs) == 0 {
		fsPath := s.viewLowerPath(snap.ID)
		if err := os.MkdirAll(fsPath, 0755); err != nil {
			return nil, fmt.Errorf("create view fs directory: %w", err)
		}
		return []mount.Mount{
			{
				Source:  fsPath,
				Type:    "bind",
				Options: []string{"ro", "rbind"},
			},
		}, nil
	}

	// Single-layer View: return EROFS mount directly.
	// Overlay with single lowerdir and no upperdir/workdir is invalid in Linux.
	if len(snap.ParentIDs) == 1 {
		layerBlob, err := s.lowerPath(snap.ParentIDs[0])
		if err != nil {
			return nil, fmt.Errorf("get layer blob for view parent %s: %w", snap.ParentIDs[0], err)
		}
		return []mount.Mount{
			{
				Source:  layerBlob,
				Type:    "erofs",
				Options: []string{"ro", "loop"},
			},
		}, nil
	}

	// Multi-layer View: mount EROFS layers
	return s.viewMounts(snap)
}

// activeMountsForKind returns mounts for KindActive snapshots.
func (s *snapshotter) activeMountsForKind(snap storage.Snapshot) ([]mount.Mount, error) {
	if len(snap.ParentIDs) == 0 {
		// No parents: return ext4 writable layer
		return s.singleLayerMounts(snap)
	}
	// With parents: mount EROFS layers and return overlay
	return s.activeMounts(snap)
}

// isExtractSnapshot returns true if the snapshot is marked for layer extraction.
// This is determined by the extractLabel in the snapshot metadata, which is set
// atomically during snapshot creation for TOCTOU safety.
func isExtractSnapshot(info snapshots.Info) bool {
	return info.Labels[extractLabel] == "true"
}

// singleLayerMounts returns mounts for an Active snapshot with no parent layers.
// Returns the ext4 writable layer as a block device for VM runtimes.
func (s *snapshotter) singleLayerMounts(snap storage.Snapshot) ([]mount.Mount, error) {
	if snap.Kind != snapshots.KindActive {
		return nil, fmt.Errorf("singleLayerMounts only supports Active snapshots, got %v", snap.Kind)
	}

	// Return the ext4 writable layer file path directly.
	// VM runtime (the consumer) passes this as a virtio-blk device to the guest.
	rwLayerPath := s.writablePath(snap.ID)
	return []mount.Mount{
		{
			Source:  rwLayerPath,
			Type:    "ext4",
			Options: []string{"rw", "loop"},
		},
	}, nil
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

	return []mount.Mount{
		{
			Type:    "bind",
			Source:  upperRoot,
			Options: []string{"rw", "rbind"},
		},
	}, nil
}

// getErofsLayerPaths returns the EROFS layer blob paths for a snapshot.
// This returns file paths without mounting - the consumer
// transforms these to virtio-blk disks or uses mount manager to mount them.
func (s *snapshotter) getErofsLayerPaths(snap storage.Snapshot) ([]string, error) {
	var paths []string
	for _, parentID := range snap.ParentIDs {
		layerBlob, err := s.lowerPath(parentID)
		if err != nil {
			return nil, err
		}
		paths = append(paths, layerBlob)
	}
	return paths, nil
}

// viewMounts returns mounts for KindView snapshots.
// Returns EROFS mount(s) that the consumer converts to virtio-blk devices.
// For merged layers with VMDK, returns single fsmeta.erofs mount.
// For multi-layer without VMDK, returns multiple EROFS mounts (one per layer).
func (s *snapshotter) viewMounts(snap storage.Snapshot) ([]mount.Mount, error) {
	// Check if we have a merged fsmeta with VMDK - preferred for the consumer
	if m, ok := s.mountFsMeta(snap); ok {
		return []mount.Mount{m}, nil
	}

	// Get EROFS layer paths
	layerPaths, err := s.getErofsLayerPaths(snap)
	if err != nil {
		return nil, err
	}

	// Return EROFS mounts for each layer (the consumer handles stacking)
	// ParentIDs are ordered from newest (immediate parent) to oldest (root).
	var mounts []mount.Mount
	for _, layerPath := range layerPaths {
		mounts = append(mounts, mount.Mount{
			Source:  layerPath,
			Type:    "erofs",
			Options: []string{"ro", "loop"},
		})
	}

	return mounts, nil
}

// activeMounts returns mounts for active (writable) snapshots.
// Returns EROFS mount(s) for read-only lower layers plus ext4 block device for writable upper.
// When fsmeta+vmdk exists, returns single merged EROFS mount (reduces virtio-blk device count).
// The consumer converts these to virtio-blk devices and handles overlay inside the VM.
func (s *snapshotter) activeMounts(snap storage.Snapshot) ([]mount.Mount, error) {
	var mounts []mount.Mount

	// Try to use merged fsmeta for read-only layers (reduces device count)
	if m, ok := s.mountFsMeta(snap); ok {
		mounts = append(mounts, m)
	} else {
		// Fallback to individual layer mounts
		layerPaths, err := s.getErofsLayerPaths(snap)
		if err != nil {
			return nil, err
		}

		// Add EROFS mounts for each lower layer
		// ParentIDs are ordered from newest (immediate parent) to oldest (root).
		for _, layerPath := range layerPaths {
			mounts = append(mounts, mount.Mount{
				Source:  layerPath,
				Type:    "erofs",
				Options: []string{"ro", "loop"},
			})
		}
	}

	// Add the writable ext4 block device as the last mount
	rwLayerPath := s.writablePath(snap.ID)
	mounts = append(mounts, mount.Mount{
		Source:  rwLayerPath,
		Type:    "ext4",
		Options: []string{"rw", "loop"},
	})

	return mounts, nil
}
