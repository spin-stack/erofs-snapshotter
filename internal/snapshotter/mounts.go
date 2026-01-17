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

	// Collect device= options by iterating backwards through ParentIDs (newest-first input).
	// This produces oldest-first order matching containerd's approach and the order
	// used when generating fsmeta with mkfs.erofs.
	// See: https://github.com/containerd/containerd/pull/12374
	var deviceOptions []string
	for i := len(snap.ParentIDs) - 1; i >= 0; i-- {
		blob, err := s.findLayerBlob(snap.ParentIDs[i])
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
//
// DECISION TREE:
//
//	Is extract snapshot (extractLabel=true)?
//	├─ YES → diffMounts(): bind mount to rw/upper/ for EROFS differ
//	└─ NO  → Check snapshot kind:
//	         ├─ KindView  → viewMountsForKind(): read-only layer access
//	         └─ KindActive → activeMountsForKind(): layers + writable ext4
//
// Mounts use raw file paths for VM consumers. The "loop" option signals
// that host mounting requires loop device setup. VM runtimes convert
// these paths to virtio-blk devices directly.
func (s *snapshotter) mounts(snap storage.Snapshot, info snapshots.Info) ([]mount.Mount, error) {
	// Extract snapshots use bind mount to upper directory.
	// The EROFS differ writes directly to this directory, which is inside
	// the mounted rwlayer.img ext4 filesystem.
	if isExtractSnapshot(info) {
		return s.diffMounts(snap)
	}

	// View snapshots: read-only access to committed layers
	if snap.Kind == snapshots.KindView {
		return s.viewMountsForKind(snap)
	}

	// Active snapshots: read-only layers + writable ext4
	if snap.Kind == snapshots.KindActive {
		return s.activeMountsForKind(snap)
	}

	return nil, fmt.Errorf("unsupported snapshot kind: %v", snap.Kind)
}

// viewMountsForKind returns mounts for KindView snapshots.
//
// DECISION TREE (by parent count):
//
//	0 parents → bind mount to empty fs/ directory
//	1 parent  → single EROFS mount (type: erofs)
//	N parents → viewMounts():
//	            ├─ fsmeta exists? → single fsmeta mount (type: format/erofs)
//	            └─ no fsmeta     → N individual EROFS mounts
func (s *snapshotter) viewMountsForKind(snap storage.Snapshot) ([]mount.Mount, error) {
	// 0 parents: bind mount to empty directory.
	// This is rare but valid for empty base images.
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

	// 1 parent: single EROFS mount.
	// No fsmeta needed for single layer. Linux overlay requires 2+ lowerdirs
	// or an upperdir, so we return the EROFS directly.
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

	// N parents: try fsmeta for efficiency, fall back to individual mounts
	return s.viewMounts(snap)
}

// activeMountsForKind returns mounts for KindActive snapshots.
//
// DECISION TREE (by parent count):
//
//	0 parents → singleLayerMounts(): ext4 writable layer only
//	N parents → activeMounts():
//	            ├─ fsmeta exists? → fsmeta mount + ext4 (2 mounts)
//	            └─ no fsmeta     → N EROFS mounts + ext4 (N+1 mounts)
//
// The VM runtime combines these into an overlay filesystem inside the guest.
func (s *snapshotter) activeMountsForKind(snap storage.Snapshot) ([]mount.Mount, error) {
	// 0 parents: only the writable ext4 layer
	if len(snap.ParentIDs) == 0 {
		return s.singleLayerMounts(snap)
	}
	// N parents: read-only EROFS layers + writable ext4
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

// buildErofsLayerMounts returns read-only EROFS layer mounts for a snapshot.
//
// Prefers fsmeta mount (type: format/erofs) when available because it reduces
// the number of virtio-blk devices the VM runtime needs to manage. Falls back
// to individual EROFS mounts when fsmeta generation failed or is pending.
//
// Return formats:
//   - With fsmeta: [{type: format/erofs, source: fsmeta.erofs, options: [device=layer1, ...]}]
//   - Without:     [{type: erofs, source: layer1.erofs}, {type: erofs, source: layer2.erofs}, ...]
func (s *snapshotter) buildErofsLayerMounts(snap storage.Snapshot) ([]mount.Mount, error) {
	// Try fsmeta first (single mount with VMDK) - preferred for efficiency
	if m, ok := s.mountFsMeta(snap); ok {
		return []mount.Mount{m}, nil
	}

	// Fallback: individual EROFS mounts (fsmeta not ready or generation failed)
	layerPaths, err := s.getErofsLayerPaths(snap)
	if err != nil {
		return nil, err
	}

	// Return one EROFS mount per layer. VM runtime handles layer stacking.
	// Order matches ParentIDs: newest (immediate parent) to oldest (root).
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

// viewMounts returns mounts for multi-layer KindView snapshots.
func (s *snapshotter) viewMounts(snap storage.Snapshot) ([]mount.Mount, error) {
	return s.buildErofsLayerMounts(snap)
}

// activeMounts returns mounts for active (writable) snapshots with parents.
//
// Returns read-only EROFS layer(s) plus a writable ext4 block device.
// The VM runtime creates an overlay filesystem from these inside the guest.
// The ext4 mount is always last, making it easy for consumers to identify
// the writable layer.
func (s *snapshotter) activeMounts(snap storage.Snapshot) ([]mount.Mount, error) {
	mounts, err := s.buildErofsLayerMounts(snap)
	if err != nil {
		return nil, err
	}

	// Writable layer: ext4 block device (always last)
	rwLayerPath := s.writablePath(snap.ID)
	mounts = append(mounts, mount.Mount{
		Source:  rwLayerPath,
		Type:    "ext4",
		Options: []string{"rw", "loop"},
	})

	return mounts, nil
}
