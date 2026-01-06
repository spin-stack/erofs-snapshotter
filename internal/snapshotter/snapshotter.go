/*
   Copyright The containerd Authors.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package erofs

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/containerd/containerd/v2/core/mount"
	"github.com/containerd/containerd/v2/core/snapshots"
	"github.com/containerd/containerd/v2/core/snapshots/storage"
	"github.com/containerd/continuity/fs"
	"github.com/containerd/errdefs"
	"github.com/containerd/log"
	"github.com/moby/sys/mountinfo"

	erofsutils "github.com/aledbf/nexuserofs/internal/erofs"
	"github.com/aledbf/nexuserofs/internal/fsverity"
	"github.com/aledbf/nexuserofs/internal/stringutil"
)

// SnapshotterConfig is used to configure the erofs snapshotter instance
type SnapshotterConfig struct {
	// ovlOptions are the base options added to the overlayfs mount (defaults to [""])
	ovlOptions []string
	// enableFsverity enables fsverity for EROFS layers
	enableFsverity bool
	// setImmutable enables IMMUTABLE_FL file attribute for EROFS layers
	setImmutable bool
	// defaultSize is the size in bytes of the ext4 writable layer (must be > 0)
	defaultSize int64
	// fsMergeThreshold is the layer count threshold for triggering fsmerge (must be >= 5)
	fsMergeThreshold uint
}

// Opt is an option to configure the erofs snapshotter
type Opt func(config *SnapshotterConfig)

// WithOvlOptions defines the extra mount options for overlayfs
func WithOvlOptions(options []string) Opt {
	return func(config *SnapshotterConfig) {
		config.ovlOptions = options
	}
}

// WithFsverity enables fsverity for EROFS layers
func WithFsverity() Opt {
	return func(config *SnapshotterConfig) {
		config.enableFsverity = true
	}
}

// WithImmutable enables IMMUTABLE_FL file attribute for EROFS layers
func WithImmutable() Opt {
	return func(config *SnapshotterConfig) {
		config.setImmutable = true
	}
}

// WithDefaultSize sets the size of the ext4 writable layer for active snapshots.
// Size must be > 0. The writable layer is an ext4 image that is loop-mounted.
func WithDefaultSize(size int64) Opt {
	return func(config *SnapshotterConfig) {
		config.defaultSize = size
	}
}

// WithFsMergeThreshold sets the layer count threshold for triggering fsmerge.
// Must be >= 5. Layers are merged when count exceeds this threshold.
func WithFsMergeThreshold(v uint) Opt {
	return func(config *SnapshotterConfig) {
		config.fsMergeThreshold = v
	}
}

type snapshotter struct {
	root             string
	ms               *storage.MetaStore
	ovlOptions       []string
	enableFsverity   bool
	setImmutable     bool
	defaultWritable  int64
	fsMergeThreshold uint
}

// extractLabel is the label key used to mark snapshots for layer extraction.
// This is stored in the snapshot metadata for atomic reads within transactions,
// avoiding TOCTOU race conditions that would occur with filesystem markers.
//
// TOCTOU Safety: The label is set atomically within a database transaction
// during CreateSnapshot, and all reads occur within transactions. This ensures
// no race window exists between checking and using the extract status.
const extractLabel = "containerd.io/snapshot/erofs.extract"

// NewSnapshotter returns a Snapshotter which uses EROFS+OverlayFS. The layers
// are stored under the provided root. A metadata file is stored under the root.
func NewSnapshotter(root string, opts ...Opt) (snapshots.Snapshotter, error) {
	config := SnapshotterConfig{
		defaultSize:      defaultWritableSize,
		fsMergeThreshold: 5, // Minimum threshold for layer merging
	}
	for _, opt := range opts {
		opt(&config)
	}

	if err := os.MkdirAll(root, 0700); err != nil {
		return nil, fmt.Errorf("failed to create root directory %q: %w", root, err)
	}

	if config.defaultSize <= 0 {
		return nil, fmt.Errorf("default_writable_size must be > 0, got %d", config.defaultSize)
	}

	if config.fsMergeThreshold < 5 {
		return nil, fmt.Errorf("fs_merge_threshold must be >= 5, got %d", config.fsMergeThreshold)
	}

	if err := checkCompatibility(root); err != nil {
		return nil, fmt.Errorf("compatibility check failed for %q: %w", root, err)
	}

	// Check fsverity support if enabled
	if config.enableFsverity {
		// TODO: Call specific function here
		supported, err := fsverity.IsSupported(root)
		if err != nil {
			return nil, fmt.Errorf("failed to check fsverity support on %q: %w", root, err)
		}
		if !supported {
			return nil, fmt.Errorf("fsverity is not supported on the filesystem of %q", root)
		}
	}

	if config.setImmutable && runtime.GOOS != "linux" {
		return nil, fmt.Errorf("setting IMMUTABLE_FL is only supported on Linux")
	}

	ms, err := storage.NewMetaStore(filepath.Join(root, "metadata.db"))
	if err != nil {
		return nil, fmt.Errorf("failed to create metadata store: %w", err)
	}

	if err := os.Mkdir(filepath.Join(root, "snapshots"), 0700); err != nil && !os.IsExist(err) {
		return nil, fmt.Errorf("failed to create snapshots directory: %w", err)
	}

	s := &snapshotter{
		root:             root,
		ms:               ms,
		ovlOptions:       config.ovlOptions,
		enableFsverity:   config.enableFsverity,
		setImmutable:     config.setImmutable,
		defaultWritable:  config.defaultSize,
		fsMergeThreshold: config.fsMergeThreshold,
	}

	// Clean up any orphaned mounts from previous runs.
	// This uses context.Background() internally since NewSnapshotter doesn't
	// take a context (per containerd snapshotter interface convention).
	s.cleanupOrphanedMounts() //nolint:contextcheck // startup cleanup uses background context

	return s, nil
}

// Close releases all resources held by the snapshotter.
// It cleans up any temporary directories and closes the metadata store (BBolt database).
// This method is safe to call multiple times; subsequent calls will return the same
// error (if any) from the first close.
func (s *snapshotter) Close() error {
	// Cleanup block mode rw mounts if any
	s.cleanupBlockMounts()

	return s.ms.Close()
}

// cleanupBlockMounts unmounts any ext4 rw mounts and layer mounts.
// Errors are logged but not returned since this is best-effort cleanup.
func (s *snapshotter) cleanupBlockMounts() {
	snapshotsDir := filepath.Join(s.root, "snapshots")
	entries, err := os.ReadDir(snapshotsDir)
	if err != nil {
		// If the directory doesn't exist, there's nothing to clean up
		return
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		snapshotDir := filepath.Join(snapshotsDir, entry.Name())

		// Cleanup block mode rw mounts (ext4 loop mounts)
		rwDir := filepath.Join(snapshotDir, "rw")
		if err := unmountAll(rwDir); err != nil {
			log.L.WithError(err).WithField("path", rwDir).Debug("failed to cleanup block rw mount during close")
		}

		// Cleanup layer mounts (EROFS loop mounts used for views)
		layersDir := filepath.Join(snapshotDir, "layers")
		if err := mount.UnmountRecursive(layersDir, 0); err != nil {
			log.L.WithError(err).WithField("path", layersDir).Debug("failed to cleanup layer mounts during close")
		}
	}
}

func (s *snapshotter) upperPath(id string) string {
	return filepath.Join(s.root, "snapshots", id, "fs")
}

// layerMountPath returns the mount path for a specific layer in a snapshot.
// Layers are indexed from 0 (bottom/oldest) to N (top/newest).
func (s *snapshotter) layerMountPath(id string, layerIndex int) string {
	return filepath.Join(s.root, "snapshots", id, "layers", fmt.Sprintf("%d", layerIndex))
}

func (s *snapshotter) writablePath(id string) string {
	return filepath.Join(s.root, "snapshots", id, "rwlayer.img")
}

// blockRwMountPath returns the mount point for the ext4 rwlayer in block mode.
func (s *snapshotter) blockRwMountPath(id string) string {
	return filepath.Join(s.root, "snapshots", id, "rw")
}

// blockUpperPath returns the overlay upperdir inside the mounted ext4.
func (s *snapshotter) blockUpperPath(id string) string {
	return filepath.Join(s.blockRwMountPath(id), "upper")
}

// blockWorkPath returns the overlay workdir inside the mounted ext4.
func (s *snapshotter) blockWorkPath(id string) string {
	return filepath.Join(s.blockRwMountPath(id), "work")
}

// mountBlockRwLayer mounts the ext4 rwlayer image at the rw mount point.
// This is idempotent - if already mounted, returns nil.
func (s *snapshotter) mountBlockRwLayer(id string) error {
	rwPath := s.writablePath(id)
	mountPoint := s.blockRwMountPath(id)

	if err := os.MkdirAll(mountPoint, 0755); err != nil {
		return fmt.Errorf("failed to create rw mount point: %w", err)
	}

	// Check if already mounted
	alreadyMounted, err := mountinfo.Mounted(mountPoint)
	if err != nil {
		return fmt.Errorf("failed to check mount status for %s: %w", mountPoint, err)
	}
	if alreadyMounted {
		return nil
	}

	m := mount.Mount{
		Source:  rwPath,
		Type:    "ext4",
		Options: []string{"rw", "loop", "user_xattr"},
	}
	if err := m.Mount(mountPoint); err != nil {
		return fmt.Errorf("failed to mount ext4 rwlayer: %w", err)
	}
	return nil
}

// createWritableLayer creates and formats an ext4 filesystem image file.
// This is called during Prepare() to eagerly create the writable layer,
// avoiding the need for lazy mkfs/ext4 mount type processing.
// The upper/work directories are created by the mount manager when mounting.
func (s *snapshotter) createWritableLayer(ctx context.Context, id string) error {
	path := s.writablePath(id)
	// TODO: Get size from snapshot labels to allow per-container custom sizes
	size := s.defaultWritable

	// Create sparse file
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("failed to create writable layer file: %w", err)
	}

	if err := f.Truncate(size); err != nil {
		f.Close()
		os.Remove(path)
		return fmt.Errorf("failed to allocate writable layer: %w", err)
	}
	f.Close()

	// Format as ext4 directly on the file (mkfs.ext4 supports this).
	// Use lazy_itable_init and lazy_journal_init to defer initialization
	// to the background, significantly speeding up mkfs for large sparse files.
	cmd := exec.CommandContext(ctx, "mkfs.ext4", "-q", "-F", "-L", "rwlayer",
		"-E", "nodiscard,lazy_itable_init=1,lazy_journal_init=1", path)
	if out, err := cmd.CombinedOutput(); err != nil {
		os.Remove(path)
		return fmt.Errorf("failed to format ext4: %w: %s", err, stringutil.TruncateOutput(out, 256))
	}

	log.G(ctx).WithField("path", path).WithField("size", size).Debug("created writable layer")
	return nil
}

// A committed layer blob generated by the EROFS differ
func (s *snapshotter) layerBlobPath(id string) string {
	return filepath.Join(s.root, "snapshots", id, erofsutils.LayerBlobFilename)
}

func (s *snapshotter) fsMetaPath(id string) string {
	return filepath.Join(s.root, "snapshots", id, "fsmeta.erofs")
}

func (s *snapshotter) vmdkPath(id string) string {
	return filepath.Join(s.root, "snapshots", id, "merged.vmdk")
}

func (s *snapshotter) lowerPath(id string) (string, error) {
	layerBlob := s.layerBlobPath(id)
	if _, err := os.Stat(layerBlob); err != nil {
		return "", fmt.Errorf("failed to find valid erofs layer blob: %w", err)
	}

	return layerBlob, nil
}

func (s *snapshotter) prepareDirectory(snapshotDir string, kind snapshots.Kind) (string, error) {
	td, err := os.MkdirTemp(snapshotDir, "new-")
	if err != nil {
		return "", fmt.Errorf("failed to create temp dir: %w", err)
	}

	if err := os.Mkdir(filepath.Join(td, "fs"), 0755); err != nil {
		return td, err
	}
	if kind == snapshots.KindActive {
		// Create EROFS layer marker at snapshot root (e.g., /snapshots/{id}/.erofslayer).
		// This is the primary marker location checked by the differ for bind/overlay mounts.
		// Uses ensureMarkerFile for atomic creation consistent with other code paths.
		if err := ensureMarkerFile(filepath.Join(td, erofsutils.ErofsLayerMarker)); err != nil {
			return td, err
		}
	}

	return td, nil
}

func (s *snapshotter) mountFsMeta(snap storage.Snapshot) (mount.Mount, bool) {
	// fsmeta is generated in rebuild/flatdev mode for use with qemubox via VMDK.
	// It cannot be mounted directly with device= options (that requires aufs mode).
	// For direct mounting, we fall back to overlay with individual EROFS layers.
	// qemubox detects the merged.vmdk file and uses it for a single virtio-blk device.
	return mount.Mount{}, false
}

// mounts returns mount specifications for a snapshot.
// All mounts are directly usable without requiring mount-manager plugins.
// EROFS layers are mounted directly and real overlay paths are returned.
func (s *snapshotter) mounts(snap storage.Snapshot, info snapshots.Info) ([]mount.Mount, error) {
	// Extract snapshots always use bind mount to fs/ directory
	if isExtractSnapshot(info) {
		return s.diffMounts(snap)
	}

	// View snapshots - read-only access to committed layers
	if snap.Kind == snapshots.KindView {
		// View with no parent: return read-only bind mount to empty fs directory
		if len(snap.ParentIDs) == 0 {
			fsPath := s.viewLowerPath(snap.ID)
			if err := os.MkdirAll(fsPath, 0755); err != nil {
				return nil, fmt.Errorf("failed to create view fs directory: %w", err)
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
				return nil, fmt.Errorf("failed to get layer blob for view parent %s: %w", snap.ParentIDs[0], err)
			}
			return []mount.Mount{
				{
					Source:  layerBlob,
					Type:    "erofs",
					Options: []string{"ro", "loop"},
				},
			}, nil
		}
		// Multi-layer View: mount EROFS layers and return overlay
		return s.viewMounts(snap)
	}

	// Active snapshots
	if snap.Kind == snapshots.KindActive {
		if len(snap.ParentIDs) == 0 {
			// No parents: bind mount to upper directory
			return s.singleLayerMounts(snap)
		}
		// With parents: mount EROFS layers and return overlay
		return s.activeMounts(snap)
	}

	return nil, fmt.Errorf("unsupported snapshot kind: %v", snap.Kind)
}

// isExtractSnapshot returns true if the snapshot is marked for layer extraction.
// This is determined by the extractLabel in the snapshot metadata, which is set
// atomically during snapshot creation for TOCTOU safety.
func isExtractSnapshot(info snapshots.Info) bool {
	return info.Labels[extractLabel] == "true"
}

// singleLayerMounts returns mounts for an Active snapshot with no parent layers.
// This is used for new snapshots created with Prepare("key", "") - i.e., no parent.
func (s *snapshotter) singleLayerMounts(snap storage.Snapshot) ([]mount.Mount, error) {
	if snap.Kind != snapshots.KindActive {
		return nil, fmt.Errorf("singleLayerMounts only supports Active snapshots, got %v", snap.Kind)
	}

	// Mount ext4 and return bind mount to upper inside it
	if err := s.mountBlockRwLayer(snap.ID); err != nil {
		return nil, fmt.Errorf("failed to mount block rw layer: %w", err)
	}
	upperPath := s.blockUpperPath(snap.ID)
	if err := os.MkdirAll(upperPath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create block upperdir: %w", err)
	}
	return []mount.Mount{
		{
			Source:  upperPath,
			Type:    "bind",
			Options: []string{"rw", "rbind"},
		},
	}, nil
}

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
// The marker file is checked by erofsutils.MountsToLayer() in the EROFS differ
// to validate that a directory is a genuine EROFS snapshotter layer. If the
// marker is missing, the differ returns ErrNotImplemented to allow fallback
// to other differs (e.g., the walking differ).
//
// This uses O_CREAT|O_EXCL for atomic creation, avoiding TOCTOU races that
// would occur with a separate existence check followed by creation.
func ensureMarkerFile(path string) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		if os.IsExist(err) {
			// File already exists, which is fine for idempotency
			return nil
		}
		return fmt.Errorf("failed to create marker file %q: %w", path, err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("failed to close marker file %q: %w", path, err)
	}
	return nil
}

func (s *snapshotter) diffMounts(snap storage.Snapshot) ([]mount.Mount, error) {
	upperRoot := s.upperPath(snap.ID)
	layerRoot := filepath.Dir(upperRoot)

	if err := os.MkdirAll(upperRoot, 0755); err != nil {
		return nil, fmt.Errorf("failed to create upper root: %w", err)
	}

	// Ensure EROFS layer marker exists at the snapshot root for diff operations.
	// This may be redundant with createSnapshotDirectory, but ensureMarkerFile
	// is idempotent and this guards against edge cases where diff mounts are
	// requested without a prior Prepare call.
	if err := ensureMarkerFile(filepath.Join(layerRoot, erofsutils.ErofsLayerMarker)); err != nil {
		return nil, fmt.Errorf("failed to create erofs marker: %w", err)
	}

	return []mount.Mount{
		{
			Type:    "bind",
			Source:  upperRoot,
			Options: []string{"rw", "rbind"},
		},
	}, nil
}

// viewLowerPath returns the path to the lower directory for View snapshots.
func (s *snapshotter) viewLowerPath(id string) string {
	return filepath.Join(s.root, "snapshots", id, "lower")
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
// For single-layer views, returns a single EROFS mount.
// For multi-layer views, mounts all layers and returns an overlay.
func (s *snapshotter) viewMounts(snap storage.Snapshot) ([]mount.Mount, error) {
	// Check if we have a merged fsmeta that collapses all layers into one
	if s.fsMergeThreshold > 0 {
		if m, ok := s.mountFsMeta(snap); ok {
			return []mount.Mount{m}, nil
		}
	}

	// Get EROFS layer paths
	layerPaths, err := s.getErofsLayerPaths(snap)
	if err != nil {
		return nil, err
	}

	// Single layer: return EROFS mount directly
	if len(layerPaths) == 1 {
		return []mount.Mount{{
			Source:  layerPaths[0],
			Type:    "erofs",
			Options: []string{"ro", "loop"},
		}}, nil
	}

	// Multi-layer: mount each layer and return overlay
	// ParentIDs are ordered from newest (immediate parent) to oldest (root).
	// For overlayfs, lowerdir must be ordered top-to-bottom (newest first).
	var lowerdirs []string
	for i, layerPath := range layerPaths {
		mountPath := s.layerMountPath(snap.ID, i)
		if err := mountErofsLayer(layerPath, mountPath); err != nil {
			// Cleanup already mounted layers on error
			for j := range i {
				_ = unmountAll(s.layerMountPath(snap.ID, j))
			}
			return nil, err
		}
		lowerdirs = append(lowerdirs, mountPath)
	}

	// Return overlay combining all layers (read-only)
	return []mount.Mount{{
		Type:    "overlay",
		Source:  "overlay",
		Options: []string{"lowerdir=" + strings.Join(lowerdirs, ":")},
	}}, nil
}

// activeMounts returns mounts for active (writable) snapshots.
// Mounts all parent layers and returns an overlay with writable upper.
// Note: fsmeta is NOT used for active mounts because the kernel's EROFS
// device= option requires loop devices to be pre-setup, which we can't do
// when mounting ourselves. For views, we return mount specs that containerd
// can mount using its mount manager with proper loop device setup.
func (s *snapshotter) activeMounts(snap storage.Snapshot) ([]mount.Mount, error) {
	// Get EROFS layer paths
	layerPaths, err := s.getErofsLayerPaths(snap)
	if err != nil {
		return nil, err
	}

	// No parent layers: return just the upper directory as bind mount
	if len(layerPaths) == 0 {
		return []mount.Mount{{
			Type:    "bind",
			Source:  s.blockUpperPath(snap.ID),
			Options: []string{"rw", "rbind"},
		}}, nil
	}

	// Mount each layer and collect mount paths
	// ParentIDs are ordered from newest (immediate parent) to oldest (root).
	// For overlayfs, lowerdir must be ordered top-to-bottom (newest first).
	var lowerdirs []string
	for i, layerPath := range layerPaths {
		mountPath := s.layerMountPath(snap.ID, i)
		if err := mountErofsLayer(layerPath, mountPath); err != nil {
			// Cleanup already mounted layers on error
			for j := range i {
				_ = unmountAll(s.layerMountPath(snap.ID, j))
			}
			return nil, err
		}
		lowerdirs = append(lowerdirs, mountPath)
	}

	return s.activeOverlayMounts(snap, lowerdirs, false)
}

// activeOverlayMounts builds the writable overlay configuration for an active snapshot.
// lowerdirs are the mounted paths for lower layers (already mounted).
// isFsmeta indicates if the lower is a merged fsmeta mount (needs special handling).
func (s *snapshotter) activeOverlayMounts(snap storage.Snapshot, lowerdirs []string, isFsmeta bool) ([]mount.Mount, error) {
	upperPath := s.blockUpperPath(snap.ID)
	workPath := s.blockWorkPath(snap.ID)

	overlayOptions := []string{
		"lowerdir=" + strings.Join(lowerdirs, ":"),
		"upperdir=" + upperPath,
		"workdir=" + workPath,
	}
	overlayOptions = append(overlayOptions, s.ovlOptions...)

	return []mount.Mount{{
		Type:    "overlay",
		Source:  "overlay",
		Options: overlayOptions,
	}}, nil
}

func (s *snapshotter) createSnapshot(ctx context.Context, kind snapshots.Kind, key, parent string, opts []snapshots.Opt) (_ []mount.Mount, err error) {
	var (
		snap     storage.Snapshot
		td, path string
		info     snapshots.Info
	)

	defer func() {
		if err != nil {
			if td != "" {
				if err1 := os.RemoveAll(td); err1 != nil {
					log.G(ctx).WithError(err1).Warn("failed to cleanup temp snapshot directory")
				}
			}
			if path != "" {
				if err1 := os.RemoveAll(path); err1 != nil {
					log.G(ctx).WithError(err1).WithField("path", path).Error("failed to reclaim snapshot directory, directory may need removal")
					err = fmt.Errorf("failed to remove path: %w: %w", err1, err)
				}
			}
		}
	}()

	// Check context before starting work
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("context cancelled before snapshot creation: %w", err)
	}

	snapshotDir := filepath.Join(s.root, "snapshots")
	td, err = s.prepareDirectory(snapshotDir, kind)
	if err != nil {
		return nil, fmt.Errorf("failed to create prepare snapshot dir: %w", err)
	}

	// Mark extract snapshots with a label for TOCTOU-safe detection.
	// This is done within the transaction so the label is atomically
	// associated with the snapshot metadata.
	if isExtractKey(key) {
		opts = append(opts, snapshots.WithLabels(map[string]string{
			extractLabel: "true",
		}))
	}

	if err := s.ms.WithTransaction(ctx, true, func(ctx context.Context) (err error) {
		snap, err = storage.CreateSnapshot(ctx, kind, key, parent, opts...)
		if err != nil {
			return fmt.Errorf("failed to create snapshot: %w", err)
		}

		_, info, _, err = storage.GetInfo(ctx, key)
		if err != nil {
			return fmt.Errorf("failed to get snapshot info: %w", err)
		}

		if len(snap.ParentIDs) > 0 {
			if err := upperDirectoryPermission(filepath.Join(td, "fs"), s.upperPath(snap.ParentIDs[0])); err != nil {
				return fmt.Errorf("failed to set upper directory permissions: %w", err)
			}
		}

		path = filepath.Join(snapshotDir, snap.ID)
		if err = os.Rename(td, path); err != nil {
			return fmt.Errorf("failed to rename: %w", err)
		}
		td = ""
		return nil
	}); err != nil {
		return nil, err
	}

	// Check context after transaction - don't proceed with expensive operations if cancelled
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("context cancelled after transaction: %w", err)
	}

	// Generate fsmeta outside of the transaction since it's unnecessary.
	// Also ignore all errors since it's a nice-to-have stuff.
	if !isExtractKey(key) {
		s.generateFsMeta(ctx, snap.ParentIDs)
	}

	// For active snapshots, create and mount the writable layer.
	// This returns directly usable mounts.
	if kind == snapshots.KindActive && !isExtractKey(key) {
		// Check context before expensive writable layer creation
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("context cancelled before writable layer creation: %w", err)
		}
		if err := s.createWritableLayer(ctx, snap.ID); err != nil {
			return nil, fmt.Errorf("failed to create writable layer: %w", err)
		}
		// Mount the ext4 layer immediately so mounts() can return real paths
		if err := s.mountBlockRwLayer(snap.ID); err != nil {
			return nil, fmt.Errorf("failed to mount writable layer: %w", err)
		}
		// Create upper/work directories inside the mounted ext4
		if err := os.MkdirAll(s.blockUpperPath(snap.ID), 0755); err != nil {
			return nil, fmt.Errorf("failed to create block upperdir: %w", err)
		}
		if err := os.MkdirAll(s.blockWorkPath(snap.ID), 0711); err != nil {
			return nil, fmt.Errorf("failed to create block workdir: %w", err)
		}
	}

	return s.mounts(snap, info)
}

func (s *snapshotter) Prepare(ctx context.Context, key, parent string, opts ...snapshots.Opt) ([]mount.Mount, error) {
	return s.createSnapshot(ctx, snapshots.KindActive, key, parent, opts)
}

func (s *snapshotter) View(ctx context.Context, key, parent string, opts ...snapshots.Opt) ([]mount.Mount, error) {
	return s.createSnapshot(ctx, snapshots.KindView, key, parent, opts)
}

func (s *snapshotter) commitBlock(ctx context.Context, layerBlob string, id string) error {
	layer := s.writablePath(id)
	if _, err := os.Stat(layer); err != nil {
		if os.IsNotExist(err) {
			// No block layer - convert directory mode upper
			upperDir := s.upperPath(id)
			if cerr := convertDirToErofs(ctx, layerBlob, upperDir); cerr != nil {
				return fmt.Errorf("failed to convert upper to erofs layer: %w", cerr)
			}
			return nil
		}
		return fmt.Errorf("failed to access writable layer %s: %w", layer, err)
	}

	// Block mode: use the new rw mount path
	rwMount := s.blockRwMountPath(id)

	// Check if already mounted (from Prepare/Mounts) before trying to mount again.
	alreadyMounted, err := mountinfo.Mounted(rwMount)
	if err != nil {
		return fmt.Errorf("failed to check mount status: %w", err)
	}
	if !alreadyMounted {
		// Mount read-only for commit (we're just reading the upper contents)
		if err := os.MkdirAll(rwMount, 0755); err != nil {
			return fmt.Errorf("failed to create rw mount point: %w", err)
		}
		m := mount.Mount{
			Source:  layer,
			Type:    "ext4",
			Options: []string{"ro", "loop", "noload"},
		}
		if err := m.Mount(rwMount); err != nil {
			return fmt.Errorf("failed to mount writable layer %s: %w", layer, err)
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
		return fmt.Errorf("failed to convert upper block to erofs layer: %w", cerr)
	}
	return nil
}

// generate a metadata-only EROFS fsmeta.erofs if all EROFS layer blobs are valid
func (s *snapshotter) generateFsMeta(ctx context.Context, snapIDs []string) {
	var blobs []string

	if s.fsMergeThreshold == 0 || uint(len(snapIDs)) <= s.fsMergeThreshold {
		return
	}

	t1 := time.Now()
	mergedMeta := s.fsMetaPath(snapIDs[0])
	// If the empty placeholder cannot be created (mainly due to os.IsExist), just return
	if _, err := os.OpenFile(mergedMeta, os.O_CREATE|os.O_EXCL, 0600); err != nil {
		return
	}

	for i := len(snapIDs) - 1; i >= 0; i-- {
		blob := s.layerBlobPath(snapIDs[i])
		if _, err := os.Stat(blob); err != nil {
			return
		}
		blobs = append(blobs, blob)
	}
	tmpMergedMeta := mergedMeta + ".tmp"
	// Use rebuild mode (no --aufs) to generate flatdev fsmeta with mapped_blkaddr.
	// This allows qemubox to consolidate layers into a single VMDK device.
	args := append([]string{"--quiet", tmpMergedMeta}, blobs...)
	log.G(ctx).Infof("merging layers with mkfs.erofs %v", args)
	cmd := exec.CommandContext(ctx, "mkfs.erofs", args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		log.G(ctx).Warnf("failed to generate merged fsmeta for %v: %q: %v", snapIDs[0], string(out), err)
		return
	}
	// Atomically replace the fsmeta with the generated file
	if err = os.Rename(tmpMergedMeta, mergedMeta); err != nil {
		log.G(ctx).Errorf("failed to rename fsmeta: %v", err)
		return
	}

	// Generate VMDK descriptor that references fsmeta + all layer blobs.
	// This allows QEMU to present them as a single concatenated block device.
	vmdkFile := s.vmdkPath(snapIDs[0])
	vmdkDevices := append([]string{mergedMeta}, blobs...)
	if err = erofsutils.WriteVMDKDescriptorToFile(vmdkFile, vmdkDevices); err != nil {
		log.G(ctx).Errorf("failed to generate VMDK descriptor: %v", err)
		// Clean up fsmeta since VMDK generation failed
		_ = os.Remove(mergedMeta)
		return
	}

	log.G(ctx).WithFields(log.Fields{
		"d": time.Since(t1),
	}).Infof("merged fsmeta and vmdk for %v generated", snapIDs[0])
}

func (s *snapshotter) Commit(ctx context.Context, name, key string, opts ...snapshots.Opt) error {
	var layerBlob string
	var id string

	// Apply the overlayfs upperdir (generated by non-EROFS differs) into a EROFS blob
	// in a read transaction first since conversion could be slow.
	err := s.ms.WithTransaction(ctx, false, func(ctx context.Context) error {
		sid, _, _, err := storage.GetInfo(ctx, key)
		if err != nil {
			return fmt.Errorf("failed to get snapshot info for %q: %w", key, err)
		}
		id = sid
		return nil
	})
	if err != nil {
		return err
	}

	// If the layer blob doesn't exist, which means this layer wasn't applied by
	// the EROFS differ (possibly the walking differ), convert the upperdir instead.
	layerBlob = s.layerBlobPath(id)
	if _, err := os.Stat(layerBlob); err != nil {
		if cerr := s.commitBlock(ctx, layerBlob, id); cerr != nil {
			if errdefs.IsNotImplemented(cerr) {
				return err
			}
			return cerr
		}
	}

	// Enable fsverity on the EROFS layer if configured
	if s.enableFsverity {
		if err := fsverity.Enable(layerBlob); err != nil {
			return fmt.Errorf("failed to enable fsverity: %w", err)
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
			return fmt.Errorf("failed to get the converted erofs blob: %w", err)
		}

		usage, err := fs.DiskUsage(ctx, layerBlob)
		if err != nil {
			return fmt.Errorf("failed to calculate disk usage for %q: %w", layerBlob, err)
		}
		if _, err = storage.CommitActive(ctx, key, name, snapshots.Usage(usage), opts...); err != nil {
			return fmt.Errorf("failed to commit snapshot %s: %w", key, err)
		}
		return nil
	})
}

func (s *snapshotter) Mounts(ctx context.Context, key string) (_ []mount.Mount, err error) {
	var snap storage.Snapshot
	var info snapshots.Info
	if err := s.ms.WithTransaction(ctx, false, func(ctx context.Context) error {
		snap, err = storage.GetSnapshot(ctx, key)
		if err != nil {
			return fmt.Errorf("failed to get active mount: %w", err)
		}

		_, info, _, err = storage.GetInfo(ctx, key)
		if err != nil {
			return fmt.Errorf("failed to get snapshot info: %w", err)
		}
		return nil
	}); err != nil {
		return nil, err
	}
	mounts, err := s.mounts(snap, info)
	if err != nil {
		return nil, err
	}
	return mounts, nil
}

func (s *snapshotter) getCleanupDirectories(ctx context.Context) ([]string, error) {
	ids, err := storage.IDMap(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get snapshot ID map: %w", err)
	}

	snapshotDir := filepath.Join(s.root, "snapshots")
	fd, err := os.Open(snapshotDir)
	if err != nil {
		return nil, fmt.Errorf("failed to open snapshots directory: %w", err)
	}
	defer fd.Close()

	dirs, err := fd.Readdirnames(0)
	if err != nil {
		return nil, fmt.Errorf("failed to read snapshots directory: %w", err)
	}

	cleanup := []string{}
	for _, d := range dirs {
		if _, ok := ids[d]; ok {
			continue
		}
		cleanup = append(cleanup, filepath.Join(snapshotDir, d))
	}

	return cleanup, nil
}

// Remove abandons the snapshot identified by key. The snapshot will
// immediately become unavailable and unrecoverable. Disk space will
// be freed up on the next call to `Cleanup`.
func (s *snapshotter) Remove(ctx context.Context, key string) (err error) {
	var removals []string
	var id string
	// Remove directories after the transaction is closed, failures must not
	// return error since the transaction is committed with the removal
	// key no longer available.
	defer func() {
		if err == nil {
			// Cleanup layer mounts (from viewMounts/activeMounts)
			layersDir := filepath.Join(s.root, "snapshots", id, "layers")
			if entries, derr := os.ReadDir(layersDir); derr == nil {
				for _, entry := range entries {
					layerMount := filepath.Join(layersDir, entry.Name())
					if uerr := unmountAll(layerMount); uerr != nil {
						log.G(ctx).WithError(uerr).WithField("path", layerMount).Warn("failed to unmount layer")
					}
				}
			}

			// Cleanup block rw mount
			if err := unmountAll(s.blockRwMountPath(id)); err != nil {
				log.G(ctx).WithError(err).WithField("id", id).Warnf("failed to cleanup block rw mount")
			}

			for _, dir := range removals {
				if err := os.RemoveAll(dir); err != nil {
					log.G(ctx).WithError(err).WithField("path", dir).Warn("failed to remove directory")
				}
			}
		}
	}()
	return s.ms.WithTransaction(ctx, true, func(ctx context.Context) error {
		var k snapshots.Kind

		id, k, err = storage.Remove(ctx, key)
		if err != nil {
			return fmt.Errorf("failed to remove snapshot %s: %w", key, err)
		}
		// Note: The prepared marker file (if any) is removed when the snapshot
		// directory is cleaned up below.

		removals, err = s.getCleanupDirectories(ctx)
		if err != nil {
			return fmt.Errorf("unable to get directories for removal: %w", err)
		}
		// The layer blob is only persisted for committed snapshots.
		if k == snapshots.KindCommitted {
			// Clear IMMUTABLE_FL before removal, since this flag avoids it.
			err = setImmutable(s.layerBlobPath(id), false)
			if err != nil && !errdefs.IsNotImplemented(err) {
				return fmt.Errorf("failed to clear IMMUTABLE_FL: %w", err)
			}
		}
		return nil
	})
}

func (s *snapshotter) Cleanup(ctx context.Context) (err error) {
	var removals []string
	if err := s.ms.WithTransaction(ctx, false, func(ctx context.Context) error {
		var err error
		removals, err = s.getCleanupDirectories(ctx)
		return err
	}); err != nil {
		return err
	}

	var cleanupErrs []error
	for _, dir := range removals {
		// Cleanup block rw mount
		if err := unmountAll(filepath.Join(dir, "rw")); err != nil {
			log.G(ctx).WithError(err).WithField("path", dir).Debug("failed to cleanup block rw mount")
			cleanupErrs = append(cleanupErrs, fmt.Errorf("cleanup rw %s: %w", dir, err))
		}
		if err := setImmutable(filepath.Join(dir, erofsutils.LayerBlobFilename), false); err != nil && !errdefs.IsNotImplemented(err) {
			log.G(ctx).WithError(err).WithField("path", dir).Debug("failed to clear immutable flag")
			// Don't add to cleanupErrs - this is best-effort and shouldn't fail cleanup
		}
		if err := os.RemoveAll(dir); err != nil {
			log.G(ctx).WithError(err).WithField("path", dir).Warn("failed to remove directory")
			cleanupErrs = append(cleanupErrs, fmt.Errorf("remove %s: %w", dir, err))
		}
	}

	if len(cleanupErrs) > 0 {
		// Log all errors but only return a summary to avoid overwhelming callers
		log.G(ctx).WithField("error_count", len(cleanupErrs)).Warn("cleanup completed with errors")
		return fmt.Errorf("cleanup had %d errors, first: %w", len(cleanupErrs), cleanupErrs[0])
	}
	return nil
}

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

func (s *snapshotter) Walk(ctx context.Context, fn snapshots.WalkFunc, fs ...string) error {
	return s.ms.WithTransaction(ctx, false, func(ctx context.Context) error {
		return storage.WalkInfo(ctx, fn, fs...)
	})
}

// Usage returns the resources taken by the snapshot identified by key.
//
// For active snapshots, this will scan the usage of the overlay "diff" (aka
// "upper") directory and may take some time.
//
// For committed snapshots, the value is returned from the metadata database.
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
			// TODO(stevvooe): Consider not reporting an error in this case.
			return snapshots.Usage{}, err
		}
		usage = snapshots.Usage(du)
	}
	return usage, nil
}

// verifyFsverity checks if fsverity is enabled on the given path.
// TODO: integrate this into the commit flow for validation.
//
//nolint:unused // Will be used when fsverity validation is integrated
func (s *snapshotter) verifyFsverity(path string) error {
	if !s.enableFsverity {
		return nil
	}
	enabled, err := fsverity.IsEnabled(path)
	if err != nil {
		return fmt.Errorf("failed to check fsverity status: %w", err)
	}
	if !enabled {
		return fmt.Errorf("fsverity is not enabled on %s", path)
	}
	return nil
}
