package snapshotter

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/spin-stack/erofs-snapshotter/internal/erofs"
)

const (
	// fallbackLayerPrefix is used for layers created by the walking differ fallback
	// when the original layer digest is not available.
	fallbackLayerPrefix = "snapshot-"
)

// Snapshot directory structure constants.
const (
	// snapshotsDirName is the name of the directory containing all snapshots.
	snapshotsDirName = "snapshots"

	// fsDirName is the overlay upper directory name within a snapshot.
	fsDirName = "fs"

	// rwLayerFilename is the filename for the ext4 writable layer image.
	rwLayerFilename = "rwlayer.img"

	// rwDirName is the directory name for the mounted ext4 rw layer.
	rwDirName = "rw"

	// upperDirName is the overlay upper directory name within the rw mount.
	upperDirName = "upper"

	// lowerDirName is the directory name for view snapshot lower paths.
	lowerDirName = "lower"

	// fsmetaFilename is the filename for merged fsmeta EROFS.
	fsmetaFilename = "fsmeta.erofs"

	// vmdkFilename is the filename for the VMDK descriptor.
	vmdkFilename = "merged.vmdk"

	// manifestFilename is the filename for the layer manifest (stores digests in VMDK order).
	manifestFilename = "layers.manifest"
)

// upperPath returns the path to the overlay upper directory for a snapshot.
func (s *snapshotter) upperPath(id string) string {
	return filepath.Join(s.root, snapshotsDirName, id, fsDirName)
}

// writablePath returns the path to the ext4 writable layer image file.
func (s *snapshotter) writablePath(id string) string {
	return filepath.Join(s.root, snapshotsDirName, id, rwLayerFilename)
}

// blockRwMountPath returns the mount point for the ext4 rwlayer in block mode.
func (s *snapshotter) blockRwMountPath(id string) string {
	return filepath.Join(s.root, snapshotsDirName, id, rwDirName)
}

// blockUpperPath returns the overlay upperdir inside the mounted ext4.
func (s *snapshotter) blockUpperPath(id string) string {
	return filepath.Join(s.blockRwMountPath(id), upperDirName)
}

// findLayerBlob finds the EROFS layer blob in a snapshot directory.
// Layer blobs are named using their content digest (sha256-xxx.erofs) or
// the snapshot ID for walking differ fallback (snapshot-xxx.erofs).
// Returns the path if found, or LayerBlobNotFoundError if no blob exists.
func (s *snapshotter) findLayerBlob(id string) (string, error) {
	dir := filepath.Join(s.root, snapshotsDirName, id)
	patterns := []string{erofs.LayerBlobPattern, fallbackLayerPrefix + "*.erofs"}

	// First try digest-based naming (primary path via EROFS differ)
	matches, err := filepath.Glob(filepath.Join(dir, erofs.LayerBlobPattern))
	if err != nil {
		return "", fmt.Errorf("glob layer blob: %w", err)
	}
	if len(matches) > 0 {
		return matches[0], nil
	}

	// Try fallback naming (walking differ creates these)
	fallbackPath := filepath.Join(dir, fallbackLayerPrefix+id+".erofs")
	if _, err := os.Stat(fallbackPath); err == nil {
		return fallbackPath, nil
	}

	return "", &LayerBlobNotFoundError{
		SnapshotID: id,
		Dir:        dir,
		Searched:   patterns,
	}
}

// fallbackLayerBlobPath returns the path for creating a layer blob when the
// digest is not available (walking differ fallback). Uses the snapshot ID.
func (s *snapshotter) fallbackLayerBlobPath(id string) string {
	return filepath.Join(s.root, snapshotsDirName, id, fallbackLayerPrefix+id+".erofs")
}

// fsMetaPath returns the path to the merged fsmeta.erofs file.
func (s *snapshotter) fsMetaPath(id string) string {
	return filepath.Join(s.root, snapshotsDirName, id, fsmetaFilename)
}

// vmdkPath returns the path to the VMDK descriptor file.
func (s *snapshotter) vmdkPath(id string) string {
	return filepath.Join(s.root, snapshotsDirName, id, vmdkFilename)
}

// manifestPath returns the path to the layer manifest file.
func (s *snapshotter) manifestPath(id string) string {
	return filepath.Join(s.root, snapshotsDirName, id, manifestFilename)
}

// viewLowerPath returns the path to the lower directory for View snapshots.
func (s *snapshotter) viewLowerPath(id string) string {
	return filepath.Join(s.root, snapshotsDirName, id, lowerDirName)
}

// snapshotDir returns the path to a snapshot directory.
func (s *snapshotter) snapshotDir(id string) string {
	return filepath.Join(s.root, snapshotsDirName, id)
}

// snapshotsDir returns the path to the snapshots root directory.
func (s *snapshotter) snapshotsDir() string {
	return filepath.Join(s.root, snapshotsDirName)
}

// lowerPath returns the EROFS layer blob path for a snapshot, validating it exists.
func (s *snapshotter) lowerPath(id string) (string, error) {
	layerBlob, err := s.findLayerBlob(id)
	if err != nil {
		return "", fmt.Errorf("failed to find valid erofs layer blob: %w", err)
	}

	return layerBlob, nil
}
