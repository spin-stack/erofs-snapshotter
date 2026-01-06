package snapshotter

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/aledbf/nexuserofs/internal/erofs"
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

// layerBlobPath returns the path to a committed EROFS layer blob.
func (s *snapshotter) layerBlobPath(id string) string {
	return filepath.Join(s.root, snapshotsDirName, id, erofs.LayerBlobFilename)
}

// fsMetaPath returns the path to the merged fsmeta.erofs file.
func (s *snapshotter) fsMetaPath(id string) string {
	return filepath.Join(s.root, snapshotsDirName, id, fsmetaFilename)
}

// vmdkPath returns the path to the VMDK descriptor file.
func (s *snapshotter) vmdkPath(id string) string {
	return filepath.Join(s.root, snapshotsDirName, id, vmdkFilename)
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
	layerBlob := s.layerBlobPath(id)
	if _, err := os.Stat(layerBlob); err != nil {
		return "", fmt.Errorf("failed to find valid erofs layer blob: %w", err)
	}

	return layerBlob, nil
}
