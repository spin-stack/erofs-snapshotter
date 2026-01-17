package snapshotter

import (
	"fmt"
	"strings"
)

// LayerBlobNotFoundError indicates no EROFS layer blob exists for a snapshot.
// This typically means the EROFS differ hasn't processed the layer yet,
// or the walking differ fallback hasn't created a blob.
//
// Recovery: The commit process will fall back to converting the upper
// directory using mkfs.erofs directly. Check that the snapshot directory
// exists and contains the expected upper directory (fs/ or rw/upper/).
type LayerBlobNotFoundError struct {
	SnapshotID string
	Dir        string
	Searched   []string
}

func (e *LayerBlobNotFoundError) Error() string {
	return fmt.Sprintf("layer blob not found for snapshot %s in %s (searched patterns: %s)",
		e.SnapshotID, e.Dir, strings.Join(e.Searched, ", "))
}

// CommitConversionError indicates EROFS conversion failure during commit.
// This occurs when mkfs.erofs fails to convert the upper directory to EROFS format.
//
// Common causes:
//   - mkfs.erofs not installed or not in PATH
//   - Upper directory is empty or inaccessible
//   - Disk space exhausted
//   - File permissions prevent reading upper directory
type CommitConversionError struct {
	SnapshotID string
	UpperDir   string
	Cause      error
}

func (e *CommitConversionError) Error() string {
	return fmt.Sprintf("failed to convert snapshot %s to EROFS (source dir: %s): %v",
		e.SnapshotID, e.UpperDir, e.Cause)
}

func (e *CommitConversionError) Unwrap() error {
	return e.Cause
}
