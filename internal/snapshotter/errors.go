package snapshotter

import (
	"fmt"
)

// LayerBlobNotFoundError indicates no EROFS layer blob exists for a snapshot.
// This typically means the EROFS differ hasn't processed the layer yet,
// or the walking differ fallback hasn't created a blob.
type LayerBlobNotFoundError struct {
	SnapshotID string
	Dir        string
	Searched   []string
}

func (e *LayerBlobNotFoundError) Error() string {
	return fmt.Sprintf("layer blob not found for snapshot %s in %s (searched: %v)",
		e.SnapshotID, e.Dir, e.Searched)
}

// BlockMountError indicates ext4 block mount failure during commit.
type BlockMountError struct {
	Source string
	Target string
	Cause  error
}

func (e *BlockMountError) Error() string {
	return fmt.Sprintf("mount block %s at %s: %v", e.Source, e.Target, e.Cause)
}

func (e *BlockMountError) Unwrap() error {
	return e.Cause
}

// CommitConversionError indicates EROFS conversion failure during commit.
type CommitConversionError struct {
	SnapshotID string
	UpperDir   string
	Cause      error
}

func (e *CommitConversionError) Error() string {
	return fmt.Sprintf("convert snapshot %s upper %s to EROFS: %v",
		e.SnapshotID, e.UpperDir, e.Cause)
}

func (e *CommitConversionError) Unwrap() error {
	return e.Cause
}
