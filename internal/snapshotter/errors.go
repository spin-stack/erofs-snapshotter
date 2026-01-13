package snapshotter

import (
	"errors"
	"fmt"
	"strings"
)

// ErrorCode represents the type of snapshotter error for programmatic handling.
type ErrorCode int

const (
	// ErrCodeUnknown indicates an unclassified error.
	ErrCodeUnknown ErrorCode = iota
	// ErrCodeLayerNotFound indicates a layer blob was not found.
	ErrCodeLayerNotFound
	// ErrCodeConversionFailed indicates EROFS conversion failed.
	ErrCodeConversionFailed
	// ErrCodeMountFailed indicates a mount operation failed.
	ErrCodeMountFailed
	// ErrCodeLockFailed indicates lock acquisition failed.
	ErrCodeLockFailed
)

// String returns the string representation of an error code.
func (c ErrorCode) String() string {
	switch c {
	case ErrCodeLayerNotFound:
		return "LAYER_NOT_FOUND"
	case ErrCodeConversionFailed:
		return "CONVERSION_FAILED"
	case ErrCodeMountFailed:
		return "MOUNT_FAILED"
	case ErrCodeLockFailed:
		return "LOCK_FAILED"
	default:
		return "UNKNOWN"
	}
}

// SnapshotterError is the base interface for all snapshotter errors.
// It provides common methods for programmatic error handling.
type SnapshotterError interface {
	error
	Code() ErrorCode
	SnapshotID() string
}

// IsErrorCode checks if an error has the specified error code.
func IsErrorCode(err error, code ErrorCode) bool {
	var se SnapshotterError
	if errors.As(err, &se) {
		return se.Code() == code
	}
	return false
}

// LayerBlobNotFoundError indicates no EROFS layer blob exists for a snapshot.
// This typically means the EROFS differ hasn't processed the layer yet,
// or the walking differ fallback hasn't created a blob.
//
// Recovery: The commit process will fall back to converting the upper
// directory using mkfs.erofs directly. Check that the snapshot directory
// exists and contains the expected upper directory (fs/ or rw/upper/).
type LayerBlobNotFoundError struct {
	ID       string   // Snapshot ID
	Dir      string   // Directory searched
	Searched []string // Patterns searched
}

func (e *LayerBlobNotFoundError) Error() string {
	return fmt.Sprintf("layer blob not found for snapshot %s in %s (searched patterns: %s)",
		e.ID, e.Dir, strings.Join(e.Searched, ", "))
}

// Code returns the error code for programmatic handling.
func (e *LayerBlobNotFoundError) Code() ErrorCode {
	return ErrCodeLayerNotFound
}

// SnapshotID returns the affected snapshot ID.
func (e *LayerBlobNotFoundError) SnapshotID() string {
	return e.ID
}

// CommitMode indicates the mode used during commit.
type CommitMode string

const (
	// CommitModeBlock indicates the ext4 block device mode was used.
	CommitModeBlock CommitMode = "block"
	// CommitModeOverlay indicates the overlay directory mode was used.
	CommitModeOverlay CommitMode = "overlay"
)

// CommitConversionError indicates EROFS conversion failure during commit.
// This occurs when mkfs.erofs fails to convert the upper directory to EROFS format.
//
// Common causes:
//   - mkfs.erofs not installed or not in PATH
//   - Upper directory is empty or inaccessible
//   - Disk space exhausted
//   - File permissions prevent reading upper directory
type CommitConversionError struct {
	ID       string     // Snapshot ID
	UpperDir string     // Source directory for conversion
	Mode     CommitMode // Commit mode (block or overlay)
	Cause    error      // Underlying error
}

func (e *CommitConversionError) Error() string {
	return fmt.Sprintf("failed to convert snapshot %s to EROFS (source: %s, mode: %s): %v",
		e.ID, e.UpperDir, e.Mode, e.Cause)
}

// Code returns the error code for programmatic handling.
func (e *CommitConversionError) Code() ErrorCode {
	return ErrCodeConversionFailed
}

// SnapshotID returns the affected snapshot ID.
func (e *CommitConversionError) SnapshotID() string {
	return e.ID
}

func (e *CommitConversionError) Unwrap() error {
	return e.Cause
}

// MountError indicates a mount operation failed.
type MountError struct {
	ID        string // Snapshot ID
	Target    string // Mount target path
	Operation string // Operation that failed (mount, unmount)
	Cause     error  // Underlying error
}

func (e *MountError) Error() string {
	return fmt.Sprintf("mount %s failed for snapshot %s at %s: %v",
		e.Operation, e.ID, e.Target, e.Cause)
}

// Code returns the error code for programmatic handling.
func (e *MountError) Code() ErrorCode {
	return ErrCodeMountFailed
}

// SnapshotID returns the affected snapshot ID.
func (e *MountError) SnapshotID() string {
	return e.ID
}

func (e *MountError) Unwrap() error {
	return e.Cause
}
