package snapshotter

import (
	"errors"
	"strings"
	"testing"
)

func TestLayerBlobNotFoundError(t *testing.T) {
	err := &LayerBlobNotFoundError{
		ID:       "snap-123",
		Dir:      "/var/lib/snapshots/123",
		Searched: []string{"sha256-*.erofs", "snapshot-*.erofs"},
	}

	// Test error message
	msg := err.Error()
	if msg == "" {
		t.Error("error message should not be empty")
	}
	if !strings.Contains(msg, "snap-123") {
		t.Errorf("error message should contain snapshot ID: %s", msg)
	}
	if !strings.Contains(msg, "sha256-*.erofs") {
		t.Errorf("error message should contain searched patterns: %s", msg)
	}

	// Test error code
	if err.Code() != ErrCodeLayerNotFound {
		t.Errorf("expected error code %v, got %v", ErrCodeLayerNotFound, err.Code())
	}

	// Test SnapshotID method
	if err.SnapshotID() != "snap-123" {
		t.Errorf("SnapshotID() should return %q, got %q", "snap-123", err.SnapshotID())
	}

	// Test errors.As for type-based matching (idiomatic Go)
	var target *LayerBlobNotFoundError
	if !errors.As(err, &target) {
		t.Error("errors.As should match LayerBlobNotFoundError")
	}
	if target.ID != "snap-123" {
		t.Errorf("errors.As should preserve fields, got ID=%q", target.ID)
	}

	// Test errors.As with different error type
	var otherTarget *CommitConversionError
	if errors.As(err, &otherTarget) {
		t.Error("errors.As should not match different error type")
	}

	// Test IsErrorCode helper
	if !IsErrorCode(err, ErrCodeLayerNotFound) {
		t.Error("IsErrorCode should return true for matching code")
	}
	if IsErrorCode(err, ErrCodeConversionFailed) {
		t.Error("IsErrorCode should return false for non-matching code")
	}
}

func TestCommitConversionError(t *testing.T) {
	cause := errors.New("no space left on device")
	err := &CommitConversionError{
		ID:       "snap-789",
		UpperDir: "/var/lib/snapshots/789/fs",
		Mode:     CommitModeOverlay,
		Cause:    cause,
	}

	// Test error message
	msg := err.Error()
	if !strings.Contains(msg, "snap-789") {
		t.Errorf("error message should contain snapshot ID: %s", msg)
	}
	if !strings.Contains(msg, "/fs") {
		t.Errorf("error message should contain upper dir: %s", msg)
	}
	if !strings.Contains(msg, "overlay") {
		t.Errorf("error message should contain mode: %s", msg)
	}

	// Test error code
	if err.Code() != ErrCodeConversionFailed {
		t.Errorf("expected error code %v, got %v", ErrCodeConversionFailed, err.Code())
	}

	// Test SnapshotID method
	if err.SnapshotID() != "snap-789" {
		t.Errorf("SnapshotID() should return %q, got %q", "snap-789", err.SnapshotID())
	}

	// Test Unwrap
	if err.Unwrap() != cause {
		t.Error("Unwrap should return the cause")
	}
}

func TestErrorWrapping(t *testing.T) {
	// Test error wrapping through CommitConversionError
	rootCause := errors.New("disk full")
	layerErr := &LayerBlobNotFoundError{
		ID:       "snap-1",
		Dir:      "/path/to/dir",
		Searched: []string{"*.erofs"},
	}
	commitErr := &CommitConversionError{
		ID:       "snap-1",
		UpperDir: "/path/to/upper",
		Mode:     CommitModeBlock,
		Cause:    layerErr,
	}

	// Should be able to find LayerBlobNotFoundError in chain
	var layerTarget *LayerBlobNotFoundError
	if !errors.As(commitErr, &layerTarget) {
		t.Error("should find LayerBlobNotFoundError in error chain")
	}

	// Test with root cause
	commitErr2 := &CommitConversionError{
		ID:       "snap-2",
		UpperDir: "/path/to/upper",
		Mode:     CommitModeOverlay,
		Cause:    rootCause,
	}
	if !errors.Is(commitErr2, rootCause) {
		t.Error("should find root cause through error chain")
	}
}

func TestMountError(t *testing.T) {
	cause := errors.New("device busy")
	err := &MountError{
		ID:        "snap-456",
		Target:    "/mnt/test",
		Operation: "unmount",
		Cause:     cause,
	}

	// Test error message
	msg := err.Error()
	if !strings.Contains(msg, "snap-456") {
		t.Errorf("error message should contain snapshot ID: %s", msg)
	}
	if !strings.Contains(msg, "/mnt/test") {
		t.Errorf("error message should contain target: %s", msg)
	}
	if !strings.Contains(msg, "unmount") {
		t.Errorf("error message should contain operation: %s", msg)
	}

	// Test error code
	if err.Code() != ErrCodeMountFailed {
		t.Errorf("expected error code %v, got %v", ErrCodeMountFailed, err.Code())
	}

	// Test SnapshotID method
	if err.SnapshotID() != "snap-456" {
		t.Errorf("SnapshotID() should return %q, got %q", "snap-456", err.SnapshotID())
	}

	// Test Unwrap
	if err.Unwrap() != cause {
		t.Error("Unwrap should return the cause")
	}
}

func TestErrorCodeString(t *testing.T) {
	tests := []struct {
		code ErrorCode
		want string
	}{
		{ErrCodeUnknown, "UNKNOWN"},
		{ErrCodeLayerNotFound, "LAYER_NOT_FOUND"},
		{ErrCodeConversionFailed, "CONVERSION_FAILED"},
		{ErrCodeMountFailed, "MOUNT_FAILED"},
		{ErrCodeLockFailed, "LOCK_FAILED"},
	}

	for _, tt := range tests {
		if got := tt.code.String(); got != tt.want {
			t.Errorf("ErrorCode(%d).String() = %q, want %q", tt.code, got, tt.want)
		}
	}
}
