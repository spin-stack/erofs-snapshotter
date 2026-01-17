package snapshotter

import (
	"errors"
	"strings"
	"testing"
)

func TestLayerBlobNotFoundError(t *testing.T) {
	err := &LayerBlobNotFoundError{
		SnapshotID: "snap-123",
		Dir:        "/var/lib/snapshots/123",
		Searched:   []string{"sha256-*.erofs", "snapshot-*.erofs"},
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

	// Test errors.As for type-based matching (idiomatic Go)
	var target *LayerBlobNotFoundError
	if !errors.As(err, &target) {
		t.Error("errors.As should match LayerBlobNotFoundError")
	}
	if target.SnapshotID != "snap-123" {
		t.Errorf("errors.As should preserve fields, got SnapshotID=%q", target.SnapshotID)
	}

	// Test errors.As with different error type
	var otherTarget *CommitConversionError
	if errors.As(err, &otherTarget) {
		t.Error("errors.As should not match different error type")
	}
}

func TestCommitConversionError(t *testing.T) {
	cause := errors.New("no space left on device")
	err := &CommitConversionError{
		SnapshotID: "snap-789",
		UpperDir:   "/var/lib/snapshots/789/fs",
		Cause:      cause,
	}

	// Test error message
	msg := err.Error()
	if !strings.Contains(msg, "snap-789") {
		t.Errorf("error message should contain snapshot ID: %s", msg)
	}
	if !strings.Contains(msg, "/fs") {
		t.Errorf("error message should contain upper dir: %s", msg)
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
		SnapshotID: "snap-1",
		Dir:        "/path/to/dir",
		Searched:   []string{"*.erofs"},
	}
	commitErr := &CommitConversionError{
		SnapshotID: "snap-1",
		UpperDir:   "/path/to/upper",
		Cause:      layerErr,
	}

	// Should be able to find LayerBlobNotFoundError in chain
	var layerTarget *LayerBlobNotFoundError
	if !errors.As(commitErr, &layerTarget) {
		t.Error("should find LayerBlobNotFoundError in error chain")
	}

	// Test with root cause
	commitErr2 := &CommitConversionError{
		SnapshotID: "snap-2",
		UpperDir:   "/path/to/upper",
		Cause:      rootCause,
	}
	if !errors.Is(commitErr2, rootCause) {
		t.Error("should find root cause through error chain")
	}
}
