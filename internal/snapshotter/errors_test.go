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
	var otherTarget *BlockMountError
	if errors.As(err, &otherTarget) {
		t.Error("errors.As should not match different error type")
	}
}

func TestBlockMountError(t *testing.T) {
	cause := errors.New("permission denied")
	err := &BlockMountError{
		Source: "/var/lib/snapshots/123/rwlayer.img",
		Target: "/var/lib/snapshots/123/rw",
		Cause:  cause,
	}

	// Test error message
	msg := err.Error()
	if !strings.Contains(msg, "rwlayer.img") {
		t.Errorf("error message should contain source: %s", msg)
	}
	if !strings.Contains(msg, "/rw") {
		t.Errorf("error message should contain target: %s", msg)
	}

	// Test Unwrap
	if err.Unwrap() != cause {
		t.Error("Unwrap should return the cause")
	}

	// Test errors.Is with cause
	if !errors.Is(err, cause) {
		t.Error("errors.Is should match wrapped cause")
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
	// Test deep error wrapping
	rootCause := errors.New("disk full")
	blockErr := &BlockMountError{
		Source: "/path/to/img",
		Target: "/path/to/mount",
		Cause:  rootCause,
	}
	commitErr := &CommitConversionError{
		SnapshotID: "snap-1",
		UpperDir:   "/path/to/upper",
		Cause:      blockErr,
	}

	// Should be able to find root cause through chain
	if !errors.Is(commitErr, rootCause) {
		t.Error("should find root cause through error chain")
	}

	// Should be able to find block error in chain
	var blockTarget *BlockMountError
	if !errors.As(commitErr, &blockTarget) {
		t.Error("should find BlockMountError in error chain")
	}
}
