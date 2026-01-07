package snapshotter

import (
	"errors"
	"testing"
)

// TestLayerBlobNotFoundErrorAs verifies errors.As works correctly for type matching.
// Note: We use errors.As (not errors.Is) for structural error types per Go idioms.
func TestLayerBlobNotFoundErrorAs(t *testing.T) {
	err := &LayerBlobNotFoundError{
		SnapshotID: "test-123",
		Dir:        "/test/path",
		Searched:   []string{"*.erofs"},
	}

	// Test errors.As for type-based matching
	var target *LayerBlobNotFoundError
	if !errors.As(err, &target) {
		t.Error("errors.As should match LayerBlobNotFoundError")
	}
	if target.SnapshotID != "test-123" {
		t.Errorf("expected snapshot ID test-123, got %s", target.SnapshotID)
	}

	// Test that wrapped error can be unwrapped with errors.As
	wrapped := &CommitConversionError{
		SnapshotID: "commit-test",
		UpperDir:   "/upper",
		Cause:      err,
	}

	var wrappedTarget *LayerBlobNotFoundError
	if !errors.As(wrapped, &wrappedTarget) {
		t.Error("errors.As should find LayerBlobNotFoundError in chain")
	}
	if wrappedTarget.SnapshotID != "test-123" {
		t.Errorf("expected snapshot ID test-123, got %s", wrappedTarget.SnapshotID)
	}
}

// TestErrorChainDepth verifies deep error chains work correctly.
func TestErrorChainDepth(t *testing.T) {
	// Create a 3-level error chain
	level1 := errors.New("root cause: filesystem full")
	level2 := &BlockMountError{
		Source: "/path/to/block.img",
		Target: "/mnt/target",
		Cause:  level1,
	}
	level3 := &CommitConversionError{
		SnapshotID: "snap-abc",
		UpperDir:   "/var/lib/snapshotter/abc/upper",
		Cause:      level2,
	}

	// Should find root cause
	if !errors.Is(level3, level1) {
		t.Error("should find root error through 3-level chain")
	}

	// Should find intermediate error
	var blockErr *BlockMountError
	if !errors.As(level3, &blockErr) {
		t.Error("should find BlockMountError in chain")
	}

	// Error message should include context from all levels
	msg := level3.Error()
	if !errContains(msg, "snap-abc") {
		t.Error("error message should contain snapshot ID")
	}
}

// TestEmptyLayerSequenceOperations verifies operations on empty sequences are safe.
func TestEmptyLayerSequenceOperations(t *testing.T) {
	empty := LayerSequence{}

	// All operations should be safe on empty sequence
	if !empty.IsEmpty() {
		t.Error("empty sequence should be empty")
	}

	if empty.Len() != 0 {
		t.Errorf("empty sequence Len() = %d, want 0", empty.Len())
	}

	reversed := empty.Reverse()
	if !reversed.IsEmpty() {
		t.Error("reversed empty sequence should be empty")
	}
}

// TestNilSliceLayerSequence verifies nil slice handling.
func TestNilSliceLayerSequence(t *testing.T) {
	seq := NewNewestFirst(nil)

	if !seq.IsEmpty() {
		t.Error("nil slice should create empty sequence")
	}

	if seq.Len() != 0 {
		t.Error("nil slice sequence should have length 0")
	}

	// Should not panic on operations
	_ = seq.Reverse()
}

// TestBlockMountErrorNilCause verifies nil cause is handled.
func TestBlockMountErrorNilCause(t *testing.T) {
	err := &BlockMountError{
		Source: "/path/source",
		Target: "/path/target",
		Cause:  nil,
	}

	// Should not panic
	msg := err.Error()
	if msg == "" {
		t.Error("error message should not be empty")
	}

	// Unwrap should return nil safely
	if err.Unwrap() != nil {
		t.Error("Unwrap with nil cause should return nil")
	}
}
