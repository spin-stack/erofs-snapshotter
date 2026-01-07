package snapshotter

import (
	"testing"
)

func TestLayerSequenceReverse(t *testing.T) {
	original := LayerSequence{
		IDs:           []string{"layer3", "layer2", "layer1", "base"},
		IsNewestFirst: true,
	}

	reversed := original.Reverse()

	// Check order changed
	if reversed.IsNewestFirst != false {
		t.Errorf("reversed.IsNewestFirst = %v, want false", reversed.IsNewestFirst)
	}

	// Check IDs reversed
	expected := []string{"base", "layer1", "layer2", "layer3"}
	if len(reversed.IDs) != len(expected) {
		t.Fatalf("reversed.IDs length = %d, want %d", len(reversed.IDs), len(expected))
	}
	for i, id := range reversed.IDs {
		if id != expected[i] {
			t.Errorf("reversed.IDs[%d] = %q, want %q", i, id, expected[i])
		}
	}

	// Original should be unchanged
	if original.IsNewestFirst != true {
		t.Error("original.IsNewestFirst should be unchanged")
	}
	if original.IDs[0] != "layer3" {
		t.Error("original.IDs should be unchanged")
	}
}

func TestLayerSequenceDoubleReverse(t *testing.T) {
	original := LayerSequence{
		IDs:           []string{"a", "b", "c"},
		IsNewestFirst: true,
	}

	doubleReversed := original.Reverse().Reverse()

	if doubleReversed.IsNewestFirst != original.IsNewestFirst {
		t.Errorf("double reverse IsNewestFirst = %v, want %v", doubleReversed.IsNewestFirst, original.IsNewestFirst)
	}

	for i, id := range doubleReversed.IDs {
		if id != original.IDs[i] {
			t.Errorf("double reverse IDs[%d] = %q, want %q", i, id, original.IDs[i])
		}
	}
}

func TestLayerSequenceLen(t *testing.T) {
	empty := LayerSequence{}
	if empty.Len() != 0 {
		t.Errorf("empty.Len() = %d, want 0", empty.Len())
	}

	three := NewNewestFirst([]string{"a", "b", "c"})
	if three.Len() != 3 {
		t.Errorf("three.Len() = %d, want 3", three.Len())
	}
}

func TestLayerSequenceIsEmpty(t *testing.T) {
	empty := LayerSequence{}
	if !empty.IsEmpty() {
		t.Error("empty sequence should be empty")
	}

	nonEmpty := NewNewestFirst([]string{"a"})
	if nonEmpty.IsEmpty() {
		t.Error("non-empty sequence should not be empty")
	}
}

func TestNewNewestFirst(t *testing.T) {
	ids := []string{"c", "b", "a"}
	seq := NewNewestFirst(ids)

	if !seq.IsNewestFirst {
		t.Error("NewNewestFirst should set IsNewestFirst=true")
	}
	if len(seq.IDs) != 3 {
		t.Errorf("IDs length = %d, want 3", len(seq.IDs))
	}
}

func TestLayerSequenceCopyOnReverse(t *testing.T) {
	original := NewNewestFirst([]string{"a", "b", "c"})
	result := original.Reverse()

	// Modify result
	result.IDs[0] = "modified"

	// Original should be unchanged
	if original.IDs[0] != "a" {
		t.Error("original was modified when result was changed")
	}
}
