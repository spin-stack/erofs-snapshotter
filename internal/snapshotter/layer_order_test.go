package snapshotter

import (
	"testing"
)

func TestReverseStrings(t *testing.T) {
	tests := []struct {
		name     string
		input    []string
		expected []string
	}{
		{
			name:     "normal order",
			input:    []string{"layer3", "layer2", "layer1", "base"},
			expected: []string{"base", "layer1", "layer2", "layer3"},
		},
		{
			name:     "empty slice",
			input:    []string{},
			expected: nil,
		},
		{
			name:     "nil slice",
			input:    nil,
			expected: nil,
		},
		{
			name:     "single element",
			input:    []string{"only"},
			expected: []string{"only"},
		},
		{
			name:     "two elements",
			input:    []string{"a", "b"},
			expected: []string{"b", "a"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := reverseStrings(tt.input)

			if len(result) != len(tt.expected) {
				t.Fatalf("len(result) = %d, want %d", len(result), len(tt.expected))
			}

			for i := range result {
				if result[i] != tt.expected[i] {
					t.Errorf("result[%d] = %q, want %q", i, result[i], tt.expected[i])
				}
			}
		})
	}
}

func TestReverseStringsCopyBehavior(t *testing.T) {
	original := []string{"a", "b", "c"}
	result := reverseStrings(original)

	// Modify result
	result[0] = "modified"

	// Original should be unchanged
	if original[0] != "a" {
		t.Error("original was modified when result was changed")
	}
}

func TestReverseStringsDoubleReverse(t *testing.T) {
	original := []string{"a", "b", "c"}
	doubleReversed := reverseStrings(reverseStrings(original))

	for i, id := range doubleReversed {
		if id != original[i] {
			t.Errorf("double reverse [%d] = %q, want %q", i, id, original[i])
		}
	}
}
