package snapshotter

import (
	"sync"
	"testing"
)

// TestLayerSequenceConcurrentAccess verifies LayerSequence is safe for concurrent reads.
// LayerSequence is immutable by design - all mutating operations return copies.
func TestLayerSequenceConcurrentAccess(t *testing.T) {
	const numGoroutines = 100
	original := NewNewestFirst([]string{"layer5", "layer4", "layer3", "layer2", "layer1"})

	var wg sync.WaitGroup
	errors := make(chan string, numGoroutines*2)

	for range numGoroutines {
		wg.Add(2)

		// Concurrent Reverse
		go func() {
			defer wg.Done()
			result := original.Reverse()
			if result.IsNewestFirst {
				errors <- "Reverse should set IsNewestFirst=false"
			}
			if result.IDs[0] != "layer1" {
				errors <- "Reverse returned wrong first element"
			}
		}()

		// Concurrent Len
		go func() {
			defer wg.Done()
			if original.Len() != 5 {
				errors <- "Len returned wrong value"
			}
		}()
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Errorf("concurrent access error: %s", err)
	}

	// Verify original is unchanged after all operations
	if !original.IsNewestFirst {
		t.Error("original IsNewestFirst was modified")
	}
	if original.IDs[0] != "layer5" {
		t.Error("original IDs were modified")
	}
}

// TestLayerSequenceNoDataRace runs operations that would trigger race detector if unsafe.
func TestLayerSequenceNoDataRace(t *testing.T) {
	// Create sequence that will be accessed concurrently
	seq := NewNewestFirst([]string{"a", "b", "c", "d", "e"})

	var wg sync.WaitGroup

	// Mix of read operations
	for i := range 50 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			switch i % 3 {
			case 0:
				_ = seq.Len()
			case 1:
				_ = seq.IsEmpty()
			case 2:
				_ = seq.Reverse()
			}
		}(i)
	}

	wg.Wait()
}
