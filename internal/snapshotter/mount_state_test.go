package snapshotter

import (
	"sync"
	"testing"
)

func TestMountTrackerBasicOperations(t *testing.T) {
	tracker := NewMountTracker()

	// Initially not mounted
	if tracker.IsMounted("snap-1") {
		t.Error("initial state should be unmounted")
	}

	// Set mounted
	tracker.SetMounted("snap-1")
	if !tracker.IsMounted("snap-1") {
		t.Error("IsMounted should return true after SetMounted")
	}

	// Set unmounted
	tracker.SetUnmounted("snap-1")
	if tracker.IsMounted("snap-1") {
		t.Error("IsMounted should return false after SetUnmounted")
	}
}

func TestMountTrackerMultipleSnapshots(t *testing.T) {
	tracker := NewMountTracker()

	tracker.SetMounted("snap-1")
	tracker.SetMounted("snap-2")
	tracker.SetMounted("snap-3")

	if !tracker.IsMounted("snap-1") {
		t.Error("snap-1 should be mounted")
	}
	if !tracker.IsMounted("snap-2") {
		t.Error("snap-2 should be mounted")
	}
	if !tracker.IsMounted("snap-3") {
		t.Error("snap-3 should be mounted")
	}

	tracker.SetUnmounted("snap-2")

	if !tracker.IsMounted("snap-1") {
		t.Error("snap-1 should still be mounted")
	}
	if tracker.IsMounted("snap-2") {
		t.Error("snap-2 should be unmounted")
	}
	if !tracker.IsMounted("snap-3") {
		t.Error("snap-3 should still be mounted")
	}
}

func TestMountTrackerClear(t *testing.T) {
	tracker := NewMountTracker()

	tracker.SetMounted("snap-1")
	tracker.SetMounted("snap-2")

	if !tracker.IsMounted("snap-1") || !tracker.IsMounted("snap-2") {
		t.Error("snapshots should be mounted before clear")
	}

	tracker.Clear()

	if tracker.IsMounted("snap-1") {
		t.Error("snap-1 should not be mounted after clear")
	}
	if tracker.IsMounted("snap-2") {
		t.Error("snap-2 should not be mounted after clear")
	}

	// New operations should work after clear
	tracker.SetMounted("snap-3")
	if !tracker.IsMounted("snap-3") {
		t.Error("should be able to mount after clear")
	}
}

func TestMountTrackerConcurrentAccess(t *testing.T) {
	tracker := NewMountTracker()
	const numGoroutines = 100

	var wg sync.WaitGroup

	// Concurrent mounts
	for i := range numGoroutines {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			snapID := string(rune('a' + id%26))
			tracker.SetMounted(snapID)
			_ = tracker.IsMounted(snapID)
		}(i)
	}

	wg.Wait()

	// Concurrent unmounts
	for i := range numGoroutines {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			snapID := string(rune('a' + id%26))
			tracker.SetUnmounted(snapID)
			_ = tracker.IsMounted(snapID)
		}(i)
	}

	wg.Wait()
}

func TestMountTrackerIdempotent(t *testing.T) {
	tracker := NewMountTracker()

	// Multiple SetMounted calls should be idempotent
	tracker.SetMounted("snap-1")
	tracker.SetMounted("snap-1")
	tracker.SetMounted("snap-1")

	if !tracker.IsMounted("snap-1") {
		t.Error("snap-1 should be mounted")
	}

	// Multiple SetUnmounted calls should be idempotent
	tracker.SetUnmounted("snap-1")
	tracker.SetUnmounted("snap-1")
	tracker.SetUnmounted("snap-1")

	if tracker.IsMounted("snap-1") {
		t.Error("snap-1 should be unmounted")
	}
}
