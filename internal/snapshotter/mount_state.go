package snapshotter

import (
	"sync"
)

// MountTracker provides thread-safe tracking of ext4 block mount states.
// It tracks which snapshots have ext4 images mounted on the host for
// extract operations (where the differ needs to write to the ext4).
//
// This is used instead of querying mountinfo on each operation because:
//   - mountinfo.Mounted() can fail on non-existent paths
//   - Explicit state avoids race conditions between check and mount
//
// On startup, cleanupOrphanedMounts() unmounts any leftover mounts,
// so the tracker starting empty is the correct initial state.
type MountTracker struct {
	mu     sync.RWMutex
	mounts map[string]bool
}

// NewMountTracker creates a new mount state tracker.
func NewMountTracker() *MountTracker {
	return &MountTracker{
		mounts: make(map[string]bool),
	}
}

// SetMounted marks a snapshot's ext4 as mounted.
func (t *MountTracker) SetMounted(id string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.mounts[id] = true
}

// SetUnmounted marks a snapshot's ext4 as unmounted.
func (t *MountTracker) SetUnmounted(id string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.mounts, id)
}

// IsMounted returns true if the snapshot's ext4 is currently mounted.
func (t *MountTracker) IsMounted(id string) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.mounts[id]
}

// Clear removes all tracked state. Used during shutdown.
func (t *MountTracker) Clear() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.mounts = make(map[string]bool)
}
