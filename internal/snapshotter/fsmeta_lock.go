package snapshotter

import (
	"fmt"
	"os"
)

// lockSuffix is appended to the fsmeta path to form the generation lock file.
const lockSuffix = ".lock"

// fsmetaLock represents a held fsmeta generation lock. The lock is a kernel
// advisory lock (flock) on the .lock file, held for the entire generation:
// if the process crashes the kernel releases it automatically, so stale
// locks cannot occur and no age-based recovery - with its inherent TOCTOU
// between the staleness check and the removal - is needed.
//
// The lock FILE is never unlinked while the snapshotter runs: removing it
// would let a new inode be created and locked while an old holder still
// holds the unlinked one, reintroducing the two-winners race. Leftover lock
// files are removed by cleanupFsmetaArtifacts during single-threaded startup
// and disappear with the snapshot directory on Remove.
type fsmetaLock struct {
	f *os.File
}

// release drops the lock. Safe to call exactly once.
func (l *fsmetaLock) release() {
	_ = l.f.Close() // closing the descriptor releases the flock
}

// acquireFsmetaLock attempts to take the fsmeta generation lock for the
// chain rooted at parentID without blocking. It returns (nil, nil) when the
// fsmeta already exists or another holder is currently generating it.
func (s *snapshotter) acquireFsmetaLock(parentID string) (*fsmetaLock, error) {
	mergedMeta := s.fsMetaPath(parentID)
	if _, err := os.Stat(mergedMeta); err == nil {
		return nil, nil
	}

	f, err := os.OpenFile(mergedMeta+lockSuffix, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open fsmeta lock: %w", err)
	}
	acquired, err := flockExclusiveNB(f)
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("acquire fsmeta lock: %w", err)
	}
	if !acquired {
		_ = f.Close()
		return nil, nil
	}

	// Re-check under the lock: the previous holder may have just finished.
	if _, err := os.Stat(mergedMeta); err == nil {
		_ = f.Close()
		return nil, nil
	}
	return &fsmetaLock{f: f}, nil
}

// fsmetaGenerationInProgress reports whether a generation currently holds
// the fsmeta lock for the given parent snapshot. A lock file without a
// holder (e.g. after a crash) does NOT count as in progress.
func (s *snapshotter) fsmetaGenerationInProgress(parentID string) bool {
	f, err := os.Open(s.fsMetaPath(parentID) + lockSuffix)
	if err != nil {
		return false
	}
	defer f.Close()

	acquired, err := flockSharedNB(f)
	if err != nil {
		return false
	}
	// A shared lock succeeds unless an exclusive holder is active.
	return !acquired
}
