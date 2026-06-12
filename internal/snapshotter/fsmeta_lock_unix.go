//go:build unix

package snapshotter

import (
	"errors"
	"os"

	"golang.org/x/sys/unix"
)

// flockExclusiveNB attempts a non-blocking exclusive flock on f.
// Returns false (without error) when another holder owns the lock.
func flockExclusiveNB(f *os.File) (bool, error) {
	if err := unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		if errors.Is(err, unix.EWOULDBLOCK) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// flockSharedNB attempts a non-blocking shared flock on f.
// Returns false (without error) when an exclusive holder owns the lock.
func flockSharedNB(f *os.File) (bool, error) {
	if err := unix.Flock(int(f.Fd()), unix.LOCK_SH|unix.LOCK_NB); err != nil {
		if errors.Is(err, unix.EWOULDBLOCK) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}
