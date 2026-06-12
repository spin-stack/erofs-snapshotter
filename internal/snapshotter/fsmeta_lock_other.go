//go:build !unix

package snapshotter

import "os"

// Non-unix platforms have no flock; the snapshotter is not functional there
// (Linux-only mounts), so locking degenerates to always-acquire. This keeps
// the package compiling for cross-platform development builds.

func flockExclusiveNB(_ *os.File) (bool, error) { return true, nil }

func flockSharedNB(_ *os.File) (bool, error) { return true, nil }
