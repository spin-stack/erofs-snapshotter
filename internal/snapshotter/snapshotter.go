/*
   Copyright The containerd Authors.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package snapshotter

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync"

	"github.com/containerd/containerd/v2/core/snapshots"
	"github.com/containerd/containerd/v2/core/snapshots/storage"
	"github.com/containerd/log"
	"github.com/moby/sys/mountinfo"

	"github.com/aledbf/nexus-erofs/internal/erofs"
	"github.com/aledbf/nexus-erofs/internal/fsverity"
	"github.com/aledbf/nexus-erofs/internal/stringutil"
)

// SnapshotterConfig is used to configure the erofs snapshotter instance
type SnapshotterConfig struct {
	// enableFsverity enables fsverity for EROFS layers
	enableFsverity bool
	// setImmutable enables IMMUTABLE_FL file attribute for EROFS layers
	setImmutable bool
	// defaultSize is the size in bytes of the ext4 writable layer (must be > 0)
	defaultSize int64
}

// Opt is an option to configure the erofs snapshotter
type Opt func(config *SnapshotterConfig)

// WithFsverity enables fsverity for EROFS layers
func WithFsverity() Opt {
	return func(config *SnapshotterConfig) {
		config.enableFsverity = true
	}
}

// WithImmutable enables IMMUTABLE_FL file attribute for EROFS layers
func WithImmutable() Opt {
	return func(config *SnapshotterConfig) {
		config.setImmutable = true
	}
}

// WithDefaultSize sets the size of the ext4 writable layer for active snapshots.
// Size must be > 0. The writable layer is an ext4 image that is loop-mounted.
func WithDefaultSize(size int64) Opt {
	return func(config *SnapshotterConfig) {
		config.defaultSize = size
	}
}

type snapshotter struct {
	root            string
	ms              *storage.MetaStore
	enableFsverity  bool
	setImmutable    bool
	defaultWritable int64

	// bgWg tracks background operations (fsmeta generation) for clean shutdown.
	bgWg sync.WaitGroup
}

// isMounted checks if a path is currently mounted.
// Returns false if the path doesn't exist or on any error.
func isMounted(target string) bool {
	if _, err := os.Stat(target); err != nil {
		return false
	}
	mounted, err := mountinfo.Mounted(target)
	if err != nil {
		return false
	}
	return mounted
}

// extractLabel is the label key used to mark snapshots for layer extraction.
// This is stored in the snapshot metadata for atomic reads within transactions,
// avoiding TOCTOU race conditions that would occur with filesystem markers.
const extractLabel = "containerd.io/snapshot/erofs.extract"

// NewSnapshotter returns a Snapshotter which uses EROFS+OverlayFS. The layers
// are stored under the provided root. A metadata file is stored under the root.
func NewSnapshotter(root string, opts ...Opt) (snapshots.Snapshotter, error) {
	config := SnapshotterConfig{
		defaultSize: defaultWritableSize,
	}
	for _, opt := range opts {
		opt(&config)
	}

	if err := os.MkdirAll(root, 0700); err != nil {
		return nil, fmt.Errorf("create root directory %q: %w", root, err)
	}

	if config.defaultSize <= 0 {
		return nil, fmt.Errorf("default_writable_size must be > 0, got %d", config.defaultSize)
	}

	if err := checkCompatibility(root); err != nil {
		return nil, fmt.Errorf("compatibility check for %q: %w", root, err)
	}

	// Check fsverity support if enabled
	if config.enableFsverity {
		supported, err := fsverity.IsSupported(root)
		if err != nil {
			return nil, fmt.Errorf("check fsverity support on %q: %w", root, err)
		}
		if !supported {
			return nil, fmt.Errorf("fsverity is not supported on the filesystem of %q", root)
		}
	}

	if config.setImmutable && runtime.GOOS != "linux" {
		return nil, fmt.Errorf("setting IMMUTABLE_FL is only supported on Linux")
	}

	ms, err := storage.NewMetaStore(filepath.Join(root, "metadata.db"))
	if err != nil {
		return nil, fmt.Errorf("create metadata store: %w", err)
	}

	if err := os.Mkdir(filepath.Join(root, snapshotsDirName), 0700); err != nil && !os.IsExist(err) {
		return nil, fmt.Errorf("create snapshots directory: %w", err)
	}

	s := &snapshotter{
		root:            root,
		ms:              ms,
		enableFsverity:  config.enableFsverity,
		setImmutable:    config.setImmutable,
		defaultWritable: config.defaultSize,
	}

	// Clean up any orphaned mounts from previous runs.
	s.cleanupOrphanedMounts() //nolint:contextcheck // startup cleanup uses background context

	return s, nil
}

// Close releases all resources held by the snapshotter.
// It waits for any background operations (fsmeta generation) to complete.
func (s *snapshotter) Close() error {
	s.bgWg.Wait() // Wait for background operations to complete
	s.cleanupBlockMounts()
	return s.ms.Close()
}

// cleanupBlockMounts unmounts any ext4 rw mounts used during conversion.
// Errors are logged but not returned since this is best-effort cleanup.
func (s *snapshotter) cleanupBlockMounts() {
	entries, err := os.ReadDir(s.snapshotsDir())
	if err != nil {
		return
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		rwDir := filepath.Join(s.snapshotsDir(), entry.Name(), rwDirName)
		if err := unmountAll(rwDir); err != nil {
			log.L.WithError(err).WithField("path", rwDir).Debug("failed to cleanup block rw mount during close")
		}
	}
}

// prepareDirectory creates a temporary snapshot directory with proper structure.
func (s *snapshotter) prepareDirectory(snapshotDir string, kind snapshots.Kind) (string, error) {
	td, err := os.MkdirTemp(snapshotDir, "new-")
	if err != nil {
		return "", fmt.Errorf("create temp dir: %w", err)
	}

	if err := os.Mkdir(filepath.Join(td, fsDirName), 0755); err != nil {
		return td, err
	}
	if kind == snapshots.KindActive {
		if err := ensureMarkerFile(filepath.Join(td, erofs.ErofsLayerMarker)); err != nil {
			return td, err
		}
	}

	return td, nil
}

// createWritableLayer creates and formats an ext4 filesystem image file.
func (s *snapshotter) createWritableLayer(ctx context.Context, id string) error {
	path := s.writablePath(id)
	size := s.defaultWritable

	// Create sparse file
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create writable layer file: %w", err)
	}

	if err := f.Truncate(size); err != nil {
		f.Close()
		os.Remove(path)
		return fmt.Errorf("allocate writable layer: %w", err)
	}
	f.Close()

	// Format as ext4 directly on the file.
	cmd := exec.CommandContext(ctx, "mkfs.ext4", "-q", "-F", "-L", "rwlayer",
		"-E", "nodiscard,lazy_itable_init=1,lazy_journal_init=1", path)
	if out, err := cmd.CombinedOutput(); err != nil {
		os.Remove(path)
		return fmt.Errorf("format ext4: %w: %s", err, stringutil.TruncateOutput(out, 256))
	}

	log.G(ctx).WithField("path", path).WithField("size", size).Debug("created writable layer")
	return nil
}
