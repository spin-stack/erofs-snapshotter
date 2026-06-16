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

	"github.com/spin-stack/erofs-snapshotter/internal/erofs"
	"github.com/spin-stack/erofs-snapshotter/internal/stringutil"
)

// SnapshotterConfig is used to configure the erofs snapshotter instance
type SnapshotterConfig struct {
	// setImmutable enables IMMUTABLE_FL file attribute for EROFS layers
	setImmutable bool
	// defaultSize is the size in bytes of the ext4 writable layer (must be > 0)
	defaultSize int64
}

// Opt is an option to configure the erofs snapshotter
type Opt func(config *SnapshotterConfig)

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
	setImmutable    bool
	defaultWritable int64

	// bgWg tracks background operations (fsmeta generation) for clean shutdown.
	bgWg sync.WaitGroup
	// bgSem bounds concurrent background fsmeta generations: each one runs an
	// mkfs.erofs process reading every blob in a chain, so an unbounded burst
	// of Prepares would cause a CPU/IO storm.
	bgSem chan struct{}
	// bgCtx is the service-lifetime context for background fsmeta generation;
	// bgCancel aborts in-flight generations so Close doesn't block on them.
	bgCtx    context.Context //nolint:containedctx // service-lifetime context, cancelled in Close
	bgCancel context.CancelFunc
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

// quiescedLabel is set by a cooperating VM runtime (qemubox) on a Commit call
// to assert that the container's writable layer is already quiesced - the VM
// is paused and its filesystems are frozen (FIFREEZE) - so commit may read the
// rwlayer image directly without taking the exclusive image lock.
//
// QEMU keeps its own image lock held even while paused, so the default lock
// gate cannot distinguish a frozen VM from a running one and rejects the
// commit ("rwlayer.img is locked by the VM"). This label is how the runtime
// signals "it is safe to read now". Setting it without an actual freeze risks
// a torn read of a live filesystem; see mountutils.WithoutImageLock.
const quiescedLabel = "containerd.io/snapshot/erofs.quiesced"

// NewSnapshotter returns a Snapshotter which uses EROFS+OverlayFS. The layers
// are stored under the provided root. A metadata file is stored under the root.
func NewSnapshotter(root string, opts ...Opt) (snapshots.Snapshotter, error) {
	config := SnapshotterConfig{
		defaultSize: defaultWritableSize,
	}
	for _, opt := range opts {
		opt(&config)
	}

	if err := os.MkdirAll(root, 0o700); err != nil {
		return nil, fmt.Errorf("create root directory %q: %w", root, err)
	}

	if config.defaultSize <= 0 {
		return nil, fmt.Errorf("default_writable_size must be > 0, got %d", config.defaultSize)
	}

	if err := checkCompatibility(root); err != nil {
		return nil, fmt.Errorf("compatibility check for %q: %w", root, err)
	}

	if config.setImmutable && runtime.GOOS != "linux" {
		return nil, fmt.Errorf("setting IMMUTABLE_FL is only supported on Linux")
	}

	ms, err := storage.NewMetaStore(filepath.Join(root, "metadata.db"))
	if err != nil {
		return nil, fmt.Errorf("create metadata store: %w", err)
	}

	if err := os.Mkdir(filepath.Join(root, snapshotsDirName), 0o700); err != nil && !os.IsExist(err) {
		return nil, fmt.Errorf("create snapshots directory: %w", err)
	}

	bgCtx, bgCancel := context.WithCancel(context.Background())
	s := &snapshotter{
		root:            root,
		ms:              ms,
		setImmutable:    config.setImmutable,
		defaultWritable: config.defaultSize,
		bgSem:           make(chan struct{}, fsmetaConcurrency()),
		bgCtx:           bgCtx,
		bgCancel:        bgCancel,
	}

	// Clean up any orphaned mounts from previous runs.
	s.cleanupOrphanedMounts() //nolint:contextcheck // startup cleanup uses background context
	s.cleanupFsmetaArtifacts()

	return s, nil
}

// fsmetaConcurrency returns the maximum number of concurrent background
// fsmeta generations (mkfs.erofs processes).
func fsmetaConcurrency() int {
	return min(max(runtime.GOMAXPROCS(0)/2, 1), 4)
}

// Close releases all resources held by the snapshotter.
// It cancels in-flight background operations (fsmeta generation) and waits
// for them to finish; an interrupted generation only leaves .tmp/.lock files
// behind, which are cleaned up on the next startup.
func (s *snapshotter) Close() error {
	s.bgCancel()
	s.bgWg.Wait()
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

	if err := os.Mkdir(filepath.Join(td, fsDirName), 0o755); err != nil {
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
