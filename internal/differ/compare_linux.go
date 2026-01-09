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

package differ

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/containerd/containerd/v2/core/content"
	"github.com/containerd/containerd/v2/core/diff"
	"github.com/containerd/containerd/v2/core/mount"
	"github.com/containerd/containerd/v2/pkg/archive"
	"github.com/containerd/containerd/v2/pkg/archive/compression"
	"github.com/containerd/containerd/v2/pkg/epoch"
	"github.com/containerd/containerd/v2/pkg/labels"
	"github.com/containerd/errdefs"
	"github.com/containerd/log"
	digest "github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"golang.org/x/sys/unix"

	"github.com/spin-stack/erofs-snapshotter/internal/cleanup"
	"github.com/spin-stack/erofs-snapshotter/internal/mountutils"
)

// diffWriteFunc is a function that writes diff content to the provided writer.
type diffWriteFunc func(ctx context.Context, w io.Writer) error

func writeDiffFromMounts(ctx context.Context, w io.Writer, lower, upper []mount.Mount, mm mount.Manager) error {
	return withLowerMount(ctx, lower, mm, func(lowerRoot string) error {
		return withUpperMount(ctx, upper, mm, func(upperRoot string) error {
			if err := archive.WriteDiff(ctx, w, lowerRoot, upperRoot); err != nil {
				return fmt.Errorf("failed to write diff: %w", err)
			}
			return nil
		})
	})
}

// mountManager resolves and returns the mount manager.
// Returns nil if no resolver is configured.
func (s *ErofsDiff) mountManager() mount.Manager {
	if s.mmResolver == nil {
		return nil
	}
	return s.mmResolver()
}

// Compare creates a diff between the given mounts and uploads the result
// to the content store.
//
// This function uses the mount manager to activate both lower and upper mounts,
// then computes the diff between them. The mount manager handles all mount
// resolution including templates, formatted mounts, and loop device setup.
//
// If the mount manager is not configured but mounts require resolution,
// Compare returns an error with "mount manager is required".
func (s *ErofsDiff) Compare(ctx context.Context, lower, upper []mount.Mount, opts ...diff.Opt) (d ocispec.Descriptor, err error) {
	var config diff.Config
	for _, opt := range opts {
		if err := opt(&config); err != nil {
			return ocispec.Descriptor{}, err
		}
	}
	if tm := epoch.FromContext(ctx); tm != nil && config.SourceDateEpoch == nil {
		config.SourceDateEpoch = tm
	}

	if config.MediaType == "" {
		config.MediaType = ocispec.MediaTypeImageLayerGzip
	}

	// Resolve mount manager lazily - this allows initialization before
	// the mount manager plugin is available
	mm := s.mountManager()

	return s.writeAndCommitDiff(ctx, config, func(ctx context.Context, w io.Writer) error {
		return writeDiffFromMounts(ctx, w, lower, upper, mm)
	})
}

// compressionTypeFromMediaType returns the compression type for a media type.
func compressionTypeFromMediaType(mediaType string) (compression.Compression, error) {
	switch mediaType {
	case ocispec.MediaTypeImageLayer:
		return compression.Uncompressed, nil
	case ocispec.MediaTypeImageLayerGzip:
		return compression.Gzip, nil
	case ocispec.MediaTypeImageLayerZstd:
		return compression.Zstd, nil
	default:
		return compression.Uncompressed, fmt.Errorf("unsupported diff media type: %v: %w", mediaType, errdefs.ErrNotImplemented)
	}
}

// writeCompressedDiff writes a compressed diff and returns the uncompressed digest.
func writeCompressedDiff(ctx context.Context, cw content.Writer, config diff.Config, compressionType compression.Compression, writeFn diffWriteFunc) (string, error) {
	dgstr := digest.SHA256.Digester()
	var compressed io.WriteCloser
	var err error

	if config.Compressor != nil {
		compressed, err = config.Compressor(cw, config.MediaType)
	} else {
		compressed, err = compression.CompressStream(cw, compressionType)
	}
	if err != nil {
		return "", fmt.Errorf("failed to get compressed stream: %w", err)
	}

	err = writeFn(ctx, io.MultiWriter(compressed, dgstr.Hash()))
	compressed.Close()
	if err != nil {
		return "", fmt.Errorf("failed to write compressed diff: %w", err)
	}

	return dgstr.Digest().String(), nil
}

// ensureUncompressedLabel ensures the uncompressed label is set on content info.
func (s *ErofsDiff) ensureUncompressedLabel(ctx context.Context, info content.Info, uncompressedDigest string) error {
	if _, ok := info.Labels[labels.LabelUncompressed]; ok {
		return nil
	}
	info.Labels[labels.LabelUncompressed] = uncompressedDigest
	_, err := s.store.Update(ctx, info, "labels."+labels.LabelUncompressed)
	if err != nil {
		return fmt.Errorf("error setting uncompressed label: %w", err)
	}
	return nil
}

// writeAndCommitDiff handles the common logic for writing a diff to the content store.
// It manages compression, content writer lifecycle, and label updates.
func (s *ErofsDiff) writeAndCommitDiff(ctx context.Context, config diff.Config, writeFn diffWriteFunc) (ocispec.Descriptor, error) {
	compressionType, err := compressionTypeFromMediaType(config.MediaType)
	if err != nil {
		return ocispec.Descriptor{}, err
	}

	newReference := config.Reference == ""
	if newReference {
		config.Reference = mountutils.UniqueRef()
	}

	cw, err := s.store.Writer(ctx,
		content.WithRef(config.Reference),
		content.WithDescriptor(ocispec.Descriptor{MediaType: config.MediaType}))
	if err != nil {
		return ocispec.Descriptor{}, fmt.Errorf("failed to open writer: %w", err)
	}

	var errOpen error
	defer func() {
		if errOpen != nil {
			cw.Close()
			if newReference {
				if abortErr := s.store.Abort(ctx, config.Reference); abortErr != nil {
					log.G(ctx).WithError(abortErr).WithField("ref", config.Reference).Warnf("failed to delete diff upload")
				}
			}
		}
	}()

	if !newReference {
		if errOpen = cw.Truncate(0); errOpen != nil {
			return ocispec.Descriptor{}, errOpen
		}
	}

	if compressionType != compression.Uncompressed {
		uncompressedDigest, werr := writeCompressedDiff(ctx, cw, config, compressionType, writeFn)
		if werr != nil {
			errOpen = werr
			return ocispec.Descriptor{}, werr
		}
		if config.Labels == nil {
			config.Labels = map[string]string{}
		}
		config.Labels[labels.LabelUncompressed] = uncompressedDigest
	} else {
		if errOpen = writeFn(ctx, cw); errOpen != nil {
			return ocispec.Descriptor{}, fmt.Errorf("failed to write diff: %w", errOpen)
		}
	}

	var commitopts []content.Opt
	if config.Labels != nil {
		commitopts = append(commitopts, content.WithLabels(config.Labels))
	}

	dgst := cw.Digest()
	if errOpen = cw.Commit(ctx, 0, dgst, commitopts...); errOpen != nil {
		if !errdefs.IsAlreadyExists(errOpen) {
			return ocispec.Descriptor{}, fmt.Errorf("failed to commit: %w", errOpen)
		}
		errOpen = nil
	}

	info, err := s.store.Info(ctx, dgst)
	if err != nil {
		return ocispec.Descriptor{}, fmt.Errorf("failed to get info from content store: %w", err)
	}
	if info.Labels == nil {
		info.Labels = make(map[string]string)
	}

	if err := s.ensureUncompressedLabel(ctx, info, config.Labels[labels.LabelUncompressed]); err != nil {
		return ocispec.Descriptor{}, err
	}

	return ocispec.Descriptor{
		MediaType: config.MediaType,
		Size:      info.Size,
		Digest:    info.Digest,
	}, nil
}

// withLowerMount resolves lower mounts and calls f with the resulting root path.
// If mounts require the mount manager (formatted mounts, templates, or EROFS),
// it activates them through the mount manager first.
func withLowerMount(ctx context.Context, lower []mount.Mount, mm mount.Manager, f func(root string) error) error {
	// Handle EROFS multi-device mounts directly - the containerd mount manager
	// cannot handle EROFS with device= options (fsmeta multi-device).
	if mountutils.HasErofsMultiDevice(lower) {
		return withErofsTempMount(ctx, lower, f)
	}

	if mountutils.NeedsMountManager(lower) {
		if mm == nil {
			return fmt.Errorf("mount manager is required to resolve formatted mounts: %w", errdefs.ErrNotImplemented)
		}
		name := "erofs-diff-lower-" + mountutils.UniqueRef()
		temporary := !mountutils.NeedsNonTemporaryActivation(lower)
		var info mount.ActivationInfo
		var err error
		if temporary {
			info, err = mm.Activate(ctx, name, lower, mount.WithTemporary)
		} else {
			info, err = mm.Activate(ctx, name, lower)
		}
		if err != nil {
			return err
		}
		defer func() {
			// Use cleanup.Do for deactivation to ensure it runs even if the
			// parent context is cancelled, with a timeout to prevent blocking.
			cleanup.Do(ctx, func(cleanupCtx context.Context) {
				if derr := mm.Deactivate(cleanupCtx, name); derr != nil {
					log.G(ctx).WithError(derr).Warnf("failed to deactivate lower mount %s", name)
				}
			})
		}()
		// Shortcut: if the result is a single bind mount, use the source directly
		if len(info.System) == 1 && mountutils.TypeSuffix(info.System[0].Type) == "bind" && info.System[0].Source != "" {
			return f(info.System[0].Source)
		}
		// Shortcut: if we have a merged EROFS and a lower-only overlay, use the EROFS mount point
		if root, ok := mergedLowerFromActive(info.Active); ok && lowerOverlayOnly(info.System) {
			return f(root)
		}
		return mount.WithTempMount(ctx, info.System, f)
	}
	return mount.WithTempMount(ctx, lower, f)
}

// withUpperMount resolves upper mounts and calls f with the resulting root path.
// If mounts require the mount manager (formatted mounts, templates, or EROFS),
// it activates them through the mount manager first.
func withUpperMount(ctx context.Context, upper []mount.Mount, mm mount.Manager, f func(root string) error) error {
	// Handle active snapshot mounts (EROFS + ext4) - create overlay on host
	if mountutils.HasActiveSnapshotMounts(upper) {
		return withActiveSnapshotMount(ctx, upper, f)
	}

	// Handle EROFS multi-device mounts directly - the containerd mount manager
	// cannot handle EROFS with device= options (fsmeta multi-device).
	if mountutils.HasErofsMultiDevice(upper) {
		return withErofsTempMount(ctx, upper, f)
	}

	if mountutils.NeedsMountManager(upper) {
		if mm == nil {
			return fmt.Errorf("mount manager is required to resolve formatted mounts: %w", errdefs.ErrNotImplemented)
		}
		name := "erofs-diff-upper-" + mountutils.UniqueRef()
		temporary := !mountutils.NeedsNonTemporaryActivation(upper)
		var info mount.ActivationInfo
		var err error
		if temporary {
			info, err = mm.Activate(ctx, name, upper, mount.WithTemporary)
		} else {
			info, err = mm.Activate(ctx, name, upper)
		}
		if err != nil {
			return err
		}
		defer func() {
			// Use cleanup.Do for deactivation to ensure it runs even if the
			// parent context is cancelled, with a timeout to prevent blocking.
			cleanup.Do(ctx, func(cleanupCtx context.Context) {
				if derr := mm.Deactivate(cleanupCtx, name); derr != nil {
					log.G(ctx).WithError(derr).Warnf("failed to deactivate upper mount %s", name)
				}
			})
		}()
		// Shortcut: if the result is a single bind mount, use the source directly
		if len(info.System) == 1 && mountutils.TypeSuffix(info.System[0].Type) == "bind" && info.System[0].Source != "" {
			return f(info.System[0].Source)
		}
		return mount.WithReadonlyTempMount(ctx, info.System, f)
	}
	return mount.WithReadonlyTempMount(ctx, upper, f)
}

// withActiveSnapshotMount handles active snapshot mounts (EROFS + ext4) by creating
// an overlay on the host. The EROFS layers form the lowerdir, and the ext4's /upper
// forms the upperdir. This allows Compare to see the changes made in the container.
func withActiveSnapshotMount(ctx context.Context, mounts []mount.Mount, f func(root string) error) error {
	// Separate EROFS and ext4 mounts
	var erofsMounts []mount.Mount
	var ext4Mount *mount.Mount
	for i := range mounts {
		m := &mounts[i]
		switch mountutils.TypeSuffix(m.Type) {
		case "erofs":
			erofsMounts = append(erofsMounts, *m)
		case "ext4":
			ext4Mount = m
		}
	}

	if ext4Mount == nil {
		return fmt.Errorf("active snapshot mount missing ext4 writable layer")
	}

	// Create temp directories for mounting
	tempBase, err := os.MkdirTemp("", "erofs-active-")
	if err != nil {
		return fmt.Errorf("failed to create temp dir: %w", err)
	}
	defer os.RemoveAll(tempBase)

	erofsDir := filepath.Join(tempBase, "erofs")
	ext4Dir := filepath.Join(tempBase, "ext4")
	overlayDir := filepath.Join(tempBase, "overlay")

	for _, d := range []string{erofsDir, ext4Dir, overlayDir} {
		if err := os.MkdirAll(d, 0755); err != nil {
			return fmt.Errorf("failed to create dir %s: %w", d, err)
		}
	}

	// Mount EROFS layers
	erofsCleanup, err := mountutils.MountAll(erofsMounts, erofsDir)
	if err != nil {
		return fmt.Errorf("failed to mount EROFS: %w", err)
	}
	defer func() {
		if cerr := erofsCleanup(); cerr != nil {
			log.G(ctx).WithError(cerr).Warn("failed to cleanup EROFS mount")
		}
	}()

	// Mount ext4 writable layer
	ext4Cleanup, err := mountutils.MountExt4(ext4Mount.Source, ext4Dir)
	if err != nil {
		return fmt.Errorf("failed to mount ext4: %w", err)
	}
	defer func() {
		if cerr := ext4Cleanup(); cerr != nil {
			log.G(ctx).WithError(cerr).Warn("failed to cleanup ext4 mount")
		}
	}()

	// The ext4 contains /upper and /work for overlay at its root.
	// Note: The "rw" in blockRwMountPath is the HOST mount point, not a directory inside ext4.
	upperDir := filepath.Join(ext4Dir, "upper")
	workDir := filepath.Join(ext4Dir, "work")

	// Ensure directories exist (they should from VM usage)
	if _, err := os.Stat(upperDir); err != nil {
		// If upper doesn't exist, the container had no changes
		log.G(ctx).Debug("ext4 upper directory doesn't exist, using empty overlay")
		if err := os.MkdirAll(upperDir, 0755); err != nil {
			return fmt.Errorf("failed to create upper dir: %w", err)
		}
	}
	if _, err := os.Stat(workDir); err != nil {
		if err := os.MkdirAll(workDir, 0755); err != nil {
			return fmt.Errorf("failed to create work dir: %w", err)
		}
	}

	// Create overlay mount
	overlayOpts := fmt.Sprintf("lowerdir=%s,upperdir=%s,workdir=%s", erofsDir, upperDir, workDir)
	if err := unix.Mount("overlay", overlayDir, "overlay", 0, overlayOpts); err != nil {
		return fmt.Errorf("failed to mount overlay: %w", err)
	}
	defer func() {
		if err := unix.Unmount(overlayDir, 0); err != nil {
			log.G(ctx).WithError(err).Warn("failed to unmount overlay")
		}
	}()

	return f(overlayDir)
}

// withErofsTempMount mounts EROFS mounts (including multi-device fsmeta) to a
// temporary directory and calls f with the mount root. This handles EROFS mounts
// that the containerd mount manager cannot handle.
func withErofsTempMount(ctx context.Context, mounts []mount.Mount, f func(root string) error) error {
	tempDir, err := os.MkdirTemp("", "erofs-diff-")
	if err != nil {
		return fmt.Errorf("failed to create temp dir: %w", err)
	}
	defer os.RemoveAll(tempDir)

	cleanup, err := mountutils.MountAll(mounts, tempDir)
	if err != nil {
		return fmt.Errorf("failed to mount EROFS: %w", err)
	}
	defer func() {
		if cerr := cleanup(); cerr != nil {
			log.G(ctx).WithError(cerr).Warn("failed to cleanup EROFS mount")
		}
	}()

	return f(tempDir)
}

// lowerOverlayOnly returns true if the mounts represent an overlay with only
// lower directories (no upperdir). This indicates a read-only overlay that
// can be accessed directly through its lower mount point.
func lowerOverlayOnly(mounts []mount.Mount) bool {
	if len(mounts) != 1 {
		return false
	}
	if mountutils.TypeSuffix(mounts[0].Type) != "overlay" {
		return false
	}
	hasLower := false
	for _, opt := range mounts[0].Options {
		if strings.HasPrefix(opt, "upperdir=") {
			return false
		}
		if strings.HasPrefix(opt, "lowerdir=") {
			hasLower = true
		}
	}
	return hasLower
}

// mergedLowerFromActive finds the mount point of a merged EROFS filesystem
// from the list of active mounts. It searches backwards since the merged
// fsmeta mount is typically the last EROFS mount in the activation chain.
// Returns the mount point if an fsmeta.erofs source or a multi-device EROFS
// mount (with device= option) is found.
func mergedLowerFromActive(active []mount.ActiveMount) (string, bool) {
	for i := len(active) - 1; i >= 0; i-- {
		if mountutils.TypeSuffix(active[i].Type) != "erofs" {
			continue
		}
		if strings.HasSuffix(active[i].Source, "fsmeta.erofs") {
			return active[i].MountPoint, true
		}
		for _, opt := range active[i].Options {
			if strings.HasPrefix(opt, "device=") {
				return active[i].MountPoint, true
			}
		}
	}
	return "", false
}
