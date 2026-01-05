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

package erofs

import (
	"context"
	"fmt"
	"io"
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

	"github.com/aledbf/nexuserofs/internal/cleanup"
	"github.com/aledbf/nexuserofs/internal/mountutils"
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

// writeAndCommitDiff handles the common logic for writing a diff to the content store.
// It manages compression, content writer lifecycle, and label updates.
func (s *ErofsDiff) writeAndCommitDiff(ctx context.Context, config diff.Config, writeFn diffWriteFunc) (ocispec.Descriptor, error) {
	var compressionType compression.Compression
	switch config.MediaType {
	case ocispec.MediaTypeImageLayer:
		compressionType = compression.Uncompressed
	case ocispec.MediaTypeImageLayerGzip:
		compressionType = compression.Gzip
	case ocispec.MediaTypeImageLayerZstd:
		compressionType = compression.Zstd
	default:
		return ocispec.Descriptor{}, fmt.Errorf("unsupported diff media type: %v: %w", config.MediaType, errdefs.ErrNotImplemented)
	}

	var newReference bool
	if config.Reference == "" {
		newReference = true
		config.Reference = mountutils.UniqueRef()
	}

	cw, err := s.store.Writer(ctx,
		content.WithRef(config.Reference),
		content.WithDescriptor(ocispec.Descriptor{
			MediaType: config.MediaType,
		}))
	if err != nil {
		return ocispec.Descriptor{}, fmt.Errorf("failed to open writer: %w", err)
	}

	// errOpen is set when an error occurs while the content writer has not been
	// committed or closed yet to force a cleanup
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
		dgstr := digest.SHA256.Digester()
		var compressed io.WriteCloser
		if config.Compressor != nil {
			compressed, errOpen = config.Compressor(cw, config.MediaType)
			if errOpen != nil {
				return ocispec.Descriptor{}, fmt.Errorf("failed to get compressed stream: %w", errOpen)
			}
		} else {
			compressed, errOpen = compression.CompressStream(cw, compressionType)
			if errOpen != nil {
				return ocispec.Descriptor{}, fmt.Errorf("failed to get compressed stream: %w", errOpen)
			}
		}
		errOpen = writeFn(ctx, io.MultiWriter(compressed, dgstr.Hash()))
		compressed.Close()
		if errOpen != nil {
			return ocispec.Descriptor{}, fmt.Errorf("failed to write compressed diff: %w", errOpen)
		}

		if config.Labels == nil {
			config.Labels = map[string]string{}
		}
		config.Labels[labels.LabelUncompressed] = dgstr.Digest().String()
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
	// Set "containerd.io/uncompressed" label if digest already existed without label
	if _, ok := info.Labels[labels.LabelUncompressed]; !ok {
		info.Labels[labels.LabelUncompressed] = config.Labels[labels.LabelUncompressed]
		if _, err := s.store.Update(ctx, info, "labels."+labels.LabelUncompressed); err != nil {
			return ocispec.Descriptor{}, fmt.Errorf("error setting uncompressed label: %w", err)
		}
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
