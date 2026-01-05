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
	"os"
	"path"
	"runtime"
	"strings"
	"time"

	"github.com/containerd/containerd/v2/core/content"
	"github.com/containerd/containerd/v2/core/diff"
	"github.com/containerd/containerd/v2/core/images"
	"github.com/containerd/containerd/v2/core/mount"
	"github.com/containerd/log"
	"github.com/google/uuid"
	digest "github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	erofsutils "github.com/aledbf/nexuserofs/internal/erofs"
)

// MountManagerResolver is a function that resolves the mount manager lazily.
// This allows the differ to look up the mount manager when it's actually needed,
// avoiding plugin initialization order issues.
type MountManagerResolver func() mount.Manager

// ErofsDiff implements diff.Applier and diff.Comparer for EROFS layers.
type ErofsDiff struct {
	store         content.Store
	mkfsExtraOpts []string
	// enableTarIndex enables generating tar index for tar content
	// instead of fully converting the tar to EROFS format
	enableTarIndex bool
	mmResolver     MountManagerResolver
}

// DifferOpt is an option for configuring the erofs differ
type DifferOpt func(d *ErofsDiff)

// WithMkfsOptions sets extra options for mkfs.erofs
func WithMkfsOptions(opts []string) DifferOpt {
	return func(d *ErofsDiff) {
		d.mkfsExtraOpts = opts
	}
}

// WithTarIndexMode enables tar index mode for EROFS layers
func WithTarIndexMode() DifferOpt {
	return func(d *ErofsDiff) {
		d.enableTarIndex = true
	}
}

// WithMountManager sets the mount manager used to resolve formatted mounts.
// Use this when the mount manager is already available at initialization time.
// For plugin contexts where initialization order matters, use WithMountManagerResolver.
func WithMountManager(mm mount.Manager) DifferOpt {
	return func(d *ErofsDiff) {
		d.mmResolver = func() mount.Manager { return mm }
	}
}

// WithMountManagerResolver sets a resolver function for the mount manager.
// The resolver is called lazily when the mount manager is actually needed,
// allowing the differ to initialize before the mount manager plugin is available.
// Use this in plugin contexts where initialization order may vary.
func WithMountManagerResolver(resolver MountManagerResolver) DifferOpt {
	return func(d *ErofsDiff) {
		d.mmResolver = resolver
	}
}

// NewErofsDiffer creates a new EROFS differ with the provided options.
// The returned *ErofsDiff implements diff.Applier and diff.Comparer.
func NewErofsDiffer(store content.Store, opts ...DifferOpt) *ErofsDiff {
	d := &ErofsDiff{
		store: store,
	}

	// Apply all options
	for _, opt := range opts {
		opt(d)
	}

	// Add default block size on darwin if not already specified
	d.mkfsExtraOpts = addDefaultMkfsOpts(d.mkfsExtraOpts)

	return d
}

// A valid EROFS native layer media type should end with ".erofs".
//
// Please avoid using any +suffix to list the algorithms used inside EROFS
// blobs, since:
//   - Each EROFS layer can use multiple compression algorithms;
//   - The suffixes should only indicate the corresponding preprocessor for
//     `images.DiffCompression`.
//
// Since `images.DiffCompression` doesn't support arbitrary media types,
// disallow non-empty suffixes for now.
func isErofsMediaType(mt string) bool {
	mediaType, _, hasExt := strings.Cut(mt, "+")
	if hasExt {
		return false
	}
	return strings.HasSuffix(mediaType, ".erofs")
}

func (s *ErofsDiff) Apply(ctx context.Context, desc ocispec.Descriptor, mounts []mount.Mount, opts ...diff.ApplyOpt) (d ocispec.Descriptor, err error) {
	t1 := time.Now()
	defer func() {
		if err == nil {
			log.G(ctx).WithFields(log.Fields{
				"d":      time.Since(t1),
				"digest": desc.Digest,
				"size":   desc.Size,
				"media":  desc.MediaType,
			}).Debugf("diff applied")
		}
	}()

	native := false
	if isErofsMediaType(desc.MediaType) {
		native = true
	} else if _, err := images.DiffCompression(ctx, desc.MediaType); err != nil {
		return ocispec.Descriptor{}, fmt.Errorf("currently unsupported media type: %s", desc.MediaType)
	}

	var config diff.ApplyConfig
	for _, o := range opts {
		if err := o(ctx, desc, &config); err != nil {
			return ocispec.Descriptor{}, fmt.Errorf("failed to apply config opt: %w", err)
		}
	}

	layer, err := erofsutils.MountsToLayer(mounts)
	if err != nil {
		return ocispec.Descriptor{}, fmt.Errorf("MountsToLayer failed: %w", err)
	}

	ra, err := s.store.ReaderAt(ctx, desc)
	if err != nil {
		return ocispec.Descriptor{}, fmt.Errorf("failed to get reader from content store: %w", err)
	}
	defer ra.Close()

	layerBlobPath := path.Join(layer, erofsutils.LayerBlobFilename)
	if native {
		f, err := os.Create(layerBlobPath)
		if err != nil {
			return ocispec.Descriptor{}, err
		}
		_, err = io.Copy(f, content.NewReader(ra))
		f.Close()
		if err != nil {
			return ocispec.Descriptor{}, err
		}
		return desc, nil
	}

	processor := diff.NewProcessorChain(desc.MediaType, content.NewReader(ra))
	for {
		if processor, err = diff.GetProcessor(ctx, processor, config.ProcessorPayloads); err != nil {
			return ocispec.Descriptor{}, fmt.Errorf("failed to get stream processor for %s: %w", desc.MediaType, err)
		}
		if processor.MediaType() == ocispec.MediaTypeImageLayer {
			break
		}
	}
	defer processor.Close()

	digester := digest.Canonical.Digester()
	rc := &readCounter{
		r: io.TeeReader(processor, digester.Hash()),
	}

	// Choose between tar index or tar conversion mode
	if s.enableTarIndex {
		// Use the tar index method: generate tar index and append tar
		err = erofsutils.GenerateTarIndexAndAppendTar(ctx, rc, layerBlobPath, s.mkfsExtraOpts)
		if err != nil {
			return ocispec.Descriptor{}, fmt.Errorf("failed to generate tar index: %w", err)
		}
	} else {
		// Use the tar method: fully convert tar to EROFS
		u := uuid.NewSHA1(uuid.NameSpaceURL, []byte("erofs:blobs/"+desc.Digest))
		err = erofsutils.ConvertTarErofs(ctx, rc, layerBlobPath, u.String(), s.mkfsExtraOpts)
		if err != nil {
			return ocispec.Descriptor{}, fmt.Errorf("failed to convert tar to erofs: %w", err)
		}
	}

	// Read any trailing data
	if _, err := io.Copy(io.Discard, rc); err != nil {
		return ocispec.Descriptor{}, err
	}

	return ocispec.Descriptor{
		MediaType: ocispec.MediaTypeImageLayer,
		Size:      rc.count,
		Digest:    digester.Digest(),
	}, nil
}

// readCounter wraps an io.Reader and counts the total bytes read.
type readCounter struct {
	r     io.Reader
	count int64
}

func (rc *readCounter) Read(p []byte) (n int, err error) {
	n, err = rc.r.Read(p)
	rc.count += int64(n)
	return
}

// addDefaultMkfsOpts adds default options for mkfs.erofs
func addDefaultMkfsOpts(mkfsExtraOpts []string) []string {
	if runtime.GOOS != "darwin" {
		return mkfsExtraOpts
	}

	// Check if -b argument is already present
	for _, opt := range mkfsExtraOpts {
		if strings.HasPrefix(opt, "-b") {
			return mkfsExtraOpts
		}
	}

	// Add -b4096 as the first option to prevent unusable block
	// size from being used on macOS.
	return append([]string{"-b4096"}, mkfsExtraOpts...)
}
