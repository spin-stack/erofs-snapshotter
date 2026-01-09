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

// Package store provides content store utilities for the spin-erofs snapshotter.
package store

import (
	"context"
	"fmt"

	containerd "github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/core/content"
	"github.com/containerd/containerd/v2/pkg/namespaces"
	"github.com/containerd/errdefs"
	digest "github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

// storeProvider is an internal interface for providing the underlying content store.
// This allows for testing without requiring a real containerd client.
type storeProvider interface {
	ContentStore() content.Store
}

// clientStoreProvider wraps a containerd client to implement storeProvider.
type clientStoreProvider struct {
	client *containerd.Client
}

func (p *clientStoreProvider) ContentStore() content.Store {
	return p.client.ContentStore()
}

// NamespaceAwareStore wraps a containerd client to provide a content.Store
// that extracts the namespace from each request's context.
//
// This is necessary for proxy plugins (like external snapshotters) where
// the containerd client is created with a default namespace, but incoming
// gRPC requests contain the actual namespace in the context metadata.
type NamespaceAwareStore struct {
	provider         storeProvider
	defaultNamespace string
}

// NewNamespaceAwareStore creates a new namespace-aware content store wrapper.
// The defaultNamespace is used when the context doesn't contain a namespace.
func NewNamespaceAwareStore(client *containerd.Client, defaultNamespace string) *NamespaceAwareStore {
	return &NamespaceAwareStore{
		provider:         &clientStoreProvider{client: client},
		defaultNamespace: defaultNamespace,
	}
}

// newNamespaceAwareStoreWithProvider creates a NamespaceAwareStore with a custom
// store provider. This is primarily used for testing.
func newNamespaceAwareStoreWithProvider(provider storeProvider, defaultNamespace string) *NamespaceAwareStore {
	return &NamespaceAwareStore{
		provider:         provider,
		defaultNamespace: defaultNamespace,
	}
}

// getNamespacedContext returns a context with the namespace set.
// If the context already has a namespace, it returns it unchanged.
// Otherwise, it uses the default namespace.
func (s *NamespaceAwareStore) getNamespacedContext(ctx context.Context) (context.Context, error) {
	ns, ok := namespaces.Namespace(ctx)
	if !ok || ns == "" {
		ns = s.defaultNamespace
	}
	if ns == "" {
		return nil, fmt.Errorf("namespace is required: %w", errdefs.ErrFailedPrecondition)
	}
	// Ensure namespace is set in context for the content store call
	return namespaces.WithNamespace(ctx, ns), nil
}

// store returns the underlying content store.
func (s *NamespaceAwareStore) store() content.Store {
	return s.provider.ContentStore()
}

// ReaderAt implements content.Provider.
func (s *NamespaceAwareStore) ReaderAt(ctx context.Context, desc ocispec.Descriptor) (content.ReaderAt, error) {
	ctx, err := s.getNamespacedContext(ctx)
	if err != nil {
		return nil, err
	}
	return s.store().ReaderAt(ctx, desc)
}

// Writer implements content.Ingester.
func (s *NamespaceAwareStore) Writer(ctx context.Context, opts ...content.WriterOpt) (content.Writer, error) {
	ctx, err := s.getNamespacedContext(ctx)
	if err != nil {
		return nil, err
	}
	return s.store().Writer(ctx, opts...)
}

// Abort implements content.IngestManager.
func (s *NamespaceAwareStore) Abort(ctx context.Context, ref string) error {
	ctx, err := s.getNamespacedContext(ctx)
	if err != nil {
		return err
	}
	return s.store().Abort(ctx, ref)
}

// Status implements content.IngestManager.
func (s *NamespaceAwareStore) Status(ctx context.Context, ref string) (content.Status, error) {
	ctx, err := s.getNamespacedContext(ctx)
	if err != nil {
		return content.Status{}, err
	}
	return s.store().Status(ctx, ref)
}

// ListStatuses implements content.IngestManager.
func (s *NamespaceAwareStore) ListStatuses(ctx context.Context, filters ...string) ([]content.Status, error) {
	ctx, err := s.getNamespacedContext(ctx)
	if err != nil {
		return nil, err
	}
	return s.store().ListStatuses(ctx, filters...)
}

// Info implements content.Manager.
func (s *NamespaceAwareStore) Info(ctx context.Context, dgst digest.Digest) (content.Info, error) {
	ctx, err := s.getNamespacedContext(ctx)
	if err != nil {
		return content.Info{}, err
	}
	return s.store().Info(ctx, dgst)
}

// Update implements content.Manager.
func (s *NamespaceAwareStore) Update(ctx context.Context, info content.Info, fieldpaths ...string) (content.Info, error) {
	ctx, err := s.getNamespacedContext(ctx)
	if err != nil {
		return content.Info{}, err
	}
	return s.store().Update(ctx, info, fieldpaths...)
}

// Walk implements content.Manager.
func (s *NamespaceAwareStore) Walk(ctx context.Context, fn content.WalkFunc, filters ...string) error {
	ctx, err := s.getNamespacedContext(ctx)
	if err != nil {
		return err
	}
	return s.store().Walk(ctx, fn, filters...)
}

// Delete implements content.Manager.
func (s *NamespaceAwareStore) Delete(ctx context.Context, dgst digest.Digest) error {
	ctx, err := s.getNamespacedContext(ctx)
	if err != nil {
		return err
	}
	return s.store().Delete(ctx, dgst)
}

// Verify that NamespaceAwareStore implements content.Store.
var _ content.Store = (*NamespaceAwareStore)(nil)
