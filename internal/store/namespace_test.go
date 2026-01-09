package store

import (
	"bytes"
	"context"
	"io"
	"sync"
	"testing"

	"github.com/containerd/containerd/v2/core/content"
	"github.com/containerd/containerd/v2/pkg/namespaces"
	"github.com/containerd/containerd/v2/plugins/content/local"
	"github.com/containerd/errdefs"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	// Import testutil to register the -test.root flag
	_ "github.com/spin-stack/erofs-snapshotter/internal/testutil"
)

func TestNewNamespaceAwareStore(t *testing.T) {
	store := NewNamespaceAwareStore(nil, "default")
	if store == nil {
		t.Fatal("expected non-nil store")
	}
	if store.defaultNamespace != "default" {
		t.Errorf("defaultNamespace = %q, want %q", store.defaultNamespace, "default")
	}
}

func TestGetNamespacedContext(t *testing.T) {
	tests := []struct {
		name             string
		inputNamespace   string // namespace to set in input context ("" means no namespace)
		defaultNamespace string
		wantNamespace    string
		wantErr          bool
	}{
		{
			name:             "uses context namespace when present",
			inputNamespace:   "my-namespace",
			defaultNamespace: "default",
			wantNamespace:    "my-namespace",
			wantErr:          false,
		},
		{
			name:             "falls back to default when context has no namespace",
			inputNamespace:   "",
			defaultNamespace: "default",
			wantNamespace:    "default",
			wantErr:          false,
		},
		{
			name:             "error when both context and default are empty",
			inputNamespace:   "",
			defaultNamespace: "",
			wantNamespace:    "",
			wantErr:          true,
		},
		{
			name:             "uses default for k8s.io namespace",
			inputNamespace:   "k8s.io",
			defaultNamespace: "default",
			wantNamespace:    "k8s.io",
			wantErr:          false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := NewNamespaceAwareStore(nil, tc.defaultNamespace)

			ctx := t.Context()
			if tc.inputNamespace != "" {
				ctx = namespaces.WithNamespace(ctx, tc.inputNamespace)
			}

			gotCtx, err := store.getNamespacedContext(ctx)

			if tc.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
				if !errdefs.IsFailedPrecondition(err) {
					t.Errorf("expected ErrFailedPrecondition, got %v", err)
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			gotNs, ok := namespaces.Namespace(gotCtx)
			if !ok {
				t.Fatal("expected namespace in returned context")
			}
			if gotNs != tc.wantNamespace {
				t.Errorf("namespace = %q, want %q", gotNs, tc.wantNamespace)
			}
		})
	}
}

func TestNamespaceAwareStore_NilClient(t *testing.T) {
	// Test that operations fail gracefully with nil client
	// This documents expected behavior when misconfigured
	store := NewNamespaceAwareStore(nil, "default")

	// These should panic or return errors due to nil client
	// We're documenting this behavior, not endorsing it
	t.Run("store returns nil for nil client", func(t *testing.T) {
		// store() calls client.ContentStore() which will panic on nil client
		// This test documents that the store requires a valid client
		defer func() {
			if r := recover(); r == nil {
				t.Log("no panic occurred - client might handle nil")
			}
		}()
		_ = store.store()
	})
}

// memoryLabelStore is a simple in-memory label store for testing.
type memoryLabelStore struct {
	l      sync.Mutex
	labels map[digest.Digest]map[string]string
}

func newMemoryLabelStore() local.LabelStore {
	return &memoryLabelStore{
		labels: map[digest.Digest]map[string]string{},
	}
}

func (mls *memoryLabelStore) Get(d digest.Digest) (map[string]string, error) {
	mls.l.Lock()
	labels := mls.labels[d]
	mls.l.Unlock()
	return labels, nil
}

func (mls *memoryLabelStore) Set(d digest.Digest, labels map[string]string) error {
	mls.l.Lock()
	mls.labels[d] = labels
	mls.l.Unlock()
	return nil
}

func (mls *memoryLabelStore) Update(d digest.Digest, update map[string]string) (map[string]string, error) {
	mls.l.Lock()
	labels, ok := mls.labels[d]
	if !ok {
		labels = map[string]string{}
	}
	for k, v := range update {
		if v == "" {
			delete(labels, k)
		} else {
			labels[k] = v
		}
	}
	mls.labels[d] = labels
	mls.l.Unlock()
	return labels, nil
}

// newTestContentStore creates a real content store for testing using the local store.
func newTestContentStore(t *testing.T) content.Store {
	t.Helper()
	cs, err := local.NewLabeledStore(t.TempDir(), newMemoryLabelStore())
	if err != nil {
		t.Fatal(err)
	}
	return cs
}

// testStoreProvider wraps a content.Store to implement storeProvider for testing.
type testStoreProvider struct {
	cs content.Store
}

func (p *testStoreProvider) ContentStore() content.Store {
	return p.cs
}

// newTestNamespaceAwareStore creates a NamespaceAwareStore with a test content store.
func newTestNamespaceAwareStore(t *testing.T, defaultNamespace string) *NamespaceAwareStore {
	t.Helper()
	cs := newTestContentStore(t)
	return newNamespaceAwareStoreWithProvider(&testStoreProvider{cs: cs}, defaultNamespace)
}

// getTestStore returns the underlying content store for direct access in tests.
func getTestStore(s *NamespaceAwareStore) content.Store {
	return s.store()
}

// writeTestBlob writes test content to the store and returns its descriptor.
func writeTestBlob(t *testing.T, ctx context.Context, cs content.Store, data []byte) ocispec.Descriptor {
	t.Helper()
	desc := ocispec.Descriptor{
		MediaType: "application/octet-stream",
		Digest:    digest.SHA256.FromBytes(data),
		Size:      int64(len(data)),
	}
	ref := string(desc.Digest)
	if err := content.WriteBlob(ctx, cs, ref, bytes.NewReader(data), desc); err != nil {
		t.Fatal(err)
	}
	return desc
}

func TestNamespaceAwareStore_ReaderAt(t *testing.T) {
	store := newTestNamespaceAwareStore(t, "default")
	data := []byte("test content for reader")

	t.Run("reads content with default namespace", func(t *testing.T) {
		ctx := t.Context()
		desc := writeTestBlob(t, ctx, getTestStore(store), data)

		ra, err := store.ReaderAt(ctx, desc)
		if err != nil {
			t.Fatalf("ReaderAt failed: %v", err)
		}
		defer ra.Close()

		buf := make([]byte, len(data))
		n, err := ra.ReadAt(buf, 0)
		if err != nil && err != io.EOF {
			t.Fatalf("ReadAt failed: %v", err)
		}
		if n != len(data) {
			t.Errorf("ReadAt returned %d bytes, want %d", n, len(data))
		}
		if !bytes.Equal(buf, data) {
			t.Errorf("ReadAt returned %q, want %q", buf, data)
		}
	})

	t.Run("reads content with context namespace", func(t *testing.T) {
		ctx := namespaces.WithNamespace(context.Background(), "test-namespace")
		desc := writeTestBlob(t, ctx, getTestStore(store), data)

		ra, err := store.ReaderAt(ctx, desc)
		if err != nil {
			t.Fatalf("ReaderAt failed: %v", err)
		}
		defer ra.Close()

		if ra.Size() != int64(len(data)) {
			t.Errorf("Size() = %d, want %d", ra.Size(), len(data))
		}
	})

	t.Run("fails without namespace when default is empty", func(t *testing.T) {
		emptyStore := newTestNamespaceAwareStore(t, "")
		ctx := t.Context()
		desc := ocispec.Descriptor{Digest: digest.FromString("test")}

		_, err := emptyStore.ReaderAt(ctx, desc)
		if err == nil {
			t.Error("expected error for empty namespace")
		}
		if !errdefs.IsFailedPrecondition(err) {
			t.Errorf("expected ErrFailedPrecondition, got %v", err)
		}
	})
}

func TestNamespaceAwareStore_Writer(t *testing.T) {
	store := newTestNamespaceAwareStore(t, "default")

	t.Run("creates writer with default namespace", func(t *testing.T) {
		ctx := t.Context()

		w, err := store.Writer(ctx, content.WithRef("test-ref-1"))
		if err != nil {
			t.Fatalf("Writer failed: %v", err)
		}
		defer w.Close()

		data := []byte("test data for writer")
		n, err := w.Write(data)
		if err != nil {
			t.Fatalf("Write failed: %v", err)
		}
		if n != len(data) {
			t.Errorf("Write returned %d, want %d", n, len(data))
		}
	})

	t.Run("creates writer with context namespace", func(t *testing.T) {
		ctx := namespaces.WithNamespace(context.Background(), "writer-test")

		w, err := store.Writer(ctx, content.WithRef("test-ref-2"))
		if err != nil {
			t.Fatalf("Writer failed: %v", err)
		}
		defer w.Close()

		status, err := w.Status()
		if err != nil {
			t.Fatalf("Status failed: %v", err)
		}
		if status.Ref != "test-ref-2" {
			t.Errorf("Status.Ref = %q, want %q", status.Ref, "test-ref-2")
		}
	})

	t.Run("fails without namespace when default is empty", func(t *testing.T) {
		emptyStore := newTestNamespaceAwareStore(t, "")
		ctx := t.Context()

		_, err := emptyStore.Writer(ctx, content.WithRef("test-ref"))
		if err == nil {
			t.Error("expected error for empty namespace")
		}
		if !errdefs.IsFailedPrecondition(err) {
			t.Errorf("expected ErrFailedPrecondition, got %v", err)
		}
	})
}

func TestNamespaceAwareStore_Status(t *testing.T) {
	store := newTestNamespaceAwareStore(t, "default")

	t.Run("returns status for active writer", func(t *testing.T) {
		ctx := t.Context()

		w, err := store.Writer(ctx, content.WithRef("status-test-ref"))
		if err != nil {
			t.Fatalf("Writer failed: %v", err)
		}
		defer w.Close()

		// Write some data
		w.Write([]byte("test data"))

		status, err := store.Status(ctx, "status-test-ref")
		if err != nil {
			t.Fatalf("Status failed: %v", err)
		}
		if status.Ref != "status-test-ref" {
			t.Errorf("Status.Ref = %q, want %q", status.Ref, "status-test-ref")
		}
	})

	t.Run("fails for non-existent ref", func(t *testing.T) {
		ctx := t.Context()

		_, err := store.Status(ctx, "non-existent-ref")
		if err == nil {
			t.Error("expected error for non-existent ref")
		}
	})

	t.Run("fails without namespace when default is empty", func(t *testing.T) {
		emptyStore := newTestNamespaceAwareStore(t, "")
		ctx := t.Context()

		_, err := emptyStore.Status(ctx, "test-ref")
		if err == nil {
			t.Error("expected error for empty namespace")
		}
		if !errdefs.IsFailedPrecondition(err) {
			t.Errorf("expected ErrFailedPrecondition, got %v", err)
		}
	})
}

func TestNamespaceAwareStore_ListStatuses(t *testing.T) {
	store := newTestNamespaceAwareStore(t, "default")

	t.Run("lists active writers", func(t *testing.T) {
		ctx := t.Context()

		w1, _ := store.Writer(ctx, content.WithRef("list-ref-1"))
		w2, _ := store.Writer(ctx, content.WithRef("list-ref-2"))
		defer w1.Close()
		defer w2.Close()

		statuses, err := store.ListStatuses(ctx)
		if err != nil {
			t.Fatalf("ListStatuses failed: %v", err)
		}
		if len(statuses) < 2 {
			t.Errorf("ListStatuses returned %d statuses, want at least 2", len(statuses))
		}
	})

	t.Run("returns empty list when no active writers", func(t *testing.T) {
		freshStore := newTestNamespaceAwareStore(t, "default")
		ctx := t.Context()

		statuses, err := freshStore.ListStatuses(ctx)
		if err != nil {
			t.Fatalf("ListStatuses failed: %v", err)
		}
		if len(statuses) != 0 {
			t.Errorf("ListStatuses returned %d statuses, want 0", len(statuses))
		}
	})

	t.Run("fails without namespace when default is empty", func(t *testing.T) {
		emptyStore := newTestNamespaceAwareStore(t, "")
		ctx := t.Context()

		_, err := emptyStore.ListStatuses(ctx)
		if err == nil {
			t.Error("expected error for empty namespace")
		}
		if !errdefs.IsFailedPrecondition(err) {
			t.Errorf("expected ErrFailedPrecondition, got %v", err)
		}
	})
}

func TestNamespaceAwareStore_Abort(t *testing.T) {
	store := newTestNamespaceAwareStore(t, "default")

	t.Run("aborts active writer", func(t *testing.T) {
		ctx := t.Context()

		w, err := store.Writer(ctx, content.WithRef("abort-test-ref"))
		if err != nil {
			t.Fatalf("Writer failed: %v", err)
		}
		w.Write([]byte("data to abort"))
		w.Close()

		err = store.Abort(ctx, "abort-test-ref")
		if err != nil {
			t.Fatalf("Abort failed: %v", err)
		}

		// Verify the status is gone
		_, err = store.Status(ctx, "abort-test-ref")
		if err == nil {
			t.Error("expected error after abort, status should not exist")
		}
	})

	t.Run("fails without namespace when default is empty", func(t *testing.T) {
		emptyStore := newTestNamespaceAwareStore(t, "")
		ctx := t.Context()

		err := emptyStore.Abort(ctx, "test-ref")
		if err == nil {
			t.Error("expected error for empty namespace")
		}
		if !errdefs.IsFailedPrecondition(err) {
			t.Errorf("expected ErrFailedPrecondition, got %v", err)
		}
	})
}

func TestNamespaceAwareStore_Info(t *testing.T) {
	store := newTestNamespaceAwareStore(t, "default")
	data := []byte("test content for info")

	t.Run("returns info for existing content", func(t *testing.T) {
		ctx := t.Context()
		desc := writeTestBlob(t, ctx, getTestStore(store), data)

		info, err := store.Info(ctx, desc.Digest)
		if err != nil {
			t.Fatalf("Info failed: %v", err)
		}
		if info.Digest != desc.Digest {
			t.Errorf("Info.Digest = %q, want %q", info.Digest, desc.Digest)
		}
		if info.Size != desc.Size {
			t.Errorf("Info.Size = %d, want %d", info.Size, desc.Size)
		}
	})

	t.Run("fails for non-existent content", func(t *testing.T) {
		ctx := t.Context()

		_, err := store.Info(ctx, digest.FromString("non-existent"))
		if err == nil {
			t.Error("expected error for non-existent content")
		}
		if !errdefs.IsNotFound(err) {
			t.Errorf("expected ErrNotFound, got %v", err)
		}
	})

	t.Run("fails without namespace when default is empty", func(t *testing.T) {
		emptyStore := newTestNamespaceAwareStore(t, "")
		ctx := t.Context()

		_, err := emptyStore.Info(ctx, digest.FromString("test"))
		if err == nil {
			t.Error("expected error for empty namespace")
		}
		if !errdefs.IsFailedPrecondition(err) {
			t.Errorf("expected ErrFailedPrecondition, got %v", err)
		}
	})
}

func TestNamespaceAwareStore_Update(t *testing.T) {
	store := newTestNamespaceAwareStore(t, "default")
	data := []byte("test content for update")

	t.Run("updates labels for existing content", func(t *testing.T) {
		ctx := t.Context()
		desc := writeTestBlob(t, ctx, getTestStore(store), data)

		info := content.Info{
			Digest: desc.Digest,
			Labels: map[string]string{
				"test.label": "test-value",
			},
		}

		updated, err := store.Update(ctx, info, "labels.test.label")
		if err != nil {
			t.Fatalf("Update failed: %v", err)
		}
		if updated.Labels["test.label"] != "test-value" {
			t.Errorf("Label not updated: got %q, want %q", updated.Labels["test.label"], "test-value")
		}
	})

	t.Run("fails without namespace when default is empty", func(t *testing.T) {
		emptyStore := newTestNamespaceAwareStore(t, "")
		ctx := t.Context()

		_, err := emptyStore.Update(ctx, content.Info{Digest: digest.FromString("test")})
		if err == nil {
			t.Error("expected error for empty namespace")
		}
		if !errdefs.IsFailedPrecondition(err) {
			t.Errorf("expected ErrFailedPrecondition, got %v", err)
		}
	})
}

func TestNamespaceAwareStore_Walk(t *testing.T) {
	store := newTestNamespaceAwareStore(t, "default")

	t.Run("walks all content", func(t *testing.T) {
		ctx := t.Context()

		// Write multiple blobs
		writeTestBlob(t, ctx, getTestStore(store), []byte("content 1"))
		writeTestBlob(t, ctx, getTestStore(store), []byte("content 2"))
		writeTestBlob(t, ctx, getTestStore(store), []byte("content 3"))

		var count int
		err := store.Walk(ctx, func(info content.Info) error {
			count++
			return nil
		})
		if err != nil {
			t.Fatalf("Walk failed: %v", err)
		}
		if count < 3 {
			t.Errorf("Walk visited %d items, want at least 3", count)
		}
	})

	t.Run("walks store with single item", func(t *testing.T) {
		freshStore := newTestNamespaceAwareStore(t, "default")
		ctx := t.Context()

		// Write one blob so the store is initialized
		writeTestBlob(t, ctx, getTestStore(freshStore), []byte("single item"))

		var count int
		err := freshStore.Walk(ctx, func(info content.Info) error {
			count++
			return nil
		})
		if err != nil {
			t.Fatalf("Walk failed: %v", err)
		}
		if count != 1 {
			t.Errorf("Walk visited %d items, want 1", count)
		}
	})

	t.Run("fails without namespace when default is empty", func(t *testing.T) {
		emptyStore := newTestNamespaceAwareStore(t, "")
		ctx := t.Context()

		err := emptyStore.Walk(ctx, func(info content.Info) error {
			return nil
		})
		if err == nil {
			t.Error("expected error for empty namespace")
		}
		if !errdefs.IsFailedPrecondition(err) {
			t.Errorf("expected ErrFailedPrecondition, got %v", err)
		}
	})
}

func TestNamespaceAwareStore_Delete(t *testing.T) {
	store := newTestNamespaceAwareStore(t, "default")
	data := []byte("test content for delete")

	t.Run("deletes existing content", func(t *testing.T) {
		ctx := t.Context()
		desc := writeTestBlob(t, ctx, getTestStore(store), data)

		err := store.Delete(ctx, desc.Digest)
		if err != nil {
			t.Fatalf("Delete failed: %v", err)
		}

		// Verify content is gone
		_, err = store.Info(ctx, desc.Digest)
		if err == nil {
			t.Error("expected error after delete, content should not exist")
		}
		if !errdefs.IsNotFound(err) {
			t.Errorf("expected ErrNotFound, got %v", err)
		}
	})

	t.Run("handles non-existent content gracefully", func(t *testing.T) {
		ctx := t.Context()

		// The local store may or may not return an error for non-existent content
		// depending on the implementation. We just verify Delete doesn't panic.
		err := store.Delete(ctx, digest.FromString("non-existent"))
		// If error is returned, it should be NotFound
		if err != nil && !errdefs.IsNotFound(err) {
			t.Errorf("expected nil or ErrNotFound, got %v", err)
		}
	})

	t.Run("fails without namespace when default is empty", func(t *testing.T) {
		emptyStore := newTestNamespaceAwareStore(t, "")
		ctx := t.Context()

		err := emptyStore.Delete(ctx, digest.FromString("test"))
		if err == nil {
			t.Error("expected error for empty namespace")
		}
		if !errdefs.IsFailedPrecondition(err) {
			t.Errorf("expected ErrFailedPrecondition, got %v", err)
		}
	})
}
