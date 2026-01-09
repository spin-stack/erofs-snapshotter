package snapshotter

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/containerd/containerd/v2/core/snapshots"

	// Import testutil to register the -test.root flag
	_ "github.com/spin-stack/erofs-snapshotter/internal/testutil"
)

const osLinux = "linux"

// checkBlockModeRequirements verifies that mkfs.ext4 is available
// for block mode testing. Returns true if requirements are met.
func checkBlockModeRequirements(t *testing.T) bool {
	t.Helper()
	if _, err := exec.LookPath("mkfs.ext4"); err != nil {
		return false
	}
	return true
}

func newTestSnapshotter(t *testing.T, opts ...Opt) snapshots.Snapshotter {
	t.Helper()
	root := t.TempDir()

	// On non-Linux, we need block mode (default is 64MB)
	// On Linux without EROFS kernel support, we also need block mode
	needsBlockMode := runtime.GOOS != osLinux
	if needsBlockMode {
		if !checkBlockModeRequirements(t) {
			t.Skip("mkfs.ext4 not available, required for block mode testing")
		}
		// Force a smaller size for faster tests
		opts = append([]Opt{WithDefaultSize(1024 * 1024)}, opts...) // 1MB
	}

	s, err := NewSnapshotter(root, opts...)
	if err != nil {
		// On Linux, this may fail if EROFS isn't available
		if runtime.GOOS == osLinux {
			t.Skipf("snapshotter creation failed (EROFS likely not available): %v", err)
		}
		t.Fatalf("failed to create snapshotter: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestNewSnapshotter(t *testing.T) {
	t.Run("creates snapshotter with defaults", func(t *testing.T) {
		if !checkBlockModeRequirements(t) {
			t.Skip("mkfs.ext4 not available")
		}

		root := t.TempDir()

		// Use block mode for cross-platform testing
		s, err := NewSnapshotter(root, WithDefaultSize(1024*1024))
		if err != nil {
			t.Fatalf("failed to create snapshotter: %v", err)
		}
		defer s.Close()

		// Verify snapshotter is functional by preparing a snapshot
		// (Walk fails on empty db because bucket isn't created until first use)
		ctx := t.Context()
		_, err = s.Prepare(ctx, "test-snapshot", "")
		if err != nil {
			t.Errorf("Prepare failed on new snapshotter: %v", err)
		}
	})

	t.Run("fails on non-existent root", func(t *testing.T) {
		if !checkBlockModeRequirements(t) {
			t.Skip("mkfs.ext4 not available")
		}

		// Create a file where we want the directory
		root := t.TempDir()
		blockerPath := filepath.Join(root, "blocker")
		if err := os.WriteFile(blockerPath, []byte("x"), 0644); err != nil {
			t.Fatal(err)
		}

		_, err := NewSnapshotter(filepath.Join(blockerPath, "subdir"), WithDefaultSize(1024*1024))
		if err == nil {
			t.Error("expected error when root parent is a file")
		}
	})

	t.Run("immutable option rejected on non-linux", func(t *testing.T) {
		if runtime.GOOS == osLinux {
			t.Skip("only applies to non-Linux")
		}
		if !checkBlockModeRequirements(t) {
			t.Skip("mkfs.ext4 not available")
		}

		root := t.TempDir()
		_, err := NewSnapshotter(root, WithImmutable(), WithDefaultSize(1024*1024))
		if err == nil {
			t.Error("expected error for WithImmutable on non-Linux")
		}
	})
}

func TestSnapshotterPrepare(t *testing.T) {
	s := newTestSnapshotter(t)
	ctx := t.Context()

	t.Run("creates snapshot without parent", func(t *testing.T) {
		mounts, err := s.Prepare(ctx, "test-1", "")
		if err != nil {
			t.Fatalf("Prepare failed: %v", err)
		}

		if len(mounts) == 0 {
			t.Error("expected at least one mount")
		}
	})

	t.Run("stat shows active snapshot", func(t *testing.T) {
		_, err := s.Prepare(ctx, "test-2", "")
		if err != nil {
			t.Fatalf("Prepare failed: %v", err)
		}

		info, err := s.Stat(ctx, "test-2")
		if err != nil {
			t.Fatalf("Stat failed: %v", err)
		}

		if info.Kind != snapshots.KindActive {
			t.Errorf("expected KindActive, got %v", info.Kind)
		}
		if info.Name != "test-2" {
			t.Errorf("expected name 'test-2', got %q", info.Name)
		}
	})

	t.Run("duplicate key fails", func(t *testing.T) {
		_, err := s.Prepare(ctx, "test-3", "")
		if err != nil {
			t.Fatalf("first Prepare failed: %v", err)
		}

		_, err = s.Prepare(ctx, "test-3", "")
		if err == nil {
			t.Error("expected error for duplicate key")
		}
	})

	t.Run("non-existent parent fails", func(t *testing.T) {
		_, err := s.Prepare(ctx, "child", "non-existent-parent")
		if err == nil {
			t.Error("expected error for non-existent parent")
		}
	})
}

func TestSnapshotterRemove(t *testing.T) {
	s := newTestSnapshotter(t)
	ctx := t.Context()

	t.Run("removes active snapshot", func(t *testing.T) {
		_, err := s.Prepare(ctx, "to-remove", "")
		if err != nil {
			t.Fatalf("Prepare failed: %v", err)
		}

		if err := s.Remove(ctx, "to-remove"); err != nil {
			t.Fatalf("Remove failed: %v", err)
		}

		// Stat should fail after removal
		_, err = s.Stat(ctx, "to-remove")
		if err == nil {
			t.Error("expected error when stating removed snapshot")
		}
	})

	t.Run("remove non-existent fails", func(t *testing.T) {
		err := s.Remove(ctx, "does-not-exist")
		if err == nil {
			t.Error("expected error when removing non-existent snapshot")
		}
	})
}

func TestSnapshotterMounts(t *testing.T) {
	s := newTestSnapshotter(t)
	ctx := t.Context()

	t.Run("returns mounts for active snapshot", func(t *testing.T) {
		_, err := s.Prepare(ctx, "mount-test", "")
		if err != nil {
			t.Fatalf("Prepare failed: %v", err)
		}

		mounts, err := s.Mounts(ctx, "mount-test")
		if err != nil {
			t.Fatalf("Mounts failed: %v", err)
		}

		if len(mounts) == 0 {
			t.Error("expected at least one mount")
		}
	})

	t.Run("fails for non-existent snapshot", func(t *testing.T) {
		_, err := s.Mounts(ctx, "does-not-exist")
		if err == nil {
			t.Error("expected error for non-existent snapshot")
		}
	})
}

func TestSnapshotterWalk(t *testing.T) {
	s := newTestSnapshotter(t)
	ctx := t.Context()

	// Create a few snapshots
	for _, key := range []string{"walk-1", "walk-2", "walk-3"} {
		if _, err := s.Prepare(ctx, key, ""); err != nil {
			t.Fatalf("Prepare %s failed: %v", key, err)
		}
	}

	t.Run("walks all snapshots", func(t *testing.T) {
		var names []string
		err := s.Walk(ctx, func(ctx context.Context, info snapshots.Info) error {
			names = append(names, info.Name)
			return nil
		})
		if err != nil {
			t.Fatalf("Walk failed: %v", err)
		}

		if len(names) < 3 {
			t.Errorf("expected at least 3 snapshots, got %d", len(names))
		}
	})
}

func TestSnapshotterUsage(t *testing.T) {
	s := newTestSnapshotter(t)
	ctx := t.Context()

	_, err := s.Prepare(ctx, "usage-test", "")
	if err != nil {
		t.Fatalf("Prepare failed: %v", err)
	}

	usage, err := s.Usage(ctx, "usage-test")
	if err != nil {
		t.Fatalf("Usage failed: %v", err)
	}

	// Usage should be non-negative
	if usage.Size < 0 {
		t.Errorf("expected non-negative size, got %d", usage.Size)
	}
}

func TestSnapshotterUpdate(t *testing.T) {
	s := newTestSnapshotter(t)
	ctx := t.Context()

	_, err := s.Prepare(ctx, "update-test", "")
	if err != nil {
		t.Fatalf("Prepare failed: %v", err)
	}

	t.Run("updates labels", func(t *testing.T) {
		info, err := s.Stat(ctx, "update-test")
		if err != nil {
			t.Fatalf("Stat failed: %v", err)
		}

		info.Labels = map[string]string{
			"custom-label": "custom-value",
		}

		updated, err := s.Update(ctx, info, "labels")
		if err != nil {
			t.Fatalf("Update failed: %v", err)
		}

		if updated.Labels["custom-label"] != "custom-value" {
			t.Error("label not updated")
		}
	})
}

func TestSnapshotterClose(t *testing.T) {
	if !checkBlockModeRequirements(t) {
		t.Skip("mkfs.ext4 not available")
	}

	root := t.TempDir()
	s, err := NewSnapshotter(root, WithDefaultSize(1024*1024))
	if err != nil {
		t.Fatalf("failed to create snapshotter: %v", err)
	}

	// Close should not error
	if err := s.Close(); err != nil {
		t.Errorf("Close failed: %v", err)
	}

	// Second close should also not panic/error
	// (the implementation handles multiple closes)
	_ = s.Close()
}
