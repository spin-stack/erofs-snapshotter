package snapshotter

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/containerd/containerd/v2/core/snapshots"
)

// TestReverseStringsDoesNotMutate verifies reverseStrings returns a new slice.
// This is a property test, not a concurrency test - reverseStrings is safe
// for concurrent use because it allocates a new slice on each call.
func TestReverseStringsDoesNotMutate(t *testing.T) {
	original := []string{"layer5", "layer4", "layer3", "layer2", "layer1"}
	originalCopy := make([]string, len(original))
	copy(originalCopy, original)

	result := reverseStrings(original)

	// Verify result is reversed
	if result[0] != "layer1" || result[4] != "layer5" {
		t.Errorf("reverseStrings returned wrong result: %v", result)
	}

	// Verify original is unchanged
	for i := range original {
		if original[i] != originalCopy[i] {
			t.Errorf("original was modified at index %d: got %s, want %s",
				i, original[i], originalCopy[i])
		}
	}

	// Verify result is a different slice (not aliased)
	result[0] = "changed"
	if original[4] == "changed" {
		t.Error("result slice aliases original slice")
	}
}

// TestConcurrentPrepare verifies concurrent Prepare calls don't race.
func TestConcurrentPrepare(t *testing.T) {
	s := newTestSnapshotter(t)
	ctx := t.Context()
	const numGoroutines = 10

	var wg sync.WaitGroup
	errors := make(chan error, numGoroutines)

	for i := range numGoroutines {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			key := fmt.Sprintf("concurrent-prepare-%d", id)
			_, err := s.Prepare(ctx, key, "")
			if err != nil {
				errors <- fmt.Errorf("prepare %d: %w", id, err)
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Errorf("unexpected error: %v", err)
	}

	// Verify all snapshots exist
	var count int
	err := s.Walk(ctx, func(_ context.Context, info snapshots.Info) error {
		count++
		return nil
	})
	if err != nil {
		t.Fatalf("walk failed: %v", err)
	}
	if count < numGoroutines {
		t.Errorf("expected at least %d snapshots, got %d", numGoroutines, count)
	}
}

// TestConcurrentView verifies concurrent View calls on the same parent don't race.
// This tests the fsmeta generation coordination (lock file): after all views and
// background generations finish, no lock or temp artifacts may remain and the
// fsmeta/VMDK pair must be consistent.
func TestConcurrentView(t *testing.T) {
	s := newTestSnapshotterInternal(t)
	ctx := t.Context()

	// Create a base snapshot to use as parent
	_, err := s.Prepare(ctx, "base-for-views", "")
	if err != nil {
		t.Fatalf("prepare base: %v", err)
	}

	// Commit the base (needed for View)
	if err := s.Commit(ctx, "committed-base", "base-for-views"); err != nil {
		t.Fatalf("commit base: %v", err)
	}

	// Create multiple views concurrently - all trigger fsmeta generation for same parent
	const numGoroutines = 10
	var wg sync.WaitGroup
	errors := make(chan error, numGoroutines)

	for i := range numGoroutines {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			key := fmt.Sprintf("concurrent-view-%d", id)
			_, err := s.View(ctx, key, "committed-base")
			if err != nil {
				errors <- fmt.Errorf("view %d: %w", id, err)
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Errorf("unexpected error: %v", err)
	}

	// All View calls have returned, so every background fsmeta generation has
	// already been registered with bgWg; wait for them to finish and verify
	// the coordination left no lock or temp artifacts behind.
	s.bgWg.Wait()

	parentID := snapshotIDForKey(t, s, ctx, "committed-base")
	for _, path := range []string{
		s.fsMetaPath(parentID) + ".lock",
		s.fsMetaPath(parentID) + ".tmp",
		s.vmdkPath(parentID) + ".tmp",
	} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Errorf("expected no residual fsmeta artifact %s, got err=%v", path, err)
		}
	}

	// fsmeta and VMDK must be consistent: either both were generated or neither
	// (generation may legitimately fail, but must not leave a partial pair).
	_, metaErr := os.Stat(s.fsMetaPath(parentID))
	_, vmdkErr := os.Stat(s.vmdkPath(parentID))
	if os.IsNotExist(metaErr) != os.IsNotExist(vmdkErr) {
		t.Errorf("inconsistent fsmeta/vmdk pair: fsmeta err=%v, vmdk err=%v", metaErr, vmdkErr)
	}
}

// TestConcurrentRemove verifies concurrent Remove calls work correctly.
func TestConcurrentRemove(t *testing.T) {
	s := newTestSnapshotter(t)
	ctx := t.Context()
	const numSnapshots = 10

	// Create snapshots to remove
	keys := make([]string, numSnapshots)
	for i := range numSnapshots {
		key := fmt.Sprintf("to-remove-%d", i)
		keys[i] = key
		if _, err := s.Prepare(ctx, key, ""); err != nil {
			t.Fatalf("prepare %d: %v", i, err)
		}
	}

	// Remove all concurrently
	var wg sync.WaitGroup
	errors := make(chan error, numSnapshots)

	for i, key := range keys {
		wg.Add(1)
		go func(id int, k string) {
			defer wg.Done()
			if err := s.Remove(ctx, k); err != nil {
				errors <- fmt.Errorf("remove %d (%s): %w", id, k, err)
			}
		}(i, key)
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Errorf("unexpected error: %v", err)
	}

	// Verify all snapshots removed
	var remaining int
	err := s.Walk(ctx, func(_ context.Context, _ snapshots.Info) error {
		remaining++
		return nil
	})
	if err != nil {
		t.Fatalf("walk failed: %v", err)
	}
	if remaining != 0 {
		t.Errorf("expected 0 snapshots after removal, got %d", remaining)
	}
}

// TestFsmetaLockFileRace verifies that concurrent fsmeta lock acquisition
// via tryAcquireFsmetaLock (the coordination used by generateFsMeta) admits
// exactly one winner.
func TestFsmetaLockFileRace(t *testing.T) {
	root := t.TempDir()
	s := &snapshotter{root: root}

	// Create snapshot directory
	snapshotDir := filepath.Join(root, "snapshots", "test-parent")
	if err := os.MkdirAll(snapshotDir, 0o755); err != nil {
		t.Fatal(err)
	}

	// Run multiple goroutines racing to acquire the fsmeta lock
	const numGoroutines = 20
	var wg sync.WaitGroup
	start := make(chan struct{})
	winners := make(chan int, numGoroutines)
	errs := make(chan error, numGoroutines)

	for i := range numGoroutines {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			<-start
			locked, err := s.tryAcquireFsmetaLock(context.Background(), "test-parent")
			if err != nil {
				errs <- fmt.Errorf("goroutine %d: %w", id, err)
				return
			}
			if locked {
				winners <- id
			}
			// Losers see the fresh lock held by the winner - that's expected
		}(i)
	}

	close(start)
	wg.Wait()
	close(winners)
	close(errs)

	for err := range errs {
		t.Errorf("unexpected error: %v", err)
	}

	// Exactly one goroutine should win
	winnerCount := 0
	for range winners {
		winnerCount++
	}

	if winnerCount != 1 {
		t.Errorf("expected exactly 1 goroutine to acquire the fsmeta lock, got %d", winnerCount)
	}

	// The winner still holds the lock, so the lock file must exist
	lockFile := s.fsMetaPath("test-parent") + ".lock"
	if _, err := os.Stat(lockFile); err != nil {
		t.Errorf("lock file should exist: %v", err)
	}
}

func TestTryAcquireFsmetaLockRejectsFreshLock(t *testing.T) {
	root := t.TempDir()
	s := &snapshotter{root: root}

	snapshotDir := filepath.Join(root, "snapshots", "test-parent")
	if err := os.MkdirAll(snapshotDir, 0o755); err != nil {
		t.Fatal(err)
	}

	lockFile := s.fsMetaPath("test-parent") + ".lock"
	if err := os.WriteFile(lockFile, nil, 0o600); err != nil {
		t.Fatal(err)
	}

	locked, err := s.tryAcquireFsmetaLock(context.Background(), "test-parent")
	if err != nil {
		t.Fatalf("tryAcquireFsmetaLock: %v", err)
	}
	if locked {
		t.Fatal("expected fresh lock to prevent acquisition")
	}

	if _, err := os.Stat(lockFile); err != nil {
		t.Fatalf("fresh lock should remain in place: %v", err)
	}
}

func TestTryAcquireFsmetaLockRecoversStaleLock(t *testing.T) {
	root := t.TempDir()
	s := &snapshotter{root: root}

	snapshotDir := filepath.Join(root, "snapshots", "test-parent")
	if err := os.MkdirAll(snapshotDir, 0o755); err != nil {
		t.Fatal(err)
	}

	lockFile := s.fsMetaPath("test-parent") + ".lock"
	tmpMeta := s.fsMetaPath("test-parent") + ".tmp"
	tmpVmdk := s.vmdkPath("test-parent") + ".tmp"

	for _, path := range []string{lockFile, tmpMeta, tmpVmdk} {
		if err := os.WriteFile(path, []byte("stale"), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	old := time.Now().Add(-fsmetaLockStaleAge - time.Minute)
	if err := os.Chtimes(lockFile, old, old); err != nil {
		t.Fatalf("chtimes lock: %v", err)
	}

	locked, err := s.tryAcquireFsmetaLock(context.Background(), "test-parent")
	if err != nil {
		t.Fatalf("tryAcquireFsmetaLock: %v", err)
	}
	if !locked {
		t.Fatal("expected stale lock to be recovered")
	}

	if _, err := os.Stat(lockFile); err != nil {
		t.Fatalf("expected lock file to be reacquired: %v", err)
	}
	if _, err := os.Stat(tmpMeta); !os.IsNotExist(err) {
		t.Fatalf("expected stale temp fsmeta to be removed, got err=%v", err)
	}
	if _, err := os.Stat(tmpVmdk); !os.IsNotExist(err) {
		t.Fatalf("expected stale temp vmdk to be removed, got err=%v", err)
	}
}

func TestCleanupFsmetaArtifactsRemovesStartupArtifacts(t *testing.T) {
	root := t.TempDir()
	s := &snapshotter{root: root}

	snapshotDir := filepath.Join(root, "snapshots", "test-parent")
	if err := os.MkdirAll(snapshotDir, 0o755); err != nil {
		t.Fatal(err)
	}

	lockFile := s.fsMetaPath("test-parent") + ".lock"
	tmpMeta := s.fsMetaPath("test-parent") + ".tmp"
	tmpVmdk := s.vmdkPath("test-parent") + ".tmp"
	finalMeta := s.fsMetaPath("test-parent")
	finalVmdk := s.vmdkPath("test-parent")

	for _, path := range []string{lockFile, tmpMeta, tmpVmdk, finalMeta, finalVmdk} {
		if err := os.WriteFile(path, []byte("artifact"), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	s.cleanupFsmetaArtifacts()

	for _, path := range []string{lockFile, tmpMeta, tmpVmdk} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("expected startup artifact %s to be removed, got err=%v", path, err)
		}
	}

	for _, path := range []string{finalMeta, finalVmdk} {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("expected final artifact %s to remain: %v", path, err)
		}
	}
}

// writeFakeErofsBlob writes a minimal file that passes erofs.GetBlockSize:
// the EROFS magic at the superblock offset (1024) and blkszbits=12, i.e. a
// 4096-byte block size compatible with fsmeta merge.
func writeFakeErofsBlob(t *testing.T, path string) {
	t.Helper()
	buf := make([]byte, 1024+16)
	// EROFS magic 0xE0F5E1E2, little-endian
	buf[1024] = 0xE2
	buf[1025] = 0xE1
	buf[1026] = 0xF5
	buf[1027] = 0xE0
	// blkszbits at superblock offset +12: 1<<12 = 4096
	buf[1036] = 12
	if err := os.WriteFile(path, buf, 0o644); err != nil {
		t.Fatal(err)
	}
}

// installFakeMkfsErofs prepends a fake mkfs.erofs to PATH that understands the
// argument shape used by generateFsMeta (--quiet --vmdk-desc=<vmdk> <out> <blobs...>)
// and writes both output files, embedding the temp output path in the VMDK so
// the fixVmdkPaths rewrite is observable.
func installFakeMkfsErofs(t *testing.T) {
	t.Helper()
	binDir := t.TempDir()
	script := `#!/bin/sh
vmdk=""
out=""
for a in "$@"; do
	case "$a" in
	--quiet) ;;
	--vmdk-desc=*) vmdk="${a#--vmdk-desc=}" ;;
	*)
		if [ -z "$out" ]; then
			out="$a"
		fi
		;;
	esac
done
printf 'fsmeta' > "$out"
printf 'RW 8 FLAT "%s" 0\n' "$out" > "$vmdk"
`
	if err := os.WriteFile(filepath.Join(binDir, "mkfs.erofs"), []byte(script), 0o755); err != nil { //nolint:gosec // test helper needs an executable script
		t.Fatal(err)
	}
	t.Setenv("PATH", binDir+string(os.PathListSeparator)+os.Getenv("PATH"))
}

// TestFsmetaAtomicRename verifies that generateFsMeta produces the final
// fsmeta/VMDK pair via atomic rename, rewrites the VMDK to reference the final
// fsmeta path, writes the layer manifest, and leaves no temp or lock files.
func TestFsmetaAtomicRename(t *testing.T) {
	installFakeMkfsErofs(t)

	root := t.TempDir()
	s := &snapshotter{root: root}

	// Create the parent snapshot directory with a digest-named layer blob
	// that passes the fsmeta block-size compatibility check.
	parentID := "test-parent"
	snapshotDir := filepath.Join(root, "snapshots", parentID)
	if err := os.MkdirAll(snapshotDir, 0o755); err != nil {
		t.Fatal(err)
	}
	blobDigest := strings.Repeat("ab", 32)
	writeFakeErofsBlob(t, filepath.Join(snapshotDir, "sha256-"+blobDigest+".erofs"))

	s.generateFsMeta(context.Background(), []string{parentID})

	fsmetaPath := s.fsMetaPath(parentID)
	vmdkPath := s.vmdkPath(parentID)

	// Verify final files exist
	if _, err := os.Stat(fsmetaPath); err != nil {
		t.Errorf("fsmeta should exist: %v", err)
	}
	vmdkContent, err := os.ReadFile(vmdkPath)
	if err != nil {
		t.Fatalf("vmdk should exist: %v", err)
	}

	// fixVmdkPaths must have rewritten the temp fsmeta path to the final one
	if strings.Contains(string(vmdkContent), ".tmp") {
		t.Errorf("vmdk should not reference temp paths, got: %s", vmdkContent)
	}
	if !strings.Contains(string(vmdkContent), fsmetaPath) {
		t.Errorf("vmdk should reference final fsmeta path %s, got: %s", fsmetaPath, vmdkContent)
	}

	// Verify temp and lock files are gone
	for _, path := range []string{fsmetaPath + ".tmp", vmdkPath + ".tmp", fsmetaPath + ".lock"} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Errorf("expected %s to be removed after generation, got err=%v", path, err)
		}
	}

	// Layer manifest must record the blob digest
	manifest, err := os.ReadFile(s.manifestPath(parentID))
	if err != nil {
		t.Fatalf("layer manifest should exist: %v", err)
	}
	if got, want := strings.TrimSpace(string(manifest)), "sha256:"+blobDigest; got != want {
		t.Errorf("layer manifest = %q, want %q", got, want)
	}
}

// TestFixVmdkPaths verifies the VMDK path replacement function.
func TestFixVmdkPaths(t *testing.T) {
	tmpDir := t.TempDir()
	vmdkFile := filepath.Join(tmpDir, "test.vmdk")

	// Sample VMDK content with temp path
	vmdkContent := `# Disk DescriptorFile
version=1
CID=91702505
parentCID=ffffffff
createType="twoGbMaxExtentFlat"
# Extent description
RW 232 FLAT "/var/lib/snapshots/11/fsmeta.erofs.tmp" 0
RW 15944 FLAT "/var/lib/snapshots/7/layer1.erofs" 0
# The Disk Data Base
#DDB
ddb.virtualHWVersion = "4"
`

	if err := os.WriteFile(vmdkFile, []byte(vmdkContent), 0o644); err != nil {
		t.Fatal(err)
	}

	// Fix the path
	oldPath := "/var/lib/snapshots/11/fsmeta.erofs.tmp"
	newPath := "/var/lib/snapshots/11/fsmeta.erofs"
	if err := fixVmdkPaths(vmdkFile, oldPath, newPath); err != nil {
		t.Fatalf("fixVmdkPaths: %v", err)
	}

	// Read back and verify
	content, err := os.ReadFile(vmdkFile)
	if err != nil {
		t.Fatal(err)
	}

	contentStr := string(content)
	if strings.Contains(contentStr, ".tmp") {
		t.Error("VMDK should not contain .tmp after fix")
	}
	if !strings.Contains(contentStr, "fsmeta.erofs") {
		t.Error("VMDK should contain final fsmeta.erofs path")
	}
	// Verify other paths are unchanged
	if !strings.Contains(contentStr, "layer1.erofs") {
		t.Error("VMDK should still contain layer1.erofs path")
	}
}
