// Command integration-commit creates a new image by committing snapshot changes.
// This works directly with snapshots using containerd native APIs.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/core/images"
	"github.com/containerd/containerd/v2/core/mount"
	"github.com/containerd/containerd/v2/core/snapshots"
	"github.com/containerd/containerd/v2/pkg/namespaces"
	"github.com/opencontainers/image-spec/identity"
)

func main() {
	var (
		address       = flag.String("address", "/run/containerd/containerd.sock", "containerd socket")
		namespace     = flag.String("namespace", "default", "containerd namespace")
		snapshotterNm = flag.String("snapshotter", "spin-erofs", "snapshotter name")
		sourceImage   = flag.String("source", "ghcr.io/containerd/alpine:3.14.0", "source image")
		targetImage   = flag.String("target", "localhost/alpine:with-new-layer", "target image name")
		markerFile    = flag.String("marker", "/root/committed-marker.txt", "marker file to create")
	)
	flag.Parse()

	if err := run(*address, *namespace, *snapshotterNm, *sourceImage, *targetImage, *markerFile); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func run(address, namespace, snapshotterName, sourceImage, targetImage, markerFile string) error {
	// Use explicit namespace context
	ctx := namespaces.WithNamespace(context.Background(), namespace)

	// Connect to containerd
	c, err := client.New(address)
	if err != nil {
		return fmt.Errorf("connect to containerd: %w", err)
	}
	defer c.Close()

	fmt.Printf("Connected to containerd at %s (namespace: %s)\n", address, namespace)

	// Get source image
	img, err := c.GetImage(ctx, sourceImage)
	if err != nil {
		return fmt.Errorf("get source image %s: %w", sourceImage, err)
	}
	fmt.Printf("Source image: %s\n", sourceImage)

	// Ensure image is unpacked for this snapshotter
	// This is idempotent - safe to call multiple times
	if err := img.Unpack(ctx, snapshotterName); err != nil {
		return fmt.Errorf("unpack image: %w", err)
	}
	fmt.Printf("Image unpacked with snapshotter: %s\n", snapshotterName)

	// Get chain ID from image's RootFS - this is the proper way to identify snapshots
	diffIDs, err := img.RootFS(ctx)
	if err != nil {
		return fmt.Errorf("get image rootfs: %w", err)
	}
	chainID := identity.ChainID(diffIDs).String()
	fmt.Printf("Image chain ID: %s\n", chainID)

	// Get snapshotter service
	snapshotService := c.SnapshotService(snapshotterName)

	// Verify the parent snapshot exists
	parentInfo, err := snapshotService.Stat(ctx, chainID)
	if err != nil {
		return fmt.Errorf("stat parent snapshot %s: %w", chainID, err)
	}
	fmt.Printf("Parent snapshot: %s (kind: %s)\n", parentInfo.Name, parentInfo.Kind)

	// Create a unique snapshot key
	snapshotKey := fmt.Sprintf("commit-test-%d", time.Now().UnixNano())

	// Prepare labels for proper GC tracking
	labels := map[string]string{
		"containerd.io/gc.root":         time.Now().Format(time.RFC3339),
		"containerd.io/snapshot.ref":    targetImage,
		"containerd.io/gc.ref.snapshot": chainID, // Links to parent
	}

	// Prepare an active snapshot from the parent (using chain ID)
	mounts, err := snapshotService.Prepare(ctx, snapshotKey, chainID, snapshots.WithLabels(labels))
	if err != nil {
		return fmt.Errorf("prepare snapshot: %w", err)
	}
	defer func() {
		// Clean up on exit
		if err := snapshotService.Remove(ctx, snapshotKey); err != nil {
			// Only warn if it's not "not found" (already committed/removed)
			if !strings.Contains(err.Error(), "not found") {
				fmt.Fprintf(os.Stderr, "Warning: failed to remove snapshot %s: %v\n", snapshotKey, err)
			}
		}
	}()

	fmt.Printf("Created active snapshot: %s\n", snapshotKey)
	fmt.Printf("Mounts returned: %d\n", len(mounts))
	for i, m := range mounts {
		fmt.Printf("  [%d] type=%s source=%s options=%v\n", i, m.Type, m.Source, m.Options)
	}

	// Use containerd's mount API to mount the snapshot
	if err := writeToSnapshot(mounts, markerFile); err != nil {
		return fmt.Errorf("write to snapshot: %w", err)
	}

	fmt.Printf("Wrote marker file: %s\n", markerFile)

	// Commit the snapshot (converts rwlayer to EROFS for spin-erofs)
	committedKey := snapshotKey + "-committed"
	commitLabels := map[string]string{
		"containerd.io/gc.root":         time.Now().Format(time.RFC3339),
		"containerd.io/snapshot.ref":    targetImage,
		"containerd.io/gc.ref.snapshot": chainID,
	}

	if err := snapshotService.Commit(ctx, committedKey, snapshotKey, snapshots.WithLabels(commitLabels)); err != nil {
		// If the active snapshot was already removed by commit, that's OK
		if !strings.Contains(err.Error(), "not found") {
			return fmt.Errorf("commit snapshot: %w", err)
		}
	}
	defer func() {
		if err := snapshotService.Remove(ctx, committedKey); err != nil {
			if !strings.Contains(err.Error(), "not found") {
				fmt.Fprintf(os.Stderr, "Warning: failed to remove committed snapshot %s: %v\n", committedKey, err)
			}
		}
	}()

	fmt.Printf("Committed snapshot: %s -> %s\n", snapshotKey, committedKey)

	// Get usage info for the committed snapshot
	usage, err := snapshotService.Usage(ctx, committedKey)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Warning: failed to get snapshot usage: %v\n", err)
	} else {
		fmt.Printf("Committed snapshot size: %d bytes, inodes: %d\n", usage.Size, usage.Inodes)
	}

	// Create a new image reference
	imgService := c.ImageService()

	// Get source image descriptor
	sourceTarget := img.Target()

	// Create new image with the target ref
	newImg := images.Image{
		Name:   targetImage,
		Target: sourceTarget,
		Labels: map[string]string{
			"io.containerd.commit.source":   sourceImage,
			"io.containerd.commit.time":     time.Now().Format(time.RFC3339),
			"io.containerd.commit.snapshot": committedKey,
		},
	}

	created, err := imgService.Create(ctx, newImg)
	if err != nil {
		// If already exists, update it
		created, err = imgService.Update(ctx, newImg)
		if err != nil {
			return fmt.Errorf("create/update image: %w", err)
		}
	}

	fmt.Printf("\n✓ Successfully created new image: %s\n", created.Name)
	fmt.Printf("  Digest: %s\n", created.Target.Digest)

	// List all images to show the new one
	fmt.Println("\n=== All images in containerd ===")
	imgs, err := imgService.List(ctx)
	if err != nil {
		return fmt.Errorf("list images: %w", err)
	}
	for _, i := range imgs {
		fmt.Printf("  %s\n", i.Name)
	}

	return nil
}

func writeToSnapshot(mounts []mount.Mount, markerFile string) error {
	// For VM-only snapshotters like spin-erofs, the mounts returned are file paths
	// meant for VMs to mount, not for host mounting via containerd's mount.All().
	// We need to find the ext4 rwlayer and mount it manually.
	//
	// Expected mounts from spin-erofs for active snapshots:
	//   [0] type=erofs source=/path/to/fsmeta.erofs options=[ro loop ...]
	//   [1] type=ext4  source=/path/to/rwlayer.img  options=[rw loop]

	var ext4Path string
	for _, m := range mounts {
		if m.Type == "ext4" {
			ext4Path = m.Source
			break
		}
	}
	if ext4Path == "" {
		return fmt.Errorf("no ext4 mount found in mounts: %#v", mounts)
	}

	fmt.Printf("Found ext4 writable layer: %s\n", ext4Path)

	// Create a temporary mount point
	mountPoint, err := os.MkdirTemp("", "snapshot-mount-")
	if err != nil {
		return fmt.Errorf("create mount point: %w", err)
	}
	defer os.RemoveAll(mountPoint)

	// Mount the ext4 image using loop device
	cmd := exec.Command("mount", "-o", "loop", ext4Path, mountPoint)
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("mount ext4: %s: %w", string(out), err)
	}
	defer func() {
		if err := exec.Command("umount", mountPoint).Run(); err != nil {
			fmt.Fprintf(os.Stderr, "Warning: failed to unmount %s: %v\n", mountPoint, err)
		}
	}()

	// Create the marker file
	markerPath := filepath.Join(mountPoint, markerFile)
	if err := os.MkdirAll(filepath.Dir(markerPath), 0755); err != nil {
		return fmt.Errorf("create parent dirs: %w", err)
	}

	content := fmt.Sprintf("Committed at %s\n", time.Now().Format(time.RFC3339))
	if err := os.WriteFile(markerPath, []byte(content), 0644); err != nil {
		return fmt.Errorf("write marker file: %w", err)
	}

	// Sync before unmount
	_ = exec.Command("sync").Run()

	return nil
}
