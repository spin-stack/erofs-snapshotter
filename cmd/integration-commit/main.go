// Command integration-commit exercises the full "commit a container to a new
// image" flow against a running containerd with the spin-erofs snapshotter,
// following the same sequence nerdctl/ctr commit use:
//
//  1. Prepare an active snapshot from the source image chain (the "container").
//  2. Write changes into upper/ inside the ext4 rwlayer (simulating the guest
//     overlay layout - VM runtimes create upper/ and work/ at the ext4 root).
//  3. diff.Compare(View(parent), Mounts(active)) -> new tar+gzip layer blob.
//  4. Build a new OCI config (rootfs.diff_ids + history) and manifest
//     referencing the new layer, with GC labels.
//  5. Prepare+Apply+Commit the layer as a snapshot named by the new chain ID.
//  6. Register the new image and verify it unpacks.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/core/content"
	"github.com/containerd/containerd/v2/core/diff"
	"github.com/containerd/containerd/v2/core/images"
	"github.com/containerd/containerd/v2/core/mount"
	"github.com/containerd/containerd/v2/core/snapshots"
	"github.com/containerd/containerd/v2/pkg/namespaces"
	"github.com/containerd/errdefs"
	"github.com/containerd/platforms"
	"github.com/opencontainers/go-digest"
	"github.com/opencontainers/image-spec/identity"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

// uncompressedLabel is the content-store label the differ sets on compressed
// layer blobs with the digest of the uncompressed tar (the layer's diff ID).
const uncompressedLabel = "containerd.io/uncompressed"

func main() {
	var (
		address       = flag.String("address", "/var/run/spin-stack/containerd.sock", "containerd socket")
		namespace     = flag.String("namespace", "default", "containerd namespace")
		snapshotterNm = flag.String("snapshotter", "spin-erofs", "snapshotter name")
		sourceImage   = flag.String("source", "ghcr.io/containerd/alpine:3.14.0", "source image")
		targetImage   = flag.String("target", "localhost/alpine:with-new-layer", "target image name")
		markerFile    = flag.String("marker", "/root/committed-marker.txt", "marker file to create")
		nativeErofs   = flag.Bool("native-erofs", false, "emit the committed layer as a native EROFS blob instead of tar+gzip")
	)
	flag.Parse()

	mediaType := ocispec.MediaTypeImageLayerGzip
	if *nativeErofs {
		mediaType = images.MediaTypeErofsLayer
	}

	if err := run(*address, *namespace, *snapshotterNm, *sourceImage, *targetImage, *markerFile, mediaType); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func run(address, namespace, snapshotterName, sourceImage, targetImage, markerFile, layerMediaType string) error {
	ctx := namespaces.WithNamespace(context.Background(), namespace)

	c, err := client.New(address)
	if err != nil {
		return fmt.Errorf("connect to containerd: %w", err)
	}
	defer c.Close()

	fmt.Printf("Connected to containerd at %s (namespace: %s)\n", address, namespace)

	// Hold a lease for the whole operation: the config/manifest blobs and the
	// new snapshot are unreferenced until images.Create registers the image,
	// and a concurrent containerd GC sweep would delete them otherwise.
	ctx, done, err := c.WithLease(ctx)
	if err != nil {
		return fmt.Errorf("create lease: %w", err)
	}
	defer func() {
		if derr := done(context.WithoutCancel(ctx)); derr != nil {
			fmt.Fprintf(os.Stderr, "Warning: failed to release lease: %v\n", derr)
		}
	}()

	img, err := c.GetImage(ctx, sourceImage)
	if err != nil {
		return fmt.Errorf("get source image %s: %w", sourceImage, err)
	}
	if err := img.Unpack(ctx, snapshotterName); err != nil {
		return fmt.Errorf("unpack image: %w", err)
	}
	fmt.Printf("Source image %s unpacked with snapshotter %s\n", sourceImage, snapshotterName)

	diffIDs, err := img.RootFS(ctx)
	if err != nil {
		return fmt.Errorf("get image rootfs: %w", err)
	}
	parentChainID := identity.ChainID(diffIDs).String()
	fmt.Printf("Source chain ID: %s\n", parentChainID)

	sn := c.SnapshotService(snapshotterName)
	cs := c.ContentStore()

	// 1. Active snapshot from the source chain - this is "the container".
	activeKey := fmt.Sprintf("commit-test-%d", time.Now().UnixNano())
	upperMounts, err := sn.Prepare(ctx, activeKey, parentChainID)
	if err != nil {
		return fmt.Errorf("prepare active snapshot: %w", err)
	}
	defer removeSnapshot(ctx, sn, activeKey)

	fmt.Printf("Active snapshot %s mounts:\n", activeKey)
	for i, m := range upperMounts {
		fmt.Printf("  [%d] type=%s source=%s options=%v\n", i, m.Type, m.Source, m.Options)
	}

	// 2. Simulate the guest writing through its overlay: changes land in
	// upper/ at the root of the ext4 rwlayer (the VM runtime contract).
	if err := writeToGuestUpper(upperMounts, markerFile); err != nil {
		return fmt.Errorf("write to snapshot upper: %w", err)
	}
	fmt.Printf("Wrote marker %s inside the rwlayer upper/\n", markerFile)

	// 3. Compare lower (parent view) against the active snapshot.
	viewKey := activeKey + "-parent-view"
	lowerMounts, err := sn.View(ctx, viewKey, parentChainID)
	if err != nil {
		return fmt.Errorf("create parent view: %w", err)
	}
	defer removeSnapshot(ctx, sn, viewKey)

	layerDesc, err := c.DiffService().Compare(ctx, lowerMounts, upperMounts,
		diff.WithMediaType(layerMediaType),
		diff.WithReference(fmt.Sprintf("commit-layer-%d", time.Now().UnixNano())))
	if err != nil {
		return fmt.Errorf("diff.Compare: %w", err)
	}
	fmt.Printf("New layer blob: %s (%d bytes, %s)\n", layerDesc.Digest, layerDesc.Size, layerDesc.MediaType)

	// 4. The diff ID (uncompressed digest) comes from the content-store label.
	info, err := cs.Info(ctx, layerDesc.Digest)
	if err != nil {
		return fmt.Errorf("layer blob info: %w", err)
	}
	newDiffID, err := digest.Parse(info.Labels[uncompressedLabel])
	if err != nil {
		return fmt.Errorf("parse %s label %q: %w", uncompressedLabel, info.Labels[uncompressedLabel], err)
	}
	fmt.Printf("New layer diff ID: %s\n", newDiffID)

	// 5. New config and manifest referencing the new layer.
	manifestDesc, err := writeImageMetadata(ctx, cs, img.Target(), layerDesc, newDiffID, sourceImage)
	if err != nil {
		return err
	}
	fmt.Printf("New manifest: %s\n", manifestDesc.Digest)

	// 6. Materialize the new layer as a committed snapshot named by the new
	// chain ID, exactly like containerd's unpacker would on pull.
	newChainID := identity.ChainID(append(diffIDs, newDiffID)).String()
	if err := applyDiffLayer(ctx, sn, c.DiffService(), parentChainID, newChainID, layerDesc); err != nil {
		return fmt.Errorf("apply new layer: %w", err)
	}
	fmt.Printf("Committed snapshot for new chain ID: %s\n", newChainID)

	if usage, err := sn.Usage(ctx, newChainID); err == nil {
		fmt.Printf("Committed snapshot size: %d bytes\n", usage.Size)
	}

	// 7. Register the image.
	newImage := images.Image{
		Name:   targetImage,
		Target: manifestDesc,
		Labels: map[string]string{
			"io.containerd.commit.source": sourceImage,
			"io.containerd.commit.time":   time.Now().Format(time.RFC3339),
		},
	}
	created, err := registerImage(ctx, c.ImageService(), newImage)
	if err != nil {
		return err
	}
	fmt.Printf("\n✓ Created image %s (digest %s)\n", created.Name, created.Target.Digest)

	// 8. Verify the new image is usable.
	if err := verifyCommittedImage(ctx, c, snapshotterName, targetImage, len(diffIDs)+1, newDiffID); err != nil {
		return err
	}
	fmt.Printf("✓ New image has %d layers and is unpacked (snapshot %s exists)\n", len(diffIDs)+1, newChainID)

	return nil
}

// registerImage creates the image record, falling back to Update when the
// name already exists.
func registerImage(ctx context.Context, imgService images.Store, newImage images.Image) (images.Image, error) {
	created, err := imgService.Create(ctx, newImage)
	if err != nil {
		if !errdefs.IsAlreadyExists(err) {
			return images.Image{}, fmt.Errorf("create image: %w", err)
		}
		if created, err = imgService.Update(ctx, newImage); err != nil {
			return images.Image{}, fmt.Errorf("update image: %w", err)
		}
	}
	return created, nil
}

// verifyCommittedImage checks the committed image resolves, has the expected
// layer count with newDiffID on top, and reports as unpacked.
func verifyCommittedImage(ctx context.Context, c *client.Client, snapshotterName, targetImage string, wantLayers int, newDiffID digest.Digest) error {
	img, err := c.GetImage(ctx, targetImage)
	if err != nil {
		return fmt.Errorf("get new image: %w", err)
	}
	diffIDs, err := img.RootFS(ctx)
	if err != nil {
		return fmt.Errorf("new image rootfs: %w", err)
	}
	if len(diffIDs) != wantLayers {
		return fmt.Errorf("new image has %d layers, want %d", len(diffIDs), wantLayers)
	}
	if diffIDs[len(diffIDs)-1] != newDiffID {
		return fmt.Errorf("new image top diff ID = %s, want %s", diffIDs[len(diffIDs)-1], newDiffID)
	}
	unpacked, err := img.IsUnpacked(ctx, snapshotterName)
	if err != nil {
		return fmt.Errorf("check unpacked: %w", err)
	}
	if !unpacked {
		return fmt.Errorf("new image reports as not unpacked for %s", snapshotterName)
	}
	return nil
}

// writeImageMetadata writes the new OCI config (diff_ids + history) and a new
// manifest referencing the committed layer, with GC labels so the content
// store keeps the config and layer blobs alive.
func writeImageMetadata(ctx context.Context, cs content.Store, sourceTarget ocispec.Descriptor, layerDesc ocispec.Descriptor, newDiffID digest.Digest, sourceImage string) (ocispec.Descriptor, error) {
	manifest, err := images.Manifest(ctx, cs, sourceTarget, platforms.Default())
	if err != nil {
		return ocispec.Descriptor{}, fmt.Errorf("read source manifest: %w", err)
	}

	configData, err := content.ReadBlob(ctx, cs, manifest.Config)
	if err != nil {
		return ocispec.Descriptor{}, fmt.Errorf("read source config: %w", err)
	}
	var config ocispec.Image
	if err := json.Unmarshal(configData, &config); err != nil {
		return ocispec.Descriptor{}, fmt.Errorf("unmarshal config: %w", err)
	}

	now := time.Now().UTC()
	config.Created = &now
	config.RootFS.DiffIDs = append(config.RootFS.DiffIDs, newDiffID)
	config.History = append(config.History, ocispec.History{
		Created:   &now,
		CreatedBy: "integration-commit",
		Comment:   "committed from " + sourceImage,
	})

	newConfigBytes, err := json.Marshal(config)
	if err != nil {
		return ocispec.Descriptor{}, fmt.Errorf("marshal new config: %w", err)
	}
	newConfigDesc := ocispec.Descriptor{
		MediaType: manifest.Config.MediaType,
		Digest:    digest.FromBytes(newConfigBytes),
		Size:      int64(len(newConfigBytes)),
	}
	if err := content.WriteBlob(ctx, cs, "commit-config-"+newConfigDesc.Digest.Encoded(),
		bytes.NewReader(newConfigBytes), newConfigDesc); err != nil {
		return ocispec.Descriptor{}, fmt.Errorf("write new config: %w", err)
	}

	manifest.Config = newConfigDesc
	manifest.Layers = append(manifest.Layers, layerDesc)
	if manifest.MediaType == "" {
		manifest.MediaType = ocispec.MediaTypeImageManifest
	}

	newManifestBytes, err := json.Marshal(manifest)
	if err != nil {
		return ocispec.Descriptor{}, fmt.Errorf("marshal new manifest: %w", err)
	}
	newManifestDesc := ocispec.Descriptor{
		MediaType: manifest.MediaType,
		Digest:    digest.FromBytes(newManifestBytes),
		Size:      int64(len(newManifestBytes)),
	}

	// GC labels: the manifest blob keeps the config and every layer alive.
	labels := map[string]string{
		"containerd.io/gc.ref.content.config": newConfigDesc.Digest.String(),
	}
	for i, l := range manifest.Layers {
		labels[fmt.Sprintf("containerd.io/gc.ref.content.l.%d", i)] = l.Digest.String()
	}
	if err := content.WriteBlob(ctx, cs, "commit-manifest-"+newManifestDesc.Digest.Encoded(),
		bytes.NewReader(newManifestBytes), newManifestDesc, content.WithLabels(labels)); err != nil {
		return ocispec.Descriptor{}, fmt.Errorf("write new manifest: %w", err)
	}

	return newManifestDesc, nil
}

// applyDiffLayer turns the layer blob into a committed snapshot named by the
// new chain ID (the same Prepare+Apply+Commit sequence containerd's unpacker
// runs on pull).
func applyDiffLayer(ctx context.Context, sn snapshots.Snapshotter, differ diff.Applier, parentChainID, newChainID string, layerDesc ocispec.Descriptor) error {
	if _, err := sn.Stat(ctx, newChainID); err == nil {
		return nil // already materialized
	}

	extractKey := fmt.Sprintf(snapshots.UnpackKeyFormat, uniquePart(), newChainID)
	mounts, err := sn.Prepare(ctx, extractKey, parentChainID)
	if err != nil {
		return fmt.Errorf("prepare extract snapshot: %w", err)
	}

	if _, err := differ.Apply(ctx, layerDesc, mounts); err != nil {
		removeSnapshot(ctx, sn, extractKey)
		return fmt.Errorf("apply layer: %w", err)
	}

	if err := sn.Commit(ctx, newChainID, extractKey); err != nil && !errdefs.IsAlreadyExists(err) {
		removeSnapshot(ctx, sn, extractKey)
		return fmt.Errorf("commit extract snapshot: %w", err)
	}
	return nil
}

func uniquePart() string {
	return fmt.Sprintf("%d", time.Now().UnixNano())
}

func removeSnapshot(ctx context.Context, sn snapshots.Snapshotter, key string) {
	if err := sn.Remove(ctx, key); err != nil && !errdefs.IsNotFound(err) {
		fmt.Fprintf(os.Stderr, "Warning: failed to remove snapshot %s: %v\n", key, err)
	}
}

// writeToGuestUpper mounts the snapshot's ext4 rwlayer and writes markerFile
// under upper/, mimicking how a VM guest persists overlay changes. The VM
// runtime contract is upper/ and work/ at the root of the ext4 (the same
// layout mountBlockRwLayer prepares for extract snapshots).
func writeToGuestUpper(mounts []mount.Mount, markerFile string) error {
	var ext4Path string
	for _, m := range mounts {
		if strings.HasSuffix(m.Type, "ext4") {
			ext4Path = m.Source
			break
		}
	}
	if ext4Path == "" {
		return fmt.Errorf("no ext4 mount found in mounts: %#v", mounts)
	}

	mountPoint, err := os.MkdirTemp("", "snapshot-mount-")
	if err != nil {
		return fmt.Errorf("create mount point: %w", err)
	}
	defer os.RemoveAll(mountPoint)

	cmd := exec.Command("mount", "-o", "loop", ext4Path, mountPoint)
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("mount ext4: %s: %w", string(out), err)
	}
	unmounted := false
	defer func() {
		if unmounted {
			return
		}
		if err := exec.Command("umount", mountPoint).Run(); err != nil {
			fmt.Fprintf(os.Stderr, "Warning: failed to unmount %s: %v\n", mountPoint, err)
		}
	}()

	// Guest overlay layout: changes go under upper/, work/ is the overlay
	// scratch directory.
	for _, d := range []string{"upper", "work"} {
		if err := os.MkdirAll(filepath.Join(mountPoint, d), 0o755); err != nil {
			return fmt.Errorf("create %s dir: %w", d, err)
		}
	}

	markerPath := filepath.Join(mountPoint, "upper", strings.TrimPrefix(markerFile, "/"))
	if err := os.MkdirAll(filepath.Dir(markerPath), 0o755); err != nil {
		return fmt.Errorf("create marker parent dirs: %w", err)
	}
	content := fmt.Sprintf("Committed at %s\n", time.Now().Format(time.RFC3339))
	if err := os.WriteFile(markerPath, []byte(content), 0o644); err != nil {
		return fmt.Errorf("write marker file: %w", err)
	}

	// Unmount before Compare/Commit run: they take the image lock and mount
	// the ext4 read-only themselves.
	if out, err := exec.Command("umount", mountPoint).CombinedOutput(); err != nil {
		return fmt.Errorf("unmount ext4: %s: %w", string(out), err)
	}
	unmounted = true
	return nil
}
