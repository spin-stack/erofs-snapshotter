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
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/containerd/containerd/v2/core/mount"
	"github.com/containerd/errdefs"
	"github.com/containerd/log"
	"github.com/opencontainers/go-digest"

	"github.com/spin-stack/erofs-snapshotter/internal/stringutil"
)

// buildTarErofsArgs constructs the command-line arguments for mkfs.erofs
// when converting a tar stream to an EROFS image.
//
// The arguments follow the pattern: --tar=f --aufs --quiet -Enoinline_data --sort=none [extraOpts] [-U uuid] FILE
// When no SOURCE is specified after FILE, mkfs.erofs reads from stdin automatically.
//
// The --sort=none option avoids unnecessary data writes when strict data order is not required.
// It takes effect when -Enoinline_data is specified and no compression is applied (the default).
// See: https://www.mail-archive.com/linux-erofs@lists.ozlabs.org/msg11685.html
func buildTarErofsArgs(layerPath, uuid string, mkfsExtraOpts []string) []string {
	args := append([]string{"--tar=f", "--aufs", "--quiet", "-Enoinline_data", "--sort=none"}, mkfsExtraOpts...)
	if uuid != "" {
		args = append(args, "-U", uuid)
	}
	args = append(args, layerPath)
	return args
}

// buildTarIndexArgs constructs the command-line arguments for mkfs.erofs
// when generating a tar index.
//
// The arguments follow the pattern: --tar=i --aufs --quiet [extraOpts] FILE
// When no SOURCE is specified after FILE, mkfs.erofs reads from stdin automatically.
func buildTarIndexArgs(layerPath string, mkfsExtraOpts []string) []string {
	args := append([]string{"--tar=i", "--aufs", "--quiet"}, mkfsExtraOpts...)
	args = append(args, layerPath)
	return args
}

// runMkfsWithStdin pipes data from reader to mkfs.erofs and captures output.
// Returns the number of bytes piped and any error.
func runMkfsWithStdin(ctx context.Context, r io.Reader, args []string) (int64, error) {
	cmd := exec.CommandContext(ctx, "mkfs.erofs", args...)

	stdin, err := cmd.StdinPipe()
	if err != nil {
		return 0, fmt.Errorf("create stdin pipe: %w", err)
	}

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Start(); err != nil {
		stdin.Close()
		return 0, fmt.Errorf("start mkfs.erofs: %w", err)
	}

	// Copy data to stdin in a goroutine
	type copyResult struct {
		n   int64
		err error
	}
	copyDone := make(chan copyResult, 1)
	go func() {
		n, copyErr := io.Copy(stdin, r)
		stdin.Close()
		copyDone <- copyResult{n, copyErr}
	}()

	waitErr := cmd.Wait()
	result := <-copyDone

	if result.err != nil {
		log.G(ctx).WithError(result.err).Debugf("pipe error (wrote %d bytes)", result.n)
	}

	if waitErr != nil {
		return result.n, fmt.Errorf("mkfs.erofs %v failed (piped %d bytes): stdout=%s stderr=%s: %w",
			args, result.n,
			stringutil.TruncateOutput(stdout.Bytes(), 512),
			stringutil.TruncateOutput(stderr.Bytes(), 512),
			waitErr)
	}

	if result.err != nil {
		return result.n, fmt.Errorf("mkfs.erofs succeeded but pipe failed (wrote %d bytes): %w", result.n, result.err)
	}

	log.G(ctx).Debugf("mkfs.erofs %v: piped %d bytes", args, result.n)
	return result.n, nil
}

// ConvertTarErofs converts a tar stream to an EROFS image.
// The tar content is read from stdin (r) and written to layerPath.
func ConvertTarErofs(ctx context.Context, r io.Reader, layerPath, uuid string, mkfsExtraOpts []string) error {
	args := buildTarErofsArgs(layerPath, uuid, mkfsExtraOpts)
	_, err := runMkfsWithStdin(ctx, r, args)
	return err
}

// GenerateTarIndexAndAppendTar calculates tar index using --tar=i option
// and appends the original tar content to create a combined EROFS layer.
//
// The `--tar=i` option instructs mkfs.erofs to only generate the tar index
// for the tar content. The resulting file structure is:
// [Tar index][Original tar content]
func GenerateTarIndexAndAppendTar(ctx context.Context, r io.Reader, layerPath string, mkfsExtraOpts []string) error {
	// Create a temporary file for storing the tar content
	tarFile, err := os.CreateTemp("", "erofs-tar-*")
	if err != nil {
		return fmt.Errorf("create temporary tar file: %w", err)
	}
	defer os.Remove(tarFile.Name())
	defer tarFile.Close()

	// Use TeeReader to process the input once while saving it to disk
	teeReader := io.TeeReader(r, tarFile)

	args := buildTarIndexArgs(layerPath, mkfsExtraOpts)
	if _, err := runMkfsWithStdin(ctx, teeReader, args); err != nil {
		return fmt.Errorf("tar index generation: %w", err)
	}

	// Open layerPath for appending
	f, err := os.OpenFile(layerPath, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return fmt.Errorf("open layer file for appending: %w", err)
	}
	defer f.Close()

	// Rewind the temporary file and append tar content
	if _, err := tarFile.Seek(0, 0); err != nil {
		return fmt.Errorf("seek to beginning of tar file: %w", err)
	}

	if _, err := io.Copy(f, tarFile); err != nil {
		return fmt.Errorf("append tar to layer: %w", err)
	}

	log.G(ctx).Debugf("generated EROFS layer with tar index: %s", layerPath)
	return nil
}

// ConvertErofs converts a directory to an EROFS image
func ConvertErofs(ctx context.Context, layerPath string, srcDir string, mkfsExtraOpts []string) error {
	args := append([]string{"--quiet", "-Enoinline_data"}, mkfsExtraOpts...)
	args = append(args, layerPath, srcDir)
	cmd := exec.CommandContext(ctx, "mkfs.erofs", args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("mkfs.erofs %v failed: %s: %w", args, stringutil.TruncateOutput(out, 256), err)
	}
	log.G(ctx).Debugf("mkfs.erofs %v: %s", args, stringutil.TruncateOutput(out, 256))
	return nil
}

// MountsToLayer extracts the snapshot layer directory from mount specifications
// for EROFS differ operations.
//
// The function identifies the layer directory from various mount types:
//   - mkfs/* mounts: directory containing the source file
//   - bind/erofs mounts: parent directory of mount source
//   - overlay mounts: upperdir parent, or top lowerdir parent for read-only
//
// Validation is performed to ensure the mounts are from the EROFS snapshotter:
//   - If any mount has type "erofs" or "format/erofs", it's trusted as EROFS
//   - Otherwise, the ".erofslayer" marker file must exist in the layer directory
//
// If validation fails, ErrNotImplemented is returned, allowing the EROFS
// differ to fall back to other differs (e.g., the walking differ).
func MountsToLayer(mounts []mount.Mount) (string, error) {
	if len(mounts) == 0 {
		return "", fmt.Errorf("no mounts provided: %w", errdefs.ErrNotImplemented)
	}

	layer, err := extractLayerPath(mounts)
	if err != nil {
		return "", err
	}

	// Trust EROFS mount types - they come directly from our snapshotter
	if hasErofsMountType(mounts) {
		return layer, nil
	}

	// For other mount types (bind, overlay), require the marker file
	if _, err := os.Stat(filepath.Join(layer, ErofsLayerMarker)); err != nil {
		return "", fmt.Errorf("mount layer type must be erofs-layer: %w", errdefs.ErrNotImplemented)
	}
	return layer, nil
}

// hasErofsMountType returns true if any mount has an EROFS-related type.
// This includes "erofs", "format/erofs", or any type ending in "/erofs".
func hasErofsMountType(mounts []mount.Mount) bool {
	for _, m := range mounts {
		baseType := mountBaseType(m.Type)
		if baseType == "erofs" {
			return true
		}
	}
	return false
}

// extractLayerPath determines the layer directory from mount specifications.
func extractLayerPath(mounts []mount.Mount) (string, error) {
	// mkfs/* mounts indicate the snapshot layer directly
	if strings.HasPrefix(mounts[0].Type, "mkfs/") {
		return filepath.Dir(mounts[0].Source), nil
	}

	// For other mount types, examine the last mount entry
	mnt := mounts[len(mounts)-1]
	baseType := mountBaseType(mnt.Type)

	switch baseType {
	case "bind":
		return layerFromBindMount(mnt.Source), nil
	case "erofs":
		return filepath.Dir(mnt.Source), nil
	case "ext4":
		// ext4 is the writable layer in active snapshots.
		// The layer directory is the parent of the .img file.
		return filepath.Dir(mnt.Source), nil
	case "overlay":
		return layerFromOverlay(mounts, mnt)
	default:
		return "", fmt.Errorf("unsupported filesystem type %q for erofs differ: %w", mnt.Type, errdefs.ErrNotImplemented)
	}
}

// layerFromBindMount extracts the snapshot layer path from a bind mount source.
//
// For directory mode: source is .../snapshots/{id}/fs, layer is parent .../snapshots/{id}
// For block mode: source is .../snapshots/{id}/rw/upper, layer is grandparent .../snapshots/{id}
func layerFromBindMount(source string) string {
	parent := filepath.Dir(source)
	// Block mode has source at .../rw/upper, so parent is .../rw
	// We need to go up one more level to get the snapshot root
	if filepath.Base(parent) == "rw" {
		return filepath.Dir(parent)
	}
	return parent
}

// mountBaseType extracts the base type from a potentially compound mount type.
// For example, "format/mkdir/overlay" returns "overlay".
func mountBaseType(mountType string) string {
	parts := strings.Split(mountType, "/")
	return parts[len(parts)-1]
}

// layerFromOverlay extracts the layer path from overlay mount options.
// It prefers upperdir (for read-write layers) and falls back to the first
// lowerdir (for read-only layers).
//
// For block mode overlays where upperdir is .../rw/upper, it goes up two levels
// to reach the snapshot root where the .erofslayer marker is located.
func layerFromOverlay(mounts []mount.Mount, mnt mount.Mount) (string, error) {
	var upperLayer, lowerLayer string

	for _, opt := range mnt.Options {
		key, value, ok := strings.Cut(opt, "=")
		if !ok {
			continue
		}
		switch key {
		case "upperdir":
			upperLayer = layerFromUpperdir(value)
		case "lowerdir":
			// For lowerdir, use the first mount source as the top lower layer
			lowerLayer = filepath.Dir(mounts[0].Source)
		}
	}

	if upperLayer != "" {
		return upperLayer, nil
	}
	if lowerLayer != "" {
		return lowerLayer, nil
	}
	return "", fmt.Errorf("overlay mount has no upperdir or lowerdir: %w", errdefs.ErrNotImplemented)
}

// layerFromUpperdir extracts the snapshot layer path from the overlay upperdir.
//
// For directory mode: upperdir is .../snapshots/{id}/fs, layer is parent .../snapshots/{id}
// For block mode: upperdir is .../snapshots/{id}/rw/upper, layer is grandparent .../snapshots/{id}
func layerFromUpperdir(upperdir string) string {
	parent := filepath.Dir(upperdir)
	// Block mode has upperdir at .../rw/upper, so parent is .../rw
	// We need to go up one more level to get the snapshot root
	if filepath.Base(parent) == "rw" {
		return filepath.Dir(parent)
	}
	return parent
}

// SupportGenerateFromTar checks if the installed version of mkfs.erofs supports
// the tar mode (--tar option).
func SupportGenerateFromTar() (bool, error) {
	cmd := exec.Command("mkfs.erofs", "--help")
	output, err := cmd.CombinedOutput()
	if err != nil {
		return false, fmt.Errorf("failed to run mkfs.erofs --help: %w", err)
	}

	return bytes.Contains(output, []byte("--tar=")), nil
}

const (
	// ErofsLayerMarker is the marker file name for EROFS layers.
	// This marker is created by the EROFS snapshotter and checked by
	// the EROFS differ to validate that a directory is a genuine
	// EROFS snapshotter layer.
	ErofsLayerMarker = ".erofslayer"

	// LayerBlobPattern is the glob pattern for finding EROFS layer blobs
	// within a snapshot directory. Layer files are named using their
	// content digest (e.g., sha256-abc123...erofs).
	LayerBlobPattern = "sha256-*.erofs"

	// layerBlobExtension is the file extension for EROFS layer blobs.
	layerBlobExtension = ".erofs"

	// erofsMinBlockSizeForFsmeta is the minimum block size required for fsmeta merge.
	// Layers created with tar index mode use 512-byte chunks which are incompatible
	// with fsmeta merge that requires 4096-byte block size.
	erofsMinBlockSizeForFsmeta = 4096

	// EROFS on-disk format constants.
	// Reference: https://docs.kernel.org/filesystems/erofs.html
	// Source: https://git.kernel.org/pub/scm/linux/kernel/git/torvalds/linux.git/tree/fs/erofs/erofs_fs.h

	// erofsSuperblocOffset is the byte offset of the EROFS superblock from the start of the image.
	erofsSuperblocOffset = 1024

	// erofsMagic is the EROFS magic number (0xE0F5E1E2 in little-endian).
	erofsMagic = 0xE0F5E1E2

	// erofsBlkszBitsOffset is the byte offset of the blkszbits field within the superblock.
	// Superblock layout: magic(4) + checksum(4) + feature_compat(4) + blkszbits(1).
	erofsBlkszBitsOffset = 12
)

// GetBlockSize reads the block size from an EROFS layer file.
// Returns the block size in bytes, or an error if the file is not a valid EROFS image.
func GetBlockSize(path string) (int, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, fmt.Errorf("failed to open EROFS file: %w", err)
	}
	defer f.Close()

	// Read the superblock (we need magic + blkszbits)
	buf := make([]byte, 16)
	if _, err := f.ReadAt(buf, erofsSuperblocOffset); err != nil {
		return 0, fmt.Errorf("failed to read EROFS superblock: %w", err)
	}

	// Check magic number (little-endian)
	magic := uint32(buf[0]) | uint32(buf[1])<<8 | uint32(buf[2])<<16 | uint32(buf[3])<<24
	if magic != erofsMagic {
		return 0, fmt.Errorf("invalid EROFS magic: 0x%X (expected 0x%X)", magic, erofsMagic)
	}

	// Get block size bits (log2 of block size)
	blkszbits := buf[erofsBlkszBitsOffset]
	blockSize := 1 << blkszbits

	return blockSize, nil
}

// CanMergeFsmeta checks if all EROFS layers have block sizes compatible with fsmeta merge.
// Returns true if all layers have block size >= 4096, false otherwise.
func CanMergeFsmeta(layerPaths []string) bool {
	for _, path := range layerPaths {
		blockSize, err := GetBlockSize(path)
		if err != nil {
			// If we can't read the block size, assume it's incompatible
			return false
		}
		if blockSize < erofsMinBlockSizeForFsmeta {
			return false
		}
	}
	return true
}

// LayerBlobFilename returns the filename for an EROFS layer blob based on its digest.
// The digest format "sha256:abc123..." is converted to "sha256-abc123....erofs".
// This allows easy correlation between layer files and container registry manifests.
func LayerBlobFilename(d string) string {
	// Replace ":" with "-" to make it filesystem-safe
	// sha256:abc123... -> sha256-abc123....erofs
	safeName := strings.ReplaceAll(d, ":", "-")
	return safeName + layerBlobExtension
}

// DigestFromLayerBlobPath extracts the digest from an EROFS layer blob path.
// The filename format "sha256-abc123....erofs" is converted back to "sha256:abc123...".
// Returns empty digest if the filename doesn't match the expected format.
func DigestFromLayerBlobPath(path string) digest.Digest {
	filename := filepath.Base(path)

	// Must have .erofs extension
	if !strings.HasSuffix(filename, layerBlobExtension) {
		return ""
	}

	// Remove extension: sha256-abc123.erofs -> sha256-abc123
	name := strings.TrimSuffix(filename, layerBlobExtension)

	// Convert back to digest format: sha256-abc123 -> sha256:abc123
	digestStr := strings.Replace(name, "-", ":", 1)

	// Validate using the proper digest parser
	d, err := digest.Parse(digestStr)
	if err != nil {
		return ""
	}

	return d
}
