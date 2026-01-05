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

package erofsutils

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

	"github.com/aledbf/nexuserofs/internal/stringutil"
)

// buildTarErofsArgs constructs the command-line arguments for mkfs.erofs
// when converting a tar stream to an EROFS image.
//
// The arguments follow the pattern: --tar=f --aufs --quiet -Enoinline_data [extraOpts] [-U uuid] FILE
// When no SOURCE is specified after FILE, mkfs.erofs reads from stdin automatically.
func buildTarErofsArgs(layerPath, uuid string, mkfsExtraOpts []string) []string {
	args := append([]string{"--tar=f", "--aufs", "--quiet", "-Enoinline_data"}, mkfsExtraOpts...)
	if uuid != "" {
		args = append(args, "-U", uuid)
	}
	args = append(args, layerPath)
	return args
}

// ConvertTarErofs converts a tar stream to an EROFS image.
// The tar content is read from stdin (r) and written to layerPath.
func ConvertTarErofs(ctx context.Context, r io.Reader, layerPath, uuid string, mkfsExtraOpts []string) error {
	args := buildTarErofsArgs(layerPath, uuid, mkfsExtraOpts)
	cmd := exec.CommandContext(ctx, "mkfs.erofs", args...)

	// Use StdinPipe for better control and error visibility
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return fmt.Errorf("failed to create stdin pipe: %w", err)
	}

	// Capture both stdout and stderr
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Start(); err != nil {
		stdin.Close()
		return fmt.Errorf("failed to start mkfs.erofs: %w", err)
	}

	// Copy tar data to stdin in a goroutine
	type copyResult struct {
		n   int64
		err error
	}
	copyDone := make(chan copyResult, 1)
	go func() {
		n, err := io.Copy(stdin, r)
		stdin.Close()
		copyDone <- copyResult{n, err}
	}()

	// Wait for command to complete
	waitErr := cmd.Wait()
	result := <-copyDone

	// Log copy result
	if result.err != nil {
		log.G(ctx).WithError(result.err).Errorf("failed to pipe tar data to mkfs.erofs (wrote %d bytes)", result.n)
	} else {
		log.G(ctx).Debugf("piped %d bytes of tar data to mkfs.erofs", result.n)
	}

	// Prioritize mkfs.erofs error since it tells us WHY it failed
	if waitErr != nil {
		return fmt.Errorf("mkfs.erofs %v failed (piped %d bytes): stdout=%s stderr=%s: %w",
			args,
			result.n,
			stringutil.TruncateOutput(stdout.Bytes(), 512),
			stringutil.TruncateOutput(stderr.Bytes(), 512),
			waitErr)
	}

	// If mkfs.erofs succeeded but we had a copy error, that's unexpected
	if result.err != nil {
		return fmt.Errorf("mkfs.erofs succeeded but pipe failed (wrote %d bytes): %w", result.n, result.err)
	}

	log.G(ctx).Debugf("mkfs.erofs %v: stdout=%s stderr=%s", args, stdout.String(), stderr.String())
	return nil
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
		return fmt.Errorf("failed to create temporary tar file: %w", err)
	}
	defer os.Remove(tarFile.Name())
	defer tarFile.Close()

	// Use TeeReader to process the input once while saving it to disk
	teeReader := io.TeeReader(r, tarFile)

	args := buildTarIndexArgs(layerPath, mkfsExtraOpts)
	cmd := exec.CommandContext(ctx, "mkfs.erofs", args...)

	// Use StdinPipe for better control and error visibility
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return fmt.Errorf("failed to create stdin pipe: %w", err)
	}

	// Capture both stdout and stderr
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Start(); err != nil {
		stdin.Close()
		return fmt.Errorf("failed to start mkfs.erofs: %w", err)
	}

	// Copy tar data to stdin in a goroutine
	type copyResult struct {
		n   int64
		err error
	}
	copyDone := make(chan copyResult, 1)
	go func() {
		n, err := io.Copy(stdin, teeReader)
		stdin.Close()
		copyDone <- copyResult{n, err}
	}()

	// Wait for command to complete
	waitErr := cmd.Wait()
	result := <-copyDone

	// Log copy result
	if result.err != nil {
		log.G(ctx).WithError(result.err).Errorf("failed to pipe tar data to mkfs.erofs --tar=i (wrote %d bytes)", result.n)
	} else {
		log.G(ctx).Debugf("piped %d bytes of tar data to mkfs.erofs --tar=i", result.n)
	}

	// Prioritize mkfs.erofs error since it tells us WHY it failed
	if waitErr != nil {
		return fmt.Errorf("tar index generation failed: mkfs.erofs %v (piped %d bytes): stdout=%s stderr=%s: %w",
			args,
			result.n,
			stringutil.TruncateOutput(stdout.Bytes(), 512),
			stringutil.TruncateOutput(stderr.Bytes(), 512),
			waitErr)
	}

	// If mkfs.erofs succeeded but we had a copy error, that's unexpected
	if result.err != nil {
		return fmt.Errorf("mkfs.erofs --tar=i succeeded but pipe failed (wrote %d bytes): %w", result.n, result.err)
	}

	log.G(ctx).Debugf("mkfs.erofs --tar=i %v: stdout=%s stderr=%s", args, stdout.String(), stderr.String())

	// Open layerPath for appending
	f, err := os.OpenFile(layerPath, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return fmt.Errorf("failed to open layer file for appending: %w", err)
	}
	defer f.Close()

	// Rewind the temporary file
	if _, err := tarFile.Seek(0, 0); err != nil {
		return fmt.Errorf("failed to seek to the beginning of tar file: %w", err)
	}

	// Append tar content
	if _, err := io.Copy(f, tarFile); err != nil {
		return fmt.Errorf("failed to append tar to layer: %w", err)
	}

	log.G(ctx).Infof("Successfully generated EROFS layer with tar index and tar content: %s", layerPath)

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
// After extracting the path, it validates the directory by checking for the
// ".erofslayer" marker file. This marker is created by the EROFS snapshotter
// to indicate a directory is managed by EROFS.
// If the marker is missing, ErrNotImplemented is returned, allowing the EROFS
// differ to fall back to other differs (e.g., the walking differ).
func MountsToLayer(mounts []mount.Mount) (string, error) {
	if len(mounts) == 0 {
		return "", fmt.Errorf("no mounts provided: %w", errdefs.ErrNotImplemented)
	}

	layer, err := extractLayerPath(mounts)
	if err != nil {
		return "", err
	}

	// Validate the layer is prepared by the EROFS snapshotter
	if _, err := os.Stat(filepath.Join(layer, ErofsLayerMarker)); err != nil {
		return "", fmt.Errorf("mount layer type must be erofs-layer: %w", errdefs.ErrNotImplemented)
	}
	return layer, nil
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

	// LayerBlobFilename is the filename for EROFS layer blobs within
	// a snapshot directory.
	LayerBlobFilename = "layer.erofs"
)
