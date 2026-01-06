package snapshotter

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

// VMDKLayerInfo contains information about a layer extracted from a VMDK descriptor.
type VMDKLayerInfo struct {
	// Path is the full path to the EROFS layer file
	Path string
	// Digest is the layer digest extracted from the filename (if digest-based naming)
	// Format: sha256:abc123... (empty if not a digest-based filename)
	Digest string
	// Sectors is the size in 512-byte sectors
	Sectors int64
}

// layerPathRegex matches FLAT extent lines in VMDK descriptors.
// Format: RW <sectors> FLAT "<path>" <offset>
var layerPathRegex = regexp.MustCompile(`^RW\s+(\d+)\s+FLAT\s+"([^"]+)"\s+\d+`)

// digestFilenameRegex extracts the digest from digest-based filenames.
// Format: sha256-<hex>.erofs
var digestFilenameRegex = regexp.MustCompile(`^sha256-([a-f0-9]+)\.erofs$`)

// ParseVMDK reads a VMDK descriptor file and extracts layer information.
// Returns layers in the order they appear in the VMDK (fsmeta first, then layers
// from newest/top to oldest/bottom).
//
// VMDK layer order is the REVERSE of OCI manifest order:
// - OCI manifest: [layer_0, layer_1, ..., layer_n] (bottom to top, base first)
// - VMDK:         [fsmeta, layer_n, ..., layer_1, layer_0] (top to bottom, newest first)
//
// See: https://github.com/opencontainers/image-spec/blob/main/manifest.md
// See: https://github.com/libyal/libvmdk/blob/main/documentation/VMWare%20Virtual%20Disk%20Format%20(VMDK).asciidoc
func ParseVMDK(vmdkPath string) ([]VMDKLayerInfo, error) {
	f, err := os.Open(vmdkPath)
	if err != nil {
		return nil, fmt.Errorf("open vmdk: %w", err)
	}
	defer f.Close()

	var layers []VMDKLayerInfo
	scanner := bufio.NewScanner(f)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())

		matches := layerPathRegex.FindStringSubmatch(line)
		if matches == nil {
			continue
		}

		var sectors int64
		if _, err := fmt.Sscanf(matches[1], "%d", &sectors); err != nil {
			// If we can't parse sectors, use 0 (non-critical for our purposes)
			sectors = 0
		}
		path := matches[2]

		layer := VMDKLayerInfo{
			Path:    path,
			Sectors: sectors,
		}

		// Extract digest from filename if it's a digest-based name
		filename := filepath.Base(path)
		if digestMatches := digestFilenameRegex.FindStringSubmatch(filename); digestMatches != nil {
			layer.Digest = "sha256:" + digestMatches[1]
		}

		layers = append(layers, layer)
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan vmdk: %w", err)
	}

	return layers, nil
}

// ExtractLayerDigests extracts just the digests from VMDK layers, filtering out
// non-layer entries (like fsmeta.erofs) and returning digests in VMDK order
// (newest/top layer first).
func ExtractLayerDigests(layers []VMDKLayerInfo) []string {
	var digests []string
	for _, layer := range layers {
		// Skip fsmeta entries and non-digest-based files
		if layer.Digest == "" {
			continue
		}
		// Skip fsmeta (it doesn't have a layer digest)
		if strings.HasSuffix(layer.Path, "fsmeta.erofs") {
			continue
		}
		digests = append(digests, layer.Digest)
	}
	return digests
}

// ReverseDigests reverses a slice of digests.
// Use this to convert between VMDK order (top-to-bottom) and OCI manifest order (bottom-to-top).
// See: https://github.com/opencontainers/image-spec/blob/main/manifest.md
func ReverseDigests(digests []string) []string {
	reversed := make([]string, len(digests))
	for i, d := range digests {
		reversed[len(digests)-1-i] = d
	}
	return reversed
}

// ParseLayerManifest reads a layer manifest file and returns the digests in VMDK order.
// The manifest file contains one digest per line (sha256:hex...), newest layer first.
// This is the authoritative source for verifying VMDK layer order.
func ParseLayerManifest(manifestPath string) ([]string, error) {
	f, err := os.Open(manifestPath)
	if err != nil {
		return nil, fmt.Errorf("open manifest: %w", err)
	}
	defer f.Close()

	var digests []string
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		// Validate digest format
		if !strings.HasPrefix(line, "sha256:") {
			continue
		}
		digests = append(digests, line)
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan manifest: %w", err)
	}

	return digests, nil
}
