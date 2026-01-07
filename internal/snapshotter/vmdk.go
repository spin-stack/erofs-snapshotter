package snapshotter

import (
	"bufio"
	"fmt"
	"os"
	"regexp"
	"strings"

	"github.com/opencontainers/go-digest"

	"github.com/aledbf/nexus-erofs/internal/erofs"
)

// VMDKLayerInfo contains information about a layer extracted from a VMDK descriptor.
type VMDKLayerInfo struct {
	// Path is the full path to the EROFS layer file
	Path string
	// Digest is the layer digest extracted from the filename (if digest-based naming)
	// Empty if not a digest-based filename (e.g., fsmeta.erofs)
	Digest digest.Digest
	// Sectors is the size in 512-byte sectors
	Sectors int64
}

// layerPathRegex matches FLAT extent lines in VMDK descriptors.
// Format: RW <sectors> FLAT "<path>" <offset>
var layerPathRegex = regexp.MustCompile(`^RW\s+(\d+)\s+FLAT\s+"([^"]+)"\s+\d+`)

// ParseVMDK reads a VMDK descriptor file and extracts layer information.
// Returns layers in the order they appear in the VMDK (fsmeta first, then layers
// from oldest/base to newest/top - matching OCI manifest order).
//
// VMDK layer order matches OCI manifest order:
// - OCI manifest: [layer_0, layer_1, ..., layer_n] (oldest to newest)
// - VMDK:         [fsmeta, layer_0, layer_1, ..., layer_n] (oldest to newest)
//
// See: https://github.com/opencontainers/image-spec/blob/main/manifest.md
// See: https://man.archlinux.org/man/extra/erofs-utils/mkfs.erofs.1.en
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
			Digest:  erofs.DigestFromLayerBlobPath(path),
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
// (oldest/base layer first, matching OCI manifest order).
func ExtractLayerDigests(layers []VMDKLayerInfo) []digest.Digest {
	var digests []digest.Digest
	for _, layer := range layers {
		// Skip fsmeta entries and non-digest-based files
		if layer.Digest == "" {
			continue
		}
		digests = append(digests, layer.Digest)
	}
	return digests
}

// ReverseDigests reverses a slice of digests.
// Note: VMDK and OCI manifest now use the same order (oldest first), so this is
// mainly useful for converting from snapshot chain order (newest first).
func ReverseDigests(digests []digest.Digest) []digest.Digest {
	reversed := make([]digest.Digest, len(digests))
	for i, d := range digests {
		reversed[len(digests)-1-i] = d
	}
	return reversed
}

// ParseLayerManifest reads a layer manifest file and returns the digests in VMDK/OCI order.
// The manifest file contains one digest per line (sha256:hex...), oldest/base layer first.
// This is the authoritative source for verifying VMDK layer order.
func ParseLayerManifest(manifestPath string) ([]digest.Digest, error) {
	f, err := os.Open(manifestPath)
	if err != nil {
		return nil, fmt.Errorf("open manifest: %w", err)
	}
	defer f.Close()

	var digests []digest.Digest
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		d, err := digest.Parse(line)
		if err != nil {
			// Skip invalid digest lines
			continue
		}
		digests = append(digests, d)
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan manifest: %w", err)
	}

	return digests, nil
}
