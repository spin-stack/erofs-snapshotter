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

// ReverseDigests reverses a slice of digests (useful for comparing VMDK order
// with manifest order, since they use opposite conventions).
func ReverseDigests(digests []string) []string {
	reversed := make([]string, len(digests))
	for i, d := range digests {
		reversed[len(digests)-1-i] = d
	}
	return reversed
}
