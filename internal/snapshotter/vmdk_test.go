package snapshotter

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestParseVMDK(t *testing.T) {
	// Create a test VMDK file with digest-based layer names
	vmdkContent := `# Disk DescriptorFile
version=1
CID=3c2a5784
parentCID=ffffffff
createType="twoGbMaxExtentFlat"

# Extent description
RW 2464 FLAT "/var/lib/snapshotter/snapshots/5/fsmeta.erofs" 0
RW 48 FLAT "/var/lib/snapshotter/snapshots/5/sha256-a3ed95caeb02ffe68cdd9fd84406680ae93d633cb16422d00e8a7c22955b46d4.erofs" 0
RW 1321720 FLAT "/var/lib/snapshotter/snapshots/4/sha256-f1b5933fe4b5f49a89c9298a5b5d232de70e5aa8de8eb8d5ccd0f5b2fd6a4810.erofs" 0
RW 40 FLAT "/var/lib/snapshotter/snapshots/3/sha256-9d7c4de7817d8b6c7e2a9d68d6b1d4d10e1f2a3b4c5d6e7f8a9b0c1d2e3f4a5b.erofs" 0
RW 359960 FLAT "/var/lib/snapshotter/snapshots/2/sha256-1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef.erofs" 0
RW 191080 FLAT "/var/lib/snapshotter/snapshots/1/sha256-fedcba0987654321fedcba0987654321fedcba0987654321fedcba0987654321.erofs" 0

# The Disk Data Base
#DDB

ddb.virtualHWVersion = "4"
ddb.geometry.cylinders = "1861"
ddb.geometry.heads = "16"
ddb.geometry.sectors = "63"
ddb.adapterType = "ide"
`

	// Write test VMDK to temp file
	tmpDir := t.TempDir()
	vmdkPath := filepath.Join(tmpDir, "test.vmdk")
	if err := os.WriteFile(vmdkPath, []byte(vmdkContent), 0644); err != nil {
		t.Fatalf("failed to write test vmdk: %v", err)
	}

	// Parse the VMDK
	layers, err := ParseVMDK(vmdkPath)
	if err != nil {
		t.Fatalf("ParseVMDK failed: %v", err)
	}

	// Verify we got 6 layers (1 fsmeta + 5 layer files)
	if len(layers) != 6 {
		t.Errorf("expected 6 layers, got %d", len(layers))
	}

	// Verify first layer is fsmeta (no digest)
	if layers[0].Digest != "" {
		t.Errorf("fsmeta layer should have empty digest, got %q", layers[0].Digest)
	}
	if !filepath.IsAbs(layers[0].Path) || !contains(layers[0].Path, "fsmeta.erofs") {
		t.Errorf("first layer should be fsmeta.erofs, got %q", layers[0].Path)
	}

	// Verify second layer has correct digest
	expectedDigest := "sha256:a3ed95caeb02ffe68cdd9fd84406680ae93d633cb16422d00e8a7c22955b46d4"
	if layers[1].Digest != expectedDigest {
		t.Errorf("second layer digest = %q, want %q", layers[1].Digest, expectedDigest)
	}

	// Verify sectors are parsed correctly
	if layers[0].Sectors != 2464 {
		t.Errorf("fsmeta sectors = %d, want 2464", layers[0].Sectors)
	}
}

func TestExtractLayerDigests(t *testing.T) {
	layers := []VMDKLayerInfo{
		{Path: "/snapshots/5/fsmeta.erofs", Digest: "", Sectors: 2464},
		{Path: "/snapshots/5/sha256-aaa.erofs", Digest: "sha256:aaa", Sectors: 48},
		{Path: "/snapshots/4/sha256-bbb.erofs", Digest: "sha256:bbb", Sectors: 1000},
		{Path: "/snapshots/3/sha256-ccc.erofs", Digest: "sha256:ccc", Sectors: 500},
	}

	digests := ExtractLayerDigests(layers)

	expected := []string{"sha256:aaa", "sha256:bbb", "sha256:ccc"}
	if !reflect.DeepEqual(digests, expected) {
		t.Errorf("ExtractLayerDigests = %v, want %v", digests, expected)
	}
}

func TestReverseDigests(t *testing.T) {
	input := []string{"sha256:aaa", "sha256:bbb", "sha256:ccc"}
	expected := []string{"sha256:ccc", "sha256:bbb", "sha256:aaa"}

	result := ReverseDigests(input)
	if !reflect.DeepEqual(result, expected) {
		t.Errorf("ReverseDigests = %v, want %v", result, expected)
	}
}

func TestParseVMDK_WithFallbackNaming(t *testing.T) {
	// Test VMDK with fallback naming (snapshot-*.erofs)
	vmdkContent := `# Disk DescriptorFile
version=1
CID=12345678
parentCID=ffffffff
createType="twoGbMaxExtentFlat"

# Extent description
RW 2464 FLAT "/var/lib/snapshotter/snapshots/5/fsmeta.erofs" 0
RW 48 FLAT "/var/lib/snapshotter/snapshots/5/snapshot-5.erofs" 0
RW 1000 FLAT "/var/lib/snapshotter/snapshots/4/snapshot-4.erofs" 0

#DDB
ddb.virtualHWVersion = "4"
`

	tmpDir := t.TempDir()
	vmdkPath := filepath.Join(tmpDir, "test.vmdk")
	if err := os.WriteFile(vmdkPath, []byte(vmdkContent), 0644); err != nil {
		t.Fatalf("failed to write test vmdk: %v", err)
	}

	layers, err := ParseVMDK(vmdkPath)
	if err != nil {
		t.Fatalf("ParseVMDK failed: %v", err)
	}

	// Should have 3 layers
	if len(layers) != 3 {
		t.Errorf("expected 3 layers, got %d", len(layers))
	}

	// Fallback naming should have empty digests
	digests := ExtractLayerDigests(layers)
	if len(digests) != 0 {
		t.Errorf("fallback naming should produce empty digests, got %v", digests)
	}
}

func TestParseVMDK_LayerOrderVerification(t *testing.T) {
	// This test verifies that the VMDK layer order matches expected order
	// based on container image manifest conventions:
	// - Manifest: layers listed bottom-to-top (oldest first)
	// - VMDK: layers listed top-to-bottom (newest first, after fsmeta)

	// Use real SHA256 hashes for testing (64 hex chars)
	layer3Digest := "3333333333333333333333333333333333333333333333333333333333333333"
	layer2Digest := "2222222222222222222222222222222222222222222222222222222222222222"
	layer1Digest := "1111111111111111111111111111111111111111111111111111111111111111"

	// Simulate a 3-layer image:
	// Manifest order (oldest first): layer1, layer2, layer3
	// VMDK order (newest first): fsmeta, layer3, layer2, layer1
	vmdkContent := `# Disk DescriptorFile
version=1
CID=abcd1234
parentCID=ffffffff
createType="twoGbMaxExtentFlat"

# Extent description - order matters!
RW 2464 FLAT "/snapshots/view/fsmeta.erofs" 0
RW 100 FLAT "/snapshots/3/sha256-` + layer3Digest + `.erofs" 0
RW 200 FLAT "/snapshots/2/sha256-` + layer2Digest + `.erofs" 0
RW 300 FLAT "/snapshots/1/sha256-` + layer1Digest + `.erofs" 0

#DDB
ddb.virtualHWVersion = "4"
`

	tmpDir := t.TempDir()
	vmdkPath := filepath.Join(tmpDir, "test.vmdk")
	if err := os.WriteFile(vmdkPath, []byte(vmdkContent), 0644); err != nil {
		t.Fatalf("failed to write test vmdk: %v", err)
	}

	layers, err := ParseVMDK(vmdkPath)
	if err != nil {
		t.Fatalf("ParseVMDK failed: %v", err)
	}

	digests := ExtractLayerDigests(layers)

	// VMDK order should be: layer3 (newest), layer2, layer1 (oldest)
	expectedVMDKOrder := []string{
		"sha256:" + layer3Digest,
		"sha256:" + layer2Digest,
		"sha256:" + layer1Digest,
	}

	if !reflect.DeepEqual(digests, expectedVMDKOrder) {
		t.Errorf("VMDK layer order = %v, want %v", digests, expectedVMDKOrder)
	}

	// When comparing with manifest, we need to reverse the VMDK order
	// Manifest order: layer1 (oldest), layer2, layer3 (newest)
	manifestOrder := []string{
		"sha256:" + layer1Digest,
		"sha256:" + layer2Digest,
		"sha256:" + layer3Digest,
	}

	reversedVMDK := ReverseDigests(digests)
	if !reflect.DeepEqual(reversedVMDK, manifestOrder) {
		t.Errorf("reversed VMDK order = %v, want manifest order %v", reversedVMDK, manifestOrder)
	}
}

func TestParseVMDK_NotFound(t *testing.T) {
	_, err := ParseVMDK("/nonexistent/path/to/vmdk")
	if err == nil {
		t.Error("ParseVMDK should fail for nonexistent file")
	}
}

func contains(s, substr string) bool {
	return filepath.Base(s) == substr || filepath.Base(s) == filepath.Base(substr)
}
