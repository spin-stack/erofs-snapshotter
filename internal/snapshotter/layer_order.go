package snapshotter

// reverseStrings returns a new slice with elements in reversed order.
// This is used to convert between snapshot chain order (newest-first)
// and OCI manifest order (oldest-first) for mkfs.erofs.
//
// OCI Image Manifest specifies layers in bottom-to-top order (base layer at index 0):
// https://github.com/opencontainers/image-spec/blob/main/manifest.md
//
// Snapshot chains from containerd are in newest-first order (most recent at index 0).
// mkfs.erofs rebuild mode expects oldest-first order.
func reverseStrings(ids []string) []string {
	if len(ids) == 0 {
		return nil
	}
	reversed := make([]string, len(ids))
	for i, id := range ids {
		reversed[len(ids)-1-i] = id
	}
	return reversed
}
