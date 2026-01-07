package snapshotter

// LayerSequence holds layer IDs with explicit ordering.
// Snapshot chains use newest-first order, but mkfs.erofs and VMDK need oldest-first.
// This type makes the conversion explicit.
type LayerSequence struct {
	IDs           []string
	IsNewestFirst bool
}

// NewNewestFirst creates a LayerSequence from snapshot chain order (newest-first).
func NewNewestFirst(ids []string) LayerSequence {
	return LayerSequence{IDs: ids, IsNewestFirst: true}
}

// Reverse returns a new LayerSequence with IDs in reversed order.
func (s LayerSequence) Reverse() LayerSequence {
	reversed := make([]string, len(s.IDs))
	for i, id := range s.IDs {
		reversed[len(s.IDs)-1-i] = id
	}
	return LayerSequence{IDs: reversed, IsNewestFirst: !s.IsNewestFirst}
}

// Len returns the number of layers.
func (s LayerSequence) Len() int {
	return len(s.IDs)
}

// IsEmpty returns true if there are no layers.
func (s LayerSequence) IsEmpty() bool {
	return len(s.IDs) == 0
}
