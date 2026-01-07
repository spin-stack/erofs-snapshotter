// Package snapshotter implements the nexus-erofs containerd snapshotter.
//
// This is a VM-only EROFS snapshotter for containerd, designed exclusively
// for use with qemubox and similar VM runtimes. It does NOT mount filesystems
// on the host - it returns raw file paths that VM runtimes pass to guests as
// virtio-blk devices.
//
// # Snapshot Lifecycle
//
// Containerd defines three snapshot kinds:
//   - KindActive: Writable snapshot from Prepare(), can be committed
//   - KindView: Read-only snapshot from View(), cannot be committed
//   - KindCommitted: Finalized layer from Commit(), immutable
//
// State transitions:
//
//	Prepare(key, parent) → KindActive → Commit(name, key) → KindCommitted
//	                                                            ↓
//	View(key, parent) → KindView                              Remove()
//	                        ↓
//	                     Remove()
//
// # Two Commit Modes
//
// The commit process reads files from either block mode or overlay mode.
// Mode detection happens in [resolveCommitSource].
//
// BLOCK MODE (extract snapshots):
//   - Condition: rwlayer.img exists in snapshot directory
//   - Used when: EROFS differ writes to host-mounted ext4
//   - Source: {snapshotDir}/rw/upper/ (inside mounted ext4)
//   - See: [commitSourceFromBlock]
//
// OVERLAY MODE (regular snapshots):
//   - Condition: rwlayer.img does NOT exist
//   - Used when: VM handles overlay internally
//   - Source: {snapshotDir}/fs/
//   - See: [commitSourceFromOverlay]
//
// # Mount Types Returned
//
// The snapshotter returns these mount types to callers:
//
//	format/erofs  - Multi-layer with fsmeta/VMDK (VM runtimes only)
//	erofs         - Single EROFS layer
//	ext4          - Writable layer for active snapshots
//	bind          - Bind mount for extract snapshots and empty views
//
// The "format/erofs" type signals VM-only mounts. Containerd's standard
// mount manager will reject it with "unsupported mount type" rather than
// the cryptic EINVAL that occurs when trying to mount EROFS with file
// paths in device= options. VM runtimes like qemubox handle this correctly.
//
// # Fsmeta/VMDK Generation
//
// Fsmeta generation runs asynchronously after Prepare() for snapshots with
// parents. This creates a merged metadata file and VMDK descriptor that
// allows QEMU to present all EROFS layers as a single block device.
//
// If fsmeta generation fails:
//   - Warning logged with stage, layer count, and error details
//   - Mounts() returns individual layer mounts as fallback
//   - No error surfaced to caller (graceful degradation)
//
// To debug fsmeta issues, check logs for "fsmeta generation" messages.
// See [generateFsMeta] for the generation logic.
//
// # Mount Decision Tree
//
// The [mounts] function determines mount type based on:
//
//	Extract snapshot? → diffMounts() (bind mount to upper)
//
//	KindView:
//	  0 parents → bind mount to empty fs/
//	  1 parent  → single EROFS mount
//	  N parents → fsmeta mount (if available) or N EROFS mounts
//
//	KindActive:
//	  0 parents → ext4 writable layer only
//	  N parents → EROFS layers + ext4 writable layer
//
// # File Layout
//
// Snapshot directory structure:
//
//	/var/lib/nexus-erofs-snapshotter/snapshots/{id}/
//	├── .erofslayer       # Marker: EROFS-managed snapshot (for differ)
//	├── fs/               # Overlay upper directory (overlay mode)
//	├── rwlayer.img       # ext4 writable layer file (block mode only)
//	├── rw/               # Mount point for rwlayer.img
//	│   └── upper/        # Actual upper directory in block mode
//	├── layer.erofs       # Committed EROFS layer (digest or fallback named)
//	├── fsmeta.erofs      # Merged metadata for multi-layer (async generated)
//	├── merged.vmdk       # VMDK descriptor for QEMU (async generated)
//	└── layers.manifest   # Layer digests in VMDK order (for verification)
//
// # Concurrency
//
// Multiple goroutines may try to generate fsmeta for the same parent chain.
// A lock file (O_EXCL) ensures only one wins; others exit silently.
// See the lock file handling in [generateFsMeta].
//
// # Error Types
//
// The package defines structured error types for programmatic handling:
//   - [LayerBlobNotFoundError]: EROFS layer blob not found for snapshot
//   - [BlockMountError]: ext4 block mount failed during commit
//   - [CommitConversionError]: EROFS conversion failed during commit
//
// Use errors.As to extract context:
//
//	var notFound *LayerBlobNotFoundError
//	if errors.As(err, &notFound) {
//	    log.Printf("searched in %s: %v", notFound.Dir, notFound.Searched)
//	}
package snapshotter
