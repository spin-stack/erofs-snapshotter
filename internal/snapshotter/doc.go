// Package snapshotter implements the spin-erofs containerd snapshotter.
//
// This is a VM-only EROFS snapshotter for containerd, designed exclusively
// for use with spinbox and similar VM runtimes. It does NOT mount filesystems
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
// Mode detection happens in [getCommitUpperDir].
//
// BLOCK MODE (rwlayer.img exists in the snapshot directory):
//   - Extract snapshots: Prepare() mounted the ext4 at rw/, the EROFS differ
//     wrote into it, and the commit source is {snapshotDir}/rw/upper/.
//   - Runtime snapshots (the "nerdctl/ctr commit" flow): the ext4 is not
//     mounted on the host; Commit mounts it read-only to read the upper the
//     guest wrote. Block mode NEVER falls back to fs/ - fs/ is always empty
//     in block mode and converting it would silently commit an empty layer.
//
// OVERLAY MODE (rwlayer.img does NOT exist):
//   - Used when: VM handles overlay internally
//   - Source: {snapshotDir}/fs/
//
// # Committing a Container (Quiesce Contract)
//
// A container's changes live inside rwlayer.img (ext4), written by the guest
// VM through its overlay mount. To turn them into an image layer, the host
// must read that ext4 (diff.Compare for the layer tar, or Commit's fallback
// conversion). This is only safe when the guest is not writing to it:
//
//   - The container MUST be stopped (not merely paused) before commit.
//     Pausing only stops vCPUs; dirty data may remain in the guest page
//     cache and the ext4 journal may be mid-transaction.
//   - Enforcement: mountutils.MountExt4 takes a flock plus a whole-file OFD
//     lock on rwlayer.img before mounting and holds both until unmount. QEMU
//     locks its disk images with OFD locks, so a running VM makes the mount
//     fail with errdefs.ErrFailedPrecondition ("container is still running").
//     Holding the lock for the mount's lifetime also prevents a VM from
//     starting while the host reads the image.
//   - The host mounts the ext4 read-only; journal recovery is permitted so a
//     crashed guest still yields a consistent view.
//
// GUEST LAYOUT CONTRACT: the VM runtime (spinbox) must build its overlay
// with upper/ and work/ at the root of the ext4 - the same layout
// mountBlockRwLayer prepares for extract snapshots. The differ and Commit
// read {ext4root}/upper as the layer content; anything written outside
// upper/ is invisible to commit.
//
// HOT COMMIT (container paused + frozen): a cooperating runtime that has
// paused the VM AND frozen its filesystems (FIFREEZE) - so the on-disk ext4
// is consistent - may set the "containerd.io/snapshot/erofs.quiesced" label on
// the Commit call. The snapshotter then mounts the rwlayer read-only WITHOUT
// the exclusive lock gate (a paused QEMU still holds its own image lock, which
// the gate cannot distinguish from a running one) and with norecovery (no
// journal replay against an image the VM still holds open). Setting the label
// without an actual freeze risks a torn read; the runtime owns that contract.
// Without the label, commit of a held image still fails loudly by design.
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
// paths in device= options. VM runtimes like spinbox handle this correctly.
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
//	/var/lib/spin-stack/erofs-snapshotter/snapshots/{id}/
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
//   - [CommitConversionError]: EROFS conversion failed during commit
//
// Use errors.As to extract context:
//
//	var notFound *LayerBlobNotFoundError
//	if errors.As(err, &notFound) {
//	    log.Printf("searched in %s: %v", notFound.Dir, notFound.Searched)
//	}
package snapshotter
