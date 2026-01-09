# Snapshotter Package - Core Implementation

**Technology**: Go (containerd snapshotter interface)
**Entry Point**: `snapshotter.go` (type `snapshotter` struct)
**Parent Context**: This extends [../../CLAUDE.md](../../CLAUDE.md)

---

## Quick Overview

**What this package does**:
Implements the containerd snapshotter interface for VM-only EROFS containers. Returns file paths (not mounted directories) that VM runtimes like qemubox pass to guests as virtio-blk devices.

**Key responsibilities**:
- Snapshot lifecycle management (Prepare, View, Commit, Remove)
- Mount specification generation (file paths for VM runtimes)
- VMDK/fsmeta generation for multi-layer images
- Layer ordering and validation

**Dependencies**:
- Internal: `erofs`, `differ`, `loop`, `mountutils`, `store`
- External: `containerd/v2`, `bbolt`, `grpc`

---

## Development Commands

### From This Directory

```bash
# Run tests for this package
go test -v ./...

# Run tests with race detection
go test -race -v ./...

# Run specific test
go test -v -run TestMounts ./...

# Run Linux-specific tests (requires root)
sudo go test -v ./... -test.root
```

### From Root Directory

```bash
# Test this package
task test -- -run TestSnapshotter

# Full test suite
task test
```

---

## Architecture

### Directory Structure

```
snapshotter/
├── doc.go              # Package documentation (READ THIS FIRST)
├── snapshotter.go      # Core struct, config, lifecycle
├── snapshotter_linux.go   # Linux-specific implementation
├── snapshotter_other.go   # Stub for non-Linux platforms
├── operations.go       # CRUD: Prepare, View, Remove, etc.
├── mounts.go           # Mount logic (view, active, diff)
├── commit.go           # Commit and EROFS conversion
├── paths.go            # Path helpers and constants
├── vmdk.go             # VMDK descriptor generation
├── layer_order.go      # Layer ordering for fsmeta
├── errors.go           # Structured error types
└── *_test.go           # Tests (23 files)
```

### Code Organization Patterns

#### Mount Type Selection

**File**: `mounts.go`

The `mounts()` function is the core decision tree:

```go
// DECISION TREE:
//
//   Is extract snapshot (extractLabel=true)?
//   ├─ YES → diffMounts(): bind mount to rw/upper/ for EROFS differ
//   └─ NO  → Check snapshot kind:
//            ├─ KindView  → viewMountsForKind(): read-only layer access
//            └─ KindActive → activeMountsForKind(): layers + writable ext4
```

**DO**: Follow this pattern when adding new mount logic
**DON'T**: Return `type: overlay` mounts (VM-only constraint)

#### Path Helpers

**File**: `paths.go`

All paths are constructed via helper methods:

```go
// DO: Use path helpers
s.upperPath(id)           // → {root}/snapshots/{id}/fs
s.writablePath(id)        // → {root}/snapshots/{id}/rwlayer.img
s.fsMetaPath(id)          // → {root}/snapshots/{id}/fsmeta.erofs
s.vmdkPath(id)            // → {root}/snapshots/{id}/merged.vmdk

// DON'T: Hardcode paths
filepath.Join(s.root, "snapshots", id, "fs")  // Bad: use s.upperPath(id)
```

#### Two Commit Modes

**File**: `commit.go`

Mode detection happens in `getCommitUpperDir()`:

```go
// BLOCK MODE: rwlayer.img exists
// - Source: {snapshotDir}/rw/upper/
// - Used by: EROFS differ (host-mounted ext4)

// OVERLAY MODE: rwlayer.img does NOT exist
// - Source: {snapshotDir}/fs/
// - Used by: VM internal overlay
```

**DO**: Check for `rwlayer.img` to determine mode
**DON'T**: Assume a single commit path

#### Error Types

**File**: `errors.go`

Use structured errors for programmatic handling:

```go
// DO: Return structured errors
return "", &LayerBlobNotFoundError{
    SnapshotID: id,
    Dir:        dir,
    Searched:   patterns,
}

// DO: Check with errors.As
var notFound *LayerBlobNotFoundError
if errors.As(err, &notFound) {
    // Handle specifically
}

// DON'T: Return generic errors for known failure modes
return "", fmt.Errorf("layer not found")  // Bad: loses context
```

---

## Key Files

### Core Files (understand these first)

- **`doc.go`** - Comprehensive package documentation with lifecycle diagrams
- **`snapshotter.go`** - Main struct, `New()` constructor, config
- **`mounts.go`** - Mount specification logic (most complex)

### Mount Logic

- **`mounts.go:mounts()`** - Main decision tree
- **`mounts.go:viewMountsForKind()`** - Read-only views
- **`mounts.go:activeMountsForKind()`** - Writable active snapshots
- **`mounts.go:mountFsMeta()`** - Multi-layer with VMDK

### Operations

- **`operations.go:Prepare()`** - Create writable snapshot
- **`operations.go:View()`** - Create read-only view
- **`operations.go:Remove()`** - Delete snapshot
- **`commit.go:Commit()`** - Finalize and convert to EROFS

### Supporting

- **`paths.go`** - All path construction (constants + methods)
- **`vmdk.go`** - VMDK descriptor generation
- **`layer_order.go`** - Layer ordering for fsmeta

---

## Quick Search Commands

### Find in This Package

```bash
# Find mount type assignments
rg -n "Type:\s*\"" internal/snapshotter/mounts.go

# Find where fsmeta is generated
rg -n "generateFsMeta|fsMetaPath" internal/snapshotter/

# Find error type usages
rg -n "LayerBlobNotFoundError|CommitConversionError" internal/snapshotter/

# Find snapshot kind handling
rg -n "KindView|KindActive|KindCommitted" internal/snapshotter/
```

### Find Tests

```bash
# Find tests for specific functionality
rg -n "func Test.*Mount" internal/snapshotter/

# Find tests requiring root
rg -n "RequiresRoot|test\.root" internal/snapshotter/

# Find skipped tests (VM-only)
rg -n "skipIfVMOnly" internal/snapshotter/
```

---

## Common Gotchas

**Gotcha 1: Mount types are for VM runtimes, not host**
- Problem: Trying to mount `format/erofs` on host fails
- Solution: These mounts are file paths for VM runtimes; containerd rejects them by design
- Example: See `mounts.go:mountFsMeta()` comments

**Gotcha 2: Layer order in fsmeta**
- Problem: Layers in wrong order cause mount failures
- Solution: ParentIDs are newest-first; iterate backwards for oldest-first
- Example: See `mounts.go:mountFsMeta()` loop

**Gotcha 3: Two commit modes**
- Problem: Commit source directory varies by mode
- Solution: Check for `rwlayer.img` to determine block vs overlay mode
- Example: See `commit.go:getCommitUpperDir()`

**Gotcha 4: Fsmeta generation is async**
- Problem: fsmeta may not exist immediately after Prepare
- Solution: `mounts()` falls back to individual layer mounts
- Example: See `mounts.go:viewMountsForKind()` fallback path

---

## Testing

### Test Organization

**Unit tests** (colocated):
- `snapshotter_test.go` - Core lifecycle tests
- `mounts_test.go` - Mount specification tests
- `vmdk_test.go` - VMDK generation tests
- `layer_order_test.go` - Layer ordering tests
- `errors_test.go` - Error type tests

**Platform-specific**:
- `snapshotter_linux_test.go` - Linux-only tests
- `snapshot_linux_test.go` - More Linux tests
- `differ_linux_test.go` - Differ integration

**Integration**:
- `integration_test.go` - Full lifecycle tests
- `concurrent_test.go` - Concurrency tests

### Testing Patterns

```go
// Skip VM-only tests that need host mounting
func TestSomethingRequiringMount(t *testing.T) {
    skipIfVMOnly(t)
    // ... test code
}

// Use testutil helpers
func TestRequiresRoot(t *testing.T) {
    testutil.RequiresRoot(t)
    // ... test code
}
```

### Running Tests

```bash
# Run all tests in package
go test -v ./internal/snapshotter/...

# Run specific test
go test -v -run TestMounts ./internal/snapshotter/

# Run with race detection
go test -race ./internal/snapshotter/...

# Run Linux tests as root
sudo go test -v ./internal/snapshotter/... -test.root
```

---

## Dependencies

### Internal Dependencies

- **`internal/erofs`** - EROFS conversion via mkfs.erofs
- **`internal/differ`** - Tar-to-EROFS conversion
- **`internal/loop`** - Loop device management
- **`internal/mountutils`** - Mount helper functions
- **`internal/store`** - Namespace-aware content store

### External Dependencies

- **`containerd/v2/core/snapshots`** - Snapshotter interface
- **`containerd/v2/core/mount`** - Mount types
- **`go.etcd.io/bbolt`** - Metadata storage
- **`google.golang.org/grpc`** - gRPC service

---

## Package-Specific Rules

### Mount Rules [CRITICAL]

- **MUST** return file paths, not mounted directories
- **MUST NOT** return `type: overlay` mounts
- **MUST** use `format/erofs` for multi-device EROFS
- **MUST** use `erofs` for single-layer read-only
- **MUST** use `ext4` for writable layers

### Fsmeta/VMDK Rules

- **MUST** always generate VMDK for multi-layer images
- **MUST NOT** add threshold configurations
- **SHOULD** generate fsmeta async after Prepare
- **SHOULD** fall back gracefully if fsmeta fails

### Layer Order Rules

- **MUST** iterate ParentIDs backwards (newest-first → oldest-first)
- **MUST** match order used by mkfs.erofs fsmeta generation
- See: https://github.com/containerd/containerd/pull/12374
