# Differ Package - EROFS Tar Conversion

**Technology**: Go (containerd diff.Applier/diff.Comparer interface)
**Entry Point**: `differ.go` (type `ErofsDiff` struct)
**Parent Context**: This extends [../../CLAUDE.md](../../CLAUDE.md)

---

## Quick Overview

**What this package does**:
Implements containerd's `diff.Applier` and `diff.Comparer` interfaces to convert OCI tar layers into EROFS format. This enables efficient read-only layer storage for VM-based container runtimes.

**Key responsibilities**:
- Apply OCI tar layers as EROFS blobs (`Apply`)
- Compare layers and produce diffs (`Compare`)
- Handle native EROFS media types
- Use digest-based naming for layer correlation

**Dependencies**:
- Internal: `erofs` (mkfs.erofs wrapper)
- External: `containerd/v2/core/diff`, `containerd/v2/core/content`, `mkfs.erofs`

---

## Development Commands

### From This Directory

```bash
# Run tests for this package
go test -v ./...

# Run Linux-specific tests (require root)
sudo go test -v ./... -test.root

# Run specific test
go test -v -run TestApply ./...
```

### From Root Directory

```bash
task test -- -run TestDiffer
```

---

## Architecture

### Directory Structure

```
differ/
├── differ.go            # Main ErofsDiff implementation
├── differ_test.go       # Basic tests
├── compare_linux.go     # Linux Compare implementation
├── compare_other.go     # Stub for non-Linux
└── compare_linux_test.go # Linux-specific tests
```

### Code Organization Patterns

#### Tar to EROFS Conversion

**File**: `differ.go:Apply()`

The Apply function converts OCI tar layers to EROFS:

```go
// Apply flow:
// 1. Check if native EROFS media type (ends with ".erofs")
// 2. If native → copy blob directly to layer path
// 3. If tar → decompress, pipe to mkfs.erofs --tar=f
```

**DO**: Use `--tar=f` mode for full tar conversion (4KB blocks, compatible with fsmeta)
**DON'T**: Use compression - it breaks fsmeta merge compatibility

#### Mount Manager Resolution

**File**: `differ.go`

Uses lazy resolution to handle plugin initialization order:

```go
// DO: Use resolver for plugin contexts
differ := NewErofsDiffer(store, WithMountManagerResolver(func() mount.Manager {
    return getMountManager() // Called when actually needed
}))

// DON'T: Assume mount manager is available at init time
differ := NewErofsDiffer(store, WithMountManager(mm)) // May fail in plugins
```

#### Media Type Detection

**File**: `differ.go:isErofsMediaType()`

```go
// Valid: ends with ".erofs", no suffix
"application/vnd.oci.image.layer.v1.erofs" ✅

// Invalid: has +suffix (reserved for DiffCompression)
"application/vnd.oci.image.layer.v1.erofs+gzip" ❌
```

**DO**: Keep media type detection simple (suffix matching)
**DON'T**: Add +suffix support (breaks DiffCompression)

---

## Key Files

### Core Files

- **`differ.go`** - Main `ErofsDiff` struct and `Apply()` implementation
- **`compare_linux.go`** - Linux-specific `Compare()` implementation
- **`compare_other.go`** - Stub returning `ErrNotImplemented` for non-Linux

### Key Functions

- **`Apply()`** - Convert OCI tar to EROFS blob
- **`Compare()`** - Generate diff between snapshots (Linux only)
- **`isErofsMediaType()`** - Detect native EROFS layers
- **`defaultMkfsOpts()`** - Hardcoded mkfs options for VM use

---

## Quick Search Commands

### Find in This Package

```bash
# Find Apply implementation
rg -n "func.*Apply" internal/differ/

# Find mkfs options
rg -n "defaultMkfsOpts|mkfsExtraOpts" internal/differ/

# Find media type handling
rg -n "isErofsMediaType|MediaType" internal/differ/
```

### Find Tests

```bash
# Find all tests
find internal/differ -name "*_test.go"

# Find platform-specific tests
rg -n "//go:build" internal/differ/
```

---

## Common Gotchas

**Gotcha 1: No compression for VMDK compatibility**
- Problem: Compressed EROFS layers (datalayout 3) can't be merged with fsmeta
- Solution: `defaultMkfsOpts()` intentionally uses no compression
- Important: Don't add compression options

**Gotcha 2: Digest-based filenames**
- Problem: Need to correlate layer files with registry manifests
- Solution: Layer blobs named `sha256-{hash}.erofs`
- Example: `erofs.LayerBlobFilename(desc.Digest.String())`

**Gotcha 3: Mount manager timing**
- Problem: Mount manager may not exist when differ initializes
- Solution: Use `WithMountManagerResolver()` for lazy resolution
- Example: See `NewErofsDiffer()` options

**Gotcha 4: Platform-specific Compare**
- Problem: `Compare()` requires Linux syscalls
- Solution: `compare_other.go` returns `ErrNotImplemented`
- Behavior: Falls back to walking differ on non-Linux

---

## Testing

### Test Organization

- **`differ_test.go`** - Basic Apply tests
- **`compare_linux_test.go`** - Linux Compare tests

### Testing Patterns

```go
// Tests require mkfs.erofs installed
func TestApply(t *testing.T) {
    // Check for mkfs.erofs availability
    if _, err := exec.LookPath("mkfs.erofs"); err != nil {
        t.Skip("mkfs.erofs not installed")
    }
    // ... test code
}
```

### Running Tests

```bash
# Basic tests
go test -v ./internal/differ/...

# Linux tests with root
sudo go test -v ./internal/differ/... -test.root
```

---

## Dependencies

### Internal Dependencies

- **`internal/erofs`** - mkfs.erofs wrapper functions
  - `ConvertTarErofs()` - Tar stream to EROFS conversion
  - `MountsToLayer()` - Extract layer path from mounts
  - `LayerBlobFilename()` - Digest-based filename generation

### External Dependencies

- **`containerd/v2/core/diff`** - Applier/Comparer interfaces
- **`containerd/v2/core/content`** - Content store for reading layers
- **`containerd/v2/core/images`** - Media type handling
- **`mkfs.erofs`** - External tool (must be installed)

---

## Package-Specific Rules

### Conversion Rules [MUST]

- **MUST** use `--tar=f` mode for tar conversion (full mode, 4KB blocks)
- **MUST NOT** add compression options (breaks fsmeta compatibility)
- **MUST** use digest-based filenames for layer correlation

### Media Type Rules

- **MUST** accept any media type ending with `.erofs` as native
- **MUST NOT** accept media types with `+suffix` (reserved for DiffCompression)
- **SHOULD** support standard OCI tar media types via decompression chain

### Platform Rules

- **MUST** implement Compare only on Linux (uses syscalls)
- **MUST** return `ErrNotImplemented` on other platforms
- **SHOULD** use build tags for platform separation
