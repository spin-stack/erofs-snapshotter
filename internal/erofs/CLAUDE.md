# EROFS Package - mkfs.erofs Wrapper

**Technology**: Go (exec wrapper for mkfs.erofs)
**Entry Point**: `convert.go`
**Parent Context**: This extends [../../CLAUDE.md](../../CLAUDE.md)

---

## Quick Overview

**What this package does**:
Wraps the `mkfs.erofs` command-line tool to convert tar streams and directories into EROFS filesystem images. Provides utilities for reading EROFS metadata and validating layer compatibility.

**Key responsibilities**:
- Convert tar streams to EROFS images (`ConvertTarErofs`)
- Convert directories to EROFS images (`ConvertErofs`)
- Generate tar index with appended tar (`GenerateTarIndexAndAppendTar`)
- Read and validate EROFS superblock metadata
- Extract layer paths from mount specifications

**Dependencies**:
- Internal: `stringutil` (output truncation)
- External: `mkfs.erofs` (must be installed on system)

---

## Development Commands

### From This Directory

```bash
# Run tests for this package
go test -v ./...

# Run specific test
go test -v -run TestConvert ./...
```

### From Root Directory

```bash
task test -- -run TestErofs
```

---

## Architecture

### Directory Structure

```
erofs/
├── convert.go       # Main conversion functions and utilities
└── convert_test.go  # Tests
```

### Code Organization Patterns

#### mkfs.erofs Argument Building

**File**: `convert.go`

Arguments are built via dedicated functions:

```go
// Tar to EROFS (full conversion)
buildTarErofsArgs(layerPath, uuid, mkfsExtraOpts)
// → ["--tar=f", "--aufs", "--quiet", "-Enoinline_data", "--sort=none", ...opts, "-U", uuid, layerPath]

// Tar index only
buildTarIndexArgs(layerPath, mkfsExtraOpts)
// → ["--tar=i", "--aufs", "--quiet", ...opts, layerPath]

// Directory to EROFS
// → ["--quiet", "-Enoinline_data", ...opts, layerPath, srcDir]
```

**DO**: Use argument builder functions for consistency
**DON'T**: Build mkfs.erofs arguments inline

#### Stdin Piping

**File**: `convert.go:runMkfsWithStdin()`

Handles piping tar data to mkfs.erofs:

```go
// Pattern: goroutine copies to stdin while main waits for command
go func() {
    n, copyErr := io.Copy(stdin, r)
    stdin.Close()
    copyDone <- copyResult{n, copyErr}
}()
waitErr := cmd.Wait()
result := <-copyDone
```

**DO**: Close stdin after copy completes
**DO**: Handle both copy error and command error
**DON'T**: Block on copy before starting command

#### Mount Path Extraction

**File**: `convert.go:MountsToLayer()`

Extracts snapshot layer directory from mount specs:

```go
// Decision tree by mount type:
// mkfs/* → filepath.Dir(source)
// bind   → parent of source (handle rw/upper nesting)
// erofs  → parent of source
// ext4   → parent of source
// overlay → upperdir parent, or first lowerdir parent
```

**DO**: Handle both directory mode and block mode paths
**DON'T**: Assume a single path structure

---

## Key Files

### Core File: `convert.go`

**Conversion Functions**:
- `ConvertTarErofs()` - Tar stream → EROFS (main path)
- `ConvertErofs()` - Directory → EROFS (fallback path)
- `GenerateTarIndexAndAppendTar()` - Tar index mode

**Argument Builders**:
- `buildTarErofsArgs()` - Full tar conversion args
- `buildTarIndexArgs()` - Tar index args

**Utilities**:
- `MountsToLayer()` - Extract layer path from mounts
- `GetBlockSize()` - Read EROFS superblock block size
- `CanMergeFsmeta()` - Validate layers for fsmeta merge
- `LayerBlobFilename()` - Convert digest to filename
- `DigestFromLayerBlobPath()` - Convert filename to digest

**Constants**:
- `ErofsLayerMarker` - `.erofslayer` marker file
- `LayerBlobPattern` - `sha256-*.erofs` glob pattern

---

## Quick Search Commands

### Find in This Package

```bash
# Find mkfs.erofs invocations
rg -n "exec.Command.*mkfs.erofs" internal/erofs/

# Find argument building
rg -n "buildTar.*Args" internal/erofs/

# Find mount type handling
rg -n "mountBaseType|extractLayerPath" internal/erofs/
```

### Find Constants

```bash
# Find all exported constants
rg -n "^const|^\t[A-Z].*=" internal/erofs/convert.go
```

---

## Common Gotchas

**Gotcha 1: Block size compatibility for fsmeta**
- Problem: Layers with <4KB blocks can't be merged with fsmeta
- Solution: Use `CanMergeFsmeta()` to validate before merging
- Constant: `erofsMinBlockSizeForFsmeta = 4096`

**Gotcha 2: Two commit modes affect path extraction**
- Problem: Block mode has `rw/upper/` nesting, directory mode doesn't
- Solution: `layerFromBindMount()` checks for `rw` parent
- Example: See `layerFromUpperdir()` implementation

**Gotcha 3: mkfs.erofs version requirements**
- Problem: `--tar` option not available in older versions
- Solution: Use `SupportGenerateFromTar()` to check
- Minimum: erofs-utils 1.8+

**Gotcha 4: Marker file validation**
- Problem: Need to verify mounts are from EROFS snapshotter
- Solution: Check for `.erofslayer` marker file
- Exception: Trust `erofs` or `format/erofs` mount types directly

---

## Testing

### Test File: `convert_test.go`

Tests require `mkfs.erofs` to be installed.

### Testing Patterns

```go
func TestConvert(t *testing.T) {
    // Check for mkfs.erofs
    if _, err := exec.LookPath("mkfs.erofs"); err != nil {
        t.Skip("mkfs.erofs not installed")
    }
    // ... test code
}
```

### Running Tests

```bash
# Run all tests
go test -v ./internal/erofs/...

# Run in Docker (has erofs-utils)
task test-docker
```

---

## Dependencies

### Internal Dependencies

- **`internal/stringutil`** - Output truncation for error messages

### External Dependencies

- **`mkfs.erofs`** - Required external tool
  - Must be in PATH
  - Version 1.8+ recommended for tar mode
  - Installed via `erofs-utils` package

### System Requirements

For full functionality:
```bash
# Ubuntu/Debian
apt-get install erofs-utils

# From source (for latest features)
./scripts/build-erofs-utils.sh
```

---

## Package-Specific Rules

### mkfs.erofs Options [MUST]

- **MUST** use `--quiet` to suppress verbose output
- **MUST** use `-Enoinline_data` for optimal layout
- **MUST** use `--sort=none` when no compression (performance)
- **MUST NOT** add compression for fsmeta compatibility

### Tar Mode Options

- **MUST** use `--tar=f` for full conversion (4KB blocks)
- **MUST** use `--aufs` for whiteout handling
- **SHOULD** specify `-U uuid` for reproducible builds

### Path Handling

- **MUST** support both directory mode (`fs/`) and block mode (`rw/upper/`)
- **MUST** validate with `.erofslayer` marker or EROFS mount type
- **SHOULD** return `ErrNotImplemented` for unsupported mount types

### Constants

```go
// Marker file created by snapshotter
ErofsLayerMarker = ".erofslayer"

// Glob pattern for finding layer blobs
LayerBlobPattern = "sha256-*.erofs"

// Minimum block size for fsmeta merge
erofsMinBlockSizeForFsmeta = 4096
```
