# erofs Snapshotter

**CLAUDE.md Version**: 1.0.0
**Last Updated**: 2025-01-09
**Last Review**: @claude

---

## Overview

- **Type**: Single Service (containerd snapshotter plugin)
- **Primary Language**: Go 1.25
- **Architecture**: VM-only EROFS snapshotter for containerd
- **Tech Stack**: containerd/v2, gRPC, bbolt, EROFS/VMDK

---

## Purpose of This File

This CLAUDE.md is the **authoritative source** for development guidelines. It is read by Claude Code before every interaction and takes priority over user prompts.

**Hierarchy**:
- This file contains universal project rules
- Subdirectory CLAUDE.md files extend these rules for specific packages
- See [Directory-Specific Context](#directory-specific-context) for links

**Maintenance**: Update this file as patterns evolve. Use `#` during sessions to add memories organically.

---

## Critical Design Constraint: VM-Only Snapshotter

**This is a VM-only EROFS snapshotter** for containerd, designed exclusively for use with qemubox (and similar VM runtimes). It does **NOT** mount filesystems on the host - it returns raw file paths that VM runtimes pass to guests as virtio-blk devices.

### What This Means

| Traditional Snapshotter | This Snapshotter |
|------------------------|------------------|
| Mounts filesystems on host | Returns file paths only |
| Returns mounted directory paths | Returns `.erofs` and `.img` file paths |
| Host kernel handles overlay | Guest VM handles overlay |
| Works with any runtime | Works only with VM runtimes (qemubox) |

### Key Design Decisions

#### 1. No Host Mounting [CRITICAL]

The snapshotter returns file paths, not mounted filesystems:
- **View mounts** → EROFS file paths (guest mounts them)
- **Active mounts** → EROFS file paths + ext4 file path (guest creates overlay)
- **Only extract snapshots** mount ext4 on host (for differ to write)

#### 2. VMDK Always Generated [CRITICAL]

VMDK descriptors are **always** generated for multi-layer images. There is NO threshold configuration.

**DO NOT add `--fs-merge-threshold` or similar flags** - they were removed because:
- This snapshotter is exclusively for qemubox
- qemubox always needs VMDK for efficient multi-layer handling
- A threshold adds complexity without benefit

#### 3. No Overlay Returns [CRITICAL]

**NEVER return `type: overlay` mounts.** The snapshotter returns:
- `type: format/erofs` for multi-device EROFS (fsmeta with device= options)
- `type: erofs` for single-layer read-only
- `type: ext4` for writable layers
- `type: bind` for extract snapshots and empty views

The `format/erofs` type signals VM-only mounts. Containerd's standard mount manager will reject it with "unsupported mount type" rather than cryptic EINVAL errors.

---

## Universal Development Rules

### Code Quality [MUST]

- **MUST** check all errors (no `_ = err` unless justified with comment)
- **MUST** use `context.Context` for cancellation and timeouts
- **MUST** run `task lint` before committing (95+ linters)
- **MUST** write tests for all new functionality
- **MUST NOT** commit secrets, API keys, or credentials
- **MUST NOT** return `type: overlay` mounts (VM-only constraint)
- **MUST NOT** add VMDK threshold configurations

### Go Best Practices [SHOULD]

- **SHOULD** prefer interfaces for dependencies (easier testing)
- **SHOULD** use structured logging with `containerd/log`
- **SHOULD** use `errors.As`/`errors.Is` for error checking
- **SHOULD** document exported functions and types
- **SHOULD** keep functions focused and under 50 lines where practical

### Anti-Patterns [MUST NOT]

- **MUST NOT** ignore errors silently
- **MUST NOT** use `panic()` for error handling (use error returns)
- **MUST NOT** mutate global state without synchronization
- **MUST NOT** use `unsafe` package without explaining why in comment
- **MUST NOT** hardcode paths (use path helpers from `paths.go`)

### Platform-Specific Code

When editing `*_linux.go` files:
- **MUST** consider if `*_other.go` needs corresponding changes
- **SHOULD** keep platform stubs in sync (even if just returning errors)
- **SHOULD** use build tags appropriately (`//go:build linux`)

---

## Core Development Commands

### Quick Reference

```bash
# Development
task build              # Build binary (CGO_ENABLED=0)
task build-linux        # Cross-compile for Linux amd64
task test               # Run tests locally
task lint               # Run golangci-lint (95+ linters)
task fmt                # Format with gofmt/gofumpt

# Testing
task test               # Run tests (no root required)
task test-root          # Run tests as root (integration tests)
task test-docker        # Run in Docker with erofs-utils

# Quality
task vet                # Run go vet
task tidy               # Update go.mod/go.sum
task clean              # Remove build artifacts

# Integration
task integration-test   # Full containerd integration tests
task integration-shell  # Interactive test container
```

### Pre-Commit Checklist [CRITICAL]

Run these commands before every commit:

```bash
task fmt && task lint && task test
```

All checks must pass before committing.

---

## Project Structure

### Root Layout

```
.
├── cmd/
│   └── spin-erofs-snapshotter/  # Main entry point
├── internal/
│   ├── snapshotter/              # Core snapshotter (see CLAUDE.md)
│   ├── differ/                   # EROFS differ (see CLAUDE.md)
│   ├── erofs/                    # mkfs.erofs wrapper (see CLAUDE.md)
│   ├── grpcservice/              # gRPC service adapter
│   ├── loop/                     # Loop device management
│   ├── mountutils/               # Mount utilities
│   ├── preflight/                # System compatibility checks
│   ├── cleanup/                  # Context cleanup utilities
│   ├── store/                    # Namespace-aware content store
│   ├── stringutil/               # String utilities
│   └── testutil/                 # Testing utilities
├── test/integration/             # Integration tests
├── config/                       # Configuration examples
├── scripts/                      # Build and test scripts
├── .github/workflows/            # CI/CD automation
├── Taskfile.yml                  # Task runner configuration
└── .golangci.yml                 # Linter configuration
```

### Key Directories

**Main Entry Point** (`cmd/spin-erofs-snapshotter/`):
- `main.go` - CLI flags, gRPC server setup, signal handling
- Configuration via flags or environment variables

**Core Implementation** (`internal/`):
- **`snapshotter/`** → Core logic ([see CLAUDE.md](internal/snapshotter/CLAUDE.md))
- **`differ/`** → EROFS conversion ([see CLAUDE.md](internal/differ/CLAUDE.md))
- **`erofs/`** → mkfs wrapper ([see CLAUDE.md](internal/erofs/CLAUDE.md))

**Testing** (`test/` and colocated):
- Unit tests: Colocated with source (`*_test.go`)
- Integration: `test/integration/integration_test.go`
- Platform-specific: `*_linux_test.go`, `*_other_test.go`

---

## Code Patterns

### Correct Mount Returns

```go
// View (multi-layer with fsmeta/VMDK) - uses format/erofs for VM-only signal
[]mount.Mount{{
    Type:   "format/erofs",
    Source: "/path/to/fsmeta.erofs",
    Options: []string{"ro", "loop", "device=/path/to/layer1.erofs", "device=/path/to/layer2.erofs"},
}}

// View (single layer, no VMDK)
[]mount.Mount{{
    Type:   "erofs",
    Source: "/path/to/layer.erofs",
    Options: []string{"ro", "loop"},
}}

// Active (writable) with fsmeta
[]mount.Mount{
    {Type: "format/erofs", Source: "/path/to/fsmeta.erofs", Options: []string{"ro", "loop", "device=..."}},
    {Type: "ext4", Source: "/path/to/rwlayer.img", Options: []string{"rw", "loop"}},
}

// Active (writable) without fsmeta (fallback)
[]mount.Mount{
    {Type: "erofs", Source: "/path/to/layer.erofs", Options: []string{"ro", "loop"}},
    {Type: "ext4", Source: "/path/to/rwlayer.img", Options: []string{"rw", "loop"}},
}
```

### Incorrect Patterns (DO NOT DO)

```go
// WRONG: overlay mount type - NEVER return this
[]mount.Mount{{
    Type: "overlay",
    Source: "overlay",
    Options: []string{"lowerdir=..."},
}}

// WRONG: mounted directory paths - should be file paths
[]mount.Mount{{
    Source: "/path/to/mounted/directory",  // Should be file path like /path/to/layer.erofs
}}
```

### Error Handling Pattern

Use structured error types for programmatic handling:

```go
// Define structured errors (see internal/snapshotter/errors.go)
type LayerBlobNotFoundError struct {
    SnapshotID string
    Dir        string
    Searched   []string
}

// Use errors.As to extract context
var notFound *LayerBlobNotFoundError
if errors.As(err, &notFound) {
    log.Printf("searched in %s: %v", notFound.Dir, notFound.Searched)
}
```

---

## File Layout

Snapshot directory structure:

```
/var/lib/spin-stack/erofs-snapshotter/
├── metadata.db                   # bbolt database
├── mounts.db                     # Mount tracking database
└── snapshots/{id}/
    ├── .erofslayer               # Marker: EROFS-managed snapshot
    ├── fs/                       # Overlay upper directory (overlay mode)
    ├── rwlayer.img               # ext4 writable layer (block mode)
    ├── rw/                       # Mount point for rwlayer.img
    │   └── upper/                # Actual upper in block mode
    ├── layer.erofs               # Committed EROFS layer
    ├── fsmeta.erofs              # Merged metadata (multi-layer)
    ├── merged.vmdk               # VMDK descriptor for QEMU
    └── layers.manifest           # Layer digests in VMDK order
```

### Two Commit Modes

**BLOCK MODE** (extract snapshots):
- Condition: `rwlayer.img` exists in snapshot directory
- Used when: EROFS differ writes to host-mounted ext4
- Source: `{snapshotDir}/rw/upper/` (inside mounted ext4)

**OVERLAY MODE** (regular snapshots):
- Condition: `rwlayer.img` does NOT exist
- Used when: VM handles overlay internally
- Source: `{snapshotDir}/fs/`

---

## Quick Find Commands

### Code Navigation

```bash
# Find all handler/service functions
rg -n "^func.*\(.*Snapshotter\)" internal/snapshotter/

# Find mount type assignments
rg -n "Type:\s*\"" internal/snapshotter/mounts.go

# Find error type definitions
rg -n "^type.*Error struct" internal/

# Find platform-specific implementations
find internal/ -name "*_linux.go" -o -name "*_other.go"
```

### Testing

```bash
# Find all test files
find . -name "*_test.go" | grep -v vendor

# Find tests requiring root
rg -n "test\.root|skipIfVMOnly" internal/

# Find integration tests
find test/ -name "*_test.go"
```

### Configuration

```bash
# Find CLI flag definitions
rg -n "cli\.(String|Int|Bool)Flag" cmd/

# Find environment variable usage
rg -n "os\.Getenv|env\." cmd/ internal/
```

---

## Security Guidelines

### Secrets Management [MUST]

- **NEVER** commit tokens, API keys, passwords, or credentials
- **NEVER** hardcode secrets in code
- Use environment variables for configuration
- Use `.env.local` for local development (gitignored)

### Safe Operations

Before running destructive commands, confirm:
1. Environment (dev/staging/prod)
2. Data backup status if applicable

**Commands requiring confirmation**:
- `git push --force`
- Loop device operations
- Database modifications

---

## Git Workflow

### Branching Strategy

- **main**: Protected, production-ready code
- **feature/[name]**: Feature branches from main
- **fix/[name]**: Bug fix branches

### Commit Messages

Use [Conventional Commits](https://www.conventionalcommits.org/):

```
<type>(<scope>): <description>

Types: feat, fix, docs, refactor, test, chore
```

**Examples**:
```bash
feat(snapshotter): add layer ordering for fsmeta generation
fix(differ): handle empty directories in tar extraction
refactor(erofs): simplify mkfs.erofs argument construction
test(vmdk): add tests for multi-layer VMDK generation
```

### Pull Request Process

1. Create feature branch from main
2. Make changes and commit
3. Run `task fmt && task lint && task test`
4. Push branch and create PR
5. Ensure CI passes (lint, test, build)
6. Address review feedback
7. Squash and merge

---

## Testing Requirements

### Test Types

**Unit Tests** (colocated):
- Location: `*_test.go` next to source files
- Run: `task test`
- Coverage target: Maintain or improve

**Integration Tests**:
- Location: `test/integration/`
- Run: `task integration-test`
- Requires: Docker, containerd

**Platform-Specific Tests**:
- Linux: `*_linux_test.go`
- Other: `*_other_test.go`

### Testing Commands

```bash
# Run all tests
task test

# Run specific package
go test -v ./internal/snapshotter/...

# Run with race detection
go test -race ./...

# Run as root (for loop device tests)
task test-root

# Run in Docker (full environment)
task test-docker
```

### Test Patterns

```go
// Skip tests requiring host mounting
func skipIfVMOnly(t *testing.T) {
    t.Skip("VM-only snapshotter: test requires host mounting")
}

// Use testutil helpers
testutil.RequiresRoot(t)
testutil.RequiresEROFS(t)
```

---

## Available Tools

### Standard Tools

- **Go toolchain**: `go build`, `go test`, `go fmt`, etc.
- **Task runner**: `task` (see Taskfile.yml)
- **Linter**: `golangci-lint` with 95+ linters
- **Git**: Full CLI for version control

### System Requirements

For full testing:
- **erofs-utils**: `mkfs.erofs` for EROFS creation
- **e2fsprogs**: `mkfs.ext4` for writable layers
- **Loop devices**: `/dev/loop*` for integration tests
- **Docker**: For containerized testing

### Tool Permissions

**Allowed Operations**:
- Read any file in repository
- Write Go code files (`*.go`)
- Run `task test`, `task lint`, `task fmt`
- Run `task build`
- Create git branches and commits

**Require Explicit Permission**:
- Run `task test-root` (requires sudo)
- Run `task integration-test` (requires Docker)
- Edit `*_linux.go` files (ask about `*_other.go` parity)
- Modify CI workflow files

**Never Allowed**:
- Force push to main branch
- Commit secrets or credentials
- Return overlay mount types (VM-only constraint)

---

## Restrictions Summary

### Do NOT Add

- Threshold configurations for VMDK generation
- Host mounting of container filesystems
- Overlay mount returns (`type: overlay`)
- Template syntax in mount paths
- Complex layer mounting logic on host

### Always Remember

- Tests requiring host mounting should use `skipIfVMOnly(t)`
- Mount paths must be real file paths, not mounted directories
- The consumer (qemubox) detects `merged.vmdk` in the same directory as `fsmeta.erofs`
- Platform-specific code needs parity between `*_linux.go` and `*_other.go`

---

## Directory-Specific Context

This root CLAUDE.md provides universal rules. For work in specific packages, refer to their specialized CLAUDE.md files:

### Core Packages

- **[`internal/snapshotter/CLAUDE.md`](internal/snapshotter/CLAUDE.md)** - Core snapshotter implementation
  - Snapshot lifecycle, mount types, commit modes
  - Path helpers, error types, VMDK generation

- **[`internal/differ/CLAUDE.md`](internal/differ/CLAUDE.md)** - EROFS differ
  - Tar-to-EROFS conversion
  - Layer comparison logic

- **[`internal/erofs/CLAUDE.md`](internal/erofs/CLAUDE.md)** - EROFS utilities
  - mkfs.erofs wrapper
  - Conversion options and error handling

**These files extend (not replace) this root CLAUDE.md.**

---

## CLAUDE.md Maintenance

### Versioning

- **Major** (X.0.0): Fundamental architecture changes
- **Minor** (0.X.0): New sections, significant additions
- **Patch** (0.0.X): Small corrections, example updates

### Changelog

**v1.0.0** (2025-01-09)
- Enhanced CLAUDE.md with comprehensive structure
- Preserved VM-only design constraints
- Added directory-specific CLAUDE.md files
- Added hooks and custom commands

### Keeping Current

**During Development**:
Use `#` to add memories:
```
# Remember: fsmeta generation is async and may fail gracefully
# Remember: All mount paths must be file paths, not directories
```

**On Pattern Changes**:
- Update relevant CLAUDE.md sections
- Keep examples using real code paths
