# Nexus EROFS

[![CI](https://github.com/aledbf/nexus-erofs/actions/workflows/ci.yml/badge.svg)](https://github.com/aledbf/nexus-erofs/actions/workflows/ci.yml)
[![codecov](https://codecov.io/github/aledbf/nexus-erofs/graph/badge.svg?token=WLFBVZ0DSM)](https://codecov.io/github/aledbf/nexus-erofs)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

A containerd snapshotter that converts OCI image layers to EROFS and returns raw file paths for VM-based container runtimes.

## Why nexus-erofs?

**The problem:** Traditional snapshotters mount filesystems on the host, then bind-mount them into containers. This doesn't work for VM-based runtimes where containers run inside guest VMs with their own kernels.

**The solution:** nexus-erofs returns *file paths* instead of *mounted directories*. VM runtimes pass these files directly to QEMU as virtio-blk devices. The guest kernel mounts them internally.

| Feature | What it means |
|---------|---------------|
| **On-the-fly conversion** | Standard OCI tar layers → EROFS during `pull` |
| **Single device for multi-layer** | VMDK descriptor concatenates all layers into one block device |
| **No host mounts for containers** | Guest VM handles all filesystem operations |
| **Works with standard images** | No pre-conversion needed, pulls from any registry |

## This is NOT

- **A general-purpose snapshotter** - Only for VM runtimes that support the VMDK format
- **A replacement for overlayfs snapshotter** - Use that for runc/crun containers
- **containerd's built-in EROFS snapshotter** - That one mounts on host; this one doesn't

## How It Differs

```
┌─────────────────────────────────────────────────────────────────────┐
│ Traditional Snapshotter (overlayfs, native EROFS)                   │
├─────────────────────────────────────────────────────────────────────┤
│  Host kernel mounts layers → Container sees mounted filesystem      │
│  Returns: /var/lib/containerd/snapshots/123/fs (mounted directory)  │
└─────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│ nexus-erofs (VM-only)                                               │
├─────────────────────────────────────────────────────────────────────┤
│  Host returns file paths → VM passes as virtio-blk → Guest mounts   │
│  Returns: /var/lib/nexus-erofs/snapshots/123/merged.vmdk (file)     │
└─────────────────────────────────────────────────────────────────────┘
```

### vs containerd's Built-in EROFS Snapshotter

| Aspect | containerd EROFS | nexus-erofs |
|--------|------------------|-------------|
| Target runtime | runc, crun (host containers) | qemubox (VM containers) |
| Layer source | Requires pre-converted EROFS | Converts tar→EROFS on pull |
| Multi-layer | Host overlayfs stacking | VMDK descriptor (single block device) |
| Returns | Mounted paths | Raw file paths |

## Quick Start

```bash
# 1. Start the snapshotter
sudo nexus-erofs-snapshotter \
  --root /var/lib/nexus-erofs-snapshotter \
  --address /run/nexus-erofs-snapshotter/snapshotter.sock \
  --containerd-address /run/containerd/containerd.sock

# 2. Pull an image using the snapshotter
ctr images pull --snapshotter nexus-erofs docker.io/library/alpine:latest

# 3. The snapshotter returns file paths (not mounted directories)
ctr snapshots --snapshotter nexus-erofs mounts /tmp/mnt alpine-snapshot
# Output: mount -t erofs /var/lib/nexus-erofs-snapshotter/snapshots/1/layer.erofs /tmp/mnt
```

See [Configuration](#configuration) for containerd integration.

## Architecture

```mermaid
flowchart LR
    subgraph containerd
        Pull[Image Pull]
    end

    subgraph nexus["nexus-erofs"]
        Differ[tar → EROFS]
        Snap[Return file paths]
    end

    subgraph vm["VM Runtime"]
        QEMU[virtio-blk]
    end

    subgraph guest["Guest VM"]
        Mount[mount -t erofs]
    end

    Pull -->|gRPC| Differ
    Differ --> Snap
    Snap -->|"merged.vmdk"| QEMU
    QEMU -->|/dev/vda| Mount
```

**Data flow:**
1. containerd pulls image, calls nexus-erofs differ
2. Differ converts each tar layer to EROFS (`mkfs.erofs --tar=f`)
3. For multi-layer: generates `fsmeta.erofs` + `merged.vmdk`
4. VM runtime receives file paths, passes to QEMU as block devices
5. Guest kernel mounts EROFS, creates overlay with ext4 upper

## How It Works

### Image Pull (Layer Extraction)

When pulling an image, containerd calls the EROFS differ to apply each layer:

```
1. Prepare("extract-sha256:abc...", parent)
   ├── Creates ext4 file (rwlayer.img)
   ├── Mounts ext4 on host via loop device
   └── Returns bind mount for fallback differs

2. EROFS differ converts tar stream directly to EROFS:
   ├── mkfs.erofs --tar=f reads tar from stdin → layer.erofs
   └── (Optional: --tar=i mode creates tar index + appends raw tar)

3. Commit("layer-sha256:abc...", "extract-sha256:abc...")
   ├── layer.erofs already exists (created by differ)
   ├── Unmounts ext4 (cleanup)
   └── Generates fsmeta.erofs + merged.vmdk (for multi-layer images)
```

**Note:** The ext4 mount is only used if a non-EROFS differ (e.g., walking differ) processes the layer. The EROFS differ bypasses this entirely by piping tar data directly to `mkfs.erofs`.

### Container Run

When running a container, the snapshotter returns raw file paths with mount options:

```go
// View (read-only) with VMDK - single fsmeta mount with device= options
// VM runtime detects merged.vmdk in same directory and uses it for QEMU
[]mount.Mount{{
    Type:   "erofs",
    Source: "/var/lib/nexus-erofs/snapshots/123/fsmeta.erofs",
    Options: []string{"ro", "loop", "device=/path/to/layer1.erofs", "device=/path/to/layer2.erofs"},
}}

// View (read-only) single layer - returns EROFS layer directly
[]mount.Mount{{
    Type:    "erofs",
    Source:  "/path/to/layer.erofs",
    Options: []string{"ro", "loop"},
}}

// View (read-only) multi-layer fallback (when fsmeta generation fails)
// This happens when layers have incompatible block sizes (e.g., tar-index mode)
[]mount.Mount{
    {Type: "erofs", Source: "/path/to/layer1.erofs", Options: []string{"ro", "loop"}},
    {Type: "erofs", Source: "/path/to/layer2.erofs", Options: []string{"ro", "loop"}},
}

// Active (with writable layer) - EROFS lower + ext4 upper
[]mount.Mount{
    {Type: "erofs", Source: "/path/to/fsmeta.erofs", Options: []string{"ro", "loop", "device=..."}},
    {Type: "ext4",  Source: "/path/to/rwlayer.img",  Options: []string{"rw", "loop"}},
}
```

The VM runtime (qemubox) passes these as virtio-blk devices. The guest VM mounts them and creates an overlay.

**Fallback behavior:** When fsmeta/VMDK generation fails (e.g., `mkfs.erofs` lacks `--aufs` support, or layers have incompatible block sizes from `--tar=i` mode), the snapshotter returns individual EROFS mounts. The consumer must handle stacking these layers.

### VMDK: Single Virtual Disk for Multiple Layers

For multi-layer images, nexus-erofs generates a **VMDK descriptor** that concatenates:
- `fsmeta.erofs` - Metadata-only EROFS referencing all layer blobs
- `layer1.erofs`, `layer2.erofs`, ... - Individual layer blobs

This allows QEMU to present all layers as a **single block device**, simplifying guest mounting:

```
merged.vmdk (descriptor)
├── fsmeta.erofs (metadata, maps to layer blobs)
├── layer-abc.erofs
├── layer-def.erofs
└── layer-123.erofs

Guest sees: /dev/vda (single device containing entire image)
```

### Container Commit

Creating a new image from a running container:

```bash
nerdctl --address /var/run/qemubox/containerd.sock \
    commit --snapshotter nexus-erofs \
    container-name docker.io/user/image:tag
```

Flow:
```
1. Get active snapshot mounts (EROFS layers + mounted ext4 upper)

2. Differ compares lower (EROFS) vs upper (ext4) to produce tar diff

3. New layer committed:
   ├── ext4 upper directory → mkfs.erofs → new-layer.erofs
   ├── Unmount ext4
   └── Update image manifest with new layer
```

The commit reads from the **mounted ext4 writable layer** where container changes accumulated, converts to EROFS, and creates a new image layer.

## Snapshot Lifecycle

```mermaid
stateDiagram-v2
    [*] --> Active: Prepare()
    Active --> Committed: Commit()
    Committed --> View: View()

    Active --> ExtractMount: extract keys

    state Active {
        [*] --> ext4_file
        ext4_file: rwlayer.img created
    }

    state ExtractMount {
        [*] --> mounted
        mounted: ext4 mounted on host for differ
    }

    state Committed {
        [*] --> erofs
        erofs: layer.erofs + fsmeta.erofs + merged.vmdk
    }

    state View {
        [*] --> readonly
        readonly: Returns VMDK/EROFS paths for VM
    }
```

## Storage Layout

```
/var/lib/nexus-erofs-snapshotter/
├── metadata.db              # BBolt database (snapshot metadata)
├── mounts.db                # BBolt database (mount manager state)
└── snapshots/
    └── {id}/
        ├── .erofslayer      # Marker file for EROFS differ
        ├── rwlayer.img      # ext4 writable layer file
        ├── rw/              # Mount point for ext4 (during extraction only)
        │   ├── upper/       # Overlay upper directory (fallback differs)
        │   └── work/        # Overlay work directory (fallback differs)
        ├── lower/           # Empty directory for View snapshots with no parent
        ├── layer.erofs      # Committed EROFS layer blob
        ├── fsmeta.erofs     # Merged metadata (multi-layer, requires --aufs)
        └── merged.vmdk      # VMDK descriptor for QEMU (requires --vmdk-desc)
```

## Requirements

### Runtime

- Linux kernel with EROFS support (5.4+)
- erofs-utils with the following features:
  - `--tar=f` : Convert tar streams directly to EROFS (required)
  - `--aufs`: AUFS-style whiteout handling for OCI layers (required)
  - `--vmdk-desc`: Generate VMDK descriptors for multi-layer images (required for fsmeta)
  - `-Enoinline_data`: Disable inline data for better block alignment
  - `--sort=none`: Skip data sorting when no compression (performance optimization)
- e2fsprogs (mkfs.ext4 for writable layers)
- util-linux (losetup for loop devices)
- containerd 2.0+

### erofs-utils

Each release includes a pre-built `mkfs.erofs` binary with all required features enabled. We recommend using this bundled version to ensure compatibility.

The bundled binary is built from the erofs-utils source with patches for features that may not yet be in upstream releases (like `--vmdk-desc`).

To verify feature support:

```bash
# Check for tar mode support
mkfs.erofs --help | grep -q '\-\-tar=' && echo "tar mode: OK"

# Check for VMDK descriptor support
mkfs.erofs --help | grep -q '\-\-vmdk-desc' && echo "vmdk-desc: OK"
```

### Build

- Go 1.25+
- [Task](https://taskfile.dev)

## Building

```bash
task build
```

Or cross-compile for Linux:

```bash
task build-linux
```

## Running

```bash
sudo ./bin/nexus-erofs-snapshotter \
  --root /var/lib/nexus-erofs-snapshotter \
  --address /run/nexus-erofs-snapshotter/snapshotter.sock \
  --containerd-address /run/containerd/containerd.sock
```

## Configuration

### containerd

Add these sections to your containerd config:

```toml
# /etc/containerd/config.toml
version = 2

# Register the external snapshotter and differ
[proxy_plugins]
  [proxy_plugins.nexus-erofs]
    type = "snapshot"
    address = "/run/nexus-erofs-snapshotter/snapshotter.sock"

  [proxy_plugins.nexus-erofs-diff]
    type = "diff"
    address = "/run/nexus-erofs-snapshotter/snapshotter.sock"

[plugins]
  # Configure diff service to use nexus-erofs-diff (with walking as fallback)
  [plugins."io.containerd.service.v1.diff-service"]
    default = ["nexus-erofs-diff", "walking"]

  # Configure layer unpacking to use nexus-erofs
  [plugins."io.containerd.transfer.v1.local"]
    [[plugins."io.containerd.transfer.v1.local".unpack_config]]
      platform = "linux/amd64"
      snapshotter = "nexus-erofs"
      differ = "nexus-erofs-diff"

  # For CRI (Kubernetes): use nexus-erofs for image pulls
  [plugins."io.containerd.cri.v1.images"]
    snapshotter = "nexus-erofs"
```

**Key configuration sections:**

| Section | Purpose |
|---------|---------|
| `proxy_plugins` | Registers the external snapshotter and differ services |
| `diff-service.default` | Prioritizes nexus-erofs-diff for layer application |
| `transfer.v1.local.unpack_config` | Tells containerd which snapshotter/differ to use for unpacking |
| `cri.v1.images.snapshotter` | (Optional) Makes CRI use nexus-erofs for Kubernetes workloads |

### Snapshotter Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--root` | `/var/lib/nexus-erofs-snapshotter` | Root directory for snapshotter data |
| `--address` | `/run/nexus-erofs-snapshotter/snapshotter.sock` | Unix socket address |
| `--containerd-address` | `/run/containerd/containerd.sock` | containerd socket |
| `--containerd-namespace` | `default` | containerd namespace to use |
| `--log-level` | `info` | Log level (debug, info, warn, error) |
| `--default-size` | `64M` | Size of ext4 writable layer (bytes) |
| `--set-immutable` | `true` | Set immutable flag on committed layers |
| `--version` | | Show version information |

### Layer Conversion

Layers are created using full conversion mode (`--tar=f`) **without compression**. This is required because:

- **fsmeta compatibility**: Compressed EROFS layers use "datalayout 3" which is incompatible with fsmeta merge
- **VMDK generation**: This snapshotter always generates VMDK descriptors for multi-layer images, which requires fsmeta merge
- **Block size**: Default 4KB blocks ensure compatibility and optimal random read performance

Compression cannot be enabled because it would break multi-layer image support.

## License

Apache 2.0
