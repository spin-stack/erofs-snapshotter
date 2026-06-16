#!/bin/bash
# Build erofs-utils from source
# This script downloads, compiles, and installs erofs-utils v1.9.1
# with static linking for portability.
#
# Usage:
#   ./build-erofs-utils.sh [install_prefix]
#
# Arguments:
#   install_prefix: Directory to install binaries (default: /usr/local)
#
# For cross-compilation, set these environment variables:
#   CROSS_COMPILE: Cross-compiler prefix (e.g., aarch64-linux-gnu-)
#   TARGET_ARCH: Target architecture for output naming (e.g., arm64)

set -euo pipefail

EROFS_VERSION="${EROFS_VERSION:-1.9.1}"
EROFS_URL="https://github.com/erofs/erofs-utils/archive/refs/tags/v${EROFS_VERSION}.tar.gz"
INSTALL_PREFIX="${1:-/usr/local}"
BUILD_DIR="${BUILD_DIR:-$(mktemp -d)}"
CROSS_COMPILE="${CROSS_COMPILE:-}"
TARGET_ARCH="${TARGET_ARCH:-$(uname -m)}"

# Map architecture names
case "$TARGET_ARCH" in
    x86_64|amd64) TARGET_ARCH="amd64" ;;
    aarch64|arm64) TARGET_ARCH="arm64" ;;
esac

echo "Building erofs-utils v${EROFS_VERSION}"
echo "  Build directory: ${BUILD_DIR}"
echo "  Install prefix: ${INSTALL_PREFIX}"
echo "  Target arch: ${TARGET_ARCH}"
[ -n "$CROSS_COMPILE" ] && echo "  Cross compile: ${CROSS_COMPILE}"

# Run command with sudo if not root
maybe_sudo() {
    if [ "$(id -u)" -eq 0 ]; then
        "$@"
    else
        sudo "$@"
    fi
}

# Install build dependencies (Ubuntu/Debian)
install_deps() {
    if command -v apt-get &>/dev/null; then
        maybe_sudo apt-get update
        maybe_sudo apt-get install -y \
            build-essential \
            autoconf \
            automake \
            libtool \
            pkg-config \
            uuid-dev \
            liblz4-dev \
            libzstd-dev \
            zlib1g-dev \
            liblzma-dev \
            curl

        # Install cross-compilation tools if needed
        if [ -n "$CROSS_COMPILE" ]; then
            maybe_sudo apt-get install -y \
                gcc-aarch64-linux-gnu \
                g++-aarch64-linux-gnu \
                libc6-dev-arm64-cross
        fi
    fi
}

# Download and extract source
download_source() {
    echo "Downloading erofs-utils v${EROFS_VERSION}..."
    cd "$BUILD_DIR"
    curl -sSL "$EROFS_URL" -o erofs-utils.tar.gz
    tar -xzf erofs-utils.tar.gz
    cd "erofs-utils-${EROFS_VERSION}"
}

# Build erofs-utils
build_erofs() {
    echo "Configuring erofs-utils..."

    # Run autogen if configure doesn't exist
    if [ ! -f configure ]; then
        ./autogen.sh
    fi

    # Set cross-compilation options
    local configure_opts=()
    if [ -n "$CROSS_COMPILE" ]; then
        configure_opts+=(
            "--host=aarch64-linux-gnu"
            "CC=${CROSS_COMPILE}gcc"
            "CXX=${CROSS_COMPILE}g++"
        )
    fi

    # Configure with static linking where possible
    # Note: We build with shared libs but the binaries are standalone
    ./configure \
        --prefix="$INSTALL_PREFIX" \
        --enable-lz4 \
        --enable-lzma \
        --enable-zstd \
        --disable-fuse \
        "${configure_opts[@]+"${configure_opts[@]}"}"

    echo "Building erofs-utils..."
    make -j"$(nproc)"
}

# Install binaries
install_binaries() {
    echo "Installing binaries to ${INSTALL_PREFIX}/bin..."
    mkdir -p "${INSTALL_PREFIX}/bin"

    # Copy the main binaries
    for bin in mkfs/mkfs.erofs fsck/fsck.erofs dump/dump.erofs; do
        if [ -f "$bin" ]; then
            cp "$bin" "${INSTALL_PREFIX}/bin/"
        fi
    done

    echo "Installed binaries:"
    ls -la "${INSTALL_PREFIX}/bin/"
}

# Verify build
verify_build() {
    echo "Verifying build..."
    if [ -z "$CROSS_COMPILE" ]; then
        "${INSTALL_PREFIX}/bin/mkfs.erofs" --version
    else
        file "${INSTALL_PREFIX}/bin/mkfs.erofs"
    fi
}

# Main
main() {
    install_deps
    download_source
    build_erofs
    install_binaries
    verify_build

    echo ""
    echo "erofs-utils v${EROFS_VERSION} built successfully!"
    echo "Binaries installed to: ${INSTALL_PREFIX}/bin/"

    # Cleanup
    if [ "${KEEP_BUILD_DIR:-false}" != "true" ]; then
        rm -rf "$BUILD_DIR"
    fi
}

main "$@"
