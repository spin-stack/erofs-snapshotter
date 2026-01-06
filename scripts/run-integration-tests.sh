#!/bin/bash
# Run integration tests in Docker with containerd and nexuserofs snapshotter
#
# Usage:
#   ./scripts/run-integration-tests.sh [options]
#
# Options:
#   --build         Force rebuild of Docker image
#   --shell         Start interactive shell instead of tests
#   --keep          Keep container data after exit for debugging
#   --test NAME     Run only the specified test (e.g., --test pull_image)
#   --verbose, -v   Enable verbose output
#   -h, --help      Show this help message
#
# Examples:
#   ./scripts/run-integration-tests.sh                    # Run all tests
#   ./scripts/run-integration-tests.sh --build            # Rebuild image and run
#   ./scripts/run-integration-tests.sh --shell            # Interactive debugging
#   ./scripts/run-integration-tests.sh --test commit      # Run only commit test
#
# AI Assistant Usage (Claude Code):
#   - Run all tests:     ./scripts/run-integration-tests.sh
#   - Debug failures:    ./scripts/run-integration-tests.sh --shell --keep
#   - Run one test:      ./scripts/run-integration-tests.sh --test pull_image

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
IMAGE_NAME="nexuserofs-integration"
FORCE_BUILD=false
INTERACTIVE=false
KEEP_DATA=false
TEST_ARGS=()

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --build)
            FORCE_BUILD=true
            shift
            ;;
        --shell)
            INTERACTIVE=true
            shift
            ;;
        --keep)
            KEEP_DATA=true
            TEST_ARGS+=("--keep")
            shift
            ;;
        --test)
            TEST_ARGS+=("--test" "$2")
            shift 2
            ;;
        --verbose|-v)
            TEST_ARGS+=("--verbose")
            shift
            ;;
        -h|--help)
            head -24 "$0" | tail -n +2 | sed 's/^# //' | sed 's/^#//'
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Check if image exists
image_exists() {
    docker image inspect "${IMAGE_NAME}" &>/dev/null
}

# Build image
build_image() {
    echo "==> Building integration test image: ${IMAGE_NAME}"
    docker build -t "${IMAGE_NAME}" -f "${ROOT_DIR}/Dockerfile.integration" "${ROOT_DIR}"
}

# Build if needed
if [[ "${FORCE_BUILD}" == "true" ]] || ! image_exists; then
    build_image
else
    echo "==> Using existing image: ${IMAGE_NAME}"
    echo "    (use --build to force rebuild)"
fi

echo "==> Running integration tests"
echo "    Project root: ${ROOT_DIR}"

# Determine Go cache directories for faster builds
GO_MOD_CACHE="${GOMODCACHE:-${GOPATH:-$HOME/go}/pkg/mod}"
GO_BUILD_CACHE="${GOCACHE:-$HOME/.cache/go-build}"

# Ensure cache directories exist
mkdir -p "${GO_MOD_CACHE}" "${GO_BUILD_CACHE}"

# Docker run options
DOCKER_OPTS=(
    --privileged
    --rm
    --cgroupns=host
    -v /dev:/dev
    -v "${ROOT_DIR}:/workspace"
    -v "${GO_MOD_CACHE}:/go/pkg/mod"
    -v "${GO_BUILD_CACHE}:/root/.cache/go-build"
    -w /workspace
    --tmpfs /tmp:exec
    --tmpfs /run:exec
    --tmpfs /var/lib/containerd-test:exec
    --tmpfs /var/lib/nexuserofs-snapshotter:exec
)

# Mount Docker credentials if available
if [[ -f "${HOME}/.docker/config.json" ]]; then
    DOCKER_OPTS+=(-v "${HOME}/.docker/config.json:/root/.docker/config.json:ro")
    echo "==> Using Docker credentials from ${HOME}/.docker/config.json"
fi

# Pass through keep option
if [[ "${KEEP_DATA}" == "true" ]]; then
    DOCKER_OPTS+=(-e CLEANUP_ON_EXIT=false)
    # Remove tmpfs mounts so data persists
    DOCKER_OPTS=("${DOCKER_OPTS[@]/--tmpfs \/var\/lib\/containerd-test:exec/}")
    DOCKER_OPTS=("${DOCKER_OPTS[@]/--tmpfs \/var\/lib\/nexuserofs-snapshotter:exec/}")
fi

if [[ "${INTERACTIVE}" == "true" ]]; then
    echo "==> Starting interactive shell..."
    echo "    Run 'integration-test.sh' to start tests manually"
    docker run -it --entrypoint /bin/bash "${DOCKER_OPTS[@]}" "${IMAGE_NAME}"
elif [[ ${#TEST_ARGS[@]} -gt 0 ]]; then
    echo "==> Running with args: ${TEST_ARGS[*]}"
    docker run "${DOCKER_OPTS[@]}" "${IMAGE_NAME}" "${TEST_ARGS[@]}"
else
    docker run "${DOCKER_OPTS[@]}" "${IMAGE_NAME}"
fi
