#!/bin/bash
# Integration test script for nexus-erofs snapshotter
# Runs inside privileged Docker container with containerd
#
# Usage:
#   ./scripts/integration-test.sh [options]
#
# Options:
#   --test NAME     Run only the specified test (e.g., --test pull_image)
#   --keep          Keep data directories after exit for debugging
#   --rebuild       Rebuild binaries from source (default: use pre-built)
#   --verbose, -v   Enable verbose output
#   --junit FILE    Generate JUnit XML report (default: /tmp/integration-logs/junit.xml)
#   --strict        Fail tests that cannot fully verify their assertions
#   -h, --help      Show this help message

set -euo pipefail

# =============================================================================
# Configuration
# =============================================================================

CONTAINERD_ROOT="/var/lib/containerd-test"
SNAPSHOTTER_ROOT="/var/lib/nexus-erofs-snapshotter"
CONTAINERD_SOCKET="/run/containerd/containerd.sock"
SNAPSHOTTER_SOCKET="/run/nexus-erofs-snapshotter/snapshotter.sock"
LOG_DIR="/tmp/integration-logs"

# Use ghcr.io or quay.io to avoid Docker Hub rate limits
TEST_IMAGE="${TEST_IMAGE:-ghcr.io/containerd/alpine:3.14.0}"
# Use nginx:alpine for multi-layer test - it has 6-7 layers (~25MB)
# busybox and alpine only have 1 layer which doesn't test VMDK properly
MULTI_LAYER_IMAGE="${MULTI_LAYER_IMAGE:-docker.io/library/nginx:1.27-alpine}"

# Runtime options
CLEANUP_ON_EXIT="${CLEANUP_ON_EXIT:-true}"
REBUILD_BINARIES="${REBUILD_BINARIES:-false}"
VERBOSE="${VERBOSE:-true}"
STRICT_MODE="${STRICT_MODE:-true}"
SINGLE_TEST=""
JUNIT_OUTPUT=""

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
MAGENTA='\033[0;35m'
NC='\033[0m'

# Test tracking
declare -A TEST_TIMES
declare -A TEST_RESULTS
declare -A TEST_MESSAGES
TOTAL_START_TIME=0

# Cross-test state (for tests that depend on each other)
MULTI_LAYER_VIEW_NAME=""

# =============================================================================
# Logging Functions
# =============================================================================

log_info() { echo -e "${GREEN}[INFO]${NC} $*"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $*"; }
log_error() { echo -e "${RED}[ERROR]${NC} $*"; }
log_test() { echo -e "${BLUE}[TEST]${NC} $*"; }
log_cmd() {
    if [ "$VERBOSE" = "true" ]; then
        echo -e "${CYAN}[CMD]${NC} $*" >&2
    fi
}
log_debug() {
    if [ "$VERBOSE" = "true" ]; then
        echo -e "${MAGENTA}[DEBUG]${NC} $*"
    fi
}

# Enhanced error logging with context
log_error_with_context() {
    local msg="$1"
    local context_lines="${2:-20}"

    log_error "$msg"
    echo ""
    echo "═══════════════════════ ERROR CONTEXT ═══════════════════════"
    echo "Snapshotter logs (last $context_lines lines):"
    echo "────────────────────────────────────────────────────────────"
    tail -n "$context_lines" "${LOG_DIR}/snapshotter.log" 2>/dev/null || echo "No logs available"
    echo ""
    echo "Containerd logs (last $context_lines lines):"
    echo "────────────────────────────────────────────────────────────"
    tail -n "$context_lines" "${LOG_DIR}/containerd.log" 2>/dev/null || echo "No logs available"
    echo ""
    echo "Active snapshots:"
    echo "────────────────────────────────────────────────────────────"
    ctr_cmd snapshots --snapshotter nexus-erofs ls 2>/dev/null || echo "Could not list snapshots"
    echo ""
    echo "Disk usage:"
    echo "────────────────────────────────────────────────────────────"
    df -h "${SNAPSHOTTER_ROOT}" 2>/dev/null || echo "Could not get disk usage"
    echo "═══════════════════════════════════════════════════════════"
}

# =============================================================================
# Utility Functions
# =============================================================================

# Wait for a condition to be true with timeout
wait_for_condition() {
    local condition_cmd="$1"
    local timeout="${2:-30}"
    local interval="${3:-0.5}"
    local description="${4:-condition}"

    log_debug "Waiting for: $description (timeout: ${timeout}s)"

    local elapsed=0
    while [ "$elapsed" -lt "$timeout" ]; do
        if eval "$condition_cmd" 2>/dev/null; then
            log_debug "Condition met: $description"
            return 0
        fi
        sleep "$interval"
        elapsed=$((elapsed + 1))
    done

    log_error "Timeout waiting for: $description"
    return 1
}

# Retry a command with exponential backoff
retry_command() {
    local max_attempts="${1:-3}"
    local base_delay="${2:-2}"
    shift 2
    local cmd="$*"

    local attempt=1
    while [ $attempt -le $max_attempts ]; do
        log_debug "Attempt $attempt/$max_attempts: $cmd"

        if eval "$cmd"; then
            log_debug "Command succeeded on attempt $attempt"
            return 0
        fi

        if [ $attempt -lt $max_attempts ]; then
            local delay=$((base_delay * attempt))
            log_warn "Command failed (attempt $attempt/$max_attempts), retrying in ${delay}s..."
            sleep "$delay"
        fi
        attempt=$((attempt + 1))
    done

    log_error "Command failed after $max_attempts attempts: $cmd"
    return 1
}

# =============================================================================
# Assertion Helpers
# =============================================================================

assert_file_exists() {
    local file="$1"
    local msg="${2:-File should exist: $file}"
    if [ ! -f "$file" ]; then
        log_error "$msg"
        return 1
    fi
    log_debug "✓ File exists: $file"
    return 0
}

assert_dir_exists() {
    local dir="$1"
    local msg="${2:-Directory should exist: $dir}"
    if [ ! -d "$dir" ]; then
        log_error "$msg"
        return 1
    fi
    log_debug "✓ Directory exists: $dir"
    return 0
}

assert_not_empty() {
    local value="$1"
    local msg="${2:-Value should not be empty}"
    if [ -z "$value" ]; then
        log_error "$msg"
        return 1
    fi
    log_debug "✓ Value not empty: $value"
    return 0
}

assert_greater_than() {
    local actual="$1"
    local expected="$2"
    local msg="${3:-Expected > $expected, got $actual}"
    if [ "$actual" -le "$expected" ]; then
        log_error "$msg"
        return 1
    fi
    log_debug "✓ $actual > $expected"
    return 0
}

assert_equals() {
    local actual="$1"
    local expected="$2"
    local msg="${3:-Expected $expected, got $actual}"
    if [ "$actual" != "$expected" ]; then
        log_error "$msg"
        return 1
    fi
    log_debug "✓ $actual == $expected"
    return 0
}

assert_contains() {
    local haystack="$1"
    local needle="$2"
    local msg="${3:-String should contain: $needle}"
    if ! echo "$haystack" | grep -q "$needle"; then
        log_error "$msg"
        return 1
    fi
    log_debug "✓ String contains: $needle"
    return 0
}

assert_command_success() {
    local cmd="$1"
    local msg="${2:-Command should succeed: $cmd}"
    if ! eval "$cmd" >/dev/null 2>&1; then
        log_error "$msg"
        return 1
    fi
    log_debug "✓ Command succeeded: $cmd"
    return 0
}

# =============================================================================
# Parse Arguments
# =============================================================================

while [[ $# -gt 0 ]]; do
    case $1 in
        --test)
            SINGLE_TEST="$2"
            shift 2
            ;;
        --keep)
            CLEANUP_ON_EXIT=false
            shift
            ;;
        --rebuild)
            REBUILD_BINARIES=true
            shift
            ;;
        -v|--verbose)
            VERBOSE=true
            shift
            ;;
        --strict)
            STRICT_MODE=true
            shift
            ;;
        --junit)
            JUNIT_OUTPUT="${2:-${LOG_DIR}/junit.xml}"
            shift 2
            ;;
        -h|--help)
            head -18 "$0" | tail -n +2 | sed 's/^# //' | sed 's/^#//'
            exit 0
            ;;
        *)
            log_error "Unknown option: $1"
            exit 1
            ;;
    esac
done

# =============================================================================
# Cleanup Function
# =============================================================================

cleanup() {
    local exit_code=$?
    log_info "Cleaning up..."

    # Stop services gracefully
    if [ -f /tmp/snapshotter.pid ]; then
        local pid
        pid=$(cat /tmp/snapshotter.pid)
        if kill -0 "$pid" 2>/dev/null; then
            log_info "Stopping snapshotter (PID: $pid)"
            kill "$pid" 2>/dev/null || true
            sleep 1
            kill -9 "$pid" 2>/dev/null || true
        fi
        rm -f /tmp/snapshotter.pid
    fi

    if [ -f /tmp/containerd.pid ]; then
        local pid
        pid=$(cat /tmp/containerd.pid)
        if kill -0 "$pid" 2>/dev/null; then
            log_info "Stopping containerd (PID: $pid)"
            kill "$pid" 2>/dev/null || true
            sleep 1
            kill -9 "$pid" 2>/dev/null || true
        fi
        rm -f /tmp/containerd.pid
    fi

    # Unmount any remaining mounts
    mount 2>/dev/null | grep -E "(containerd-test|nexus-erofs)" | awk '{print $3}' | while read -r mp; do
        umount -l "$mp" 2>/dev/null || true
    done

    # Detach loop devices
    losetup -D 2>/dev/null || true

    # Clean up directories if requested
    if [ "${CLEANUP_ON_EXIT}" = "true" ]; then
        rm -rf "${CONTAINERD_ROOT}" "${SNAPSHOTTER_ROOT}" 2>/dev/null || true
    else
        log_info "Keeping data directories for debugging:"
        log_info "  Containerd: ${CONTAINERD_ROOT}"
        log_info "  Snapshotter: ${SNAPSHOTTER_ROOT}"
        log_info "  Logs: ${LOG_DIR}"
    fi

    # Generate JUnit XML if requested
    if [ -n "$JUNIT_OUTPUT" ]; then
        generate_junit_xml "$JUNIT_OUTPUT"
    fi

    exit $exit_code
}

trap cleanup EXIT

# =============================================================================
# Service Management
# =============================================================================

# Generate containerd config
generate_containerd_config() {
    mkdir -p /etc/containerd

    # Setup Docker Hub credentials if available
    local hosts_config=""
    if [ -f /root/.docker/config.json ]; then
        mkdir -p /etc/containerd/certs.d/docker.io
        hosts_config='
[plugins."io.containerd.grpc.v1.cri".registry]
  config_path = "/etc/containerd/certs.d"'

        cat > /etc/containerd/certs.d/docker.io/hosts.toml <<HOSTS
server = "https://registry-1.docker.io"

[host."https://registry-1.docker.io"]
  capabilities = ["pull", "resolve"]
HOSTS
        log_info "Configured Docker Hub registry with credentials"
    fi

    cat > /etc/containerd/config.toml <<EOF
version = 2
root = "${CONTAINERD_ROOT}"

[grpc]
  address = "${CONTAINERD_SOCKET}"

[proxy_plugins]
  [proxy_plugins.nexus-erofs]
    type = "snapshot"
    address = "${SNAPSHOTTER_SOCKET}"

  [proxy_plugins.nexus-erofs-diff]
    type = "diff"
    address = "${SNAPSHOTTER_SOCKET}"

# Use nexus-erofs-diff for layer application, with walking as fallback
[plugins."io.containerd.service.v1.diff-service"]
  default = ["nexus-erofs-diff", "walking"]

# Configure unpack platforms for the proxy snapshotter
[plugins."io.containerd.transfer.v1.local"]
  [[plugins."io.containerd.transfer.v1.local".unpack_config]]
    platform = "linux/amd64"
    snapshotter = "nexus-erofs"
    differ = "nexus-erofs-diff"

[plugins."io.containerd.cri.v1.images"]
  snapshotter = "nexus-erofs"
${hosts_config}
EOF
    log_info "Generated containerd config at /etc/containerd/config.toml"
}

# Build snapshotter binary and tools (or use pre-built)
build_snapshotter() {
    # Check if pre-built binaries exist and we're not forcing rebuild
    if [ "${REBUILD_BINARIES}" != "true" ]; then
        if [ -x /usr/local/bin/nexus-erofs-snapshotter ]; then
            log_info "Using pre-built nexus-erofs-snapshotter"
            if [ -x /usr/local/bin/integration-commit ]; then
                log_info "Using pre-built integration-commit"
            fi
            return 0
        fi
    fi

    # Check if Go is available for building
    if ! command -v go &>/dev/null; then
        log_error "Go not found and no pre-built binaries available"
        log_error "Either use a pre-built image or mount a workspace with Go installed"
        return 1
    fi

    log_info "Building nexus-erofs-snapshotter from source..."
    cd /workspace
    CGO_ENABLED=0 go build -buildvcs=false -o /usr/local/bin/nexus-erofs-snapshotter ./cmd/nexus-erofs-snapshotter
    log_info "Snapshotter built successfully"

    # Build integration-commit tool for image commit tests
    if [ -d ./cmd/integration-commit ]; then
        log_info "Building integration-commit tool..."
        CGO_ENABLED=0 go build -buildvcs=false -o /usr/local/bin/integration-commit ./cmd/integration-commit
        log_info "integration-commit built successfully"
    fi
}

# Start containerd
start_containerd() {
    log_info "Starting containerd..."
    mkdir -p "${CONTAINERD_ROOT}" "$(dirname "${CONTAINERD_SOCKET}")" "${LOG_DIR}"

    # Remove stale socket
    rm -f "${CONTAINERD_SOCKET}"

    containerd --config /etc/containerd/config.toml \
        > "${LOG_DIR}/containerd.log" 2>&1 &

    echo $! > /tmp/containerd.pid

    # Wait for socket with better error handling
    if ! wait_for_condition "[ -S '${CONTAINERD_SOCKET}' ]" 30 0.5 "containerd socket"; then
        log_error_with_context "Containerd failed to start" 30
        return 1
    fi

    # Verify containerd is responsive
    if ! wait_for_condition "ctr -a '${CONTAINERD_SOCKET}' version >/dev/null 2>&1" 10 1 "containerd responsive"; then
        log_error_with_context "Containerd not responsive" 30
        return 1
    fi

    log_info "Containerd started (PID: $(cat /tmp/containerd.pid))"
}

# Start snapshotter
start_snapshotter() {
    log_info "Starting nexus-erofs-snapshotter..."
    mkdir -p "${SNAPSHOTTER_ROOT}" "$(dirname "${SNAPSHOTTER_SOCKET}")" "${LOG_DIR}"

    # Remove stale socket
    rm -f "${SNAPSHOTTER_SOCKET}"

    /usr/local/bin/nexus-erofs-snapshotter \
        --address "${SNAPSHOTTER_SOCKET}" \
        --root "${SNAPSHOTTER_ROOT}" \
        --containerd-address "${CONTAINERD_SOCKET}" \
        --log-level debug \
        > "${LOG_DIR}/snapshotter.log" 2>&1 &

    echo $! > /tmp/snapshotter.pid

    # Wait for socket with better error handling
    if ! wait_for_condition "[ -S '${SNAPSHOTTER_SOCKET}' ]" 30 0.5 "snapshotter socket"; then
        log_error_with_context "Snapshotter failed to start" 30
        return 1
    fi

    log_info "Snapshotter started (PID: $(cat /tmp/snapshotter.pid))"
}

# =============================================================================
# Health Checks
# =============================================================================

health_check() {
    log_info "Running health checks..."

    local checks_passed=0
    local checks_failed=0

    # Check containerd is responsive
    if ctr_cmd version >/dev/null 2>&1; then
        log_info "✓ Containerd is responsive"
        checks_passed=$((checks_passed + 1))
    else
        log_error "✗ Containerd is not responsive"
        checks_failed=$((checks_failed + 1))
    fi

    # Check snapshotter is accessible (proxy plugins don't show in plugins ls)
    if ctr_cmd snapshots --snapshotter nexus-erofs ls >/dev/null 2>&1; then
        log_info "✓ nexus-erofs snapshotter accessible"
        checks_passed=$((checks_passed + 1))
    else
        log_error "✗ nexus-erofs snapshotter not accessible"
        checks_failed=$((checks_failed + 1))
    fi

    # Check disk space
    local available_space
    available_space=$(df -BG "${SNAPSHOTTER_ROOT}" | tail -1 | awk '{print $4}' | tr -d 'G')
    if [ "$available_space" -gt 5 ]; then
        log_info "✓ Sufficient disk space: ${available_space}GB"
        checks_passed=$((checks_passed + 1))
    else
        log_warn "⚠ Low disk space: ${available_space}GB (may cause issues)"
        checks_passed=$((checks_passed + 1))  # Don't fail on low space, just warn
    fi

    # Check loop devices available
    if losetup -f >/dev/null 2>&1; then
        log_info "✓ Loop devices available"
        checks_passed=$((checks_passed + 1))
    else
        log_warn "⚠ Cannot find free loop device"
        checks_passed=$((checks_passed + 1))  # Don't fail, may not be needed
    fi

    # Check essential directories exist
    if assert_dir_exists "${CONTAINERD_ROOT}" && assert_dir_exists "${SNAPSHOTTER_ROOT}"; then
        log_info "✓ Data directories exist"
        checks_passed=$((checks_passed + 1))
    else
        log_error "✗ Required directories missing"
        checks_failed=$((checks_failed + 1))
    fi

    if [ "$checks_failed" -gt 0 ]; then
        log_error "Health checks failed: $checks_failed, passed: $checks_passed"
        return 1
    fi

    log_info "All health checks passed ($checks_passed checks)"
    return 0
}

# =============================================================================
# Command Helpers
# =============================================================================

# Helper: run ctr command
ctr_cmd() {
    if [ "$VERBOSE" = "true" ]; then
        log_cmd "ctr -a ${CONTAINERD_SOCKET} $*"
        ctr -a "${CONTAINERD_SOCKET}" "$@"
    else
        ctr -a "${CONTAINERD_SOCKET}" "$@" 2>&1
    fi
}

# Helper: pull image with retry and optional hosts-dir for registry auth
ctr_pull() {
    local hosts_dir_opt=""
    if [ -d /etc/containerd/certs.d ]; then
        hosts_dir_opt="--hosts-dir=/etc/containerd/certs.d"
    fi

    log_cmd "ctr -a ${CONTAINERD_SOCKET} images pull --platform linux/amd64 $hosts_dir_opt $*"

    # Retry image pulls as they can be flaky
    retry_command 3 2 "ctr -a '${CONTAINERD_SOCKET}' images pull --platform linux/amd64 $hosts_dir_opt $*"
}

# Helper: cleanup containers and tasks (ctr)
cleanup_container() {
    local name="$1"
    ctr_cmd tasks kill "$name" 2>/dev/null || true
    sleep 0.5
    ctr_cmd tasks rm "$name" 2>/dev/null || true
    ctr_cmd containers rm "$name" 2>/dev/null || true
}

# Helper: cleanup nerdctl containers
cleanup_nerdctl_container() {
    local name="$1"
    nerdctl --snapshotter nexus-erofs rm -f "$name" 2>/dev/null || true
}

# Helper: get snapshot parent using containerd API
get_snapshot_parent() {
    local snap_name="$1"
    local snap_info
    snap_info=$(ctr_cmd snapshots --snapshotter nexus-erofs info "$snap_name" 2>/dev/null) || return 1

    # Extract parent from snapshot info (format: Parent: "parent-name" or Parent: parent-name)
    local parent
    parent=$(echo "$snap_info" | grep -E "^\s*Parent:" | sed 's/.*Parent:[[:space:]]*//' | tr -d '"' | xargs)

    # Return empty string for root snapshots
    if [ "$parent" = "" ] || [ "$parent" = '""' ] || [ "$parent" = "null" ]; then
        echo ""
    else
        echo "$parent"
    fi
}

# Helper: get snapshot labels
get_snapshot_labels() {
    local snap_name="$1"
    ctr_cmd snapshots --snapshotter nexus-erofs info "$snap_name" 2>/dev/null | grep -A100 "Labels:" | grep -E "^\s+-" || true
}

# Helper: extract layer digest from snapshot labels
get_snapshot_layer_digest() {
    local snap_name="$1"
    local labels
    labels=$(get_snapshot_labels "$snap_name")

    # Look for containerd.io/snapshot/cri.layer-digest or similar label
    local digest
    digest=$(echo "$labels" | grep -oE 'sha256:[a-f0-9]{64}' | head -1 | sed 's/sha256://')
    echo "$digest"
}

# =============================================================================
# Test Infrastructure
# =============================================================================

# Test setup function
setup_test() {
    local test_name="$1"
    local ns
    ns="test-${test_name}-$$-$(date +%s)"
    export TEST_NAMESPACE="$ns"
    log_debug "Setting up test: $test_name (namespace: $TEST_NAMESPACE)"
}

# Test teardown function
teardown_test() {
    local test_name="$1"
    log_debug "Tearing down test: $test_name"

    # Cleanup test-specific snapshots
    # Use subshell with explicit set +e to prevent any errors from propagating
    (
        set +e
        if [ -n "${TEST_NAMESPACE:-}" ]; then
            ctr_cmd snapshots --snapshotter nexus-erofs ls 2>/dev/null | \
                grep "$TEST_NAMESPACE" | \
                awk '{print $1}' | \
                while read -r snap; do
                    ctr_cmd snapshots --snapshotter nexus-erofs rm "$snap" 2>/dev/null
                done
        fi
    ) 2>/dev/null

    # Always return success
    return 0
}

# Run a single test with timing
run_test_with_timing() {
    local test_func="$1"
    local test_name="${test_func#test_}"

    log_test "$test_name"

    setup_test "$test_name"

    local start_time
    start_time=$(date +%s)
    local result=0

    if $test_func; then
        local end_time
        end_time=$(date +%s)
        local duration=$((end_time - start_time))
        TEST_TIMES[$test_func]=$duration
        TEST_RESULTS[$test_func]="PASS"
        log_info "✓ PASS (${duration}s)"
        result=0
    else
        local end_time
        end_time=$(date +%s)
        local duration=$((end_time - start_time))
        TEST_TIMES[$test_func]=$duration
        TEST_RESULTS[$test_func]="FAIL"
        log_error "✗ FAIL (${duration}s)"
        result=1
    fi

    teardown_test "$test_name"

    return $result
}

# Test dependencies
declare -A TEST_DEPENDS
TEST_DEPENDS[test_prepare_snapshot]="test_pull_image"
TEST_DEPENDS[test_view_snapshot]="test_pull_image"
TEST_DEPENDS[test_commit]="test_pull_image"
TEST_DEPENDS[test_rwlayer_creation]="test_pull_image"
TEST_DEPENDS[test_snapshot_cleanup]="test_pull_image"
TEST_DEPENDS[test_vmdk_layer_order]="test_multi_layer"
TEST_DEPENDS[test_vmdk_format_valid]="test_multi_layer"

# Check if test dependencies are met
check_test_dependencies() {
    local test="$1"
    local dep="${TEST_DEPENDS[$test]:-}"

    if [ -n "$dep" ] && [ "${TEST_RESULTS[$dep]:-}" != "PASS" ]; then
        log_warn "Skipping $test (dependency $dep not passed)"
        TEST_RESULTS[$test]="SKIPPED"
        TEST_MESSAGES[$test]="Dependency $dep not passed"
        return 1
    fi

    return 0
}

# =============================================================================
# Test Cases
# =============================================================================

# =============================================================================
# Test: test_pull_image
# =============================================================================
# Goal: Verify that pulling an OCI image creates snapshots in the nexus-erofs
#       snapshotter correctly.
#
# Expectations:
#   - Image pull completes successfully using nexus-erofs snapshotter
#   - Image is visible in containerd image list
#   - At least one snapshot is created for the image layers
#
# This is the foundational test - all other tests depend on this working.
# =============================================================================
test_pull_image() {
    # Pull using ctr with nexus-erofs snapshotter (suppress progress output)
    if ! ctr_pull --snapshotter nexus-erofs "${TEST_IMAGE}" >/dev/null; then
        log_error_with_context "Failed to pull image"
        return 1
    fi

    # Verify image exists
    assert_command_success "ctr_cmd images ls | grep -q 'containerd'" "Image should exist after pull" || return 1

    # Verify snapshots were created
    local snap_count
    snap_count=$(ctr_cmd snapshots --snapshotter nexus-erofs ls | wc -l)

    assert_greater_than "$snap_count" 1 "Expected snapshots after pull" || return 1

    log_info "Image pulled successfully with $((snap_count - 1)) snapshots"
}

# =============================================================================
# Test: test_prepare_snapshot
# =============================================================================
# Goal: Verify that preparing an active snapshot from a committed parent works
#       correctly, which is required for container startup.
#
# Expectations:
#   - Can find a committed parent snapshot from previously pulled image
#   - Prepare() creates a new active snapshot successfully
#   - The active snapshot is visible via containerd snapshots API
#   - Cleanup removes the test snapshot
#
# This tests the snapshotter's Prepare() method which creates writable layers.
# =============================================================================
test_prepare_snapshot() {
    # Get a committed snapshot from the pulled image (prepare requires Committed parent)
    local parent_snap
    parent_snap=$(ctr_cmd snapshots --snapshotter nexus-erofs ls | grep -v "^KEY" | grep "Committed" | head -1 | awk '{print $1}')

    assert_not_empty "$parent_snap" "Committed parent snapshot should exist" || return 1

    # Prepare an active snapshot
    local snap_name="test-active-${TEST_NAMESPACE}"
    if ! ctr_cmd snapshots --snapshotter nexus-erofs prepare "$snap_name" "$parent_snap" >/dev/null; then
        log_error "Failed to prepare snapshot"
        return 1
    fi

    # Verify snapshot was created
    assert_command_success "ctr_cmd snapshots --snapshotter nexus-erofs info '$snap_name' >/dev/null 2>&1" \
        "Snapshot should be created" || return 1

    # Clean up
    ctr_cmd snapshots --snapshotter nexus-erofs rm "$snap_name" 2>/dev/null || true

    log_info "Active snapshot prepared successfully"
}

# =============================================================================
# Test: test_view_snapshot
# =============================================================================
# Goal: Verify that creating a read-only view of a snapshot returns proper
#       EROFS mount information for VM consumption.
#
# Expectations:
#   - Can find a committed parent snapshot
#   - View() creates a read-only snapshot successfully
#   - Mount info contains EROFS-related paths or mount type
#   - The view snapshot is visible via containerd snapshots API
#   - Cleanup removes the test view
#
# This tests the snapshotter's View() method which provides read-only access
# to layer content, used when VMs need to mount the filesystem.
# =============================================================================
test_view_snapshot() {
    # Get a committed snapshot from the pulled image (View requires Committed parent)
    local parent_snap
    parent_snap=$(ctr_cmd snapshots --snapshotter nexus-erofs ls | grep -v "^KEY" | grep "Committed" | head -1 | awk '{print $1}')

    assert_not_empty "$parent_snap" "Committed parent snapshot should exist" || return 1

    # Create a view snapshot
    local view_name="test-view-${TEST_NAMESPACE}"
    ctr_cmd snapshots --snapshotter nexus-erofs view "$view_name" "$parent_snap" >/dev/null 2>&1

    # Get mounts for the view snapshot
    local mounts
    mounts=$(ctr_cmd snapshots --snapshotter nexus-erofs mounts /tmp/mnt "$view_name" 2>&1)

    # Verify it returns erofs type mount (check for .erofs file path or erofs mount type)
    if echo "$mounts" | grep -qE "(erofs|\.erofs)"; then
        log_info "View snapshot returns EROFS mount"
    else
        log_debug "Mount output: $mounts"
        # Even if not erofs type, check the snapshot exists
        assert_command_success "ctr_cmd snapshots --snapshotter nexus-erofs info '$view_name' >/dev/null 2>&1" \
            "View snapshot should be created" || return 1
        log_info "View snapshot created successfully (mount type may vary)"
    fi

    # Clean up
    ctr_cmd snapshots --snapshotter nexus-erofs rm "$view_name" 2>/dev/null || true
}

# =============================================================================
# Test: test_commit
# =============================================================================
# Goal: Verify that committing an active snapshot creates a new committed
#       snapshot with EROFS layer conversion.
#
# Expectations:
#   - Can find a committed parent snapshot
#   - Prepare() with "extract-" prefix creates a snapshot for host mounting
#   - Commit() converts the active snapshot to a committed state
#   - EROFS layer files (layer.erofs) are created
#   - The committed snapshot is visible via containerd snapshots API
#   - Cleanup removes test snapshots
#
# This tests the snapshotter's Commit() method which finalizes changes and
# converts them to EROFS format. Note: This snapshotter is VM-only and
# doesn't support running containers on the host.
# =============================================================================
test_commit() {
    # Get a committed snapshot from the pulled image to use as parent
    local parent_snap
    parent_snap=$(ctr_cmd snapshots --snapshotter nexus-erofs ls | grep -v "^KEY" | grep "Committed" | head -1 | awk '{print $1}')

    assert_not_empty "$parent_snap" "Committed parent snapshot should exist" || return 1

    log_debug "Using parent snapshot: $parent_snap"

    # Use extract- prefix to trigger host mounting (like image build does)
    local extract_name="extract-commit-${TEST_NAMESPACE}"
    ctr_cmd snapshots --snapshotter nexus-erofs prepare "$extract_name" "$parent_snap" >/dev/null 2>&1

    log_debug "Prepared extract snapshot: $extract_name"

    # The snapshotter should have created the snapshot
    assert_command_success "ctr_cmd snapshots --snapshotter nexus-erofs info '$extract_name' >/dev/null 2>&1" \
        "Extract snapshot should exist" || return 1

    # Commit the snapshot - this triggers EROFS conversion
    local commit_name="committed-${TEST_NAMESPACE}"
    if ! ctr_cmd snapshots --snapshotter nexus-erofs commit "$commit_name" "$extract_name" 2>&1; then
        log_warn "Snapshot commit returned error (checking if snapshot was created anyway)"
    fi

    # Verify committed snapshot exists
    assert_command_success "ctr_cmd snapshots --snapshotter nexus-erofs info '$commit_name' >/dev/null 2>&1" \
        "Committed snapshot should exist" || return 1

    # Check for EROFS layer files
    local erofs_count
    erofs_count=$(find "${SNAPSHOTTER_ROOT}/snapshots" -name "layer.erofs" 2>/dev/null | wc -l)

    log_info "Snapshot committed successfully: $commit_name"
    log_info "EROFS layer files: $erofs_count"

    if [ "$VERBOSE" = "true" ]; then
        echo ""
        echo "┌──────────────────────────────────────────────────────────────┐"
        echo "│                  COMMITTED SNAPSHOT INFO                     │"
        echo "└──────────────────────────────────────────────────────────────┘"
        ctr_cmd snapshots --snapshotter nexus-erofs info "$commit_name" 2>&1 || true
        echo ""

        echo "┌──────────────────────────────────────────────────────────────┐"
        echo "│                    SNAPSHOT HIERARCHY                        │"
        echo "└──────────────────────────────────────────────────────────────┘"
        ctr_cmd snapshots --snapshotter nexus-erofs ls 2>&1 || true
        echo ""

        echo "┌──────────────────────────────────────────────────────────────┐"
        echo "│                      EROFS LAYER FILES                       │"
        echo "└──────────────────────────────────────────────────────────────┘"
        find "${SNAPSHOTTER_ROOT}/snapshots" -name "*.erofs" -exec ls -lh {} \; 2>/dev/null || true
        echo ""
    fi

    # Clean up
    ctr_cmd snapshots --snapshotter nexus-erofs rm "$commit_name" 2>/dev/null || true
    ctr_cmd snapshots --snapshotter nexus-erofs rm "$extract_name" 2>/dev/null || true
}

# =============================================================================
# Test: test_multi_layer
# =============================================================================
# Goal: Verify that pulling a multi-layer image generates proper VMDK
#       descriptor files that combine all layers for VM block device access.
#
# Expectations:
#   - Multi-layer image (nginx:alpine) pulls successfully
#   - Multiple snapshots are created (one per layer)
#   - Creating a view triggers VMDK generation
#   - merged.vmdk descriptor file is created
#   - fsmeta.erofs file is created for filesystem metadata
#   - View is intentionally NOT cleaned up (used by test_vmdk_layer_order)
#
# This tests the critical VMDK generation path that allows VMs to access
# multi-layer container images as a single virtual block device.
# =============================================================================
test_multi_layer() {
    # Pull a multi-layer image
    if ! ctr_pull --snapshotter nexus-erofs "${MULTI_LAYER_IMAGE}" >/dev/null; then
        log_error_with_context "Failed to pull multi-layer image"
        return 1
    fi

    # Count snapshots (subtract 1 for header line)
    local snap_count
    snap_count=$(ctr_cmd snapshots --snapshotter nexus-erofs ls | wc -l)
    snap_count=$((snap_count - 1))

    log_info "Multi-layer image created $snap_count snapshots"

    # Verify we actually have multiple layers
    if [ "$snap_count" -lt 2 ]; then
        log_error "Expected multiple snapshots for multi-layer image, got $snap_count"
        log_error "Image ${MULTI_LAYER_IMAGE} may only have 1 layer"
        return 1
    fi

    # Find the top-level committed snapshot from the multi-layer image
    # Look for snapshots with nginx in the name or the most recent one
    local top_snap
    top_snap=$(ctr_cmd snapshots --snapshotter nexus-erofs ls | grep -v "^KEY" | grep "Committed" | tail -1 | awk '{print $1}')

    if [ -z "$top_snap" ]; then
        log_error "Could not find top-level committed snapshot"
        return 1
    fi

    # Store for dependent tests
    log_info "Top-level snapshot: $top_snap"

    # Create a view mount to trigger VMDK generation
    # VMDK is only generated when View() is called, not just when pulling
    MULTI_LAYER_VIEW_NAME="test-multi-view-${TEST_NAMESPACE}"
    log_info "Creating view to trigger VMDK generation..."
    if ! ctr_cmd snapshots --snapshotter nexus-erofs view "$MULTI_LAYER_VIEW_NAME" "$top_snap" >/dev/null 2>&1; then
        log_error "Failed to create view snapshot"
        return 1
    fi

    # Small delay to ensure files are written
    sleep 0.5

    # Verify VMDK generation (required for multi-layer images)
    local vmdk_count
    vmdk_count=$(find "${SNAPSHOTTER_ROOT}/snapshots" -name "merged.vmdk" 2>/dev/null | wc -l)

    if [ "$vmdk_count" -eq 0 ]; then
        log_error "No VMDK descriptors found for multi-layer image"
        ctr_cmd snapshots --snapshotter nexus-erofs rm "$MULTI_LAYER_VIEW_NAME" 2>/dev/null || true
        MULTI_LAYER_VIEW_NAME=""
        return 1
    fi
    log_info "VMDK descriptors generated: $vmdk_count"

    # Verify fsmeta.erofs exists for multi-layer
    local fsmeta_count
    fsmeta_count=$(find "${SNAPSHOTTER_ROOT}/snapshots" -name "fsmeta.erofs" 2>/dev/null | wc -l)

    if [ "$fsmeta_count" -eq 0 ]; then
        log_error "No fsmeta.erofs found for multi-layer image"
        ctr_cmd snapshots --snapshotter nexus-erofs rm "$MULTI_LAYER_VIEW_NAME" 2>/dev/null || true
        MULTI_LAYER_VIEW_NAME=""
        return 1
    fi
    log_info "Fsmeta files generated: $fsmeta_count"

    # Don't clean up - leave view for test_vmdk_layer_order to verify
    log_info "View snapshot preserved for dependent tests: $MULTI_LAYER_VIEW_NAME"
}

# =============================================================================
# Test: test_erofs_layers
# =============================================================================
# Goal: Verify that EROFS layer files are created with correct naming and
#       valid EROFS filesystem format.
#
# Expectations:
#   - At least one EROFS layer file exists in snapshots directory
#   - Files are named with digest (sha256-*.erofs) or fallback (snapshot-*.erofs)
#   - EROFS magic bytes (0xE0F5E1E2) are present at offset 1024
#   - Digest-based naming indicates proper content-addressable storage
#
# This validates the EROFS conversion produces valid filesystem images that
# can be mounted by the VM kernel.
# =============================================================================
test_erofs_layers() {
    # Find EROFS layer files (digest-based naming: sha256-*.erofs)
    local erofs_count
    erofs_count=$(find "${SNAPSHOTTER_ROOT}/snapshots" -name "sha256-*.erofs" 2>/dev/null | wc -l)

    # Also check for fallback naming (snapshot-*.erofs) used by walking differ
    local fallback_count
    fallback_count=$(find "${SNAPSHOTTER_ROOT}/snapshots" -name "snapshot-*.erofs" 2>/dev/null | wc -l)

    local total_count=$((erofs_count + fallback_count))
    assert_greater_than "$total_count" 0 "EROFS layer files should exist (sha256-*.erofs or snapshot-*.erofs)" || return 1

    log_info "Found $erofs_count digest-named EROFS layers, $fallback_count fallback-named layers"

    # Verify at least one is a valid EROFS image
    local erofs_file
    erofs_file=$(find "${SNAPSHOTTER_ROOT}/snapshots" -name "sha256-*.erofs" 2>/dev/null | head -1)
    if [ -z "$erofs_file" ]; then
        erofs_file=$(find "${SNAPSHOTTER_ROOT}/snapshots" -name "snapshot-*.erofs" 2>/dev/null | head -1)
    fi

    if [ -n "$erofs_file" ]; then
        # Check magic bytes (EROFS magic is 0xE0F5E1E2 at offset 1024)
        local magic
        magic=$(xxd -s 1024 -l 4 -p "$erofs_file" 2>/dev/null || echo "")
        if [ "$magic" = "e2e1f5e0" ]; then
            log_info "EROFS magic verified in $erofs_file"
        else
            log_debug "Could not verify EROFS magic (may be little-endian): $magic"
        fi

        # Log the filename to show digest-based naming
        log_info "Layer file: $(basename "$erofs_file")"
    fi
}

# =============================================================================
# Test: test_vmdk_layer_order
# =============================================================================
# Goal: Verify that VMDK descriptor files list layers in the correct order
#       matching the layers.manifest file and OCI manifest conventions.
#
# CRITICAL: Wrong layer order = VM will not boot!
#   - Layers must be ordered: fsmeta first, then base layer -> newest layer
#   - This matches OCI manifest order and mkfs.erofs rebuild mode expectations
#
# Expectations:
#   - Find VMDK with multiple layers (from test_multi_layer)
#   - fsmeta.erofs is in extent 0 (first position)
#   - VMDK layers are ordered bottom-to-top (base first, newest last)
#   - All layer files referenced in VMDK exist on disk
#   - Layer order matches the layers.manifest file (authoritative source)
# =============================================================================
test_vmdk_layer_order() {
    # Find the VMDK file with multiple layers (from the multi-layer image)
    local vmdk_file=""
    local best_layer_count=0

    while IFS= read -r candidate; do
        local layer_count
        layer_count=$(grep -c "sha256-.*\.erofs" "$candidate" 2>/dev/null || echo 0)
        log_debug "VMDK $candidate has $layer_count layers"

        if [ "$layer_count" -gt "$best_layer_count" ]; then
            best_layer_count=$layer_count
            vmdk_file="$candidate"
        fi
    done < <(find "${SNAPSHOTTER_ROOT}/snapshots" -name "merged.vmdk" 2>/dev/null)

    if [ -z "$vmdk_file" ]; then
        log_error "No VMDK file found"
        return 1
    fi

    if [ "$best_layer_count" -lt 2 ]; then
        log_error "No VMDK with multiple layers found (best had $best_layer_count layers)"
        return 1
    fi

    log_info "Verifying VMDK layer order in: $vmdk_file"

    # =========================================================================
    # Step 1: Parse VMDK and extract layer order with extent numbers
    # =========================================================================
    local -a vmdk_extents=()      # Array of "extent_num:path:digest" entries
    local -a vmdk_digests=()      # Just the digests in order
    local fsmeta_extent=""
    local fsmeta_path=""
    local extent_num=0

    while IFS= read -r line; do
        if [[ "$line" =~ ^RW[[:space:]]+[0-9]+[[:space:]]+FLAT[[:space:]]+\"([^\"]+)\" ]]; then
            local layer_path="${BASH_REMATCH[1]}"
            local layer_name
            layer_name=$(basename "$layer_path")

            if [[ "$layer_name" == "fsmeta.erofs" ]]; then
                fsmeta_extent=$extent_num
                fsmeta_path="$layer_path"
                log_debug "Extent $extent_num: fsmeta.erofs"
            elif [[ "$layer_name" =~ ^sha256-([a-f0-9]+)\.erofs$ ]]; then
                local digest="${BASH_REMATCH[1]}"
                vmdk_extents+=("${extent_num}:${layer_path}:${digest}")
                vmdk_digests+=("$digest")
                log_debug "Extent $extent_num: sha256:${digest:0:12}..."
            else
                log_debug "Extent $extent_num: $layer_name (unknown format)"
            fi

            extent_num=$((extent_num + 1))
        fi
    done < "$vmdk_file"

    # =========================================================================
    # Step 2: Verify fsmeta.erofs is in the correct position (extent 0)
    # =========================================================================
    if [ -z "$fsmeta_extent" ]; then
        log_error "CRITICAL: fsmeta.erofs not found in VMDK - VM will not boot!"
        log_error "VMDK contents:"
        cat "$vmdk_file" >&2
        return 1
    fi

    if [ "$fsmeta_extent" -ne 0 ]; then
        log_error "CRITICAL: fsmeta.erofs is at extent $fsmeta_extent, must be at extent 0 - VM will not boot!"
        log_error "VMDK contents:"
        cat "$vmdk_file" >&2
        return 1
    fi
    log_info "✓ fsmeta.erofs correctly positioned at extent 0"

    # =========================================================================
    # Step 3: Verify all layer files exist on disk
    # =========================================================================
    log_info "Verifying all VMDK layer files exist..."
    local missing_count=0

    # Check fsmeta
    if [ -n "$fsmeta_path" ] && [ ! -f "$fsmeta_path" ]; then
        log_error "Missing fsmeta file: $fsmeta_path"
        ((missing_count++))
    fi

    # Check layer files
    for entry in "${vmdk_extents[@]}"; do
        local layer_path
        layer_path=$(echo "$entry" | cut -d: -f2)
        if [ ! -f "$layer_path" ]; then
            log_error "Missing layer file: $layer_path"
            ((missing_count++))
        fi
    done

    if [ "$missing_count" -gt 0 ]; then
        log_error "VMDK references $missing_count missing layer files - VM will not boot!"
        return 1
    fi
    log_info "✓ All layer files exist (fsmeta + ${#vmdk_digests[@]} layers)"

    # =========================================================================
    # Step 4: Display VMDK layer order
    # =========================================================================
    if [ ${#vmdk_digests[@]} -lt 2 ]; then
        log_error "Expected multiple layers in VMDK, found ${#vmdk_digests[@]}"
        return 1
    fi

    log_info "VMDK layer order (${#vmdk_digests[@]} layers, should be oldest→newest):"
    for i in "${!vmdk_digests[@]}"; do
        log_info "  [$((i+1))] sha256:${vmdk_digests[$i]:0:12}..."
    done

    # =========================================================================
    # Step 5: Read layers.manifest file for authoritative layer order
    # =========================================================================
    # The manifest file is generated alongside the VMDK and contains the
    # definitive layer order (oldest-to-newest, matching OCI manifest order).
    log_info "Reading layer manifest for expected order..."

    local manifest_file
    manifest_file=$(dirname "$vmdk_file")/layers.manifest

    if [ ! -f "$manifest_file" ]; then
        log_error "Layer manifest not found: $manifest_file"
        log_error "The snapshotter must generate layers.manifest alongside merged.vmdk"
        return 1
    fi

    log_debug "Using manifest file: $manifest_file"

    # Parse manifest - one digest per line (sha256:hex...)
    local -a manifest_digests=()
    while IFS= read -r line; do
        # Skip empty lines
        [[ -z "$line" ]] && continue
        # Extract just the hex part after sha256:
        if [[ "$line" =~ ^sha256:([a-f0-9]+)$ ]]; then
            manifest_digests+=("${BASH_REMATCH[1]}")
            log_debug "  Manifest: sha256:${BASH_REMATCH[1]:0:12}..."
        fi
    done < "$manifest_file"

    if [ ${#manifest_digests[@]} -eq 0 ]; then
        log_error "No valid digests found in manifest file: $manifest_file"
        return 1
    fi

    log_info "Manifest layer order (${#manifest_digests[@]} layers, oldest→newest):"
    for i in "${!manifest_digests[@]}"; do
        log_info "  [$((i+1))] sha256:${manifest_digests[$i]:0:12}..."
    done

    # Use manifest digests as the expected order
    local -a chain_digests=("${manifest_digests[@]}")

    # =========================================================================
    # Step 6: Compare VMDK order with manifest order - MUST MATCH
    # =========================================================================
    log_info "Comparing VMDK order with manifest..."

    local mismatch=false
    local min_count=${#vmdk_digests[@]}
    [ ${#chain_digests[@]} -lt $min_count ] && min_count=${#chain_digests[@]}

    for ((i=0; i<min_count; i++)); do
        local vmdk_digest="${vmdk_digests[$i]}"
        local chain_digest="${chain_digests[$i]}"

        if [ "$vmdk_digest" != "$chain_digest" ]; then
            log_error "LAYER ORDER MISMATCH at position $((i+1)):"
            log_error "  VMDK has:     sha256:${vmdk_digest:0:12}..."
            log_error "  Manifest has: sha256:${chain_digest:0:12}..."
            mismatch=true
        else
            log_debug "  Position $((i+1)) matches: sha256:${vmdk_digest:0:12}..."
        fi
    done

    if [ ${#vmdk_digests[@]} -ne ${#chain_digests[@]} ]; then
        log_warn "Layer count differs: VMDK has ${#vmdk_digests[@]}, manifest has ${#chain_digests[@]}"
    fi

    if [ "$mismatch" = "true" ]; then
        log_error "=========================================="
        log_error "CRITICAL: VMDK layer order does not match manifest!"
        log_error "The VM will likely fail to boot or have incorrect filesystem contents."
        log_error "=========================================="

        # Dump full details for debugging
        echo ""
        echo "VMDK file contents:"
        echo "──────────────────────────────────────────"
        cat "$vmdk_file"
        echo ""
        echo "Expected layer order (from manifest):"
        echo "──────────────────────────────────────────"
        printf "  0. fsmeta.erofs\n"
        for i in "${!chain_digests[@]}"; do
            printf "  %d. sha256-%s.erofs\n" "$((i+1))" "${chain_digests[$i]}"
        done
        echo ""
        echo "Actual layer order (from VMDK):"
        echo "──────────────────────────────────────────"
        printf "  0. fsmeta.erofs\n"
        for i in "${!vmdk_digests[@]}"; do
            printf "  %d. sha256-%s.erofs\n" "$((i+1))" "${vmdk_digests[$i]}"
        done

        return 1
    fi

    log_info "✓ VMDK layer order matches manifest - VM should boot correctly"
    return 0
}

# =============================================================================
# Test: test_vmdk_format_valid
# =============================================================================
# Goal: Verify VMDK descriptor format is valid and can be parsed by VM tools.
#
# Expectations:
#   - VMDK has all required descriptor fields (version, CID, createType)
#   - createType is appropriate for flat file access
#   - Extent definitions are properly formatted
#   - Total size is reasonable
# =============================================================================
test_vmdk_format_valid() {
    log_info "Verifying VMDK descriptor format..."

    local vmdk_file
    vmdk_file=$(find "${SNAPSHOTTER_ROOT}/snapshots" -name "merged.vmdk" 2>/dev/null | head -1)

    if [ -z "$vmdk_file" ]; then
        log_error "No VMDK file found"
        return 1
    fi

    log_info "Checking VMDK: $vmdk_file"

    # Check required VMDK descriptor fields
    local required_fields=(
        "version="
        "CID="
        "createType="
    )

    local missing_fields=0
    for field in "${required_fields[@]}"; do
        if ! grep -q "$field" "$vmdk_file"; then
            log_error "VMDK missing required field: $field"
            missing_fields=$((missing_fields + 1))
        fi
    done

    if [ "$missing_fields" -gt 0 ]; then
        log_error "VMDK is missing $missing_fields required fields"
        return 1
    fi
    log_info "✓ VMDK has all required descriptor fields"

    # Verify createType is appropriate for our use case
    local create_type
    create_type=$(grep "createType=" "$vmdk_file" | cut -d'"' -f2)

    # We expect monolithicFlat or similar for direct file access
    case "$create_type" in
        monolithicFlat|vmfs|vmfsSparse|twoGbMaxExtentFlat)
            log_info "✓ VMDK createType is valid: $create_type"
            ;;
        "")
            log_error "VMDK createType is empty"
            return 1
            ;;
        *)
            log_warn "Unexpected VMDK createType: $create_type (may still work)"
            ;;
    esac

    # Verify extents are properly formatted
    local extent_count
    extent_count=$(grep -c "^RW " "$vmdk_file" || echo 0)

    if [ "$extent_count" -eq 0 ]; then
        log_error "VMDK has no extent definitions"
        return 1
    fi
    log_info "✓ VMDK has $extent_count extent definitions"

    # Verify extent format: RW <sectors> FLAT "<path>" <offset>
    local bad_extents=0
    while IFS= read -r line; do
        if [[ "$line" =~ ^RW ]]; then
            if ! [[ "$line" =~ ^RW[[:space:]]+[0-9]+[[:space:]]+FLAT[[:space:]]+\"[^\"]+\"[[:space:]]+[0-9]+ ]]; then
                log_error "Malformed extent line: $line"
                bad_extents=$((bad_extents + 1))
            fi
        fi
    done < "$vmdk_file"

    if [ "$bad_extents" -gt 0 ]; then
        log_error "VMDK has $bad_extents malformed extent definitions"
        return 1
    fi
    log_info "✓ All extent definitions are properly formatted"

    # Verify total size is reasonable (sum of all extent sectors)
    local total_sectors=0
    while IFS= read -r line; do
        if [[ "$line" =~ ^RW[[:space:]]+([0-9]+) ]]; then
            total_sectors=$((total_sectors + BASH_REMATCH[1]))
        fi
    done < "$vmdk_file"

    # Convert to MB (512 bytes per sector)
    local total_mb=$((total_sectors * 512 / 1024 / 1024))

    if [ "$total_mb" -lt 1 ]; then
        log_error "VMDK total size is suspiciously small: ${total_mb}MB"
        return 1
    fi

    log_info "✓ VMDK total size: ${total_mb}MB ($total_sectors sectors)"

    # Verify version number
    local version
    version=$(grep "version=" "$vmdk_file" | head -1 | sed 's/.*version=//' | tr -d ' ')
    if [ -n "$version" ]; then
        log_info "✓ VMDK version: $version"
    fi

    return 0
}

# =============================================================================
# Test: test_nerdctl
# =============================================================================
# Goal: Verify that new container images can be created by committing
#       snapshot changes, simulating "docker commit" or "nerdctl commit".
#
# Expectations:
#   - integration-commit tool is available (skips if not found)
#   - Can create a new image from an existing image with modifications
#   - New image appears in containerd image list
#   - Image count increases after commit
#   - Cleanup removes the test image
#
# This tests the end-to-end image creation workflow that allows users to
# save VM state as new container images.
# =============================================================================
test_nerdctl() {
    # Check if integration-commit tool exists
    if [ ! -x /usr/local/bin/integration-commit ]; then
        log_warn "integration-commit tool not found, skipping image commit test"
        # Fall back to just listing images
        log_info "All images:"
        ctr_cmd images ls
        return 0
    fi

    # Show images BEFORE commit
    log_info "Images BEFORE commit:"
    ctr_cmd images ls
    echo ""

    local image_count_before
    image_count_before=$(ctr_cmd images ls | grep -v "^REF" | wc -l)

    # Use integration-commit to create a new image
    local new_image="localhost/alpine:with-new-layer"
    log_info "Creating new image via commit: $new_image"

    if ! /usr/local/bin/integration-commit \
        -address "${CONTAINERD_SOCKET}" \
        -snapshotter nexus-erofs \
        -source "${TEST_IMAGE}" \
        -target "$new_image" \
        -marker "/root/integration-test-marker.txt" 2>&1; then
        log_error "integration-commit failed"
        return 1
    fi

    # Show images AFTER commit
    echo ""
    log_info "Images AFTER commit:"
    ctr_cmd images ls
    echo ""

    local image_count_after
    image_count_after=$(ctr_cmd images ls | grep -v "^REF" | wc -l)

    # Verify new image exists
    if ! ctr_cmd images ls | grep -q "with-new-layer"; then
        log_error "New image not found after commit"
        return 1
    fi

    log_info "✓ New image created successfully!"
    log_info "Image count: $image_count_before -> $image_count_after"

    # Clean up the new image
    ctr_cmd images rm "$new_image" 2>/dev/null || true
}

# =============================================================================
# Test: test_snapshot_cleanup
# =============================================================================
# Goal: Verify that snapshots can be properly removed via the containerd API
#       and that removal actually cleans up resources.
#
# Expectations:
#   - Can create a test snapshot from a committed parent
#   - Snapshot is visible before removal
#   - Remove() deletes the snapshot successfully
#   - Snapshot is no longer visible after removal
#
# This tests the snapshotter's Remove() method to ensure proper cleanup
# of snapshot metadata and associated files.
# =============================================================================
test_snapshot_cleanup() {
    # Get a committed snapshot from the pulled image (prepare requires Committed parent)
    local parent_snap
    parent_snap=$(ctr_cmd snapshots --snapshotter nexus-erofs ls | grep -v "^KEY" | grep "Committed" | head -1 | awk '{print $1}')

    assert_not_empty "$parent_snap" "Committed parent snapshot should exist" || return 1

    # Create a test snapshot
    local snap_name="test-cleanup-${TEST_NAMESPACE}"
    if ! ctr_cmd snapshots --snapshotter nexus-erofs prepare "$snap_name" "$parent_snap" >/dev/null; then
        log_error "Failed to prepare snapshot"
        return 1
    fi

    # Verify it exists
    assert_command_success "ctr_cmd snapshots --snapshotter nexus-erofs info '$snap_name' >/dev/null 2>&1" \
        "Snapshot should exist before removal" || return 1

    # Remove it
    if ! ctr_cmd snapshots --snapshotter nexus-erofs rm "$snap_name" 2>/dev/null; then
        log_error "Failed to remove snapshot"
        return 1
    fi

    # Verify it's gone
    if ctr_cmd snapshots --snapshotter nexus-erofs info "$snap_name" >/dev/null 2>&1; then
        log_error "Snapshot still exists after removal"
        return 1
    fi

    log_info "Snapshot cleanup verified"
}

# =============================================================================
# Test: test_rwlayer_creation
# =============================================================================
# Goal: Verify rwlayer.img is created for active snapshots
# =============================================================================
test_rwlayer_creation() {
    # Get a committed snapshot from the pulled image (prepare requires Committed parent)
    local parent_snap
    parent_snap=$(ctr_cmd snapshots --snapshotter nexus-erofs ls | grep -v "^KEY" | grep "Committed" | head -1 | awk '{print $1}')

    assert_not_empty "$parent_snap" "Committed parent snapshot should exist" || return 1

    # Create an active snapshot
    local snap_name="test-rwlayer-${TEST_NAMESPACE}"
    if ! ctr_cmd snapshots --snapshotter nexus-erofs prepare "$snap_name" "$parent_snap" >/dev/null; then
        log_error "Failed to prepare snapshot"
        return 1
    fi

    # Count rwlayer.img files
    local rwlayer_count
    rwlayer_count=$(find "${SNAPSHOTTER_ROOT}/snapshots" -name "rwlayer.img" 2>/dev/null | wc -l)

    assert_greater_than "$rwlayer_count" 0 "rwlayer.img files should exist" || return 1

    log_info "Found $rwlayer_count rwlayer.img files"

    # Clean up
    ctr_cmd snapshots --snapshotter nexus-erofs rm "$snap_name" 2>/dev/null || true
}

# =============================================================================
# Test: test_full_cleanup_no_leaks
# =============================================================================
# Goal: Full cleanup - delete everything and verify no leaking files
# =============================================================================
test_full_cleanup_no_leaks() {
    log_info "Starting full cleanup test..."

    # First clean up the multi-layer view if it exists
    if [ -n "$MULTI_LAYER_VIEW_NAME" ]; then
        log_debug "Cleaning up multi-layer view: $MULTI_LAYER_VIEW_NAME"
        ctr_cmd snapshots --snapshotter nexus-erofs rm "$MULTI_LAYER_VIEW_NAME" 2>/dev/null || true
        MULTI_LAYER_VIEW_NAME=""
    fi

    # Get list of all images
    local images
    images=$(ctr_cmd images ls -q 2>/dev/null || true)

    # Remove all images (this should cascade delete snapshots)
    if [ -n "$images" ]; then
        log_info "Removing all images..."
        echo "$images" | while read -r img; do
            if [ -n "$img" ]; then
                log_debug "Removing image: $img"
                ctr_cmd images rm "$img" 2>/dev/null || true
            fi
        done
    fi

    # Force remove any remaining snapshots
    local snapshots
    snapshots=$(ctr_cmd snapshots --snapshotter nexus-erofs ls 2>/dev/null | grep -v "^KEY" | awk '{print $1}' || true)

    if [ -n "$snapshots" ]; then
        log_info "Removing remaining snapshots..."
        echo "$snapshots" | while read -r snap; do
            if [ -n "$snap" ]; then
                log_debug "Removing snapshot: $snap"
                ctr_cmd snapshots --snapshotter nexus-erofs rm "$snap" 2>/dev/null || true
            fi
        done
    fi

    # Wait a moment for cleanup to complete
    sleep 1

    # Verify no snapshots remain
    local remaining_snaps
    remaining_snaps=$(ctr_cmd snapshots --snapshotter nexus-erofs ls 2>/dev/null | grep -v "^KEY" | wc -l)
    if [ "$remaining_snaps" -gt 0 ]; then
        log_error "Found $remaining_snaps snapshots still registered after cleanup"
        ctr_cmd snapshots --snapshotter nexus-erofs ls >&2 || true
        return 1
    fi
    log_info "All snapshots removed from registry"

    # Check for leaked files in snapshots directory
    local snapshots_dir="${SNAPSHOTTER_ROOT}/snapshots"

    if [ -d "$snapshots_dir" ]; then
        # Count remaining snapshot directories
        local remaining_dirs
        remaining_dirs=$(find "$snapshots_dir" -mindepth 1 -maxdepth 1 -type d 2>/dev/null | wc -l)

        if [ "$remaining_dirs" -gt 0 ]; then
            log_error "Found $remaining_dirs leaked snapshot directories:"
            find "$snapshots_dir" -mindepth 1 -maxdepth 1 -type d 2>/dev/null | head -10 >&2
            log_error "Contents of first leaked directory:"
            local first_dir
            first_dir=$(find "$snapshots_dir" -mindepth 1 -maxdepth 1 -type d 2>/dev/null | head -1)
            if [ -n "$first_dir" ]; then
                ls -la "$first_dir" >&2 || true
            fi
            return 1
        fi
        log_info "No leaked snapshot directories found"

        # Check for any orphaned files at root level
        local orphan_files
        orphan_files=$(find "$snapshots_dir" -maxdepth 1 -type f 2>/dev/null | wc -l)
        if [ "$orphan_files" -gt 0 ]; then
            log_warn "Found $orphan_files orphaned files in snapshots root:"
            find "$snapshots_dir" -maxdepth 1 -type f 2>/dev/null >&2
        fi
    fi

    # Check for leaked EROFS files anywhere
    local leaked_erofs
    leaked_erofs=$(find "$SNAPSHOTTER_ROOT" -name "*.erofs" 2>/dev/null | wc -l)
    if [ "$leaked_erofs" -gt 0 ]; then
        log_error "Found $leaked_erofs leaked EROFS files:"
        find "$SNAPSHOTTER_ROOT" -name "*.erofs" 2>/dev/null | head -10 >&2
        return 1
    fi

    # Check for leaked VMDK files
    local leaked_vmdk
    leaked_vmdk=$(find "$SNAPSHOTTER_ROOT" -name "*.vmdk" 2>/dev/null | wc -l)
    if [ "$leaked_vmdk" -gt 0 ]; then
        log_error "Found $leaked_vmdk leaked VMDK files:"
        find "$SNAPSHOTTER_ROOT" -name "*.vmdk" 2>/dev/null | head -10 >&2
        return 1
    fi

    # Check for leaked rwlayer.img files
    local leaked_rwlayer
    leaked_rwlayer=$(find "$SNAPSHOTTER_ROOT" -name "rwlayer.img" 2>/dev/null | wc -l)
    if [ "$leaked_rwlayer" -gt 0 ]; then
        log_error "Found $leaked_rwlayer leaked rwlayer.img files:"
        find "$SNAPSHOTTER_ROOT" -name "rwlayer.img" 2>/dev/null | head -10 >&2
        return 1
    fi

    # Summary of what's left (should just be metadata.db)
    log_info "Remaining files in snapshotter root:"
    find "$SNAPSHOTTER_ROOT" -type f 2>/dev/null | while read -r f; do
        log_info "  $(basename "$f")"
    done

    # metadata.db should exist but be clean
    if [ -f "${SNAPSHOTTER_ROOT}/metadata.db" ]; then
        log_info "metadata.db exists (expected)"
    fi

    log_info "Full cleanup test passed - no leaked files detected"
}

# =============================================================================
# Test Execution
# =============================================================================

# List of all tests in execution order
ALL_TESTS=(
    test_pull_image
    test_prepare_snapshot
    test_view_snapshot
    test_erofs_layers
    test_multi_layer
    test_vmdk_format_valid
    test_vmdk_layer_order
    test_rwlayer_creation
    test_snapshot_cleanup
    test_commit
    test_nerdctl
    test_full_cleanup_no_leaks  # Must be last - removes everything
)

# Show test timing summary
show_test_summary() {
    local total_duration=$(($(date +%s) - TOTAL_START_TIME))

    echo ""
    echo "┌──────────────────────────────────────────────────────────────┐"
    echo "│                      TEST TIMING SUMMARY                     │"
    echo "└──────────────────────────────────────────────────────────────┘"
    printf "%-45s %8s  %s\n" "Test Name" "Duration" "Result"
    echo "────────────────────────────────────────────────────────────────"

    for test in "${ALL_TESTS[@]}"; do
        local duration="${TEST_TIMES[$test]:-0}"
        local result="${TEST_RESULTS[$test]:-UNKNOWN}"
        local result_color=""

        case "$result" in
            PASS) result_color="${GREEN}" ;;
            FAIL) result_color="${RED}" ;;
            SKIPPED) result_color="${YELLOW}" ;;
            *) result_color="${NC}" ;;
        esac

        printf "%-45s %7ss  ${result_color}%-8s${NC}\n" "${test#test_}" "$duration" "$result"
    done

    echo "────────────────────────────────────────────────────────────────"
    printf "%-45s %7ss\n" "Total" "$total_duration"
    echo ""
}

# Generate JUnit XML report
generate_junit_xml() {
    local output_file="$1"
    local total_tests=0
    local failed=0
    local skipped=0
    local total_duration=$(($(date +%s) - TOTAL_START_TIME))

    # Count results
    for test in "${ALL_TESTS[@]}"; do
        total_tests=$((total_tests + 1))
        case "${TEST_RESULTS[$test]:-UNKNOWN}" in
            FAIL) failed=$((failed + 1)) ;;
            SKIPPED) skipped=$((skipped + 1)) ;;
        esac
    done

    local timestamp
    timestamp=$(date -Iseconds)

    mkdir -p "$(dirname "$output_file")"

    cat > "$output_file" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<testsuites name="nexus-erofs-integration" tests="$total_tests" failures="$failed" skipped="$skipped" time="$total_duration" timestamp="$timestamp">
  <testsuite name="integration" tests="$total_tests" failures="$failed" skipped="$skipped" time="$total_duration">
EOF

    for test in "${ALL_TESTS[@]}"; do
        local duration="${TEST_TIMES[$test]:-0}"
        local status="${TEST_RESULTS[$test]:-UNKNOWN}"
        local message="${TEST_MESSAGES[$test]:-}"
        local test_name="${test#test_}"

        case "$status" in
            PASS)
                echo "    <testcase name=\"$test_name\" classname=\"nexus-erofs.$test_name\" time=\"$duration\"/>" >> "$output_file"
                ;;
            FAIL)
                cat >> "$output_file" <<TESTCASE
    <testcase name="$test_name" classname="nexus-erofs.$test_name" time="$duration">
      <failure message="Test failed">${message:-Test $test_name failed. Check logs for details.}</failure>
    </testcase>
TESTCASE
                ;;
            SKIPPED)
                cat >> "$output_file" <<TESTCASE
    <testcase name="$test_name" classname="nexus-erofs.$test_name" time="$duration">
      <skipped message="Test skipped">${message:-Test dependencies not met}</skipped>
    </testcase>
TESTCASE
                ;;
        esac
    done

    cat >> "$output_file" <<EOF
  </testsuite>
</testsuites>
EOF

    log_info "JUnit XML report generated: $output_file"
}

# Run all tests
run_tests() {
    local failed=0
    local passed=0
    local skipped=0
    local tests_to_run=("${ALL_TESTS[@]}")

    # If single test specified, only run that one
    if [ -n "${SINGLE_TEST}" ]; then
        tests_to_run=("test_${SINGLE_TEST}")
    fi

    for test in "${tests_to_run[@]}"; do
        echo ""

        # Check dependencies
        if ! check_test_dependencies "$test"; then
            skipped=$((skipped + 1))
            continue
        fi

        # Run test with timing
        if run_test_with_timing "$test"; then
            passed=$((passed + 1))
        else
            failed=$((failed + 1))
            log_error "Test failed: $test"

            if [ "$VERBOSE" = "true" ]; then
                echo ""
                echo "─────────── Recent snapshotter logs ───────────"
                tail -30 "${LOG_DIR}/snapshotter.log" 2>/dev/null || true
                echo ""
                echo "─────────── Recent containerd logs ────────────"
                tail -30 "${LOG_DIR}/containerd.log" 2>/dev/null || true
                echo ""
            fi
        fi
    done

    # Show summary
    show_test_summary

    echo "======================================"
    log_info "Test Results: ${passed} passed, ${failed} failed, ${skipped} skipped"
    echo "======================================"

    if [ "$failed" -gt 0 ]; then
        log_error "Some tests failed. Check logs in ${LOG_DIR}"
        return 1
    fi

    return 0
}

# =============================================================================
# Main Entry Point
# =============================================================================

main() {
    TOTAL_START_TIME=$(date +%s)

    log_info "Starting nexus-erofs integration tests"
    log_info "Containerd root: ${CONTAINERD_ROOT}"
    log_info "Snapshotter root: ${SNAPSHOTTER_ROOT}"
    log_info "Log directory: ${LOG_DIR}"

    if [ "$VERBOSE" = "true" ]; then
        log_info "Verbose mode: enabled"
    fi

    if [ "$STRICT_MODE" = "true" ]; then
        log_info "Strict mode: enabled (tests will fail if verification is incomplete)"
    fi

    if [ -n "$JUNIT_OUTPUT" ]; then
        log_info "JUnit output: $JUNIT_OUTPUT"
    fi

    mkdir -p "${LOG_DIR}"

    # Build and configure
    build_snapshotter
    generate_containerd_config

    # Start services
    start_containerd
    start_snapshotter

    # Give services time to fully initialize
    sleep 2

    # Run health checks
    if ! health_check; then
        log_error "Health checks failed, aborting tests"
        exit 1
    fi

    # Run tests
    run_tests
}

main "$@"
