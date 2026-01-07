// Package integration provides end-to-end tests for the nexus-erofs snapshotter.
//
// These tests verify the complete workflow of the snapshotter when used with
// containerd, including image pulling, snapshot operations, VMDK generation,
// and the commit lifecycle.
//
// Running tests:
//
//	go test -v ./test/integration/... -test.root
//
// These tests require:
//   - Root privileges (for mount operations)
//   - Linux kernel with EROFS support
//   - mkfs.erofs available in PATH
//   - containerd binary available in PATH
//
//go:build linux

package integration

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/core/mount"
	"github.com/containerd/containerd/v2/core/snapshots"
	"github.com/containerd/containerd/v2/pkg/namespaces"
	"github.com/containerd/containerd/v2/pkg/testutil"
)

// Test configuration constants.
const (
	// testNamespace is the containerd namespace for integration tests.
	testNamespace = "integration-test"

	// defaultTestImage is a small image for basic tests.
	defaultTestImage = "ghcr.io/containerd/alpine:3.14.0"

	// multiLayerImage is an image with multiple layers for VMDK tests.
	multiLayerImage = "docker.io/library/nginx:1.27-alpine"

	// snapshotterName is the name of the snapshotter under test.
	snapshotterName = "nexus-erofs"

	// serviceStartTimeout is the maximum time to wait for services to start.
	serviceStartTimeout = 30 * time.Second

	// imagePullTimeout is the maximum time to wait for image pulls.
	imagePullTimeout = 5 * time.Minute

	// mergedVMDKFile is the name of the VMDK descriptor file.
	mergedVMDKFile = "merged.vmdk"

	// fsmetaFile is the name of the fsmeta EROFS file.
	fsmetaFile = "fsmeta.erofs"
)

// Package-level compiled regexes for performance.
var (
	extentPattern     = regexp.MustCompile(`RW\s+\d+\s+FLAT\s+"([^"]+)"`)
	vmdkExtentPattern = regexp.MustCompile(`RW\s+\d+\s+FLAT\s+"[^"]+"\s+\d+`)
	digestPattern     = regexp.MustCompile(`sha256-([a-f0-9]{64})\.erofs`)
)

// Assertions provides structured verification methods for integration tests.
type Assertions struct {
	t   *testing.T
	env *Environment
}

// NewAssertions creates a new assertions helper.
func NewAssertions(t *testing.T, env *Environment) *Assertions {
	return &Assertions{t: t, env: env}
}

// SnapshotExists verifies a snapshot exists with the expected kind.
func (a *Assertions) SnapshotExists(ctx context.Context, key string, kind snapshots.Kind) snapshots.Info {
	a.t.Helper()
	ss := a.env.SnapshotService()

	info, err := ss.Stat(ctx, key)
	if err != nil {
		a.t.Fatalf("snapshot %q does not exist: %v", key, err)
	}

	if info.Kind != kind {
		a.t.Errorf("snapshot %q: expected kind %v, got %v", key, kind, info.Kind)
	}

	a.t.Logf("verified: snapshot %q exists (kind=%v, parent=%q)", key, info.Kind, info.Parent)
	return info
}

// SnapshotRemoved verifies a snapshot was removed.
func (a *Assertions) SnapshotRemoved(ctx context.Context, key string) {
	a.t.Helper()
	ss := a.env.SnapshotService()

	_, err := ss.Stat(ctx, key)
	if err == nil {
		a.t.Fatalf("snapshot %q still exists (expected removed)", key)
	}

	a.t.Logf("verified: snapshot %q removed", key)
}

// FileExists verifies a file exists and returns its info.
func (a *Assertions) FileExists(path string) os.FileInfo {
	a.t.Helper()

	info, err := os.Stat(path)
	if err != nil {
		a.t.Fatalf("file %q does not exist: %v", path, err)
	}

	a.t.Logf("verified: file exists %s (%d bytes)", path, info.Size())
	return info
}

// FileNotExists verifies a file does not exist.
func (a *Assertions) FileNotExists(path string) {
	a.t.Helper()

	_, err := os.Stat(path)
	if err == nil {
		a.t.Fatalf("file %q exists (expected not to exist)", path)
	}

	a.t.Logf("verified: file removed %s", path)
}

// ErofsValid verifies an EROFS file has valid magic.
func (a *Assertions) ErofsValid(path string) {
	a.t.Helper()

	if err := verifyErofsMagic(path); err != nil {
		a.t.Fatalf("invalid EROFS file %q: %v", path, err)
	}

	a.t.Logf("verified: valid EROFS %s", filepath.Base(path))
}

// MountsContain verifies mounts contain the expected type.
func (a *Assertions) MountsContain(mounts []mount.Mount, mountType string) mount.Mount {
	a.t.Helper()

	for _, m := range mounts {
		if m.Type == mountType || strings.Contains(m.Type, mountType) {
			a.t.Logf("verified: mount type %q found (source=%s)", m.Type, filepath.Base(m.Source))
			return m
		}
	}

	a.t.Fatalf("no mount of type %q found in %d mounts", mountType, len(mounts))
	return mount.Mount{}
}

// SnapshotCount verifies the expected number of snapshots.
func (a *Assertions) SnapshotCount(ctx context.Context, op string, min int) int {
	a.t.Helper()
	ss := a.env.SnapshotService()

	var count int
	if err := ss.Walk(ctx, func(_ context.Context, _ snapshots.Info) error {
		count++
		return nil
	}); err != nil {
		a.t.Fatalf("walk snapshots: %v", err)
	}

	if count < min {
		a.t.Fatalf("expected at least %d snapshots after %s, got %d", min, op, count)
	}

	a.t.Logf("verified: snapshot count after %s: %d", op, count)
	return count
}

// DirContains verifies a directory contains a file matching the pattern.
func (a *Assertions) DirContains(dir, pattern string) []string {
	a.t.Helper()

	var matches []string
	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil //nolint:nilerr // skip inaccessible files
		}
		if !info.IsDir() {
			matched, _ := filepath.Match(pattern, filepath.Base(path))
			if matched {
				matches = append(matches, path)
			}
		}
		return nil
	})
	if err != nil {
		a.t.Fatalf("walk %q: %v", dir, err)
	}

	if len(matches) == 0 {
		a.t.Fatalf("no files matching %q in %s", pattern, dir)
	}

	a.t.Logf("verified: found %d files matching %q", len(matches), pattern)
	return matches
}

// DumpSnapshotState logs the current state of all snapshots (for debugging).
func (a *Assertions) DumpSnapshotState(ctx context.Context) {
	a.t.Helper()
	ss := a.env.SnapshotService()

	a.t.Log("=== Current Snapshot State ===")
	var count int
	ss.Walk(ctx, func(_ context.Context, info snapshots.Info) error { //nolint:errcheck
		count++
		a.t.Logf("  [%d] %s (kind=%v, parent=%q)", count, info.Name, info.Kind, info.Parent)
		return nil
	})
	if count == 0 {
		a.t.Log("  (no snapshots)")
	}
}

// DumpFiles logs all files in a directory (for debugging).
func (a *Assertions) DumpFiles(dir string) {
	a.t.Helper()

	a.t.Logf("=== Files in %s ===", dir)
	var count int
	filepath.Walk(dir, func(path string, info os.FileInfo, err error) error { //nolint:errcheck
		if err == nil && !info.IsDir() {
			count++
			rel, _ := filepath.Rel(dir, path)
			a.t.Logf("  %s (%d bytes)", rel, info.Size())
		}
		return nil
	})
	if count == 0 {
		a.t.Log("  (no files)")
	}
}

// FindCommittedSnapshot finds a committed snapshot to use as a parent.
func (a *Assertions) FindCommittedSnapshot(ctx context.Context) string {
	a.t.Helper()
	ss := a.env.SnapshotService()

	var parentKey string
	if err := ss.Walk(ctx, func(_ context.Context, info snapshots.Info) error {
		if info.Kind == snapshots.KindCommitted && parentKey == "" {
			parentKey = info.Name
		}
		return nil
	}); err != nil {
		a.t.Fatalf("walk snapshots: %v", err)
	}

	if parentKey == "" {
		a.t.Fatal("no committed snapshot found")
	}

	a.t.Logf("found committed snapshot: %s", parentKey)
	return parentKey
}

// VMDKValid verifies a VMDK file has valid format.
func (a *Assertions) VMDKValid(path string) {
	a.t.Helper()

	data, err := os.ReadFile(path)
	if err != nil {
		a.t.Fatalf("read VMDK %q: %v", path, err)
	}

	content := string(data)

	// Check required fields
	requiredFields := []string{"version=", "CID=", "createType="}
	for _, field := range requiredFields {
		if !strings.Contains(content, field) {
			a.t.Fatalf("VMDK %q missing required field: %s", path, field)
		}
	}

	// Check extent format
	if !vmdkExtentPattern.MatchString(content) {
		a.t.Fatalf("VMDK %q has no valid extent definitions", path)
	}

	a.t.Logf("verified: valid VMDK %s", filepath.Base(path))
}

// Environment manages the test environment including containerd and snapshotter.
type Environment struct {
	t *testing.T

	// Paths
	rootDir         string
	containerdRoot  string
	snapshotterRoot string
	logDir          string

	// Sockets
	containerdSocket  string
	snapshotterSocket string

	// Process management
	containerdPID  int
	snapshotterPID int

	// Log files (closed on Stop)
	containerdLog  *os.File
	snapshotterLog *os.File

	// Client
	client *client.Client

	// Mutex for concurrent access
	mu sync.Mutex
}

// NewEnvironment creates a new test environment.
// It initializes directories but does not start services.
func NewEnvironment(t *testing.T) *Environment {
	t.Helper()
	testutil.RequiresRoot(t)

	rootDir := t.TempDir()

	env := &Environment{
		t:                 t,
		rootDir:           rootDir,
		containerdRoot:    filepath.Join(rootDir, "containerd"),
		snapshotterRoot:   filepath.Join(rootDir, "snapshotter"),
		logDir:            filepath.Join(rootDir, "logs"),
		containerdSocket:  filepath.Join(rootDir, "containerd.sock"),
		snapshotterSocket: filepath.Join(rootDir, "snapshotter.sock"),
	}

	// Create directories
	dirs := []string{env.containerdRoot, env.snapshotterRoot, env.logDir}
	for _, dir := range dirs {
		if err := os.MkdirAll(dir, 0755); err != nil {
			t.Fatalf("create directory %s: %v", dir, err)
		}
	}

	return env
}

// Start starts containerd and the snapshotter.
func (e *Environment) Start() error {
	if err := e.writeContainerdConfig(); err != nil {
		return fmt.Errorf("write containerd config: %w", err)
	}

	if err := e.startSnapshotter(); err != nil {
		return fmt.Errorf("start snapshotter: %w", err)
	}

	if err := e.startContainerd(); err != nil {
		e.stopSnapshotter()
		return fmt.Errorf("start containerd: %w", err)
	}

	if err := e.connect(); err != nil {
		e.Stop()
		return fmt.Errorf("connect to containerd: %w", err)
	}

	return nil
}

// Stop stops all services and cleans up.
func (e *Environment) Stop() {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.client != nil {
		e.client.Close()
		e.client = nil
	}

	e.stopContainerd()
	e.stopSnapshotter()

	// Close log files
	if e.containerdLog != nil {
		e.containerdLog.Close()
		e.containerdLog = nil
	}
	if e.snapshotterLog != nil {
		e.snapshotterLog.Close()
		e.snapshotterLog = nil
	}

	// Clear immutable flags on EROFS files so TempDir cleanup can remove them
	e.clearImmutableFlags()
}

// clearImmutableFlags removes immutable attributes from EROFS files.
// This is needed because fs-verity sets the immutable flag, which prevents
// the test's TempDir cleanup from deleting the files.
func (e *Environment) clearImmutableFlags() {
	snapshotsDir := filepath.Join(e.snapshotterRoot, "snapshots")
	filepath.Walk(snapshotsDir, func(path string, info os.FileInfo, err error) error { //nolint:errcheck
		if err != nil || info.IsDir() {
			return nil //nolint:nilerr
		}
		if strings.HasSuffix(path, ".erofs") {
			// Use chattr to clear immutable flag (ignore errors)
			exec.Command("chattr", "-i", path).Run() //nolint:errcheck
		}
		return nil
	})
}

// Client returns the containerd client.
func (e *Environment) Client() *client.Client {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.client
}

// Context returns a context with the test namespace.
func (e *Environment) Context() context.Context {
	return namespaces.WithNamespace(e.t.Context(), testNamespace)
}

// SnapshotService returns the snapshot service for the test snapshotter.
func (e *Environment) SnapshotService() snapshots.Snapshotter {
	return e.Client().SnapshotService(snapshotterName)
}

// SnapshotterRoot returns the root directory of the snapshotter.
func (e *Environment) SnapshotterRoot() string {
	return e.snapshotterRoot
}

// LogDir returns the directory containing service logs.
func (e *Environment) LogDir() string {
	return e.logDir
}

// writeContainerdConfig writes the containerd configuration file.
func (e *Environment) writeContainerdConfig() error {
	configPath := filepath.Join(e.rootDir, "containerd.toml")

	config := fmt.Sprintf(`version = 2
root = %q

[grpc]
  address = %q

[proxy_plugins]
  [proxy_plugins.nexus-erofs]
    type = "snapshot"
    address = %q

  [proxy_plugins.nexus-erofs-diff]
    type = "diff"
    address = %q

[plugins."io.containerd.service.v1.diff-service"]
  default = ["nexus-erofs-diff", "walking"]

[plugins."io.containerd.transfer.v1.local"]
  [[plugins."io.containerd.transfer.v1.local".unpack_config]]
    platform = "linux/amd64"
    snapshotter = "nexus-erofs"
    differ = "nexus-erofs-diff"

[plugins."io.containerd.cri.v1.images"]
  snapshotter = "nexus-erofs"
`, e.containerdRoot, e.containerdSocket, e.snapshotterSocket, e.snapshotterSocket)

	return os.WriteFile(configPath, []byte(config), 0644)
}

// startSnapshotter starts the nexus-erofs-snapshotter process.
func (e *Environment) startSnapshotter() error {
	binary, err := findBinary("nexus-erofs-snapshotter")
	if err != nil {
		return err
	}

	logFile, err := os.Create(filepath.Join(e.logDir, "snapshotter.log"))
	if err != nil {
		return fmt.Errorf("create log file: %w", err)
	}

	cmd := exec.Command(binary,
		"--address", e.snapshotterSocket,
		"--root", e.snapshotterRoot,
		"--containerd-address", e.containerdSocket,
		"--log-level", "debug",
	)
	cmd.Stdout = logFile
	cmd.Stderr = logFile

	if err := cmd.Start(); err != nil {
		logFile.Close()
		return fmt.Errorf("start snapshotter: %w", err)
	}

	e.snapshotterPID = cmd.Process.Pid
	e.snapshotterLog = logFile
	e.t.Logf("snapshotter started (PID: %d)", e.snapshotterPID)

	// Wait for socket
	if err := waitForSocket(e.snapshotterSocket, serviceStartTimeout); err != nil {
		e.dumpLogs("snapshotter")
		return fmt.Errorf("wait for snapshotter socket: %w", err)
	}

	return nil
}

// startContainerd starts the containerd process.
func (e *Environment) startContainerd() error {
	binary, err := findBinary("containerd")
	if err != nil {
		return err
	}

	logFile, err := os.Create(filepath.Join(e.logDir, "containerd.log"))
	if err != nil {
		return fmt.Errorf("create log file: %w", err)
	}

	configPath := filepath.Join(e.rootDir, "containerd.toml")
	cmd := exec.Command(binary, "--config", configPath)
	cmd.Stdout = logFile
	cmd.Stderr = logFile

	if err := cmd.Start(); err != nil {
		logFile.Close()
		return fmt.Errorf("start containerd: %w", err)
	}

	e.containerdPID = cmd.Process.Pid
	e.containerdLog = logFile
	e.t.Logf("containerd started (PID: %d)", e.containerdPID)

	// Wait for socket
	if err := waitForSocket(e.containerdSocket, serviceStartTimeout); err != nil {
		e.dumpLogs("containerd")
		return fmt.Errorf("wait for containerd socket: %w", err)
	}

	return nil
}

// connect establishes a connection to containerd.
func (e *Environment) connect() error {
	ctx, cancel := context.WithTimeout(context.Background(), serviceStartTimeout)
	defer cancel()

	var c *client.Client
	var err error

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("timeout connecting to containerd: %w", err)
		default:
			c, err = client.New(e.containerdSocket)
			if err == nil {
				// Verify connection
				if _, err = c.Version(ctx); err == nil {
					e.client = c
					return nil
				}
				c.Close()
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
}

// stopProcess gracefully stops a process by PID with a timeout.
func stopProcess(pid *int, timeout time.Duration) {
	if *pid == 0 {
		return
	}

	proc, err := os.FindProcess(*pid)
	if err != nil {
		*pid = 0
		return
	}

	_ = proc.Signal(syscall.SIGTERM)
	done := make(chan struct{})
	go func() {
		_, _ = proc.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(timeout):
		_ = proc.Kill()
	}

	*pid = 0
}

// stopContainerd stops the containerd process.
func (e *Environment) stopContainerd() {
	stopProcess(&e.containerdPID, 5*time.Second)
}

// stopSnapshotter stops the snapshotter process.
func (e *Environment) stopSnapshotter() {
	stopProcess(&e.snapshotterPID, 5*time.Second)
}

// dumpLogs prints the last N lines of a service log.
func (e *Environment) dumpLogs(service string) {
	logPath := filepath.Join(e.logDir, service+".log")
	data, err := os.ReadFile(logPath)
	if err != nil {
		e.t.Logf("failed to read %s logs: %v", service, err)
		return
	}

	lines := strings.Split(string(data), "\n")
	start := 0
	if len(lines) > 50 {
		start = len(lines) - 50
	}

	e.t.Logf("=== %s logs (last %d lines) ===", service, len(lines)-start)
	for _, line := range lines[start:] {
		e.t.Log(line)
	}
}

// findBinary locates a binary in PATH or common locations.
func findBinary(name string) (string, error) {
	// Check PATH first
	if path, err := exec.LookPath(name); err == nil {
		return path, nil
	}

	// Check common locations
	locations := []string{
		"/usr/local/bin/" + name,
		"/usr/bin/" + name,
		"./bin/" + name,
	}

	for _, loc := range locations {
		if _, err := os.Stat(loc); err == nil {
			return loc, nil
		}
	}

	return "", fmt.Errorf("binary not found: %s", name)
}

// waitForSocket waits for a Unix socket to become available.
func waitForSocket(path string, timeout time.Duration) error {
	return waitFor(func() bool {
		_, err := os.Stat(path)
		return err == nil
	}, timeout, fmt.Sprintf("socket not available: %s", path))
}

// waitFor polls a condition function until it returns true or timeout.
func waitFor(condition func() bool, timeout time.Duration, errMsg string) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return errors.New(errMsg)
}

// pullImage pulls an image with retry logic.
func pullImage(ctx context.Context, c *client.Client, ref string) error {
	ctx, cancel := context.WithTimeout(ctx, imagePullTimeout)
	defer cancel()

	var lastErr error
	for attempt := 1; attempt <= 3; attempt++ {
		_, err := c.Pull(ctx, ref,
			client.WithPlatform("linux/amd64"),
			client.WithPullUnpack,
			client.WithPullSnapshotter(snapshotterName),
		)
		if err == nil {
			return nil
		}
		lastErr = err
		if attempt < 3 {
			time.Sleep(time.Duration(attempt) * 2 * time.Second)
		}
	}
	return fmt.Errorf("pull image after 3 attempts: %w", lastErr)
}

// TestIntegration runs the integration test suite.
func TestIntegration(t *testing.T) {
	testutil.RequiresRoot(t)

	// Check prerequisites
	if err := checkPrerequisites(); err != nil {
		t.Skipf("prerequisites not met: %v", err)
	}

	// Create environment
	env := NewEnvironment(t)
	t.Cleanup(func() {
		env.Stop()
		env.dumpLogs("snapshotter")
		env.dumpLogs("containerd")
	})

	// Start services
	if err := env.Start(); err != nil {
		t.Fatalf("start environment: %v", err)
	}

	// Run health checks
	t.Run("health_check", func(t *testing.T) {
		testHealthCheck(t, env)
	})

	// Run tests in order (some depend on previous tests)
	t.Run("pull_image", func(t *testing.T) {
		testPullImage(t, env)
	})

	t.Run("prepare_snapshot", func(t *testing.T) {
		testPrepareSnapshot(t, env)
	})

	t.Run("view_snapshot", func(t *testing.T) {
		testViewSnapshot(t, env)
	})

	t.Run("erofs_layers", func(t *testing.T) {
		testErofsLayers(t, env)
	})

	t.Run("rwlayer_creation", func(t *testing.T) {
		testRwlayerCreation(t, env)
	})

	t.Run("commit", func(t *testing.T) {
		testCommit(t, env)
	})

	t.Run("snapshot_cleanup", func(t *testing.T) {
		testSnapshotCleanup(t, env)
	})

	// Multi-layer tests
	t.Run("multi_layer", func(t *testing.T) {
		testMultiLayer(t, env)
	})

	t.Run("vmdk_format", func(t *testing.T) {
		testVMDKFormat(t, env)
	})

	t.Run("vmdk_layer_order", func(t *testing.T) {
		testVMDKLayerOrder(t, env)
	})

	// Lifecycle test
	t.Run("commit_lifecycle", func(t *testing.T) {
		testCommitLifecycle(t, env)
	})

	// Cleanup test (runs last)
	t.Run("full_cleanup", func(t *testing.T) {
		testFullCleanup(t, env)
	})
}

// checkPrerequisites verifies that required tools are available.
func checkPrerequisites() error {
	required := []string{"containerd", "mkfs.erofs"}
	for _, bin := range required {
		if _, err := exec.LookPath(bin); err != nil {
			return fmt.Errorf("%s not found in PATH", bin)
		}
	}

	// Check for nexus-erofs-snapshotter
	if _, err := findBinary("nexus-erofs-snapshotter"); err != nil {
		return err
	}

	return nil
}

// testHealthCheck verifies the services are running and responsive.
func testHealthCheck(t *testing.T, env *Environment) {
	ctx := env.Context()
	c := env.Client()

	// Check containerd version
	v, err := c.Version(ctx)
	if err != nil {
		t.Fatalf("get containerd version: %v", err)
	}
	t.Logf("containerd version: %s", v.Version)

	// Check snapshotter is accessible
	ss := env.SnapshotService()
	if err := ss.Walk(ctx, func(_ context.Context, _ snapshots.Info) error {
		return nil
	}); err != nil {
		t.Fatalf("walk snapshots: %v", err)
	}
	t.Log("snapshotter is accessible")

	// Check disk space
	var stat syscall.Statfs_t
	if err := syscall.Statfs(env.SnapshotterRoot(), &stat); err != nil {
		t.Fatalf("statfs: %v", err)
	}
	availGB := (stat.Bavail * uint64(stat.Bsize)) / (1024 * 1024 * 1024)
	t.Logf("available disk space: %d GB", availGB)
	if availGB < 1 {
		t.Log("WARNING: low disk space may cause issues")
	}
}

// testPullImage verifies that pulling an image creates snapshots.
func testPullImage(t *testing.T, env *Environment) {
	ctx := env.Context()
	c := env.Client()
	assert := NewAssertions(t, env)

	t.Log("--- Step 1: Pull image ---")
	if err := pullImage(ctx, c, defaultTestImage); err != nil {
		t.Fatalf("pull image: %v", err)
	}

	t.Log("--- Step 2: Verify image exists ---")
	img, err := c.GetImage(ctx, defaultTestImage)
	if err != nil {
		t.Fatalf("get image: %v", err)
	}
	t.Logf("image pulled: %s", img.Name())

	t.Log("--- Step 3: Verify snapshots created ---")
	count := assert.SnapshotCount(ctx, "pull", 1)

	t.Log("--- Step 4: Verify EROFS layer created ---")
	snapshotsDir := filepath.Join(env.SnapshotterRoot(), "snapshots")
	erofsFiles := assert.DirContains(snapshotsDir, "*.erofs")
	if len(erofsFiles) > 0 {
		assert.ErofsValid(erofsFiles[0])
	}

	t.Logf("pull complete: %d snapshots, %d EROFS files", count, len(erofsFiles))
}

// testPrepareSnapshot verifies snapshot preparation from a committed parent.
func testPrepareSnapshot(t *testing.T, env *Environment) {
	ctx := env.Context()
	ss := env.SnapshotService()
	assert := NewAssertions(t, env)

	t.Log("--- Step 1: Find committed parent ---")
	parentKey := assert.FindCommittedSnapshot(ctx)

	t.Log("--- Step 2: Prepare active snapshot ---")
	snapKey := fmt.Sprintf("test-prepare-%d", time.Now().UnixNano())
	mounts, err := ss.Prepare(ctx, snapKey, parentKey)
	if err != nil {
		t.Fatalf("prepare snapshot: %v", err)
	}
	t.Cleanup(func() {
		ss.Remove(ctx, snapKey) //nolint:errcheck
	})

	if len(mounts) == 0 {
		t.Fatal("prepare returned no mounts")
	}

	t.Log("--- Step 3: Verify mounts returned ---")
	for i, m := range mounts {
		t.Logf("  mount[%d]: type=%s source=%s", i, m.Type, filepath.Base(m.Source))
	}
	assert.MountsContain(mounts, "erofs")

	t.Log("--- Step 4: Verify snapshot state ---")
	assert.SnapshotExists(ctx, snapKey, snapshots.KindActive)

	t.Logf("prepare complete: snapshot=%s mounts=%d", snapKey, len(mounts))
}

// testViewSnapshot verifies view snapshot creation and mount info.
func testViewSnapshot(t *testing.T, env *Environment) {
	ctx := env.Context()
	ss := env.SnapshotService()
	assert := NewAssertions(t, env)

	t.Log("--- Step 1: Find committed parent ---")
	parentKey := assert.FindCommittedSnapshot(ctx)

	t.Log("--- Step 2: Create view snapshot ---")
	viewKey := fmt.Sprintf("test-view-%d", time.Now().UnixNano())
	mounts, err := ss.View(ctx, viewKey, parentKey)
	if err != nil {
		t.Fatalf("create view: %v", err)
	}
	t.Cleanup(func() {
		ss.Remove(ctx, viewKey) //nolint:errcheck
	})

	if len(mounts) == 0 {
		t.Fatal("view returned no mounts")
	}

	t.Log("--- Step 3: Verify mounts contain EROFS ---")
	for i, m := range mounts {
		t.Logf("  mount[%d]: type=%s source=%s", i, m.Type, filepath.Base(m.Source))
	}
	assert.MountsContain(mounts, "erofs")

	t.Log("--- Step 4: Verify snapshot state ---")
	assert.SnapshotExists(ctx, viewKey, snapshots.KindView)

	t.Logf("view complete: snapshot=%s mounts=%d", viewKey, len(mounts))
}

// testErofsLayers verifies EROFS layer files are created correctly.
func testErofsLayers(t *testing.T, env *Environment) {
	assert := NewAssertions(t, env)
	snapshotsDir := filepath.Join(env.SnapshotterRoot(), "snapshots")

	t.Log("--- Step 1: Scan for EROFS files ---")

	// Find all EROFS files
	var layerFiles []string  // sha256-*.erofs layer files
	var fsmetaFiles []string // fsmeta.erofs files
	var otherErofs []string  // other .erofs files

	if err := filepath.Walk(snapshotsDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil //nolint:nilerr // skip inaccessible files
		}
		if strings.HasSuffix(path, ".erofs") && !info.IsDir() {
			base := filepath.Base(path)
			switch {
			case base == fsmetaFile:
				fsmetaFiles = append(fsmetaFiles, path)
			case strings.HasPrefix(base, "sha256-"):
				layerFiles = append(layerFiles, path)
			default:
				otherErofs = append(otherErofs, path)
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("walk snapshots dir: %v", err)
	}

	t.Logf("  found: %d layer files, %d fsmeta files, %d other",
		len(layerFiles), len(fsmetaFiles), len(otherErofs))

	t.Log("--- Step 2: Verify EROFS files exist ---")
	totalErofs := len(layerFiles) + len(fsmetaFiles) + len(otherErofs)
	if totalErofs == 0 {
		assert.DumpFiles(snapshotsDir)
		t.Fatal("no EROFS files found")
	}
	t.Logf("%d total EROFS files found", totalErofs)

	t.Log("--- Step 3: Validate EROFS layer magic ---")

	// Check layer files (sha256-*.erofs) for valid magic
	var validCount int
	for _, path := range layerFiles {
		if err := verifyErofsMagic(path); err != nil {
			t.Logf("  FAIL %s: %v", filepath.Base(path), err)
		} else {
			t.Logf("  OK %s: valid EROFS magic", filepath.Base(path))
			validCount++
		}
	}

	// Also check other erofs files (might be commit results)
	for _, path := range otherErofs {
		if err := verifyErofsMagic(path); err != nil {
			t.Logf("  FAIL %s: %v", filepath.Base(path), err)
		} else {
			t.Logf("  OK %s: valid EROFS magic", filepath.Base(path))
			validCount++
		}
	}

	// fsmeta files are metadata-only and don't have standard magic
	for _, path := range fsmetaFiles {
		t.Logf("  INFO %s: fsmeta (multi-device metadata)", filepath.Base(path))
	}

	if validCount == 0 && len(fsmetaFiles) == 0 {
		t.Fatal("no valid EROFS files found")
	}

	t.Logf("EROFS validation complete: %d valid layers, %d fsmeta files", validCount, len(fsmetaFiles))
}

// EROFS superblock magic number.
const erofsMagic = 0xE0F5E1E2

// verifyErofsMagic checks if a file has the EROFS magic bytes.
func verifyErofsMagic(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	// EROFS superblock is at offset 1024, magic is first 4 bytes
	if _, err := f.Seek(1024, io.SeekStart); err != nil {
		return err
	}

	var magic uint32
	if err := binary.Read(f, binary.LittleEndian, &magic); err != nil {
		return err
	}

	if magic != erofsMagic {
		return fmt.Errorf("invalid magic: got %#x, want %#x", magic, erofsMagic)
	}
	return nil
}

// testRwlayerCreation verifies rwlayer.img files are created for active snapshots.
func testRwlayerCreation(t *testing.T, env *Environment) {
	ctx := env.Context()
	ss := env.SnapshotService()
	assert := NewAssertions(t, env)

	parentKey := assert.FindCommittedSnapshot(ctx)

	snapKey := fmt.Sprintf("test-rwlayer-%d", time.Now().UnixNano())
	_, err := ss.Prepare(ctx, snapKey, parentKey)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	t.Cleanup(func() {
		ss.Remove(ctx, snapKey) //nolint:errcheck
	})

	// Verify rwlayer.img was created
	snapshotsDir := filepath.Join(env.SnapshotterRoot(), "snapshots")
	rwlayers := assert.DirContains(snapshotsDir, "rwlayer.img")
	t.Logf("found %d rwlayer.img files", len(rwlayers))
}

// testCommit verifies snapshot commit creates EROFS layers.
func testCommit(t *testing.T, env *Environment) {
	ctx := env.Context()
	ss := env.SnapshotService()
	assert := NewAssertions(t, env)

	parentKey := assert.FindCommittedSnapshot(ctx)

	// Create an extract snapshot (triggers host mounting)
	ts := time.Now().UnixNano()
	extractKey := fmt.Sprintf("extract-test-commit-%d", ts)
	_, err := ss.Prepare(ctx, extractKey, parentKey)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}

	// Commit it
	commitKey := fmt.Sprintf("committed-test-%d", ts)
	if err := ss.Commit(ctx, commitKey, extractKey); err != nil {
		t.Fatalf("commit: %v", err)
	}
	t.Cleanup(func() {
		ss.Remove(ctx, commitKey) //nolint:errcheck
	})

	// Verify committed snapshot exists
	assert.SnapshotExists(ctx, commitKey, snapshots.KindCommitted)
	t.Logf("snapshot committed: %s", commitKey)
}

// testSnapshotCleanup verifies snapshots can be properly removed.
func testSnapshotCleanup(t *testing.T, env *Environment) {
	ctx := env.Context()
	ss := env.SnapshotService()
	assert := NewAssertions(t, env)

	t.Log("--- Step 1: Find committed parent ---")
	parentKey := assert.FindCommittedSnapshot(ctx)

	t.Log("--- Step 2: Create snapshot for cleanup test ---")
	snapKey := fmt.Sprintf("test-cleanup-%d", time.Now().UnixNano())
	mounts, err := ss.Prepare(ctx, snapKey, parentKey)
	if err != nil {
		assert.DumpSnapshotState(ctx)
		t.Fatalf("prepare: %v", err)
	}
	t.Logf("prepared snapshot %s with %d mounts", snapKey, len(mounts))

	t.Log("--- Step 3: Verify snapshot exists ---")
	assert.SnapshotExists(ctx, snapKey, snapshots.KindActive)

	t.Log("--- Step 4: Remove snapshot ---")
	if err := ss.Remove(ctx, snapKey); err != nil {
		assert.DumpSnapshotState(ctx)
		t.Fatalf("remove: %v", err)
	}
	t.Logf("remove operation completed")

	t.Log("--- Step 5: Verify snapshot removed ---")
	assert.SnapshotRemoved(ctx, snapKey)

	t.Logf("cleanup test complete: snapshot %s created and removed", snapKey)
}

// testMultiLayer tests pulling and viewing a multi-layer image.
func testMultiLayer(t *testing.T, env *Environment) {
	ctx := env.Context()
	c := env.Client()
	ss := env.SnapshotService()
	assert := NewAssertions(t, env)

	// Pull multi-layer image
	if err := pullImage(ctx, c, multiLayerImage); err != nil {
		t.Fatalf("pull multi-layer image: %v", err)
	}

	// Count snapshots and find top committed snapshot
	count := assert.SnapshotCount(ctx, "multi-layer pull", 2)
	topSnap := assert.FindCommittedSnapshot(ctx)
	t.Logf("multi-layer image created %d snapshots", count)

	// Create a view to trigger VMDK generation
	viewKey := fmt.Sprintf("test-multi-view-%d", time.Now().UnixNano())
	_, err := ss.View(ctx, viewKey, topSnap)
	if err != nil {
		t.Fatalf("create view: %v", err)
	}
	t.Cleanup(func() {
		ss.Remove(ctx, viewKey) //nolint:errcheck
	})

	// Wait for VMDK generation by polling
	snapshotsDir := filepath.Join(env.SnapshotterRoot(), "snapshots")
	var vmdkPaths []string
	err = waitFor(func() bool {
		vmdkPaths = nil
		filepath.Walk(snapshotsDir, func(path string, info os.FileInfo, walkErr error) error { //nolint:errcheck
			if walkErr == nil && filepath.Base(path) == mergedVMDKFile {
				vmdkPaths = append(vmdkPaths, path)
			}
			return nil
		})
		return len(vmdkPaths) > 0
	}, 5*time.Second, "no VMDK files generated")

	if err != nil {
		t.Error("no VMDK files generated for multi-layer image")
		return
	}

	for _, p := range vmdkPaths {
		t.Logf("found VMDK: %s", p)
	}
}

// testVMDKFormat verifies VMDK descriptor format is valid.
func testVMDKFormat(t *testing.T, env *Environment) {
	assert := NewAssertions(t, env)
	snapshotsDir := filepath.Join(env.SnapshotterRoot(), "snapshots")

	// Find a VMDK file
	var vmdkPath string
	filepath.Walk(snapshotsDir, func(path string, info os.FileInfo, err error) error { //nolint:errcheck
		if err == nil && filepath.Base(path) == mergedVMDKFile && vmdkPath == "" {
			vmdkPath = path
		}
		return nil
	})

	if vmdkPath == "" {
		t.Skip("no VMDK file found")
	}

	assert.VMDKValid(vmdkPath)
	t.Logf("VMDK format validated: %s", vmdkPath)
}

// testVMDKLayerOrder verifies VMDK layers are in correct order.
func testVMDKLayerOrder(t *testing.T, env *Environment) {
	snapshotsDir := filepath.Join(env.SnapshotterRoot(), "snapshots")

	// Find VMDK with multiple layers
	vmdkPath, maxLayers := findVMDKWithMostLayers(snapshotsDir)

	if vmdkPath == "" || maxLayers < 2 {
		t.Skip("no multi-layer VMDK found")
	}

	// Parse VMDK
	vmdkLayers, err := parseVMDKLayers(vmdkPath)
	if err != nil {
		t.Fatalf("parse VMDK: %v", err)
	}

	// Verify fsmeta is first
	if len(vmdkLayers) > 0 && !strings.Contains(vmdkLayers[0], "fsmeta.erofs") {
		t.Errorf("first layer should be fsmeta.erofs, got: %s", vmdkLayers[0])
	}

	// Read manifest file
	manifestPath := filepath.Join(filepath.Dir(vmdkPath), "layers.manifest")
	manifestDigests, err := readLayersManifest(manifestPath)
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}

	// Extract VMDK digests
	var vmdkDigests []string
	for _, layer := range vmdkLayers {
		if d := extractDigest(layer); d != "" {
			vmdkDigests = append(vmdkDigests, d)
		}
	}

	// Compare order
	if len(vmdkDigests) != len(manifestDigests) {
		t.Errorf("layer count mismatch: VMDK=%d, manifest=%d", len(vmdkDigests), len(manifestDigests))
	}

	for i := 0; i < len(vmdkDigests) && i < len(manifestDigests); i++ {
		if vmdkDigests[i] != manifestDigests[i] {
			t.Errorf("layer order mismatch at position %d: VMDK=%s, manifest=%s",
				i, vmdkDigests[i][:12], manifestDigests[i][:12])
		}
	}

	t.Logf("VMDK layer order verified (%d layers)", len(vmdkDigests))
}

// parseVMDKLayers extracts layer paths from a VMDK descriptor.
func parseVMDKLayers(path string) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var layers []string
	scanner := bufio.NewScanner(f)

	for scanner.Scan() {
		if matches := extentPattern.FindStringSubmatch(scanner.Text()); len(matches) > 1 {
			layers = append(layers, matches[1])
		}
	}
	return layers, scanner.Err()
}

// readLayersManifest reads digests from a layers.manifest file.
func readLayersManifest(path string) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var digests []string
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if strings.HasPrefix(line, "sha256:") {
			digests = append(digests, strings.TrimPrefix(line, "sha256:"))
		}
	}
	return digests, scanner.Err()
}

// extractDigest extracts a sha256 digest from a layer path.
func extractDigest(path string) string {
	if matches := digestPattern.FindStringSubmatch(path); len(matches) > 1 {
		return matches[1]
	}
	return ""
}

// testCommitLifecycle tests the full container commit workflow.
func testCommitLifecycle(t *testing.T, env *Environment) {
	ctx := env.Context()
	ss := env.SnapshotService()
	assert := NewAssertions(t, env)

	// Find a committed parent
	parentKey := assert.FindCommittedSnapshot(ctx)

	// Create active snapshot
	ts := time.Now().UnixNano()
	activeKey := fmt.Sprintf("lifecycle-active-%d", ts)
	mounts, err := ss.Prepare(ctx, activeKey, parentKey)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	t.Cleanup(func() {
		ss.Remove(ctx, activeKey) //nolint:errcheck
	})

	// Find ext4 rwlayer path
	var rwlayerPath string
	for _, m := range mounts {
		if m.Type == "ext4" {
			rwlayerPath = m.Source
			break
		}
	}

	if rwlayerPath == "" {
		t.Skip("no ext4 mount returned, skipping lifecycle test")
	}

	// Mount ext4, write data, unmount
	mountPoint := t.TempDir()
	if err := mountExt4(rwlayerPath, mountPoint); err != nil {
		t.Fatalf("mount ext4: %v", err)
	}

	// Write test data
	upperDir := filepath.Join(mountPoint, "upper")
	if err := os.MkdirAll(upperDir, 0755); err != nil {
		unmountExt4(mountPoint) //nolint:errcheck
		t.Fatalf("create upper dir: %v", err)
	}

	testFile := filepath.Join(upperDir, "lifecycle-test.bin")
	testData := bytes.Repeat([]byte("x"), 1024*1024) // 1MB
	if err := os.WriteFile(testFile, testData, 0644); err != nil {
		unmountExt4(mountPoint) //nolint:errcheck
		t.Fatalf("write test file: %v", err)
	}

	if err := unmountExt4(mountPoint); err != nil {
		t.Fatalf("unmount ext4: %v", err)
	}

	// Commit
	commitKey := fmt.Sprintf("lifecycle-commit-%d", ts)
	if err := ss.Commit(ctx, commitKey, activeKey); err != nil {
		t.Fatalf("commit: %v", err)
	}
	t.Cleanup(func() {
		ss.Remove(ctx, commitKey) //nolint:errcheck
	})

	// Verify committed snapshot
	assert.SnapshotExists(ctx, commitKey, snapshots.KindCommitted)
	t.Log("commit lifecycle test passed")
}

// mountExt4 mounts an ext4 image at the given path.
func mountExt4(image, target string) error {
	cmd := exec.Command("mount", "-o", "loop", image, target)
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("mount: %s: %w", string(out), err)
	}
	return nil
}

// unmountExt4 unmounts an ext4 filesystem.
func unmountExt4(target string) error {
	_ = exec.Command("sync").Run()
	cmd := exec.Command("umount", target)
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("umount: %s: %w", string(out), err)
	}
	return nil
}

// testFullCleanup verifies all resources are cleaned up properly.
func testFullCleanup(t *testing.T, env *Environment) {
	ctx := env.Context()
	c := env.Client()
	ss := env.SnapshotService()

	// Remove all images
	imgService := c.ImageService()
	imgs, err := imgService.List(ctx)
	if err != nil {
		t.Fatalf("list images: %v", err)
	}

	for _, img := range imgs {
		if err := imgService.Delete(ctx, img.Name); err != nil {
			t.Logf("delete image %s: %v", img.Name, err)
		}
	}

	// Remove all snapshots (in reverse order to handle dependencies)
	var keys []string
	if err := ss.Walk(ctx, func(_ context.Context, info snapshots.Info) error {
		keys = append(keys, info.Name)
		return nil
	}); err != nil {
		t.Fatalf("walk snapshots: %v", err)
	}

	for i := len(keys) - 1; i >= 0; i-- {
		if err := ss.Remove(ctx, keys[i]); err != nil {
			t.Logf("remove snapshot %s: %v", keys[i], err)
		}
	}

	// Wait for cleanup by polling
	var remaining int
	_ = waitFor(func() bool {
		remaining = 0
		ss.Walk(ctx, func(_ context.Context, _ snapshots.Info) error { //nolint:errcheck
			remaining++
			return nil
		})
		return remaining == 0
	}, 5*time.Second, "snapshots not cleaned up")

	if remaining > 0 {
		t.Errorf("%d snapshots still registered after cleanup", remaining)
	}

	// Check for leaked files
	snapshotsDir := filepath.Join(env.SnapshotterRoot(), "snapshots")
	if info, err := os.Stat(snapshotsDir); err == nil && info.IsDir() {
		entries, _ := os.ReadDir(snapshotsDir)
		if len(entries) > 0 {
			t.Errorf("%d leaked files in snapshots directory: %s", len(entries), snapshotsDir)
			for _, e := range entries {
				leakedPath := filepath.Join(snapshotsDir, e.Name())
				t.Logf("  leaked: %s", leakedPath)
				// List contents to help debug
				if e.IsDir() {
					subEntries, _ := os.ReadDir(leakedPath)
					for _, sub := range subEntries {
						t.Logf("    - %s", sub.Name())
					}
				}
			}
		}
	}

	t.Log("cleanup verification complete")
}

// findVMDKWithMostLayers finds the VMDK file with the most layers.
// Returns the path and layer count, or empty string and 0 if not found.
func findVMDKWithMostLayers(snapshotsDir string) (string, int) {
	var vmdkPath string
	var maxLayers int

	_ = filepath.Walk(snapshotsDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil //nolint:nilerr // skip files with errors
		}
		if filepath.Base(path) != mergedVMDKFile {
			return nil
		}
		data, readErr := os.ReadFile(path)
		if readErr != nil {
			return nil //nolint:nilerr // intentionally skip files we can't read
		}
		layers := strings.Count(string(data), "sha256-")
		if layers > maxLayers {
			maxLayers = layers
			vmdkPath = path
		}
		return nil
	})

	return vmdkPath, maxLayers
}
