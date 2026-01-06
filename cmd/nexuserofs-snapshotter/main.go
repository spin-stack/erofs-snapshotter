/*
   Copyright The containerd Authors.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	diffapi "github.com/containerd/containerd/api/services/diff/v1"
	snapshotsapi "github.com/containerd/containerd/api/services/snapshots/v1"
	containerd "github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/contrib/diffservice"
	"github.com/containerd/containerd/v2/contrib/snapshotservice"
	"github.com/containerd/containerd/v2/core/mount/manager"
	"github.com/containerd/log"
	"github.com/urfave/cli/v2"
	bolt "go.etcd.io/bbolt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/grpclog"

	differ "github.com/aledbf/nexuserofs/internal/differ"
	"github.com/aledbf/nexuserofs/internal/preflight"
	snapshotter "github.com/aledbf/nexuserofs/internal/snapshotter"
	"github.com/aledbf/nexuserofs/internal/store"
)

// Version information - set via ldflags at build time
// Example: go build -ldflags "-X main.version=1.0.0 -X main.gitCommit=$(git rev-parse HEAD)"
var (
	version   = "dev"
	gitCommit = "unknown"
	buildDate = "unknown"
)

const (
	defaultAddress          = "/run/nexuserofs-snapshotter/snapshotter.sock"
	defaultRoot             = "/var/lib/nexuserofs-snapshotter"
	defaultContainerdSocket = "/run/containerd/containerd.sock"
)

func main() {
	// Run preflight checks early to fail fast
	if err := preflight.Check(); err != nil {
		fmt.Fprintf(os.Stderr, "preflight check failed: %v\n", err)
		os.Exit(1)
	}

	app := &cli.App{
		Name:    "nexuserofs-snapshotter",
		Usage:   "External EROFS snapshotter for containerd",
		Version: fmt.Sprintf("%s (commit: %s, built: %s)", version, gitCommit, buildDate),
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "address",
				Aliases: []string{"a"},
				Usage:   "Address for the snapshotter socket",
				Value:   defaultAddress,
				EnvVars: []string{"NEXUSEROFS_SNAPSHOTTER_ADDRESS"},
			},
			&cli.StringFlag{
				Name:    "root",
				Aliases: []string{"r"},
				Usage:   "Root directory for snapshotter data",
				Value:   defaultRoot,
				EnvVars: []string{"NEXUSEROFS_SNAPSHOTTER_ROOT"},
			},
			&cli.StringFlag{
				Name:    "containerd-address",
				Usage:   "Address of containerd socket (for content store access)",
				Value:   defaultContainerdSocket,
				EnvVars: []string{"CONTAINERD_ADDRESS"},
			},
			&cli.StringFlag{
				Name:    "containerd-namespace",
				Usage:   "Containerd namespace to use",
				Value:   "default",
				EnvVars: []string{"CONTAINERD_NAMESPACE"},
			},
			&cli.StringFlag{
				Name:    "log-level",
				Usage:   "Log level (debug, info, warn, error)",
				Value:   "info",
				EnvVars: []string{"LOG_LEVEL"},
			},
			&cli.Int64Flag{
				Name:    "default-size",
				Usage:   "Size of ext4 writable layer in bytes (must be > 0)",
				Value:   64 * 1024 * 1024, // 64 MiB
				EnvVars: []string{"NEXUSEROFS_DEFAULT_SIZE"},
			},
			&cli.BoolFlag{
				Name:    "enable-fsverity",
				Usage:   "Enable fsverity for layer validation",
				Value:   false,
				EnvVars: []string{"NEXUSEROFS_ENABLE_FSVERITY"},
			},
			&cli.BoolFlag{
				Name:    "set-immutable",
				Usage:   "Set immutable flag on committed layers",
				Value:   true,
				EnvVars: []string{"NEXUSEROFS_SET_IMMUTABLE"},
			},
			&cli.BoolFlag{
				Name:    "tar-index-mode",
				Usage:   "Use tar index mode for layer conversion (requires erofs-utils 1.8+)",
				Value:   false,
				EnvVars: []string{"NEXUSEROFS_TAR_INDEX_MODE"},
			},
			&cli.StringSliceFlag{
				Name:    "mkfs-options",
				Usage:   "Extra options for mkfs.erofs",
				EnvVars: []string{"NEXUSEROFS_MKFS_OPTIONS"},
			},
			&cli.StringSliceFlag{
				Name:    "overlay-options",
				Usage:   "Extra options for overlay mounts",
				EnvVars: []string{"NEXUSEROFS_OVERLAY_OPTIONS"},
			},
		},
		Action: run,
	}

	if err := app.Run(os.Args); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func run(cliCtx *cli.Context) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Discard grpc logs so that they don't mess with our stdio
	grpclog.SetLoggerV2(grpclog.NewLoggerV2(io.Discard, io.Discard, io.Discard))

	// Set up logging using containerd's log package
	if err := log.SetLevel(cliCtx.String("log-level")); err != nil {
		return err
	}

	address := cliCtx.String("address")
	root := cliCtx.String("root")
	containerdAddress := cliCtx.String("containerd-address")
	containerdNamespace := cliCtx.String("containerd-namespace")

	// Ensure root directory exists
	if err := os.MkdirAll(root, 0700); err != nil {
		return fmt.Errorf("failed to create root directory: %w", err)
	}

	// Ensure socket directory exists
	socketDir := filepath.Dir(address)
	if err := os.MkdirAll(socketDir, 0700); err != nil {
		return fmt.Errorf("failed to create socket directory: %w", err)
	}

	// Remove existing socket if present
	if err := os.Remove(address); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to remove existing socket: %w", err)
	}

	// Build snapshotter options
	var snapshotterOpts []snapshotter.Opt
	if size := cliCtx.Int64("default-size"); size > 0 {
		snapshotterOpts = append(snapshotterOpts, snapshotter.WithDefaultSize(size))
	}
	if cliCtx.Bool("enable-fsverity") {
		snapshotterOpts = append(snapshotterOpts, snapshotter.WithFsverity())
	}
	if cliCtx.Bool("set-immutable") {
		snapshotterOpts = append(snapshotterOpts, snapshotter.WithImmutable())
	}
	if opts := cliCtx.StringSlice("overlay-options"); len(opts) > 0 {
		snapshotterOpts = append(snapshotterOpts, snapshotter.WithOvlOptions(opts))
	}

	// Create snapshotter
	sn, err := snapshotter.NewSnapshotter(root, snapshotterOpts...)
	if err != nil {
		return fmt.Errorf("failed to create snapshotter: %w", err)
	}
	defer sn.Close()

	// Connect to containerd for content store access
	client, err := containerd.New(containerdAddress, containerd.WithDefaultNamespace(containerdNamespace))
	if err != nil {
		return fmt.Errorf("failed to connect to containerd: %w", err)
	}
	defer client.Close()

	// Use namespace-aware store to properly handle namespace from gRPC request context.
	// This is necessary because proxy plugins receive namespace in gRPC metadata,
	// not from the client's default namespace.
	contentStore := store.NewNamespaceAwareStore(client, containerdNamespace)

	// Build differ options
	var differOpts []differ.DifferOpt
	if cliCtx.Bool("tar-index-mode") {
		differOpts = append(differOpts, differ.WithTarIndexMode())
	}
	if opts := cliCtx.StringSlice("mkfs-options"); len(opts) > 0 {
		differOpts = append(differOpts, differ.WithMkfsOptions(opts))
	}

	dbPath := filepath.Join(root, "mounts.db")
	db, err := bolt.Open(dbPath, 0600, nil)
	if err != nil {
		return fmt.Errorf("failed to open mount database: %w", err)
	}
	defer db.Close()

	mountRoot := filepath.Join(root, "mounts")
	mm, err := manager.NewManager(db, mountRoot, manager.WithAllowedRoot(root))
	if err != nil {
		return fmt.Errorf("failed to create mount manager: %w", err)
	}
	if closer, ok := mm.(interface{ Close() error }); ok {
		defer closer.Close()
	}

	// Add mount manager to differ options for template resolution
	differOpts = append(differOpts, differ.WithMountManager(mm))

	// Create differ
	df := differ.NewErofsDiffer(contentStore, differOpts...)

	// Create gRPC server
	rpc := grpc.NewServer()

	// Register snapshot service
	snapshotsapi.RegisterSnapshotsServer(rpc, snapshotservice.FromSnapshotter(sn))

	// Register diff service
	diffapi.RegisterDiffServer(rpc, diffservice.FromApplierAndComparer(df, df))

	// Listen on socket
	l, err := net.Listen("unix", address)
	if err != nil {
		return fmt.Errorf("failed to listen on socket: %w", err)
	}
	defer l.Close()

	log.G(ctx).WithField("address", address).Info("Starting EROFS snapshotter")
	log.G(ctx).WithField("root", root).Info("Snapshotter root directory")
	log.G(ctx).WithField("containerd", containerdAddress).Info("Connected to containerd")

	// Handle shutdown signals
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	errCh := make(chan error, 1)
	go func() {
		errCh <- rpc.Serve(l)
	}()

	select {
	case sig := <-sigCh:
		log.G(ctx).WithField("signal", sig).Info("Received shutdown signal")
		rpc.GracefulStop()
	case err := <-errCh:
		if err != nil {
			return fmt.Errorf("server error: %w", err)
		}
	}

	log.G(ctx).Info("Shutting down")
	return nil
}
