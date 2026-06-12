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
	"strings"
	"syscall"
	"time"

	diffapi "github.com/containerd/containerd/api/services/diff/v1"
	snapshotsapi "github.com/containerd/containerd/api/services/snapshots/v1"
	containerd "github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/contrib/diffservice"
	"github.com/containerd/containerd/v2/core/mount/manager"
	"github.com/containerd/log"
	"github.com/urfave/cli/v2"
	bolt "go.etcd.io/bbolt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/grpclog"
	"google.golang.org/grpc/metadata"

	"github.com/spin-stack/erofs-snapshotter/internal/differ"
	"github.com/spin-stack/erofs-snapshotter/internal/grpcservice"
	"github.com/spin-stack/erofs-snapshotter/internal/preflight"
	"github.com/spin-stack/erofs-snapshotter/internal/snapshotter"
	"github.com/spin-stack/erofs-snapshotter/internal/store"
)

// Version information - set via ldflags at build time
// Example: go build -ldflags "-X main.version=1.0.0 -X main.gitCommit=$(git rev-parse HEAD)"
var (
	version   = "dev"
	gitCommit = "unknown"
	buildDate = "unknown"
)

const (
	defaultAddress          = "/run/spin-stack/erofs-snapshotter.sock"
	defaultRoot             = "/var/lib/spin-stack/erofs-snapshotter"
	defaultContainerdSocket = "/var/run/spin-stack/containerd.sock"
)

func main() {
	app := &cli.App{
		Name:    "spin-erofs-snapshotter",
		Usage:   "External EROFS snapshotter for containerd",
		Version: fmt.Sprintf("%s (commit: %s, built: %s)", version, gitCommit, buildDate),
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "address",
				Aliases: []string{"a"},
				Usage:   "Address for the snapshotter socket",
				Value:   defaultAddress,
				EnvVars: []string{"EROFS_SNAPSHOTTER_ADDRESS"},
			},
			&cli.StringFlag{
				Name:    "root",
				Aliases: []string{"r"},
				Usage:   "Root directory for snapshotter data",
				Value:   defaultRoot,
				EnvVars: []string{"EROFS_SNAPSHOTTER_ROOT"},
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
				EnvVars: []string{"EROFS_SNAPSHOTTER_DEFAULT_SIZE"},
			},
			&cli.BoolFlag{
				Name:    "set-immutable",
				Usage:   "Set immutable flag on committed layers",
				Value:   true,
				EnvVars: []string{"EROFS_SNAPSHOTTER_SET_IMMUTABLE"},
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
	// Preflight checks run here (not in main) so --help/--version work on
	// hosts that don't satisfy the runtime requirements.
	if err := preflight.Check(); err != nil {
		return fmt.Errorf("preflight check failed: %w", err)
	}

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
	if err := os.MkdirAll(root, 0o700); err != nil {
		return fmt.Errorf("failed to create root directory: %w", err)
	}

	// Ensure socket directory exists
	socketDir := filepath.Dir(address)
	if err := os.MkdirAll(socketDir, 0o700); err != nil {
		return fmt.Errorf("failed to create socket directory: %w", err)
	}

	// Remove an existing socket only if no other instance is serving on it:
	// silently unlinking a live socket would orphan the running snapshotter
	// and hijack its address.
	if conn, err := net.DialTimeout("unix", address, time.Second); err == nil {
		_ = conn.Close()
		return fmt.Errorf("socket %s is already in use by a running instance", address)
	}
	if err := os.Remove(address); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to remove existing socket: %w", err)
	}

	// Build snapshotter options
	var snapshotterOpts []snapshotter.Opt
	size := cliCtx.Int64("default-size")
	if size <= 0 {
		return fmt.Errorf("default-size must be > 0, got %d", size)
	}
	snapshotterOpts = append(snapshotterOpts, snapshotter.WithDefaultSize(size))
	if cliCtx.Bool("set-immutable") {
		snapshotterOpts = append(snapshotterOpts, snapshotter.WithImmutable())
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

	dbPath := filepath.Join(root, "mounts.db")
	db, err := bolt.Open(dbPath, 0o600, nil)
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

	// Create gRPC server with request logging for debugging.
	// Use both unary and stream interceptors to catch all request types.
	// Enable verbose gRPC logging to diagnose connection issues.
	rpc := grpc.NewServer(
		grpc.UnaryInterceptor(grpcLoggingInterceptor),
		grpc.StreamInterceptor(grpcStreamLoggingInterceptor),
		grpc.MaxConcurrentStreams(1000), // Ensure we can handle many concurrent requests
	)

	// Register snapshot service
	snapshotsapi.RegisterSnapshotsServer(rpc, grpcservice.FromSnapshotter(sn))

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

func grpcStreamLoggingInterceptor(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	log.G(ss.Context()).WithFields(log.Fields{
		"method":         info.FullMethod,
		"isClientStream": info.IsClientStream,
		"isServerStream": info.IsServerStream,
	}).Debug("grpc: STREAM request received")
	return handler(srv, ss)
}

func grpcLoggingInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	fields := log.Fields{
		"method": info.FullMethod,
	}
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if ns := md.Get("containerd-namespace"); len(ns) > 0 {
			fields["md.namespace"] = strings.Join(ns, ",")
		}
	}

	// Per-request tracing is debug-level; failures stay at Warn.
	log.G(ctx).WithFields(fields).Debug("grpc: request received")

	resp, err := handler(ctx, req)
	if err != nil {
		log.G(ctx).WithFields(fields).WithError(err).Warn("grpc: request failed")
		return resp, err
	}
	log.G(ctx).WithFields(fields).Debug("grpc: request completed")
	return resp, nil
}
