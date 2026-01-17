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

// Package grpcservice provides gRPC service wrappers for containerd plugins.
// This is a fork of containerd's contrib/snapshotservice with fixes for
// parallel unpacking support (rebase capability).
package grpcservice

import (
	"context"
	"strconv"
	"strings"

	snapshotsapi "github.com/containerd/containerd/api/services/snapshots/v1"
	"github.com/containerd/containerd/v2/core/mount"
	"github.com/containerd/containerd/v2/core/snapshots"
	"github.com/containerd/containerd/v2/core/snapshots/proxy"
	"github.com/containerd/containerd/v2/pkg/namespaces"
	ptypes "github.com/containerd/containerd/v2/pkg/protobuf/types"
	"github.com/containerd/errdefs"
	"github.com/containerd/errdefs/pkg/errgrpc"
	"github.com/containerd/log"
)

var empty = &ptypes.Empty{}

type service struct {
	sn snapshots.Snapshotter
	snapshotsapi.UnimplementedSnapshotsServer
}

// FromSnapshotter returns a Snapshot API server from a containerd snapshotter.
// This is a fixed version of containerd's contrib/snapshotservice that properly
// handles the Parent field in CommitSnapshotRequest for rebase support.
func FromSnapshotter(sn snapshots.Snapshotter) snapshotsapi.SnapshotsServer {
	return &service{sn: sn}
}

// normalizeProxyKey strips the proxy prefix "namespace/txID/" when present.
// Containerd's snapshotter proxy injects a transaction ID that changes across
// calls; we must normalize to the stable snapshot name for cross-call lookups.
func normalizeProxyKey(namespace, key string) string {
	if key == "" {
		return key
	}
	parts := strings.SplitN(key, "/", 3)
	if len(parts) != 3 {
		return key
	}
	if parts[0] != namespace {
		return key
	}
	if _, err := strconv.Atoi(parts[1]); err != nil {
		return key
	}
	return parts[2]
}

func (s *service) Prepare(ctx context.Context, pr *snapshotsapi.PrepareSnapshotRequest) (*snapshotsapi.PrepareSnapshotResponse, error) {
	ns, _ := namespaces.Namespace(ctx)
	key := normalizeProxyKey(ns, pr.Key)
	parent := normalizeProxyKey(ns, pr.Parent)
	log.G(ctx).WithFields(log.Fields{
		"namespace": ns,
		"key":       key,
		"parent":    parent,
	}).Debug("grpc: received prepare request")

	var opts []snapshots.Opt
	if pr.Labels != nil {
		opts = append(opts, snapshots.WithLabels(pr.Labels))
	}
	mounts, err := s.sn.Prepare(ctx, key, parent, opts...)
	if err != nil {
		log.G(ctx).WithError(err).WithFields(log.Fields{
			"namespace": ns,
			"key":       key,
		}).Debug("grpc: prepare failed")
		return nil, errgrpc.ToGRPC(err)
	}

	log.G(ctx).WithFields(log.Fields{
		"namespace":   ns,
		"key":         pr.Key,
		"mount_count": len(mounts),
	}).Debug("grpc: prepare succeeded")

	return &snapshotsapi.PrepareSnapshotResponse{
		Mounts: mount.ToProto(mounts),
	}, nil
}

func (s *service) View(ctx context.Context, pr *snapshotsapi.ViewSnapshotRequest) (*snapshotsapi.ViewSnapshotResponse, error) {
	ns, _ := namespaces.Namespace(ctx)
	key := normalizeProxyKey(ns, pr.Key)
	parent := normalizeProxyKey(ns, pr.Parent)
	var opts []snapshots.Opt
	if pr.Labels != nil {
		opts = append(opts, snapshots.WithLabels(pr.Labels))
	}
	mounts, err := s.sn.View(ctx, key, parent, opts...)
	if err != nil {
		return nil, errgrpc.ToGRPC(err)
	}
	return &snapshotsapi.ViewSnapshotResponse{
		Mounts: mount.ToProto(mounts),
	}, nil
}

func (s *service) Mounts(ctx context.Context, mr *snapshotsapi.MountsRequest) (*snapshotsapi.MountsResponse, error) {
	ns, _ := namespaces.Namespace(ctx)
	key := normalizeProxyKey(ns, mr.Key)
	mounts, err := s.sn.Mounts(ctx, key)
	if err != nil {
		return nil, errgrpc.ToGRPC(err)
	}
	return &snapshotsapi.MountsResponse{
		Mounts: mount.ToProto(mounts),
	}, nil
}

func (s *service) Commit(ctx context.Context, cr *snapshotsapi.CommitSnapshotRequest) (*ptypes.Empty, error) {
	// Log immediately at handler entry to diagnose if we're even reaching here
	log.L.WithFields(log.Fields{
		"name": cr.Name,
		"key":  cr.Key,
	}).Debug("grpc: commit handler entry")

	ns, _ := namespaces.Namespace(ctx)
	name := normalizeProxyKey(ns, cr.Name)
	key := normalizeProxyKey(ns, cr.Key)
	parent := normalizeProxyKey(ns, cr.Parent)
	log.G(ctx).WithFields(log.Fields{
		"namespace": ns,
		"name":      name,
		"key":       key,
		"parent":    parent,
	}).Debug("grpc: received commit request")

	var opts []snapshots.Opt
	if cr.Labels != nil {
		opts = append(opts, snapshots.WithLabels(cr.Labels))
	}
	// FIX: Pass Parent field for rebase support (missing in contrib/snapshotservice)
	// This is required for parallel layer unpacking with the "rebase" capability.
	// See: https://github.com/containerd/containerd/issues/8881
	if parent != "" {
		opts = append(opts, snapshots.WithParent(parent))
	}

	log.G(ctx).WithFields(log.Fields{
		"namespace": ns,
		"key":       key,
	}).Debug("grpc: commit calling snapshotter")

	if err := s.sn.Commit(ctx, name, key, opts...); err != nil {
		log.G(ctx).WithError(err).WithFields(log.Fields{
			"namespace": ns,
			"name":      name,
			"key":       key,
		}).Debug("grpc: commit failed")
		return nil, errgrpc.ToGRPC(err)
	}

	log.G(ctx).WithFields(log.Fields{
		"namespace": ns,
		"name":      name,
		"key":       key,
	}).Debug("grpc: commit succeeded")

	return empty, nil
}

func (s *service) Remove(ctx context.Context, rr *snapshotsapi.RemoveSnapshotRequest) (*ptypes.Empty, error) {
	ns, _ := namespaces.Namespace(ctx)
	key := normalizeProxyKey(ns, rr.Key)
	log.G(ctx).WithFields(log.Fields{
		"namespace": ns,
		"key":       key,
	}).Debug("grpc: received remove request")

	if err := s.sn.Remove(ctx, key); err != nil {
		log.G(ctx).WithError(err).WithFields(log.Fields{
			"namespace": ns,
			"key":       key,
		}).Debug("grpc: remove failed")
		return nil, errgrpc.ToGRPC(err)
	}

	log.G(ctx).WithFields(log.Fields{
		"namespace": ns,
		"key":       key,
	}).Debug("grpc: remove succeeded")

	return empty, nil
}

func (s *service) Stat(ctx context.Context, sr *snapshotsapi.StatSnapshotRequest) (*snapshotsapi.StatSnapshotResponse, error) {
	ns, _ := namespaces.Namespace(ctx)
	key := normalizeProxyKey(ns, sr.Key)
	info, err := s.sn.Stat(ctx, key)
	if err != nil {
		log.G(ctx).WithError(err).WithFields(log.Fields{
			"namespace": ns,
			"key":       key,
		}).Debug("grpc: stat failed")
		return nil, errgrpc.ToGRPC(err)
	}

	log.G(ctx).WithFields(log.Fields{
		"namespace": ns,
		"key":       key,
		"kind":      info.Kind,
	}).Debug("grpc: stat succeeded")

	return &snapshotsapi.StatSnapshotResponse{Info: proxy.InfoToProto(info)}, nil
}

func (s *service) Update(ctx context.Context, sr *snapshotsapi.UpdateSnapshotRequest) (*snapshotsapi.UpdateSnapshotResponse, error) {
	ns, _ := namespaces.Namespace(ctx)
	info := proxy.InfoFromProto(sr.Info)
	info.Name = normalizeProxyKey(ns, info.Name)
	info.Parent = normalizeProxyKey(ns, info.Parent)
	info, err := s.sn.Update(ctx, info, sr.UpdateMask.GetPaths()...)
	if err != nil {
		return nil, errgrpc.ToGRPC(err)
	}

	return &snapshotsapi.UpdateSnapshotResponse{Info: proxy.InfoToProto(info)}, nil
}

func (s *service) List(sr *snapshotsapi.ListSnapshotsRequest, ss snapshotsapi.Snapshots_ListServer) error {
	var (
		buffer    []*snapshotsapi.Info
		sendBlock = func(block []*snapshotsapi.Info) error {
			return ss.Send(&snapshotsapi.ListSnapshotsResponse{
				Info: block,
			})
		}
	)
	err := s.sn.Walk(ss.Context(), func(ctx context.Context, info snapshots.Info) error {
		buffer = append(buffer, proxy.InfoToProto(info))

		if len(buffer) >= 100 {
			if err := sendBlock(buffer); err != nil {
				return err
			}

			buffer = buffer[:0]
		}

		return nil
	}, sr.Filters...)
	if err != nil {
		return err
	}
	if len(buffer) > 0 {
		// Send remaining infos
		if err := sendBlock(buffer); err != nil {
			return err
		}
	}

	return nil
}

func (s *service) Usage(ctx context.Context, ur *snapshotsapi.UsageRequest) (*snapshotsapi.UsageResponse, error) {
	ns, _ := namespaces.Namespace(ctx)
	key := normalizeProxyKey(ns, ur.Key)
	usage, err := s.sn.Usage(ctx, key)
	if err != nil {
		return nil, errgrpc.ToGRPC(err)
	}

	return &snapshotsapi.UsageResponse{
		Inodes: usage.Inodes,
		Size:   usage.Size,
	}, nil
}

func (s *service) Cleanup(ctx context.Context, cr *snapshotsapi.CleanupRequest) (*ptypes.Empty, error) {
	c, ok := s.sn.(snapshots.Cleaner)
	if !ok {
		return nil, errgrpc.ToGRPCf(errdefs.ErrNotImplemented, "snapshotter does not implement Cleanup method")
	}

	if err := c.Cleanup(ctx); err != nil {
		return nil, errgrpc.ToGRPC(err)
	}

	return empty, nil
}
