//go:build !linux

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

package snapshotter

import (
	"context"

	"github.com/containerd/errdefs"
)

// defaultWritableSize is the default size for the ext4 writable layer.
// An ext4 image file of this size is created and loop-mounted for each
// active snapshot's writable layer.
const defaultWritableSize = 64 * 1024 * 1024 // 64 MiB

func checkCompatibility(root string) error {
	return nil
}

func setImmutable(path string, enable bool) error {
	return errdefs.ErrNotImplemented
}

func unmountAll(target string) error {
	return nil
}

func upperDirectoryPermission(p, parent string) error {
	return nil
}

func convertDirToErofs(ctx context.Context, layerBlob, upperDir string) error {
	return errdefs.ErrNotImplemented
}

func (s *snapshotter) cleanupOrphanedMounts() {
	// No-op on non-Linux platforms
}

func (s *snapshotter) mountBlockRwLayer(ctx context.Context, id string) error {
	return errdefs.ErrNotImplemented
}

// syncFile is a no-op on non-Linux platforms.
func syncFile(path string) error {
	return nil
}
