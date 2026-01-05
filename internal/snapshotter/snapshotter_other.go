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

package erofs

import (
	"context"

	"github.com/containerd/errdefs"
)

// defaultWritableSize controls the default writable layer mode.
//
// On non-Linux platforms, this is set to 64 MiB to enable "block mode" -
// an ext4 image file is created for the writable layer. This is required
// because non-Linux systems typically lack native overlayfs support.
//
// The size can be adjusted via the WithDefaultSize option or per-snapshot
// configuration. Set to 0 to use "directory mode" (Linux-only).
//
// See snapshotter_linux.go for the Linux default (0 = directory mode).
const defaultWritableSize = 64 * 1024 * 1024

func checkCompatibility(root string) error {
	return nil
}

func setImmutable(path string, enable bool) error {
	return errdefs.ErrNotImplemented
}

func cleanupUpper(upper string) error {
	return nil
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
