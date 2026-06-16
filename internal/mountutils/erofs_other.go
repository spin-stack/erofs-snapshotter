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

package mountutils

import (
	"fmt"
	"runtime"

	"github.com/containerd/containerd/v2/core/mount"
)

// MountAll mounts all provided mounts to the target directory.
// On non-Linux platforms, EROFS mounts are not supported.
func MountAll(_ []mount.Mount, _ string) (cleanup func() error, err error) {
	return func() error { return nil }, fmt.Errorf("EROFS mounts not supported on %s", runtime.GOOS)
}

// ext4MountConfig holds options for MountExt4.
type ext4MountConfig struct {
	skipLock bool
}

// Ext4Opt configures MountExt4.
type Ext4Opt func(*ext4MountConfig)

// WithoutImageLock skips the exclusive image-lock gate (no-op stub).
func WithoutImageLock() Ext4Opt {
	return func(c *ext4MountConfig) { c.skipLock = true }
}

// MountExt4 mounts an ext4 filesystem image to the target directory.
func MountExt4(_, _ string, _ ...Ext4Opt) (cleanup func() error, err error) {
	return func() error { return nil }, fmt.Errorf("ext4 mounts not supported on %s", runtime.GOOS)
}
