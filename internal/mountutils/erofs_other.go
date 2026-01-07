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
func MountAll(mounts []mount.Mount, target string) (cleanup func() error, err error) {
	return func() error { return nil }, fmt.Errorf("EROFS mounts not supported on %s", runtime.GOOS)
}

// HasErofsMultiDevice returns true if any mount is an EROFS with device= options.
func HasErofsMultiDevice(mounts []mount.Mount) bool {
	return false
}

// HasActiveSnapshotMounts returns true if the mounts represent an active snapshot.
func HasActiveSnapshotMounts(mounts []mount.Mount) bool {
	return false
}

// MountExt4 mounts an ext4 filesystem image to the target directory.
func MountExt4(source, target string) (cleanup func() error, err error) {
	return func() error { return nil }, fmt.Errorf("ext4 mounts not supported on %s", runtime.GOOS)
}
