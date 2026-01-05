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

// Package mountutils provides internal helper functions for mount operations.
// These functions support the mount manager and differ implementations.
package mountutils

import (
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"strings"
	"time"

	"github.com/containerd/containerd/v2/core/mount"
)

// NeedsMountManager returns true if any mount requires the mount manager to resolve.
// This includes mounts with template syntax (e.g., "{{ mount 0 }}") and formatted mounts
// (format/, mkfs/, mkdir/).
func NeedsMountManager(mounts []mount.Mount) bool {
	for _, m := range mounts {
		if HasTemplate(m) {
			return true
		}
		mt := TypeBase(m.Type)
		if mt == "format" || mt == "mkfs" || mt == "mkdir" {
			return true
		}
	}
	return false
}

// NeedsNonTemporaryActivation returns true if mounts require non-temporary
// activation. Format, mkfs, and mkdir mounts may create persistent state that
// should not be cleaned up immediately.
func NeedsNonTemporaryActivation(mounts []mount.Mount) bool {
	for _, m := range mounts {
		if strings.HasPrefix(m.Type, "format/") || strings.HasPrefix(m.Type, "mkfs/") || strings.HasPrefix(m.Type, "mkdir/") {
			return true
		}
	}
	return false
}

// HasTemplate returns true if the mount contains template syntax (e.g., "{{ mount 0 }}")
// in its source, target, or options. Such mounts require resolution by the mount manager.
func HasTemplate(m mount.Mount) bool {
	if strings.Contains(m.Source, "{{") || strings.Contains(m.Target, "{{") {
		return true
	}
	for _, opt := range m.Options {
		if strings.Contains(opt, "{{") {
			return true
		}
	}
	return false
}

// TypeBase returns the base component of a mount type.
// For "format/mkdir/overlay", it returns "format".
// For simple types like "bind", it returns "bind".
func TypeBase(t string) string {
	if t == "" {
		return ""
	}
	parts := strings.Split(t, "/")
	if len(parts) == 1 {
		return t
	}
	return parts[0]
}

// TypeSuffix returns the final component of a mount type.
// For "format/mkdir/overlay", it returns "overlay".
// For simple types like "bind", it returns "bind".
func TypeSuffix(t string) string {
	if t == "" {
		return ""
	}
	parts := strings.Split(t, "/")
	return parts[len(parts)-1]
}

// UniqueRef generates a unique reference string suitable for identifying
// temporary mount activations. The reference combines a nanosecond timestamp
// with random bytes to ensure uniqueness.
func UniqueRef() string {
	t := time.Now()
	var b [3]byte
	_, _ = rand.Read(b[:]) // Ignore read failures, just decreases uniqueness
	return fmt.Sprintf("%d-%s", t.UnixNano(), base64.URLEncoding.EncodeToString(b[:]))
}
