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

// Package cleanup provides utilities to help cleanup.
package cleanup

import (
	"context"
	"time"
)

// cleanupTimeout is the maximum time allowed for cleanup operations.
// 10 seconds provides enough time for typical unmount and file removal
// operations while preventing indefinite hangs during shutdown.
const cleanupTimeout = 10 * time.Second

// Do runs the provided function with a context that:
// 1. Is not cancelled when the parent context is cancelled
// 2. Has a timeout of cleanupTimeout (10 seconds)
//
// This is useful for cleanup operations that should complete even
// after the main operation's context has been cancelled.
func Do(ctx context.Context, do func(context.Context)) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cleanupTimeout)
	do(ctx)
	cancel()
}
