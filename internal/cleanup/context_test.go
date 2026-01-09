package cleanup

import (
	"context"
	"testing"
	"time"

	// Import testutil to register the -test.root flag
	_ "github.com/spin-stack/erofs-snapshotter/internal/testutil"
)

func TestDo(t *testing.T) {
	t.Run("executes function", func(t *testing.T) {
		var called bool
		Do(context.Background(), func(ctx context.Context) {
			called = true
		})
		if !called {
			t.Error("function was not called")
		}
	})

	t.Run("provides timeout context", func(t *testing.T) {
		Do(context.Background(), func(ctx context.Context) {
			deadline, ok := ctx.Deadline()
			if !ok {
				t.Error("expected context to have deadline")
				return
			}
			remaining := time.Until(deadline)
			if remaining <= 0 || remaining > 11*time.Second {
				t.Errorf("deadline should be ~10s in future, got %v", remaining)
			}
		})
	})

	t.Run("clears parent cancellation", func(t *testing.T) {
		canceled, cancel := context.WithCancel(context.Background())
		cancel() // Cancel the parent

		Do(canceled, func(ctx context.Context) {
			if ctx.Err() != nil {
				t.Errorf("expected clean context, got error: %v", ctx.Err())
			}
		})
	})

	t.Run("clears parent deadline", func(t *testing.T) {
		expired, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Hour))
		defer cancel()

		Do(expired, func(ctx context.Context) {
			if ctx.Err() != nil {
				t.Errorf("expected clean context despite expired parent, got: %v", ctx.Err())
			}
		})
	})

	t.Run("preserves values from parent", func(t *testing.T) {
		type key struct{}
		parent := context.WithValue(context.Background(), key{}, "test-value")

		Do(parent, func(ctx context.Context) {
			if v := ctx.Value(key{}); v != "test-value" {
				t.Errorf("expected value to be preserved, got %v", v)
			}
		})
	})
}
