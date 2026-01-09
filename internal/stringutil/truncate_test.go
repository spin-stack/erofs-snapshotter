package stringutil

import (
	"testing"

	// Import testutil to register the -test.root flag
	_ "github.com/spin-stack/erofs-snapshotter/internal/testutil"
)

func TestTruncateOutput(t *testing.T) {
	tests := []struct {
		name   string
		input  []byte
		maxLen int
		want   string
	}{
		{
			name:   "empty input",
			input:  []byte{},
			maxLen: 10,
			want:   "",
		},
		{
			name:   "under limit",
			input:  []byte("hello"),
			maxLen: 10,
			want:   "hello",
		},
		{
			name:   "at limit",
			input:  []byte("hello"),
			maxLen: 5,
			want:   "hello",
		},
		{
			name:   "over limit",
			input:  []byte("hello world"),
			maxLen: 5,
			want:   "hello... (truncated)",
		},
		{
			name:   "zero limit truncates everything",
			input:  []byte("hello"),
			maxLen: 0,
			want:   "... (truncated)",
		},
		{
			name:   "single byte over",
			input:  []byte("abcdef"),
			maxLen: 5,
			want:   "abcde... (truncated)",
		},
		{
			name:   "nil input",
			input:  nil,
			maxLen: 10,
			want:   "",
		},
		{
			name:   "binary data",
			input:  []byte{0x00, 0x01, 0x02, 0x03, 0x04, 0x05},
			maxLen: 3,
			want:   "\x00\x01\x02... (truncated)",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := TruncateOutput(tc.input, tc.maxLen)
			if got != tc.want {
				t.Errorf("TruncateOutput(%q, %d) = %q, want %q", tc.input, tc.maxLen, got, tc.want)
			}
		})
	}
}
