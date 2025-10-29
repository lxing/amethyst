package common

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestFormatDuration(t *testing.T) {
	tests := []struct {
		name     string
		duration time.Duration
		expected string
	}{
		// Microseconds (< 0.01 ms)
		{"5 microseconds", 5 * time.Microsecond, "5.00 us"},
		{"9.5 microseconds", 9500 * time.Nanosecond, "9.50 us"},

		// Sub-millisecond
		{"0.01 ms", 10 * time.Microsecond, "0.01 ms"},
		{"0.1 ms", 100 * time.Microsecond, "0.10 ms"},
		{"0.5 ms", 500 * time.Microsecond, "0.50 ms"},

		// 1-10 ms
		{"1 ms", 1 * time.Millisecond, "1.00 ms"},
		{"1.234 ms", 1234 * time.Microsecond, "1.23 ms"},
		{"5.678 ms", 5678 * time.Microsecond, "5.68 ms"},
		{"9.999 ms", 9999 * time.Microsecond, "10.00 ms"},

		// 10-100 ms
		{"12.34 ms", 12340 * time.Microsecond, "12.34 ms"},
		{"50 ms", 50 * time.Millisecond, "50.00 ms"},
		{"99.9 ms", 99900 * time.Microsecond, "99.90 ms"},

		// 100-1000 ms
		{"123 ms", 123 * time.Millisecond, "123.00 ms"},
		{"456 ms", 456 * time.Millisecond, "456.00 ms"},
		{"999 ms", 999 * time.Millisecond, "999.00 ms"},

		// Seconds
		{"1.234 s", 1234 * time.Millisecond, "1.23 s"},
		{"5.678 s", 5678 * time.Millisecond, "5.68 s"},
		{"12.34 s", 12340 * time.Millisecond, "12.34 s"},
		{"123.4 s", 123400 * time.Millisecond, "123.40 s"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := formatDuration(tt.duration)
			require.Equal(t, tt.expected, result, "duration %v", tt.duration)
		})
	}
}
