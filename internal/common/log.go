package common

import (
	"fmt"
	"time"
)

// LoggingEnabled controls whether Logf produces output.
var LoggingEnabled = true

// Logf prints a formatted message if logging is enabled.
func Logf(format string, args ...interface{}) {
	if LoggingEnabled {
		fmt.Printf(format, args...)
	}
}

// formatDuration formats a duration with 2 decimal places.
// Returns a string like "1.23 ms" (no padding).
func formatDuration(d time.Duration) string {
	ms := float64(d) / float64(time.Millisecond)

	// Handle durations >= 1 second
	if ms >= 1000 {
		sec := ms / 1000
		return fmt.Sprintf("%.2f s", sec)
	} else if ms < 0.01 {
		// Sub-0.01 ms: show in microseconds
		us := ms * 1000
		return fmt.Sprintf("%.2f us", us)
	}
	// Everything else in milliseconds with 2 decimal places
	return fmt.Sprintf("%.2f ms", ms)
}

// LogDuration prints a message with the elapsed time since start.
// The duration is formatted with tight parens and right-padded to align messages.
func LogDuration(start time.Time, format string, args ...interface{}) {
	elapsed := time.Since(start)
	msg := fmt.Sprintf(format, args...)
	durStr := fmt.Sprintf("(%s)", formatDuration(elapsed))
	Logf("%-10s%s\n", durStr, msg)
}
