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

// LogDuration prints a message with the elapsed time since start.
func LogDuration(start time.Time, format string, args ...interface{}) {
	elapsed := time.Since(start)
	msg := fmt.Sprintf(format, args...)
	Logf("(%v) %s\n", elapsed, msg)
}
