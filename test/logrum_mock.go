package test

import (
	"bytes"
	"fmt"
	"sync"
)

// MockLogrum is a mock implementation of the logrus interface for testing
type MockLogrum struct {
	mu       sync.Mutex
	buf      *bytes.Buffer
	level    int
	showTime bool
}

// NewMockLogrum creates a new MockLogrum instance for testing
func NewMockLogrum() *MockLogrum {
	mock := &MockLogrum{
		buf:   &bytes.Buffer{},
		level: 0, // Default to error level
	}
	DebugPrint("Created new MockLogrum instance, level=%d", mock.level)
	return mock
}

// Error logs at error level
func (l *MockLogrum) Error(msg string, args ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	formatted := fmt.Sprintf(msg, args...)
	fmt.Fprintf(l.buf, "[ERROR] "+msg+"\n", args...)
	DebugPrint("MockLogrum.Error called: '%s', current level=%d", formatted, l.level)
}

// Info logs at info level
func (l *MockLogrum) Info(msg string, args ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	formatted := fmt.Sprintf(msg, args...)
	DebugPrint("MockLogrum.Info called: '%s', current level=%d", formatted, l.level)
	if l.level >= 1 {
		fmt.Fprintf(l.buf, "[INFO] "+msg+"\n", args...)
		DebugPrint("MockLogrum.Info - message logged")
	} else {
		DebugPrint("MockLogrum.Info - message NOT logged (level too low)")
	}
}

// Debug logs at debug level
func (l *MockLogrum) Debug(msg string, args ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	formatted := fmt.Sprintf(msg, args...)
	DebugPrint("MockLogrum.Debug called: '%s', current level=%d", formatted, l.level)
	if l.level >= 2 {
		fmt.Fprintf(l.buf, "[DEBUG] "+msg+"\n", args...)
		DebugPrint("MockLogrum.Debug - message logged")
	} else {
		DebugPrint("MockLogrum.Debug - message NOT logged (level too low)")
	}
}

// Trace logs at trace level
func (l *MockLogrum) Trace(msg string, args ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	formatted := fmt.Sprintf(msg, args...)
	DebugPrint("MockLogrum.Trace called: '%s', current level=%d", formatted, l.level)
	if l.level >= 3 {
		fmt.Fprintf(l.buf, "[TRACE] "+msg+"\n", args...)
		DebugPrint("MockLogrum.Trace - message logged")
	} else {
		DebugPrint("MockLogrum.Trace - message NOT logged (level too low)")
	}
}

// SetLevel sets the logging level
func (l *MockLogrum) SetLevel(level int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	DebugPrint("MockLogrum.SetLevel: %d -> %d", l.level, level)
	l.level = level
}

// GetLevel returns the current logging level
func (l *MockLogrum) GetLevel() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	DebugPrint("MockLogrum.GetLevel: %d", l.level)
	return l.level
}

// GetOutput returns the logged output
func (l *MockLogrum) GetOutput() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	output := l.buf.String()
	DebugPrint("MockLogrum.GetOutput: '%s'", output)
	return output
}

// Reset clears the buffer
func (l *MockLogrum) Reset() {
	l.mu.Lock()
	defer l.mu.Unlock()
	DebugPrint("MockLogrum.Reset called")
	l.buf.Reset()
}
