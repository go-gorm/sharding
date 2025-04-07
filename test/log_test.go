package test

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"gorm.io/sharding"
)

// TestLogger is a special logger implementation for testing
type TestLogger struct {
	buf      *bytes.Buffer
	level    sharding.LogLevel
	showTime bool
}

func NewTestLogger(level sharding.LogLevel) *TestLogger {
	return &TestLogger{
		buf:   &bytes.Buffer{},
		level: level,
	}
}

func (l *TestLogger) Error(format string, args ...interface{}) {
	log.Printf(format, args...)
	fmt.Fprintf(l.buf, format+"\n", args...)
}

func (l *TestLogger) Info(format string, args ...interface{}) {
	if l.level >= sharding.LogLevelInfo {
		log.Printf(format, args...)
		fmt.Fprintf(l.buf, format+"\n", args...)
	}
}

func (l *TestLogger) Debug(format string, args ...interface{}) {
	if l.level >= sharding.LogLevelDebug {
		log.Printf(format, args...)
		fmt.Fprintf(l.buf, format+"\n", args...)
	}
}

func (l *TestLogger) Trace(format string, args ...interface{}) {
	if l.level >= sharding.LogLevelTrace {
		log.Printf(format, args...)
		fmt.Fprintf(l.buf, format+"\n", args...)
	}
}

func (l *TestLogger) SetLevel(level sharding.LogLevel) {
	l.level = level
}

func (l *TestLogger) GetLevel() sharding.LogLevel {
	return l.level
}

func (l *TestLogger) GetOutput() string {
	return l.buf.String()
}

func (l *TestLogger) Reset() {
	l.buf.Reset()
}

func TestLogLevelConstants(t *testing.T) {
	// Reset config before test
	resetConfig()

	// Verify log level constants are defined in correct order
	assert.Equal(t, sharding.LogLevel(0), sharding.LogLevelError)
	assert.Equal(t, sharding.LogLevel(1), sharding.LogLevelInfo)
	assert.Equal(t, sharding.LogLevel(2), sharding.LogLevelDebug)
	assert.Equal(t, sharding.LogLevel(3), sharding.LogLevelTrace)
}

func TestSetLogLevel(t *testing.T) {
	// Reset config before test
	resetConfig()

	// Set log level to Debug
	sharding.SetLogLevel(sharding.LogLevelDebug)

	// Verify the log level was set correctly
	config := sharding.GetConfig()
	assert.Equal(t, sharding.LogLevelDebug, config.Logging.Level)

	// Set log level to Error
	sharding.SetLogLevel(sharding.LogLevelError)

	// Verify the log level was set correctly
	config = sharding.GetConfig()
	assert.Equal(t, sharding.LogLevelError, config.Logging.Level)
}

func TestLogFunctionsRespectLevel(t *testing.T) {
	// Reset config before test
	resetConfig()

	// Create a test logger
	testLogger := NewTestLogger(sharding.LogLevelError)
	sharding.SetLogger(testLogger)

	// Test error logs always show up
	debugLog("debug message")
	assert.False(t, strings.Contains(testLogger.GetOutput(), "debug message"))

	errorLog("error message")
	assert.True(t, strings.Contains(testLogger.GetOutput(), "error message"))

	// Reset buffer
	testLogger.Reset()

	// Test info logs don't show up at error level
	infoLog("info message")
	assert.False(t, strings.Contains(testLogger.GetOutput(), "info message"))

	// Set level to Info and test again
	testLogger.SetLevel(sharding.LogLevelInfo)
	infoLog("info message")
	assert.True(t, strings.Contains(testLogger.GetOutput(), "info message"))

	// Reset buffer
	testLogger.Reset()

	// Test debug logs don't show up at info level
	debugLog("debug message")
	assert.False(t, strings.Contains(testLogger.GetOutput(), "debug message"))

	// Set level to Debug and test again
	testLogger.SetLevel(sharding.LogLevelDebug)
	debugLog("debug message")
	assert.True(t, strings.Contains(testLogger.GetOutput(), "debug message"))
}

func TestLoggerOutput(t *testing.T) {
	// Reset config before test
	resetConfig()

	// Save original output
	originalOutput := log.Writer()
	defer log.SetOutput(originalOutput)

	// Test stdout output
	config := sharding.DefaultConfig()
	config.Logging.Output = "stdout"
	sharding.SetConfig(config)

	// This is a bit tricky to test programmatically, so we'll just check that
	// the logger is configured rather than capturing actual output
	assert.Equal(t, "stdout", sharding.GetConfig().Logging.Output)

	// Test file output
	tempFile, err := os.CreateTemp("", "sharding-log-*.log")
	assert.NoError(t, err)
	defer os.Remove(tempFile.Name())

	config = sharding.GetConfig()
	config.Logging.Output = tempFile.Name()
	sharding.SetConfig(config)

	// Write a log message - use direct errorLog function which will write to file
	errorLog("test file output")

	// Read back the file contents
	tempFile.Seek(0, io.SeekStart)
	contents, err := io.ReadAll(tempFile)
	assert.NoError(t, err)

	// Verify log message was written to file
	assert.Contains(t, string(contents), "test file output")
}

// Helper functions to make tests work
func errorLog(format string, args ...interface{}) {
	sharding.GetLogger().Error(format, args...)
}

func infoLog(format string, args ...interface{}) {
	sharding.GetLogger().Info(format, args...)
}

func debugLog(format string, args ...interface{}) {
	sharding.GetLogger().Debug(format, args...)
}
