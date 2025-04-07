package test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"gorm.io/sharding"
)

// mockConnPool mimics a subset of ConnPool for testing
type mockConnPool struct{}

func TestCrossFileLogging(t *testing.T) {
	// Setup mock logrum logger
	mockLogrum := NewMockLogrum()
	mockLogrum.SetLevel(3) // Set mock to trace level to capture everything

	// Mock the logger initialization that would happen in logger.go
	logger := sharding.NewLogrumLoggerWithProvider(mockLogrum)
	sharding.SetLogger(logger)

	// Test logging functions from conn_pool.go

	// debugLog function
	sharding.TestDebugLog("Debug message from test")
	assert.Contains(t, mockLogrum.GetOutput(), "Debug message from test")

	// traceLog function
	mockLogrum.Reset()
	sharding.TestTraceLog("Trace message from test")
	assert.Contains(t, mockLogrum.GetOutput(), "Trace message from test")

	// infoLog function
	mockLogrum.Reset()
	sharding.TestInfoLog("Info message from test")
	assert.Contains(t, mockLogrum.GetOutput(), "Info message from test")

	// errorLog function
	mockLogrum.Reset()
	sharding.TestErrorLog("Error message from test")
	assert.Contains(t, mockLogrum.GetOutput(), "Error message from test")
}

func TestLoggerWithDifferentLevels(t *testing.T) {
	// Setup mock logrum logger
	mockLogrum := NewMockLogrum()

	// Create the logger
	logger := sharding.NewLogrumLoggerWithProvider(mockLogrum)

	// Set as global logger
	sharding.SetLogger(logger)

	// Set to error level
	logger.SetLevel(sharding.LogLevelError)

	// Test logging with different levels

	// Error logs should appear
	sharding.TestErrorLog("Error log test")
	assert.Contains(t, mockLogrum.GetOutput(), "Error log test")

	// Info logs should not appear at Error level
	mockLogrum.Reset()
	sharding.TestInfoLog("Info log test")
	assert.Empty(t, mockLogrum.GetOutput())

	// Change level to info
	logger.SetLevel(sharding.LogLevelInfo)

	// Info logs should now appear
	sharding.TestInfoLog("Info log test")
	assert.Contains(t, mockLogrum.GetOutput(), "Info log test")

	// Debug logs still should not appear
	mockLogrum.Reset()
	sharding.TestDebugLog("Debug log test")
	assert.Empty(t, mockLogrum.GetOutput())
}

func TestLoggingFromConfig(t *testing.T) {
	// Create a default config with logrum enabled
	config := sharding.DefaultConfig()

	// We need to add the UseLogrum field to the config - this will fail until implemented
	config.Logging.UseLogrum = true
	config.Logging.Level = sharding.LogLevelTrace

	// Apply the config
	sharding.SetConfig(config)

	// Get the logger
	logger := sharding.GetLogger()

	// Check if it's a LogrumLogger
	_, ok := logger.(*sharding.LogrumLogger)
	assert.True(t, ok, "Logger should be a LogrumLogger when UseLogrum is true")

	// Set a standard logger and verify
	sharding.SetConfig(sharding.DefaultConfig()) // Default config without logrum
	logger = sharding.GetLogger()
	_, ok = logger.(*sharding.StandardLogger)
	assert.True(t, ok, "Logger should be a StandardLogger when UseLogrum is false")
}
