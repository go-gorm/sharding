package test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"gorm.io/sharding"
)

func TestLogrumLoggerImplementsLoggerInterface(t *testing.T) {
	// Check that LogrumLogger implements the Logger interface
	var _ sharding.Logger = (*sharding.LogrumLogger)(nil)
}

func TestLogrumLoggerInitialization(t *testing.T) {
	// Verify we can create a LogrumLogger
	logger := sharding.NewLogrumLogger()
	assert.NotNil(t, logger)
}

func TestLogrumLoggerLevelMapping(t *testing.T) {
	// Test mapping from sharding.LogLevel to logrus levels
	errorLevel := sharding.MapToLogrumLevel(sharding.LogLevelError)
	infoLevel := sharding.MapToLogrumLevel(sharding.LogLevelInfo)
	debugLevel := sharding.MapToLogrumLevel(sharding.LogLevelDebug)
	traceLevel := sharding.MapToLogrumLevel(sharding.LogLevelTrace)

	// Verify mappings are correct
	assert.Equal(t, 0, errorLevel) // Should match logrus error level
	assert.Equal(t, 1, infoLevel)  // Should match logrus info level
	assert.Equal(t, 2, debugLevel) // Should match logrus debug level
	assert.Equal(t, 3, traceLevel) // Should match logrus trace level
}

func TestLogrumLoggerLogMethods(t *testing.T) {
	// Create the mock logger with high enough level to capture all messages
	mockLogrum := NewMockLogrum()
	mockLogrum.SetLevel(3) // Set to trace level

	// Create the LogrumLogger with our mock
	logger := sharding.NewLogrumLoggerWithProvider(mockLogrum)

	// Test Error logging
	logger.Error("Test error message: %s", "details")
	output := mockLogrum.GetOutput()
	assert.Contains(t, output, "Test error message: details")
	assert.Contains(t, output, "[ERROR]")

	mockLogrum.Reset()

	// Test Info logging - with trace level, this should be logged
	logger.Info("Test info message: %s", "details")
	output = mockLogrum.GetOutput()
	assert.Contains(t, output, "Test info message: details")
	assert.Contains(t, output, "[INFO]")

	mockLogrum.Reset()

	// Test Debug logging - with trace level, this should be logged
	logger.Debug("Test debug message: %s", "details")
	output = mockLogrum.GetOutput()
	assert.Contains(t, output, "Test debug message: details")
	assert.Contains(t, output, "[DEBUG]")

	mockLogrum.Reset()

	// Test Trace logging - with trace level, this should be logged
	logger.Trace("Test trace message: %s", "details")
	output = mockLogrum.GetOutput()
	assert.Contains(t, output, "Test trace message: details")
	assert.Contains(t, output, "[TRACE]")
}

func TestLogrumLoggerRespectLogLevels(t *testing.T) {
	// Create the mock logger
	mockLogrum := NewMockLogrum()

	// Create the LogrumLogger with our mock
	logger := sharding.NewLogrumLoggerWithProvider(mockLogrum)

	// Set logger's level to error only
	logger.SetLevel(sharding.LogLevelError)

	// Error logs should always appear
	logger.Error("error message")
	assert.Contains(t, mockLogrum.GetOutput(), "error message")

	mockLogrum.Reset()

	// Info logs should not appear at Error level
	logger.Info("info message")
	assert.Empty(t, mockLogrum.GetOutput())

	// Set level to Info
	logger.SetLevel(sharding.LogLevelInfo)

	// Info logs should now appear
	logger.Info("info message")
	assert.Contains(t, mockLogrum.GetOutput(), "info message")

	mockLogrum.Reset()

	// Debug logs should not appear at Info level
	logger.Debug("debug message")
	assert.Empty(t, mockLogrum.GetOutput())

	// Set level to Debug
	logger.SetLevel(sharding.LogLevelDebug)

	// Debug logs should now appear
	logger.Debug("debug message")
	assert.Contains(t, mockLogrum.GetOutput(), "debug message")

	mockLogrum.Reset()

	// Trace logs should not appear at Debug level
	logger.Trace("trace message")
	assert.Empty(t, mockLogrum.GetOutput())

	// Set level to Trace
	logger.SetLevel(sharding.LogLevelTrace)

	// Trace logs should now appear
	logger.Trace("trace message")
	assert.Contains(t, mockLogrum.GetOutput(), "trace message")
}
