package sharding

import (
	"fmt"
	"io"
	"log"
	"os"
	"sync"

	"github.com/sirupsen/logrus"
)

// Logger defines the interface for logging in the sharding package.
// This allows users to plug in their own logging implementations.
type Logger interface {
	// Error logs error messages that should always be displayed
	Error(format string, args ...interface{})

	// Info logs informational messages about normal operations
	Info(format string, args ...interface{})

	// Debug logs detailed information for debugging purposes
	Debug(format string, args ...interface{})

	// Trace logs highly detailed tracing information
	Trace(format string, args ...interface{})

	// SetLevel changes the current logging level
	SetLevel(level LogLevel)

	// GetLevel returns the current logging level
	GetLevel() LogLevel
}

// StandardLogger implements the Logger interface using Go's standard log package
type StandardLogger struct {
	mutex sync.RWMutex
	level LogLevel
	log   *log.Logger
}

// NewStandardLogger creates a new StandardLogger with the specified level and output
func NewStandardLogger(level LogLevel, out io.Writer, showTime bool) *StandardLogger {
	flags := 0
	if showTime {
		flags = log.LstdFlags
	}

	return &StandardLogger{
		level: level,
		log:   log.New(out, "", flags),
	}
}

// Error logs error messages
func (l *StandardLogger) Error(format string, args ...interface{}) {
	l.log.Printf("[ERROR] "+format, args...)
}

// Info logs informational messages
func (l *StandardLogger) Info(format string, args ...interface{}) {
	l.mutex.RLock()
	defer l.mutex.RUnlock()

	if l.level >= LogLevelInfo {
		l.log.Printf("[INFO] "+format, args...)
	}
}

// Debug logs debug messages
func (l *StandardLogger) Debug(format string, args ...interface{}) {
	l.mutex.RLock()
	defer l.mutex.RUnlock()

	if l.level >= LogLevelDebug {
		l.log.Printf("[DEBUG] "+format, args...)
	}
}

// Trace logs trace messages
func (l *StandardLogger) Trace(format string, args ...interface{}) {
	l.mutex.RLock()
	defer l.mutex.RUnlock()

	if l.level >= LogLevelTrace {
		l.log.Printf("[TRACE] "+format, args...)
	}
}

// SetLevel changes the current logging level
func (l *StandardLogger) SetLevel(level LogLevel) {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	l.level = level
}

// GetLevel returns the current logging level
func (l *StandardLogger) GetLevel() LogLevel {
	l.mutex.RLock()
	defer l.mutex.RUnlock()

	return l.level
}

// MockLogrusProvider defines the interface for a mock logrus provider for testing
type MockLogrusProvider interface {
	Error(msg string, args ...interface{})
	Info(msg string, args ...interface{})
	Debug(msg string, args ...interface{})
	Trace(msg string, args ...interface{})
	SetLevel(level int)
	GetLevel() int
}

// LogrumLogger implements the Logger interface using logrus
// It provides structured logging capabilities with additional features
// like log levels, fields, and different output formats.
type LogrumLogger struct {
	mutex        sync.RWMutex
	level        LogLevel
	logrus       *logrus.Logger
	appName      string
	mockProvider MockLogrusProvider // Used for testing
}

// NewLogrumLogger creates a new LogrumLogger with default settings
// It configures a logrus logger with reasonable defaults.
func NewLogrumLogger() *LogrumLogger {
	// Create a new logrus logger
	logger := logrus.New()
	logger.SetOutput(os.Stdout)
	logger.SetLevel(logrus.InfoLevel)

	return &LogrumLogger{
		level:   LogLevelInfo,
		logrus:  logger,
		appName: "gorm-sharding",
	}
}

// NewLogrumLoggerWithProvider creates a LogrumLogger with a custom logrus provider
// This is primarily used for testing with a mock implementation.
func NewLogrumLoggerWithProvider(provider MockLogrusProvider) *LogrumLogger {
	// Create a wrapper around the provider
	return &LogrumLogger{
		level:        LogLevel(provider.GetLevel()),
		mockProvider: provider,
	}
}

// Error logs error messages
// Error logs are always output regardless of the log level.
func (l *LogrumLogger) Error(format string, args ...interface{}) {
	// Error logs are always logged
	if l.mockProvider != nil {
		l.mockProvider.Error(format, args...)
		return
	}

	// Format the message with the provided arguments
	message := fmt.Sprintf(format, args...)

	// Create fields for the log entry
	fields := logrus.Fields{}
	if l.appName != "" {
		fields["app"] = l.appName
	}

	l.logrus.WithFields(fields).Error(message)
}

// Info logs informational messages
// Info logs are only output if the log level is Info or higher.
func (l *LogrumLogger) Info(format string, args ...interface{}) {
	l.mutex.RLock()
	level := l.level
	l.mutex.RUnlock()

	if level >= LogLevelInfo {
		if l.mockProvider != nil {
			l.mockProvider.Info(format, args...)
			return
		}

		// Format the message with the provided arguments
		message := fmt.Sprintf(format, args...)

		// Create fields for the log entry
		fields := logrus.Fields{}
		if l.appName != "" {
			fields["app"] = l.appName
		}

		l.logrus.WithFields(fields).Info(message)
	}
}

// Debug logs debug messages
// Debug logs are only output if the log level is Debug or higher.
func (l *LogrumLogger) Debug(format string, args ...interface{}) {
	l.mutex.RLock()
	level := l.level
	l.mutex.RUnlock()

	if level >= LogLevelDebug {
		if l.mockProvider != nil {
			l.mockProvider.Debug(format, args...)
			return
		}

		// Format the message with the provided arguments
		message := fmt.Sprintf(format, args...)

		// Create fields for the log entry
		fields := logrus.Fields{}
		if l.appName != "" {
			fields["app"] = l.appName
		}

		l.logrus.WithFields(fields).Debug(message)
	}
}

// Trace logs trace messages
// Trace logs are only output if the log level is Trace.
func (l *LogrumLogger) Trace(format string, args ...interface{}) {
	l.mutex.RLock()
	level := l.level
	l.mutex.RUnlock()

	if level >= LogLevelTrace {
		if l.mockProvider != nil {
			l.mockProvider.Trace(format, args...)
			return
		}

		// Format the message with the provided arguments
		message := fmt.Sprintf(format, args...)

		// Create fields for the log entry
		fields := logrus.Fields{}
		if l.appName != "" {
			fields["app"] = l.appName
		}

		l.logrus.WithFields(fields).Trace(message)
	}
}

// SetLevel changes the current logging level
// It updates both the internal level tracking and the logrus logger's level.
func (l *LogrumLogger) SetLevel(level LogLevel) {
	l.mutex.Lock()
	l.level = level
	l.mutex.Unlock()

	if l.mockProvider != nil {
		l.mockProvider.SetLevel(MapToLogrumLevel(level))
		return
	}

	// Map to logrus level
	switch level {
	case LogLevelError:
		l.logrus.SetLevel(logrus.ErrorLevel)
	case LogLevelInfo:
		l.logrus.SetLevel(logrus.InfoLevel)
	case LogLevelDebug:
		l.logrus.SetLevel(logrus.DebugLevel)
	case LogLevelTrace:
		l.logrus.SetLevel(logrus.TraceLevel)
	}
}

// GetLevel returns the current logging level
func (l *LogrumLogger) GetLevel() LogLevel {
	l.mutex.RLock()
	defer l.mutex.RUnlock()

	if l.mockProvider != nil {
		return LogLevel(l.mockProvider.GetLevel())
	}

	return l.level
}

// MapToLogrumLevel maps a sharding LogLevel to a logrus level value for testing
func MapToLogrumLevel(level LogLevel) int {
	switch level {
	case LogLevelError:
		return 0 // Error level
	case LogLevelInfo:
		return 1 // Info level
	case LogLevelDebug:
		return 2 // Debug level
	case LogLevelTrace:
		return 3 // Trace level
	default:
		return 1 // Default to Info
	}
}

// global logger instance
var (
	defaultLogger Logger = NewStandardLogger(LogLevelInfo, os.Stdout, true)
	loggerMutex   sync.RWMutex
)

// GetLogger returns the current global logger
func GetLogger() Logger {
	loggerMutex.RLock()
	defer loggerMutex.RUnlock()

	return defaultLogger
}

// SetLogger sets a custom logger as the global logger
func SetLogger(logger Logger) {
	loggerMutex.Lock()
	defer loggerMutex.Unlock()

	defaultLogger = logger
}

// configureStandardLogger configures the default logger based on configuration
func configureStandardLogger(config LogConfig) {
	// Determine output destination
	var output io.Writer
	switch config.Output {
	case "stdout":
		output = os.Stdout
	case "stderr":
		output = os.Stderr
	default:
		// Assume file path
		file, err := os.OpenFile(config.Output, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to open log file %s: %v\n", config.Output, err)
			output = os.Stderr
		} else {
			output = file
		}
	}

	// Create and configure the logger
	newLogger := NewStandardLogger(config.Level, output, config.ShowTime)
	SetLogger(newLogger)
}

// configureLogrumLogger configures a logrus logger based on configuration
// It sets up the output, log level, and formatter based on the provided configuration.
func configureLogrumLogger(config LogConfig) {
	// Create a new logrus logger
	logrusLogger := logrus.New()

	// Configure output destination
	switch config.Output {
	case "stdout":
		logrusLogger.SetOutput(os.Stdout)
	case "stderr":
		logrusLogger.SetOutput(os.Stderr)
	default:
		// Assume file path
		file, err := os.OpenFile(config.Output, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to open log file %s for logrus: %v\n", config.Output, err)
			logrusLogger.SetOutput(os.Stderr)
		} else {
			logrusLogger.SetOutput(file)
		}
	}

	// Map log level
	switch config.Level {
	case LogLevelError:
		logrusLogger.SetLevel(logrus.ErrorLevel)
	case LogLevelInfo:
		logrusLogger.SetLevel(logrus.InfoLevel)
	case LogLevelDebug:
		logrusLogger.SetLevel(logrus.DebugLevel)
	case LogLevelTrace:
		logrusLogger.SetLevel(logrus.TraceLevel)
	}

	// Configure formatter
	formatter := &logrus.TextFormatter{
		DisableTimestamp: !config.ShowTime,
		FullTimestamp:    config.ShowTime,
	}

	// Apply LogrumOptions if provided
	if config.LogrumOptions.TimestampFormat != "" {
		formatter.TimestampFormat = config.LogrumOptions.TimestampFormat
	}

	logrusLogger.SetFormatter(formatter)

	// Set up report caller if needed
	if config.LogrumOptions.IncludeCaller {
		// Report the caller in logs
		logrusLogger.SetReportCaller(true)
	}

	// Create and set the LogrumLogger
	logger := &LogrumLogger{
		level:   config.Level,
		logrus:  logrusLogger,
		appName: config.LogrumOptions.AppName,
	}

	// Set as global logger
	SetLogger(logger)
}
