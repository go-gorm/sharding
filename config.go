package sharding

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"
)

// LogConfig holds logging configuration options for the sharding package.
// It controls log verbosity, formatting, and output destination.
type LogConfig struct {
	// Level determines the verbosity of logging (0=Error, 1=Info, 2=Debug, 3=Trace)
	Level LogLevel `yaml:"level"`

	// ShowTime controls whether log entries include timestamps
	ShowTime bool `yaml:"show_time"`

	// Format specifies the log format (currently only "text" is supported)
	Format string `yaml:"format,omitempty"`

	// Output determines where logs are written: "stdout", "stderr", or a file path
	Output string `yaml:"output,omitempty"`

	// UseLogrum enables the logrum logger instead of the standard logger
	UseLogrum bool `yaml:"use_logrum"`

	// LogrumOptions contains configuration options specific to logrum
	LogrumOptions LogrumOptions `yaml:"logrum_options"`
}

// LogrumOptions holds configuration options specific to logrum.
type LogrumOptions struct {
	// AppName is the application name to include in log output
	AppName string `yaml:"app_name"`

	// IncludeCaller adds the caller information to log entries
	IncludeCaller bool `yaml:"include_caller"`

	// TimestampFormat defines the format for timestamps
	TimestampFormat string `yaml:"timestamp_format"`
}

// ShardingConfig holds all configuration settings for the sharding package.
type ShardingConfig struct {
	// Logging contains all logging-related configuration
	Logging LogConfig `yaml:"logging"`
}

// DefaultConfig provides sensible defaults for all settings.
// This is used when no configuration file is provided or when settings are missing.
func DefaultConfig() *ShardingConfig {
	return &ShardingConfig{
		Logging: LogConfig{
			Level:     LogLevelInfo, // Default to Info level for balance of verbosity and performance
			ShowTime:  true,         // Include timestamps by default
			Format:    "text",       // Only text format is currently supported
			Output:    "stdout",     // Default to stdout for easy visibility
			UseLogrum: false,        // Use standard logger by default
			LogrumOptions: LogrumOptions{
				AppName:         "gorm-sharding",
				IncludeCaller:   false,
				TimestampFormat: "2006-01-02T15:04:05.000Z07:00",
			},
		},
	}
}

var (
	// globalConfig holds the active configuration instance.
	// It's initialized with default values and updated when configuration is loaded.
	globalConfig = DefaultConfig()
)

// LoadConfigFromFile loads configuration from a YAML file.
// If path is empty, it searches in common locations for a configuration file.
// If no configuration file is found, default values are used.
func LoadConfigFromFile(path string) error {
	// Try to find config file in standard locations if not provided
	if path == "" {
		// Look in agent/etc first, then current directory, then $HOME/.config, then /etc
		possiblePaths := []string{
			"agent/etc/sharding.yml",
			"agent/etc/sharding.yaml",
			"sharding.yml",
			"sharding.yaml",
			filepath.Join(os.Getenv("HOME"), ".config", "sharding.yml"),
			"/etc/sharding.yml",
		}

		foundPath := false
		for _, p := range possiblePaths {
			if _, err := os.Stat(p); err == nil {
				path = p
				foundPath = true
				break
			}
		}

		if !foundPath {
			// No config file found, using defaults
			log.Printf("No configuration file found in standard locations, using defaults")
			return nil
		}
	}

	// Check if file exists before attempting to read
	fileInfo, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("config file not found: %s", path)
		}
		return fmt.Errorf("error accessing config file: %w", err)
	}

	// Make sure it's a regular file, not a directory
	if fileInfo.IsDir() {
		return fmt.Errorf("config path is a directory, not a file: %s", path)
	}

	// Check file permissions
	if fileInfo.Mode().Perm()&0400 == 0 {
		return fmt.Errorf("config file is not readable: %s", path)
	}

	// Read the file with explicit error handling
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return fmt.Errorf("failed to read config file: %w", err)
	}

	// Make sure we have some actual content
	if len(data) == 0 {
		return fmt.Errorf("config file is empty: %s", path)
	}

	// Parse the configuration with validation
	config := DefaultConfig()
	if err := yaml.Unmarshal(data, config); err != nil {
		return fmt.Errorf("failed to parse config file: %w", err)
	}

	// Validate config values
	if err := validateConfig(config); err != nil {
		return fmt.Errorf("invalid configuration: %w", err)
	}

	// Apply the loaded configuration
	globalConfig = config

	// Update the default log level based on config
	DefaultLogLevel = config.Logging.Level

	// Configure the logger
	configureLogger()

	log.Printf("Successfully loaded configuration from %s", path)
	return nil
}

// validateConfig ensures the loaded configuration has valid values
func validateConfig(config *ShardingConfig) error {
	// Validate log level
	if config.Logging.Level < LogLevelError || config.Logging.Level > LogLevelTrace {
		return fmt.Errorf("invalid log level: %d (must be between %d and %d)",
			config.Logging.Level, LogLevelError, LogLevelTrace)
	}

	// Validate log format
	if config.Logging.Format != "" && config.Logging.Format != "text" {
		return fmt.Errorf("unsupported log format: %s (only 'text' is supported)",
			config.Logging.Format)
	}

	// If output is a file path, check if it's writable
	if config.Logging.Output != "stdout" && config.Logging.Output != "stderr" {
		// Check if directory exists and is writable
		dir := filepath.Dir(config.Logging.Output)
		if _, err := os.Stat(dir); os.IsNotExist(err) {
			return fmt.Errorf("log file directory does not exist: %s", dir)
		}

		// Try to open file for writing
		f, err := os.OpenFile(config.Logging.Output, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
		if err != nil {
			return fmt.Errorf("cannot write to log file: %w", err)
		}
		f.Close()
	}

	return nil
}

// GetConfig returns the current configuration.
// This allows application code to check what settings are active.
func GetConfig() *ShardingConfig {
	return globalConfig
}

// SetLogLevel sets the log level programmatically.
// This is useful when log levels need to be changed at runtime.
func SetLogLevel(level LogLevel) {
	globalConfig.Logging.Level = level
	DefaultLogLevel = level
}

// SetConfig sets the entire configuration programmatically.
// This replaces all current settings with the provided configuration.
func SetConfig(config *ShardingConfig) {
	globalConfig = config
	DefaultLogLevel = config.Logging.Level
	configureLogger()
}

// configureLogger sets up the logger based on configuration settings.
// It configures the output destination and formatting options.
func configureLogger() {
	// Check if we should use logrum
	if globalConfig.Logging.UseLogrum {
		// The actual implementation will be provided in logger.go
		configureLogrumLogger(globalConfig.Logging)
	} else {
		// Use the standard logger configuration function
		configureStandardLogger(globalConfig.Logging)
	}
}

// Test helper functions for logging tests
func TestErrorLog(msg string) {
	errorLog(msg)
}

func TestInfoLog(msg string) {
	infoLog(msg)
}

func TestDebugLog(msg string) {
	debugLog(msg)
}

func TestTraceLog(msg string) {
	traceLog(msg)
}
