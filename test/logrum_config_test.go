package test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v3"
	"gorm.io/sharding"
)

func TestConfigLogrumIntegration(t *testing.T) {
	// This test will fail initially because the logrum configuration isn't implemented yet

	// Create a temporary config file
	tempDir, err := os.MkdirTemp("", "sharding-test")
	assert.NoError(t, err)
	defer os.RemoveAll(tempDir)

	configPath := filepath.Join(tempDir, "sharding.yml")

	// Create config with logrum settings
	config := sharding.ShardingConfig{
		Logging: sharding.LogConfig{
			Level:     sharding.LogLevelDebug,
			ShowTime:  true,
			Format:    "text",
			Output:    "stdout",
			UseLogrum: true, // New setting to enable logrum
		},
	}

	// Write config to file
	configData, err := yaml.Marshal(config)
	assert.NoError(t, err)
	err = os.WriteFile(configPath, configData, 0644)
	assert.NoError(t, err)

	// Load the config file
	err = sharding.LoadConfigFromFile(configPath)
	assert.NoError(t, err)

	// The test will verify that the logrum logger is configured correctly
	// Get the current logger
	logger := sharding.GetLogger()

	// The test will fail because LogrumLogger doesn't exist yet
	logrumLogger, ok := logger.(*sharding.LogrumLogger)
	assert.True(t, ok, "Logger should be a LogrumLogger")
	assert.NotNil(t, logrumLogger)
}

func TestConfigLogrumConfigurationOptions(t *testing.T) {
	// This test will fail initially because the logrum configuration options aren't implemented yet

	// Create a temporary config file
	tempDir, err := os.MkdirTemp("", "sharding-test")
	assert.NoError(t, err)
	defer os.RemoveAll(tempDir)

	configPath := filepath.Join(tempDir, "sharding.yml")

	// Create config with logrum settings and additional options
	configYaml := `
logging:
  level: 2
  show_time: true
  format: text
  output: stdout
  use_logrum: true
  logrum_options:
    app_name: "test-app"
    include_caller: true
    timestamp_format: "2006-01-02 15:04:05"
`

	// Write config to file
	err = os.WriteFile(configPath, []byte(configYaml), 0644)
	assert.NoError(t, err)

	// Load the config file
	err = sharding.LoadConfigFromFile(configPath)
	assert.NoError(t, err)

	// Get the current config
	config := sharding.GetConfig()

	// Verify logrum options are parsed
	assert.True(t, config.Logging.UseLogrum)
	assert.Equal(t, "test-app", config.Logging.LogrumOptions.AppName)
	assert.True(t, config.Logging.LogrumOptions.IncludeCaller)
	assert.Equal(t, "2006-01-02 15:04:05", config.Logging.LogrumOptions.TimestampFormat)
}

func TestFileOutputWithLogrum(t *testing.T) {
	// This test will fail initially because the logrum file output isn't implemented yet

	// Create a temporary config file and log file
	tempDir, err := os.MkdirTemp("", "sharding-test")
	assert.NoError(t, err)
	defer os.RemoveAll(tempDir)

	configPath := filepath.Join(tempDir, "sharding.yml")
	logPath := filepath.Join(tempDir, "sharding.log")

	// Create config with logrum settings and file output
	configYaml := `
logging:
  level: 0
  show_time: true
  format: text
  output: ` + logPath + `
  use_logrum: true
`

	// Write config to file
	err = os.WriteFile(configPath, []byte(configYaml), 0644)
	assert.NoError(t, err)

	// Load the config file
	err = sharding.LoadConfigFromFile(configPath)
	assert.NoError(t, err)

	// Write a log message
	sharding.TestErrorLog("Test file output with logrum")

	// Read the log file
	logContent, err := os.ReadFile(logPath)
	assert.NoError(t, err)

	// Verify the log message was written
	assert.True(t, strings.Contains(string(logContent), "Test file output with logrum"))
}
