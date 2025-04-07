package test

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"gorm.io/sharding"
)

// Setup and teardown helper
func resetConfig() {
	// Reset to default configuration
	defaultConfig := sharding.DefaultConfig()
	sharding.SetConfig(defaultConfig)
}

func TestDefaultConfig(t *testing.T) {
	// Reset config before test
	resetConfig()

	// Get the default configuration
	config := sharding.DefaultConfig()

	// Verify default values
	assert.Equal(t, sharding.LogLevelInfo, config.Logging.Level)
	assert.True(t, config.Logging.ShowTime)
	assert.Equal(t, "text", config.Logging.Format)
	assert.Equal(t, "stdout", config.Logging.Output)
}

func TestLoadConfigFromFile(t *testing.T) {
	// Reset config before test
	resetConfig()

	// Create a temporary config file
	dir, err := ioutil.TempDir("", "sharding-test")
	assert.NoError(t, err)
	defer os.RemoveAll(dir)

	configPath := filepath.Join(dir, "sharding.yml")

	// Write a valid YAML config
	configContent := []byte(`
logging:
  level: 2
  show_time: true
  format: text
  output: stdout
`)

	err = ioutil.WriteFile(configPath, configContent, 0644)
	assert.NoError(t, err)

	// Load the config file
	err = sharding.LoadConfigFromFile(configPath)
	assert.NoError(t, err)

	// Verify loaded config values
	config := sharding.GetConfig()
	assert.Equal(t, sharding.LogLevelDebug, config.Logging.Level)
	assert.True(t, config.Logging.ShowTime)
	assert.Equal(t, "text", config.Logging.Format)
	assert.Equal(t, "stdout", config.Logging.Output)
}

func TestLoadInvalidConfigFile(t *testing.T) {
	// Reset config before test
	resetConfig()

	// Create a temporary config file with invalid YAML
	dir, err := ioutil.TempDir("", "sharding-test")
	assert.NoError(t, err)
	defer os.RemoveAll(dir)

	configPath := filepath.Join(dir, "sharding.yml")

	// Write invalid YAML config
	configContent := []byte(`
logging:
  level: not-a-number
  show_time: "invalid"
  format: 123  # Should be a string
}  # Unbalanced bracket
`)

	err = ioutil.WriteFile(configPath, configContent, 0644)
	assert.NoError(t, err)

	// Load the config file - should return an error
	err = sharding.LoadConfigFromFile(configPath)
	assert.Error(t, err)
}

func TestLoadNonExistentFile(t *testing.T) {
	// Reset config before test
	resetConfig()

	// Attempt to load a non-existent file
	err := sharding.LoadConfigFromFile("does-not-exist.yml")
	assert.Error(t, err)

	// Ensure defaults are used when no file is specified
	err = sharding.LoadConfigFromFile("")
	assert.NoError(t, err)

	// Verify default values were used
	config := sharding.GetConfig()
	assert.Equal(t, sharding.LogLevelInfo, config.Logging.Level)
}
