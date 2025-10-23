package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds all server configuration
type Config struct {
	Host            string
	Port            int
	ReadTimeout     time.Duration
	WriteTimeout    time.Duration
	ShutdownTimeout time.Duration
	MaxBufferSize   int
	LogDirectory    string
}

// DefaultConfig returns the default server configuration
func DefaultConfig() *Config {
	return &Config{
		Host:            "0.0.0.0",
		Port:            9092,
		ReadTimeout:     30 * time.Second,
		WriteTimeout:    30 * time.Second,
		ShutdownTimeout: 30 * time.Second,
		MaxBufferSize:   1024,
		LogDirectory:    "/tmp/kraft-combined-logs/__cluster_metadata-0/",
	}
}

// Address returns the full address string for binding
func (c *Config) Address() string {
	return fmt.Sprintf("%s:%d", c.Host, c.Port)
}

// Validate checks if the configuration is valid
func (c *Config) Validate() error {
	if c.Port < 1 || c.Port > 65535 {
		return fmt.Errorf("invalid port: %d", c.Port)
	}
	if c.MaxBufferSize < 1 {
		return fmt.Errorf("invalid max buffer size: %d", c.MaxBufferSize)
	}
	return nil
}

// LoadConfigFromFile loads configuration from a properties file
func LoadConfigFromFile(filename string) (*Config, error) {
	// Start with default config
	config := DefaultConfig()

	// Open the properties file
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to open config file: %w", err)
	}
	defer file.Close()

	// Parse the properties file
	scanner := bufio.NewScanner(file)
	lineNum := 0
	for scanner.Scan() {
		lineNum++
		line := strings.TrimSpace(scanner.Text())

		// Skip empty lines and comments
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		// Split on '=' to get key-value pairs
		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid property at line %d: %s", lineNum, line)
		}

		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])

		// Apply configuration based on key
		switch key {
		case "host":
			config.Host = value
		case "port":
			port, err := strconv.Atoi(value)
			if err != nil {
				return nil, fmt.Errorf("invalid port value at line %d: %s", lineNum, value)
			}
			config.Port = port
		case "max.buffer.size":
			size, err := strconv.Atoi(value)
			if err != nil {
				return nil, fmt.Errorf("invalid max.buffer.size value at line %d: %s", lineNum, value)
			}
			config.MaxBufferSize = size
		case "log.directory":
			config.LogDirectory = value
		// Add more properties as needed
		default:
			// Ignore unknown properties (for forward compatibility)
			fmt.Printf("Warning: unknown property '%s' at line %d\n", key, lineNum)
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading config file: %w", err)
	}

	return config, nil
}
