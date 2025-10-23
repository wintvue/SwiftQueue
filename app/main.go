// Package main implements a SwiftQueue protocol server that handles basic SwiftQueue API requests.
// This server supports the following SwiftQueue APIs:
//   - ApiVersions (API Key 18): Returns the list of supported API versions
//   - DescribeTopicPartitions (API Key 1): Returns metadata about topics
//   - DescribeCluster (API Key 75): Returns cluster metadata
//
// The server is designed with production-grade patterns including:
//   - Graceful shutdown on SIGINT/SIGTERM
//   - Concurrent connection handling with proper lifecycle management
//   - Structured logging with contextual information
//   - Configuration management with validation
//   - Proper error handling throughout the request/response cycle
//   - Timeout handling for network operations
package main

import (
	"fmt"
	"log"
	"os"
)

func main() {
	// Load configuration
	var config *Config
	var err error

	// Check if a config file was provided as a command-line argument
	if len(os.Args) > 1 {
		configFile := os.Args[1]
		fmt.Printf("Loading configuration from: %s\n", configFile)
		config, err = LoadConfigFromFile(configFile)
		if err != nil {
			log.Fatalf("Failed to load config file: %v", err)
		}
	} else {
		// Use default configuration
		fmt.Println("No config file provided, using default configuration")
		config = DefaultConfig()
	}

	// Create server instance
	server, err := NewServer(config)
	if err != nil {
		log.Fatalf("Failed to create server: %v", err)
	}

	// Run the server (blocks until shutdown)
	fmt.Println("Starting SwiftQueue protocol server...")
	if err := server.Run(); err != nil {
		log.Fatalf("Server error: %v", err)
	}

	fmt.Println("Server shutdown complete")
	os.Exit(0)
}
