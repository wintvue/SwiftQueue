package main

import (
	"fmt"
	"log"
)

// Example usage of LogReader - this shows how to use the LogReader class
func ExampleLogReader() {
	// Create a new log reader for the cluster metadata directory
	logPath := "/tmp/kraft-combined-logs/__cluster_metadata-0"
	reader, err := NewLogReader(logPath)
	if err != nil {
		log.Fatalf("Failed to create log reader: %v", err)
	}
	defer reader.Close()

	// Example 1: List all log files
	fmt.Println("=== Listing all log files ===")
	logFiles, err := reader.ListLogFiles()
	if err != nil {
		log.Fatalf("Failed to list log files: %v", err)
	}
	for _, fileName := range logFiles {
		fmt.Printf("Found log file: %s\n", fileName)
	}

	// Example 2: Get information about each log file
	fmt.Println("\n=== Log file information ===")
	for _, fileName := range logFiles {
		info, err := reader.GetLogFileInfo(fileName)
		if err != nil {
			fmt.Printf("Error getting info for %s: %v\n", fileName, err)
			continue
		}
		fmt.Printf("File: %s, Size: %d bytes, Path: %s\n",
			info.FileName, info.Size, info.FilePath)
	}

	// Example 3: Read a specific log file
	if len(logFiles) > 0 {
		fmt.Println("\n=== Reading first log file ===")
		entry, err := reader.ReadLogFile(logFiles[0])
		if err != nil {
			fmt.Printf("Failed to read log file: %v\n", err)
		} else {
			fmt.Printf("Read %d bytes from %s\n", len(entry.Data), entry.FileName)
			// Print first 100 bytes (or less if file is smaller)
			previewSize := 100
			if len(entry.Data) < previewSize {
				previewSize = len(entry.Data)
			}
			fmt.Printf("First %d bytes: %v\n", previewSize, entry.Data[:previewSize])
		}
	}

	// Example 4: Read specific bytes from a log file
	if len(logFiles) > 0 {
		fmt.Println("\n=== Reading specific bytes ===")
		data, err := reader.ReadLogFileBytes(logFiles[0], 0, 50)
		if err != nil {
			fmt.Printf("Failed to read bytes: %v\n", err)
		} else {
			fmt.Printf("Read %d bytes from offset 0: %v\n", len(data), data)
		}
	}

	// Example 5: Get total size of all logs
	fmt.Println("\n=== Total log size ===")
	totalSize, err := reader.GetTotalLogSize()
	if err != nil {
		fmt.Printf("Failed to get total size: %v\n", err)
	} else {
		fmt.Printf("Total log size: %d bytes (%.2f KB)\n", totalSize, float64(totalSize)/1024)
	}

	// Example 6: Read all log files at once
	fmt.Println("\n=== Reading all log files ===")
	allEntries, err := reader.ReadAllLogFiles()
	if err != nil {
		fmt.Printf("Failed to read all logs: %v\n", err)
	} else {
		fmt.Printf("Successfully read %d log files\n", len(allEntries))
		for _, entry := range allEntries {
			fmt.Printf("  - %s: %d bytes\n", entry.FileName, entry.Size)
		}
	}
}

// You can integrate this into your server like this:
func IntegrateWithServer(config *Config) {
	// Create log reader from config
	reader, err := NewLogReader(config.LogDirectory)
	if err != nil {
		log.Printf("Warning: Could not initialize log reader: %v", err)
		return
	}
	defer reader.Close()

	// Use the reader to access log files
	logFiles, _ := reader.ListLogFiles()
	log.Printf("Found %d log files in %s", len(logFiles), config.LogDirectory)
}
