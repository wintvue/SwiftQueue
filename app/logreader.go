package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// LogReader handles reading log files from the cluster metadata directory
type LogReader struct {
	basePath string
}

// LogEntry represents a single log entry read from a file
type LogEntry struct {
	FileName string
	FilePath string
	Size     int64
	Data     []byte
}

// NewLogReader creates a new LogReader instance
func NewLogReader(basePath string) (*LogReader, error) {
	// Validate that the directory exists
	if _, err := os.Stat(basePath); os.IsNotExist(err) {
		return nil, fmt.Errorf("log directory does not exist: %s", basePath)
	}

	return &LogReader{
		basePath: basePath,
	}, nil
}

// GetLogDirectory returns the full path to the log directory
func (lr *LogReader) GetLogDirectory() string {
	return lr.basePath
}

// ListLogFiles returns a sorted list of all .log files in the directory
func (lr *LogReader) ListLogFiles() ([]string, error) {
	files, err := os.ReadDir(lr.basePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read directory: %w", err)
	}

	var logFiles []string
	for _, file := range files {
		if !file.IsDir() && strings.HasSuffix(file.Name(), ".log") {
			logFiles = append(logFiles, file.Name())
		}
	}

	// Sort files by name (typically log files are numbered)
	sort.Strings(logFiles)

	return logFiles, nil
}

// ReadLogFile reads a specific log file by name
func (lr *LogReader) ReadLogFile(fileName string) (*LogEntry, error) {
	filePath := filepath.Join(lr.basePath, fileName)
	// Check if file exists
	fileInfo, err := os.Stat(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to stat file %s: %w", fileName, err)
	}

	// Open the file
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %w", fileName, err)
	}
	defer file.Close()

	// Read the file contents
	data, err := io.ReadAll(file)
	if err != nil {
		return nil, fmt.Errorf("failed to read file %s: %w", fileName, err)
	}

	return &LogEntry{
		FileName: fileName,
		FilePath: filePath,
		Size:     fileInfo.Size(),
		Data:     data,
	}, nil
}

// ReadAllLogFiles reads all .log files in the directory
func (lr *LogReader) ReadAllLogFiles() ([]*LogEntry, error) {
	logFiles, err := lr.ListLogFiles()
	if err != nil {
		return nil, err
	}

	var entries []*LogEntry
	for _, fileName := range logFiles {
		entry, err := lr.ReadLogFile(fileName)
		if err != nil {
			// Log the error but continue with other files
			fmt.Printf("Warning: failed to read %s: %v\n", fileName, err)
			continue
		}
		entries = append(entries, entry)
	}

	return entries, nil
}

// ReadLogFileBytes reads a specific number of bytes from a log file
func (lr *LogReader) ReadLogFileBytes(fileName string, offset int64, length int) ([]byte, error) {
	filePath := filepath.Join(lr.basePath, fileName)

	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %w", fileName, err)
	}
	defer file.Close()

	// Seek to offset
	if _, err := file.Seek(offset, 0); err != nil {
		return nil, fmt.Errorf("failed to seek to offset %d: %w", offset, err)
	}

	// Read the specified number of bytes
	buffer := make([]byte, length)
	n, err := file.Read(buffer)
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("failed to read bytes: %w", err)
	}

	return buffer[:n], nil
}

// GetLogFileInfo returns information about a specific log file
func (lr *LogReader) GetLogFileInfo(fileName string) (*LogEntry, error) {
	filePath := filepath.Join(lr.basePath, fileName)

	fileInfo, err := os.Stat(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to stat file %s: %w", fileName, err)
	}

	return &LogEntry{
		FileName: fileName,
		FilePath: filePath,
		Size:     fileInfo.Size(),
		Data:     nil, // Don't read data for info only
	}, nil
}

// GetTotalLogSize returns the total size of all log files
func (lr *LogReader) GetTotalLogSize() (int64, error) {
	logFiles, err := lr.ListLogFiles()
	if err != nil {
		return 0, err
	}

	var totalSize int64
	for _, fileName := range logFiles {
		info, err := lr.GetLogFileInfo(fileName)
		if err != nil {
			continue
		}
		totalSize += info.Size
	}

	return totalSize, nil
}

// Close cleans up any resources (placeholder for future use)
func (lr *LogReader) Close() error {
	// If we add file handles or other resources, clean them up here
	return nil
}
