
How to run

1. Ensure you have `go (1.24)` installed locally
2. Run `./your_program.sh` to run the broker, which is implemented in
   `app/main.go`.

## Architecture

This SwiftQueue implementation is structured with clean separation of concerns:

### Core Components

- **`main.go`**: Application entry point with configuration loading
- **`server.go`**: TCP server with graceful shutdown and connection handling
- **`handler.go`**: Connection handler for processing individual client requests
- **`protocol.go`**: SwiftQueue protocol constants and API version definitions
- **`request.go`**: Request parsing and deserialization
- **`response.go`**: Response building and serialization
- **`metadata.go`**: Metadata service for reading topics and partitions from logs
- **`logreader.go`**: Log file reading utilities
- **`config.go`**: Configuration management with file support
- **`topic.go`**: Topic and Partition data structures

### Supported APIs

- **ApiVersions (API Key 18)**: Returns supported API versions
- **DescribeTopicPartitions (API Key 75)**: Returns topic and partition metadata

### Configuration

The server can be configured via:

1. **Default configuration**: No arguments
   ```bash
   ./your_program.sh
   ```

2. **Properties file**: Pass a properties file as argument
   ```bash
   ./your_program.sh server.properties
   ```

Properties file format:
```properties
host=0.0.0.0
port=9092
max.buffer.size=1024
log.directory=/tmp/kraft-combined-logs/__cluster_metadata-0
```

### Running the Server

```bash
# Build and run
go run app/*.go

# Or use the provided script
./your_program.sh

# With custom configuration
./your_program.sh server.properties
```

### Production Features

- ✅ Graceful shutdown on SIGINT/SIGTERM
- ✅ Concurrent connection handling
- ✅ Structured logging with context
- ✅ Configuration validation
- ✅ Proper error handling and wrapping
- ✅ Read/Write timeouts
- ✅ Clean separation of concerns
- ✅ Exported types for extensibility

### Code Quality

The codebase follows Go best practices:
- Exported fields for public API
- Comprehensive documentation
- Clean function separation
- Constants for magic numbers
- Error wrapping for traceability
- No debug prints in production code
