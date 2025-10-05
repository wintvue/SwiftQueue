package main

import (
	"encoding/binary"
	"fmt"
	"net"
	"os"
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit

// buildApiVersionsResponse creates a Kafka ApiVersions response
func buildApiVersionsResponse(correlationID int32, apiVersion int16) []byte {
	headerSize := 4 
	errorCodeSize := 2   
	apiVersionArraySize := 22
	throttleTimeSize := 4
	tagBufferSize := 1
	messageSizeValue := int32(headerSize + errorCodeSize + apiVersionArraySize + throttleTimeSize + tagBufferSize)
	totalResponseSize := 4 + messageSizeValue 

	responseBytes := make([]byte, totalResponseSize)

	// 1. Message size (4 bytes)
	binary.BigEndian.PutUint32(responseBytes[0:4], uint32(messageSizeValue))
	
	// 2. Header - Correlation ID (4 bytes)
	binary.BigEndian.PutUint32(responseBytes[4:8], uint32(correlationID))

	// 3. Body - Error code (2 bytes)
	if apiVersion == 4 {
		binary.BigEndian.PutUint16(responseBytes[8:10], uint16(0)) // NO_ERROR
	} else {
		binary.BigEndian.PutUint16(responseBytes[8:10], uint16(35)) // UNSUPPORTED_VERSION
	}

	// 4. API versions array
	buildApiVersionsArray(responseBytes[10:])

	// 5. Throttle time (4 bytes) - set to 0
	binary.BigEndian.PutUint32(responseBytes[32:36], uint32(0))
	
	// 6. Tag buffer (1 byte) - empty
	responseBytes[36] = uint8(0)

	return responseBytes
}

// buildApiVersionsArray builds the supported API versions array
func buildApiVersionsArray(buffer []byte) {
	offset := 0
	
	// Array length: 4 (compact array, so +1)
	buffer[offset] = uint8(4)
	offset++

	// API 1: ApiVersions (key=18, min=0, max=4)
	binary.BigEndian.PutUint16(buffer[offset:offset+2], uint16(1))   // api_key
	binary.BigEndian.PutUint16(buffer[offset+2:offset+4], uint16(0)) // min_version
	binary.BigEndian.PutUint16(buffer[offset+4:offset+6], uint16(17)) // max_version
	buffer[offset+6] = uint8(0) // tag_buffer
	offset += 7

	// API 2: Describe Topic Partitions (key=75, min=0, max=0)
	binary.BigEndian.PutUint16(buffer[offset:offset+2], uint16(18))  // api_key
	binary.BigEndian.PutUint16(buffer[offset+2:offset+4], uint16(0)) // min_version
	binary.BigEndian.PutUint16(buffer[offset+4:offset+6], uint16(4)) // max_version
	buffer[offset+6] = uint8(0) // tag_buffer
	offset += 7

	// API 3: Fetch (key=1, min=0, max=17)
	binary.BigEndian.PutUint16(buffer[offset:offset+2], uint16(75))  // api_key
	binary.BigEndian.PutUint16(buffer[offset+2:offset+4], uint16(0)) // min_version
	binary.BigEndian.PutUint16(buffer[offset+4:offset+6], uint16(0)) // max_version
	buffer[offset+6] = uint8(0) // tag_buffer
}

// parseKafkaRequest extracts correlation ID and API version from request
func parseKafkaRequest(requestBytes []byte) (correlationID int32, apiVersion int16) {
	if len(requestBytes) >= 12 {
		apiVersion = int16(binary.BigEndian.Uint16(requestBytes[6:8]))
		correlationID = int32(binary.BigEndian.Uint32(requestBytes[8:12]))
	}
	return
}

func main() {
	fmt.Println("Logs from your program will appear here!")

	l, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Println("Failed to bind to port 9092")
		os.Exit(1)
	}
	
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			continue
		}

		// Handle each connection in a goroutine for concurrent connections
		go func(c net.Conn) {
			defer c.Close()
			fmt.Println("Client connected!")

			// Read request from client
			buffer := make([]byte, 1024)
			n, err := c.Read(buffer)
			if err != nil {
				fmt.Println("Error reading from connection:", err.Error())
				return
			}

			fmt.Printf("Received: %s\n", string(buffer[:n]))

			// Parse the Kafka request
			correlationID, apiVersion := parseKafkaRequest(buffer[:n])
			
			// Build the response
			responseBytes := buildApiVersionsResponse(correlationID, apiVersion)

			// Debug output
			fmt.Printf("  message_size: %d bytes\n", len(responseBytes)-4)
			fmt.Printf("  Header (correlation_id): %d\n", correlationID)
			fmt.Printf("  API Version: %d\n", apiVersion)
			fmt.Printf("  Total response: %v (hex: %x)\n", responseBytes, responseBytes)

			// Send response
			_, err = c.Write(responseBytes)
			if err != nil {
				fmt.Println("Error writing response:", err.Error())
				return
			}

			fmt.Printf("Response sent: %d\n", correlationID)
			fmt.Println("Response sent to client!")
		}(conn)
	}
}
