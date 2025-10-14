package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"os"
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit

// buildResponse creates a Kafka ApiVersions response
func buildResponse(requestBytes []byte, requestOffset int, correlationID int32, apiVersion int16, topicArrayLength int, contentLength int16, content string) []byte {
	responseBytes := make([]byte, 14)

	// 1. Header - Correlation ID (4 bytes)
	binary.BigEndian.PutUint32(responseBytes[4:8], uint32(correlationID))

	// 2. Tag Buffer
	responseBytes[8] = uint8(0)

	// 3. Throttle time
	binary.BigEndian.PutUint32(responseBytes[9:13], uint32(0))

	// 4. Topic Array Length
	responseBytes[13] = uint8(topicArrayLength)

	// 5. Topic Array
	responseBytes = buildTopicResponse(requestBytes, requestOffset, topicArrayLength, responseBytes)

	// 6. Topic Authorized Response
	responseBytes = append(responseBytes, []byte{0, 0, uint8(13), uint8(248)}...)

	// 7. Tag Buffer
	responseBytes = append(responseBytes, []byte{0, uint8(255), 0}...)

	// 8. Message size (4 bytes)
	binary.BigEndian.PutUint32(responseBytes[0:4], uint32(len(responseBytes)-4))

	return responseBytes
}

// buildTopicResponse builds the supported API versions array
func buildTopicResponse(requestBytes []byte, requestOffset int, topicArrayLength int, responseBytes []byte) []byte {
	for i := 0; i < topicArrayLength-1; i++ {
		errorCode := 3

		topicNameLength := int(requestBytes[requestOffset])
		requestOffset++
		topicName := requestBytes[requestOffset : requestOffset+topicNameLength-1]

		requestOffset += topicNameLength - 1
		requestOffset++

		responseBytes = append(responseBytes, append([]byte{0, byte(errorCode), byte(topicNameLength)}, topicName...)...)
		responseBytes = append(responseBytes, []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}...)
		responseBytes = append(responseBytes, uint8(0))
		responseBytes = append(responseBytes, uint8(1))
	}
	return responseBytes
}

// parseQueueRequest extracts correlation ID and API version from request
func parseQueueRequest(requestBytes []byte) (correlationID int32, apiVersion int16, contentLength int16, content string, topicArrayLength int, requestOffset int) {
	requestOffset = 6

	apiVersion = int16(binary.BigEndian.Uint16(requestBytes[requestOffset : requestOffset+2]))
	requestOffset += 2

	correlationID = int32(binary.BigEndian.Uint32(requestBytes[requestOffset : requestOffset+4]))
	requestOffset += 4

	contentLength = int16(binary.BigEndian.Uint16(requestBytes[requestOffset : requestOffset+2]))
	requestOffset += 2

	content = string(requestBytes[requestOffset : requestOffset+int(contentLength)])
	requestOffset += int(contentLength)
	requestOffset++

	topicArrayLength = int(requestBytes[requestOffset])
	requestOffset++

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

			for {
				// Read request from client
				buffer := make([]byte, 1024)
				n, err := c.Read(buffer)
				if err != nil {
					if err == io.EOF {
						log.Printf("Client closed connection: %s", "peer")
					} else {
						fmt.Println("Error reading from connection:", err.Error())
					}
					c.Close()
					break
				}

				fmt.Printf("Received: %s\n", string(buffer[:n]))

				// Parse the Queue request
				correlationID, apiVersion, contentLength, content, topicArrayLength, requestOffset := parseQueueRequest(buffer[:n])

				// Build the response
				responseBytes := buildResponse(buffer[:n], requestOffset, correlationID, apiVersion, topicArrayLength, contentLength, content)

				// Debug output
				fmt.Printf("  message_size: %d bytes\n", len(responseBytes)-4)
				fmt.Printf("  Header (correlation_id): %d\n", correlationID)
				fmt.Printf("  API Version: %d\n", apiVersion)
				fmt.Printf("  Total response: %v (hex: %x)\n", responseBytes, responseBytes)

				// Send response
				_, err = c.Write(responseBytes)
				if err != nil {
					fmt.Println("Error writing response:", err.Error())
				}

				fmt.Printf("Response sent: %d\n", correlationID)
				fmt.Println("Response sent to client!")
			}
		}(conn)
	}
}
