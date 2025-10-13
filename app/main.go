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

// buildApiVersionsResponse creates a Kafka ApiVersions response
func buildApiVersionsResponse(correlationID int32, apiVersion int16, arrayLength int, contentLength int16, content string, topicLength int, topicName string) []byte {
	totalResponseSize := 100

	responseBytes := make([]byte, totalResponseSize)

	// 1. Message size (4 bytes)
	binary.BigEndian.PutUint32(responseBytes[0:4], uint32(41))

	// 2. Header - Correlation ID (4 bytes)
	binary.BigEndian.PutUint32(responseBytes[4:8], uint32(correlationID))

	responseBytes[8] = uint8(0)
	binary.BigEndian.PutUint32(responseBytes[9:13], uint32(0))
	responseBytes[13] = uint8(arrayLength)

	// 3. Body - Error code (2 bytes)
	// if topicName == "foo" {
	// 	binary.BigEndian.PutUint16(responseBytes[14:16], uint16(3))
	// } else {
	// 	binary.BigEndian.PutUint16(responseBytes[14:16], uint16(0))
	// }
	binary.BigEndian.PutUint16(responseBytes[14:16], uint16(3))

	buildTopicResponse(responseBytes[16:], topicLength, topicName)

	return responseBytes
}

// buildTopicResponse builds the supported API versions array
func buildTopicResponse(responseBytes []byte, topicLength int, topicName string) {
	offset := 0

	// Array length: 4 (compact array, so +1)
	responseBytes[offset] = byte(topicLength)
	offset++

	// API 1: ApiVersions (key=18, min=0, max=4)
	topicNameBytes := []byte(topicName)
	fmt.Println("topicNameBytes", topicLength)
	copy(responseBytes[offset:offset+topicLength-1], topicNameBytes)
	offset += topicLength - 1

	copy(responseBytes[offset:offset+16], []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0})
	offset += 16
	responseBytes[offset] = uint8(0)
	offset++
	responseBytes[offset] = uint8(1)
	offset++
	copy(responseBytes[offset:offset+4], []byte{0, 0, uint8(13), uint8(248)})
	copy(responseBytes[offset+4:offset+7], []byte{0, uint8(255), 0})
}

// parseKafkaRequest extracts correlation ID and API version from request
func parseKafkaRequest(requestBytes []byte) (correlationID int32, apiVersion int16, contentLength int16, content string, arrayLength int, topicLength int, topicName string) {
	if len(requestBytes) >= 6 {
		offset := 6

		apiVersion = int16(binary.BigEndian.Uint16(requestBytes[offset : offset+2]))
		offset += 2

		correlationID = int32(binary.BigEndian.Uint32(requestBytes[offset : offset+4]))
		offset += 4

		contentLength = int16(binary.BigEndian.Uint16(requestBytes[offset : offset+2]))
		offset += 2

		content = string(requestBytes[offset : offset+int(contentLength)])
		offset += int(contentLength)

		arrayLength = int(requestBytes[offset+1])
		offset += 2

		topicLength = int(requestBytes[offset])
		offset++

		topicName = string(requestBytes[offset : offset+topicLength-1])
		offset += topicLength - 1
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

	// conn, err := l.Accept()
	// if err != nil {
	// 	fmt.Println("Error accepting connection: ", err.Error())
	// }
	// defer conn.Close()

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

				// Parse the Kafka request
				correlationID, apiVersion, contentLength, content, arrayLength, topicLength, topicName := parseKafkaRequest(buffer[:n])

				// Build the response
				responseBytes := buildApiVersionsResponse(correlationID, apiVersion, arrayLength, contentLength, content, topicLength, topicName)

				// Debug output
				fmt.Printf("  message_size: %d bytes\n", len(responseBytes)-4)
				fmt.Printf("  Header (correlation_id): %d\n", correlationID)
				fmt.Printf("  API Version: %d\n", apiVersion)
				fmt.Printf("  Total response: %v (hex: %x)\n", responseBytes, responseBytes)

				// Send response
				// _, err = c.Write(responseBytes)
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
