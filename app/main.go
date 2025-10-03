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

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
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

			// Read data from the client
			buffer := make([]byte, 1024)
			n, err := c.Read(buffer)
			if err != nil {
				fmt.Println("Error reading from connection:", err.Error())
				return
			}

			// Print what we received
			fmt.Printf("Received: %s\n", string(buffer[:n]))

			// Kafka Response Message Structure:
			// 1. message_size (4 bytes) - size of header + body
			// 2. Header (response header v0) - correlation_id (4 bytes)
			// 3. Body (empty for this stage)

			correlationID := int32(7) // Hard-coded correlation_id as required

			// Header: response header v0 contains only correlation_id
			headerSize := 4 // correlation_id is 4 bytes
			bodySize := 0   // empty body for this stage

			// message_size = header + body
			messageSize := int32(headerSize + bodySize)

			// Create complete response: message_size + header + body
			totalResponseSize := 4 + headerSize + bodySize // 4 bytes for message_size + header + body
			responseBytes := make([]byte, totalResponseSize)

			// 1. Encode message_size (first 4 bytes)
			binary.BigEndian.PutUint32(responseBytes[0:4], uint32(messageSize))

			// 2. Encode header - correlation_id (next 4 bytes)
			binary.BigEndian.PutUint32(responseBytes[4:8], uint32(correlationID))

			// 3. Body is empty for this stage (no additional bytes to add)

			fmt.Printf("Kafka Response Message:\n")
			fmt.Printf("  message_size: %d bytes\n", messageSize)
			fmt.Printf("  Header (correlation_id): %d\n", correlationID)
			fmt.Printf("  Body: empty\n")
			fmt.Printf("  Total response: %v (hex: %x)\n", responseBytes, responseBytes)

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
