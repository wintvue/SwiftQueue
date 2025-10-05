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

			fmt.Printf("Received: %s\n", string(buffer[:n]))

			requestBytes := buffer[:n]
			// request_api_key := int32(binary.BigEndian.Uint32(requestBytes[4:6]))
			request_api_version := int16(binary.BigEndian.Uint16(requestBytes[6:8]))
			correlationID := int32(binary.BigEndian.Uint32(requestBytes[8:12]))
			// client_id := string(binary.BigEndian.string(requestBytes[12:28]))

			headerSize := 4 
			bodySize := 0
			response_api_version_size := 2   
			messageSizeValue := int32(headerSize + bodySize + response_api_version_size)
			totalResponseSize := 4 + messageSizeValue 

			responseBytes := make([]byte, totalResponseSize)

			binary.BigEndian.PutUint32(responseBytes[0:4], uint32(messageSizeValue))
			binary.BigEndian.PutUint32(responseBytes[4:8], uint32(correlationID))

			if request_api_version == 4 {
				binary.BigEndian.PutUint16(responseBytes[8:10], uint16(0))
			} else {
				binary.BigEndian.PutUint16(responseBytes[8:10], uint16(35))
			}

			fmt.Printf("  message_size: %d bytes\n", messageSizeValue)
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
