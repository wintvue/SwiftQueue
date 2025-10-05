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
			request_api_version := int16(binary.BigEndian.Uint16(requestBytes[6:8]))
			correlationID := int32(binary.BigEndian.Uint32(requestBytes[8:12]))

			headerSize := 4 
			error_code_size := 2   
			api_version_array_size := 22
			throttle_time_size := 4
			tag_buffer_size := 1
			messageSizeValue := int32(headerSize + error_code_size + api_version_array_size + throttle_time_size + tag_buffer_size)
			totalResponseSize := 4 + messageSizeValue 

			responseBytes := make([]byte, totalResponseSize)

			binary.BigEndian.PutUint32(responseBytes[0:4], uint32(messageSizeValue))
			binary.BigEndian.PutUint32(responseBytes[4:8], uint32(correlationID))

			if request_api_version == 4 {
				binary.BigEndian.PutUint16(responseBytes[8:10], uint16(0))
			} else {
				binary.BigEndian.PutUint16(responseBytes[8:10], uint16(35))
			}

			responseBytes[10] = uint8(4)
			// binary.BigEndian.PutUint8(responseBytes[10:11], uint8(4))
			binary.BigEndian.PutUint16(responseBytes[11:13], uint16(1))
			binary.BigEndian.PutUint16(responseBytes[13:15], uint16(0))
			binary.BigEndian.PutUint16(responseBytes[15:17], uint16(17))
			responseBytes[17] = uint8(0)
			// binary.BigEndian.PutUint8(responseBytes[17:18], uint8(0))

			binary.BigEndian.PutUint16(responseBytes[18:20], uint16(18))
			binary.BigEndian.PutUint16(responseBytes[20:22], uint16(0))
			binary.BigEndian.PutUint16(responseBytes[22:24], uint16(4))
			responseBytes[24] = uint8(0)
			// binary.BigEndian.PutUint8(responseBytes[24:25], uint8(0))

			binary.BigEndian.PutUint16(responseBytes[25:27], uint16(75))
			binary.BigEndian.PutUint16(responseBytes[27:29], uint16(0))
			binary.BigEndian.PutUint16(responseBytes[29:31], uint16(0))
			responseBytes[31] = uint8(0)
			// binary.BigEndian.PutUint8(responseBytes[31:32], uint8(0))

			binary.BigEndian.PutUint32(responseBytes[32:36], uint32(0))
			responseBytes[36] = uint8(0)
			// binary.BigEndian.PutUint8(responseBytes[36:37], uint8(0))


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
