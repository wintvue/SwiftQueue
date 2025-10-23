package main

import (
	"encoding/binary"
	"fmt"
	"net"
	"os"
)

var _ = net.Listen
var _ = os.Exit

func main() {
	fmt.Println("Logs!")

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

			responseValue := int32(7) // 32-bit signed integer
			responseBytes := make([]byte, 4)
			binary.BigEndian.PutUint32(responseBytes, uint32(responseValue))

			fmt.Printf("Sending bytes: %v (hex: %x)\n", responseBytes, responseBytes)

			_, err = c.Write(responseBytes)

			if err != nil {
				fmt.Println("Error writing response:", err.Error())
				return
			}

			fmt.Printf("Response sent: %d\n", responseValue)
			fmt.Println("Response sent to client!")
		}(conn)
	}

}
