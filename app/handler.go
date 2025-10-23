package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"time"
)

// ConnectionHandler handles individual client connections
type ConnectionHandler struct {
	conn   net.Conn
	config *Config
	logger *log.Logger
}

// NewConnectionHandler creates a new connection handler
func NewConnectionHandler(conn net.Conn, config *Config, logger *log.Logger) *ConnectionHandler {
	return &ConnectionHandler{
		conn:   conn,
		config: config,
		logger: logger,
	}
}

// Handle processes the connection lifecycle
func (h *ConnectionHandler) Handle(ctx context.Context) error {
	defer h.conn.Close()

	h.logger.Printf("New connection from %s", h.conn.RemoteAddr())

	// Create a buffer for reading requests
	buffer := make([]byte, h.config.MaxBufferSize)

	for {
		select {
		case <-ctx.Done():
			h.logger.Printf("Connection handler shutting down for %s", h.conn.RemoteAddr())
			return ctx.Err()
		default:
		}

		// Set read deadline
		if err := h.conn.SetReadDeadline(time.Now().Add(h.config.ReadTimeout)); err != nil {
			return fmt.Errorf("failed to set read deadline: %w", err)
		}

		// Read from connection
		n, err := h.conn.Read(buffer)
		if err != nil {
			if err == io.EOF {
				h.logger.Printf("Client %s closed connection", h.conn.RemoteAddr())
				return nil
			}
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				h.logger.Printf("Read timeout for %s", h.conn.RemoteAddr())
				return nil
			}
			return fmt.Errorf("error reading from connection: %w", err)
		}

		if n == 0 {
			continue
		}

		// Process the request
		response, err := h.processRequest(buffer[:n])
		if err != nil {
			h.logger.Printf("Error processing request from %s: %v", h.conn.RemoteAddr(), err)
			// Send error response or continue based on error type
			continue
		}

		// Set write deadline
		if err := h.conn.SetWriteDeadline(time.Now().Add(h.config.WriteTimeout)); err != nil {
			return fmt.Errorf("failed to set write deadline: %w", err)
		}

		// Send response
		if _, err := h.conn.Write(response); err != nil {
			return fmt.Errorf("error writing response: %w", err)
		}

		h.logger.Printf("Response sent to %s", h.conn.RemoteAddr())
	}
}

// processRequest handles a single request and returns the response
func (h *ConnectionHandler) processRequest(data []byte) ([]byte, error) {
	// Parse the base request to determine API key
	baseReq, err := ParseApiVersionRequest(data)
	if err != nil {
		return nil, fmt.Errorf("failed to parse request: %w", err)
	}

	h.logger.Printf("Request: APIKey=%d, Version=%d, CorrelationID=%d, ClientID=%s",
		baseReq.APIKey, baseReq.APIVersion, baseReq.CorrelationID, baseReq.ClientID)

	// Route based on API key
	switch baseReq.APIKey {
	case APIKeyApiVersions:
		headerResponse := BuildApiVersionsResponse(baseReq.CorrelationID, baseReq.APIVersion, APIVersionsMinVersion, APIVersionsMaxVersion)
		return headerResponse, nil
	case APIKeyFetch:
		req, err := ParseFetchRequest(data)
		if err != nil {
			return nil, fmt.Errorf("failed to parse fetch request: %w", err)
		}
		fetchResponse := BuildFetchResponse(baseReq, req)
		return fetchResponse, nil
	case APIKeyDescribeCluster:
		headerResponse := BuildApiVersionsResponse(baseReq.CorrelationID, baseReq.APIVersion, DescribeClusterMinVersion, DescribeClusterMaxVersion)
		return headerResponse, nil
	case APIKeyDescribeTopicPartitions:
		req, err := ParseDescribeTopicRequest(baseReq, data)
		if err != nil {
			return nil, fmt.Errorf("failed to parse describe topic request: %w", err)
		}
		return BuildDescribeTopicResponse(req, data, h.config), nil

	default:
		h.logger.Printf("Unsupported API key: %d", baseReq.APIKey)
		// Return minimal error response
		return []byte{0, 0, 0, 0}, nil
	}

}
