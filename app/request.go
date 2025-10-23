package main

import (
	"encoding/binary"
	"fmt"
)

// SwiftQueueRequest represents a parsed SwiftQueue request
type SwiftQueueRequest struct {
	MessageSize           int32
	APIKey                int16
	APIVersion            int16
	CorrelationID         int32
	ClientID              string
	TaggedFields          []byte
	Body                  []byte
	DescribeTopicRequests []*DescribeTopicRequest
	TopicArrayLength      int
	RequestOffset         int
}

// DescribeTopicRequest represents a DescribeTopicPartitions request for a single topic
type FetchRequest struct {
	MaxWaitMs      int32
	MinBytes       int32
	MaxBytes       int32
	IsolationLevel int
	SessionID      int32
	SessionEpoch   int32
}

// DescribeTopicRequest represents a DescribeTopicPartitions request for a single topic
type DescribeTopicRequest struct {
	TopicName       string
	TopicNameLength int
}

// TopicInfo holds information about a requested topic
type TopicInfo struct {
	Name string
}

// ParseApiVersionRequest parses the common header of a SwiftQueue request
func ParseApiVersionRequest(data []byte) (*SwiftQueueRequest, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("request too short: need at least 4 bytes for message size")
	}

	req := &SwiftQueueRequest{}
	offset := 0

	// Message size (4 bytes)
	req.MessageSize = int32(binary.BigEndian.Uint32(data[offset : offset+SizeInt32]))
	offset += SizeInt32

	if len(data) < int(req.MessageSize)+SizeInt32 {
		return nil, fmt.Errorf("incomplete request: expected %d bytes, got %d", req.MessageSize+SizeInt32, len(data))
	}

	// API Key (2 bytes)
	if offset+SizeInt16 > len(data) {
		return nil, fmt.Errorf("request too short: cannot read API key")
	}
	req.APIKey = int16(binary.BigEndian.Uint16(data[offset : offset+SizeInt16]))
	offset += SizeInt16

	// API Version (2 bytes)
	if offset+SizeInt16 > len(data) {
		return nil, fmt.Errorf("request too short: cannot read API version")
	}
	req.APIVersion = int16(binary.BigEndian.Uint16(data[offset : offset+SizeInt16]))
	offset += SizeInt16

	// Correlation ID (4 bytes)
	if offset+SizeInt32 > len(data) {
		return nil, fmt.Errorf("request too short: cannot read correlation ID")
	}
	req.CorrelationID = int32(binary.BigEndian.Uint32(data[offset : offset+SizeInt32]))
	offset += SizeInt32

	// Client ID (2 bytes length + string)
	if offset+SizeInt16 > len(data) {
		return nil, fmt.Errorf("request too short: cannot read client ID length")
	}
	clientIDLength := int16(binary.BigEndian.Uint16(data[offset : offset+SizeInt16]))
	offset += SizeInt16

	if clientIDLength > 0 {
		if offset+int(clientIDLength) > len(data) {
			return nil, fmt.Errorf("request too short: cannot read client ID")
		}
		req.ClientID = string(data[offset : offset+int(clientIDLength)])
		offset += int(clientIDLength)
	}

	// Tagged fields (1 byte indicating empty)
	if offset+SizeUInt8 > len(data) {
		return nil, fmt.Errorf("request too short: cannot read tagged fields")
	}
	offset++ // Skip tagged fields

	// Remaining data is the body
	req.Body = data[offset:]

	return req, nil
}

// ParseDescribeTopicRequest parses a DescribeTopicPartitions request and returns the SwiftQueueRequest with parsed topic requests
func ParseDescribeTopicRequest(baseReq *SwiftQueueRequest, data []byte) (*SwiftQueueRequest, error) {
	// Calculate offset: skip the header we already parsed
	headerSize := SizeInt32 + SizeInt16 + SizeInt16 + SizeInt32 + SizeInt16
	if len(baseReq.ClientID) > 0 {
		headerSize += len(baseReq.ClientID)
	}
	headerSize += SizeUInt8 // tagged fields

	offset := headerSize

	// Topic array length (compact array: actual length + 1)
	if offset >= len(data) {
		return nil, fmt.Errorf("request too short: cannot read topic array length")
	}
	topicArrayLength := int(data[offset])
	offset++

	requestOffset := offset
	topicRequests := make([]*DescribeTopicRequest, 0, topicArrayLength-1)

	// Parse each topic and create a separate DescribeTopicRequest for it
	for i := 0; i < topicArrayLength-1; i++ {
		topicNameLength := int(data[offset])
		offset++

		topicName := string(data[offset : offset+topicNameLength-1])

		// Create a separate DescribeTopicRequest for each topic
		topicReq := &DescribeTopicRequest{
			TopicName:       topicName,
			TopicNameLength: topicNameLength,
		}
		topicRequests = append(topicRequests, topicReq)

		offset += topicNameLength - 1
		offset++ // Skip tag buffer
	}

	// Store the parsed topic requests and metadata in the base request
	baseReq.DescribeTopicRequests = topicRequests
	baseReq.TopicArrayLength = topicArrayLength
	baseReq.RequestOffset = requestOffset

	return baseReq, nil
}

func ParseFetchRequest(data []byte) (*FetchRequest, error) {
	// Calculate offset: skip the header we already parsed
	fetchRequest := &FetchRequest{
		MaxWaitMs:      int32(binary.BigEndian.Uint32(data[0:4])),
		MinBytes:       int32(binary.BigEndian.Uint32(data[4:8])),
		MaxBytes:       int32(binary.BigEndian.Uint32(data[8:12])),
		IsolationLevel: int(data[12]),
		SessionID:      int32(binary.BigEndian.Uint32(data[13:17])),
		SessionEpoch:   int32(binary.BigEndian.Uint32(data[17:21])),
	}
	return fetchRequest, nil

}
