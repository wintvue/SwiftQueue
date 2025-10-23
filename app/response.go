package main

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
)

// ResponseBuilder handles building SwiftQueue protocol responses using the binary wire format.
// It provides a fluent API for constructing responses with proper encoding.
//
// The builder maintains an internal buffer that grows as data is written,
// and supports writing various primitive types in big-endian format as required
// by the SwiftQueue protocol specification.
type ResponseBuilder struct {
	buffer []byte
}

// NewResponseBuilder creates a new response builder
func NewResponseBuilder() *ResponseBuilder {
	return &ResponseBuilder{
		buffer: make([]byte, 0, 1024),
	}
}

// Bytes returns the built response
func (rb *ResponseBuilder) Bytes() []byte {
	return rb.buffer
}

// WriteInt32 writes a 32-bit integer
func (rb *ResponseBuilder) WriteInt32(val int32) {
	buf := make([]byte, SizeInt32)
	binary.BigEndian.PutUint32(buf, uint32(val))
	rb.buffer = append(rb.buffer, buf...)
}

// WriteUInt32 writes an unsigned 32-bit integer
func (rb *ResponseBuilder) WriteUInt32(val uint32) {
	buf := make([]byte, SizeUInt32)
	binary.BigEndian.PutUint32(buf, val)
	rb.buffer = append(rb.buffer, buf...)
}

// WriteInt16 writes a 16-bit integer
func (rb *ResponseBuilder) WriteInt16(val int16) {
	buf := make([]byte, SizeInt16)
	binary.BigEndian.PutUint16(buf, uint16(val))
	rb.buffer = append(rb.buffer, buf...)
}

// WriteUInt16 writes an unsigned 16-bit integer
func (rb *ResponseBuilder) WriteUInt16(val uint16) {
	buf := make([]byte, SizeUInt16)
	binary.BigEndian.PutUint16(buf, val)
	rb.buffer = append(rb.buffer, buf...)
}

// WriteUInt8 writes an 8-bit unsigned integer
func (rb *ResponseBuilder) WriteUInt8(val uint8) {
	rb.buffer = append(rb.buffer, val)
}

// WriteBytes appends raw bytes
func (rb *ResponseBuilder) WriteBytes(data []byte) {
	rb.buffer = append(rb.buffer, data...)
}

// WriteString writes a compact string (length + content)
func (rb *ResponseBuilder) WriteString(s string) {
	rb.WriteUInt8(uint8(len(s) + 1))
	rb.WriteBytes([]byte(s))
}

// PrependMessageSize prepends the message size to the beginning
func (rb *ResponseBuilder) PrependMessageSize() {
	messageSize := len(rb.buffer)
	sizeBuf := make([]byte, SizeInt32)
	binary.BigEndian.PutUint32(sizeBuf, uint32(messageSize))
	rb.buffer = append(sizeBuf, rb.buffer...)
}

// BuildApiVersionsResponse creates a response for ApiVersions request
func BuildApiVersionsResponse(correlationID int32, apiVersion int16, minVersion int16, maxVersion int16) []byte {
	rb := NewResponseBuilder()

	// Reserve space for message size (will be prepended later)
	// Correlation ID
	rb.WriteInt32(correlationID)

	// Error code
	errorCode := ErrorCodeNone
	if apiVersion < minVersion || apiVersion > maxVersion {
		errorCode = ErrorCodeUnsupportedVersion
	}
	rb.WriteInt16(int16(errorCode))

	// API versions array (compact array)
	supportedAPIs := SupportedAPIs()
	rb.WriteUInt8(uint8(len(supportedAPIs) + 1)) // compact array length

	for _, api := range supportedAPIs {
		rb.WriteUInt16(api.APIKey)
		rb.WriteUInt16(api.MinVersion)
		rb.WriteUInt16(api.MaxVersion)
		rb.WriteUInt8(0) // tag buffer
	}

	// Throttle time (ms)
	rb.WriteUInt32(0)

	// Tag buffer
	rb.WriteUInt8(0)

	rb.PrependMessageSize()

	return rb.Bytes()
}

// BuildDescribeTopicResponse creates a response for DescribeTopicPartitions request
func BuildDescribeTopicResponse(req *SwiftQueueRequest, requestBytes []byte, config *Config) []byte {
	rb := NewResponseBuilder()

	if len(req.DescribeTopicRequests) == 0 {
		return rb.Bytes()
	}

	// Correlation ID
	rb.WriteInt32(req.CorrelationID)

	// Tag Buffer
	rb.WriteUInt8(0)

	// Throttle time
	rb.WriteUInt32(0)

	// Topic array length (compact array)
	rb.WriteUInt8(uint8(req.TopicArrayLength))

	// Build topic responses
	buildTopicArray(rb, requestBytes, req, config)

	// Cursor (next continuation point) - indicates no more data
	rb.WriteUInt8(CursorNoMoreData)
	// rb.WriteUInt8(uint8(-1))

	// Final tag buffer
	rb.WriteUInt8(0)

	// Prepend message size
	rb.PrependMessageSize()

	return rb.Bytes()
}

// buildTopicArray builds the topic array portion of the response
func buildTopicArray(rb *ResponseBuilder, requestBytes []byte, req *SwiftQueueRequest, config *Config) {
	topics, partitions, _ := getMetadataFromConfig(config)

	describeTopicRequest := req.DescribeTopicRequests
	for i := 0; i < req.TopicArrayLength-1; i++ {
		// Parse topic name from request
		topicName := []byte(describeTopicRequest[i].TopicName)
		topicNameLength := describeTopicRequest[i].TopicNameLength

		// Find matching topic
		matchedTopic := findTopicByName(topics, string(topicName))

		if matchedTopic != nil {
			buildTopicResponse(rb, topicName, topicNameLength, matchedTopic, partitions, config)
		} else {
			buildTopicNotFoundResponse(rb, topicName, topicNameLength)
		}
	}
}

// findTopicByName searches for a topic by name and returns it if found
func findTopicByName(topics []Topic, name string) *Topic {
	for i := range topics {
		if topics[i].Name == name {
			return &topics[i]
		}
	}
	return nil
}

// buildTopicResponse builds a response for a found topic
func buildTopicResponse(rb *ResponseBuilder, topicName []byte, topicNameLength int, topic *Topic, partitions []Partition, config *Config) {
	// Error code (NONE - success)
	rb.WriteUInt16(ErrorCodeNone)

	// Topic name
	rb.WriteUInt8(byte(topicNameLength))
	rb.WriteBytes(topicName)

	// Topic UUID
	topicUUID, _ := hex.DecodeString(topic.UUID)
	rb.WriteBytes(topicUUID)

	// Partition authorized operations (unused)
	rb.WriteUInt8(PartitionUnusedOperations)

	// Build partition array for this topic
	buildPartitionArray(rb, partitions, config, topic)

	rb.WriteUInt32(TopicAuthorizedOperations)
	rb.WriteUInt8(0)
}

// buildTopicNotFoundResponse builds a response for a topic that wasn't found
func buildTopicNotFoundResponse(rb *ResponseBuilder, topicName []byte, topicNameLength int) {
	// Error code (UNKNOWN_TOPIC_OR_PARTITION)
	rb.WriteUInt16(uint16(ErrorCodeUnknownTopicOrPart))

	// Topic name
	rb.WriteUInt8(byte(topicNameLength))
	rb.WriteBytes(topicName)

	// Topic ID (16 bytes of zeros for unknown topic)
	for j := 0; j < UUIDSize; j++ {
		rb.WriteUInt8(0)
	}

	// Partition authorized operations (unused)
	rb.WriteUInt8(PartitionUnusedOperations)

	// Tag buffer
	rb.WriteUInt8(1)
	rb.WriteUInt32(TopicAuthorizedOperations)
	rb.WriteUInt8(0)
}

// buildPartitionArray builds the partition array for a specific topic
func buildPartitionArray(rb *ResponseBuilder, partitions []Partition, config *Config, topic *Topic) {
	// Filter partitions that belong to this topic
	topicPartitions := filterPartitionsByTopicUUID(partitions, topic.UUID)
	// Write partition count (compact array encoding)
	rb.WriteUInt8(uint8(len(topicPartitions) + 1))

	// Write each partition
	for _, partition := range topicPartitions {
		buildPartitionResponse(rb, &partition)
	}
}

// filterPartitionsByTopicUUID returns only partitions that belong to the given topic
func filterPartitionsByTopicUUID(partitions []Partition, topicUUID string) []Partition {
	filtered := make([]Partition, 0)
	for _, partition := range partitions {
		if partition.TopicUUID == topicUUID {
			filtered = append(filtered, partition)
		}
	}
	return filtered
}

// buildPartitionResponse builds the response for a single partition
func buildPartitionResponse(rb *ResponseBuilder, partition *Partition) {
	// Error code (NONE - success)
	rb.WriteUInt16(ErrorCodeNone)

	// Partition metadata
	rb.WriteUInt32(partition.ID)
	rb.WriteUInt32(partition.LeaderID)
	rb.WriteUInt32(partition.LeaderEpoch)

	// Replica information
	rb.WriteUInt8(uint8(partition.ReplicaLength))
	rb.WriteUInt32(partition.ReplicaID)

	// In-sync replica information
	rb.WriteUInt8(uint8(partition.LengthSyncReplica))
	rb.WriteUInt32(partition.InSyncReplica)

	// Eligible leader replicas
	rb.WriteUInt8(EligibleLeaderReplicasCount)

	// Last known LSR
	rb.WriteUInt8(LastKnownLSRCount)
	rb.WriteUInt8(LastKnownLSRCount)

	// Tag buffer
	rb.WriteUInt8(0)
}

// getMetadataFromConfig retrieves topics and partitions using the metadata service
func getMetadataFromConfig(config *Config) ([]Topic, []Partition, error) {
	metadataService, err := NewMetadataService(config)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create metadata service: %w", err)
	}
	defer metadataService.Close()

	topics, partitions, err := metadataService.GetTopicsAndPartitions()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get topics and partitions: %w", err)
	}
	return topics, partitions, nil
}
