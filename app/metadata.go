package main

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
)

// Constants for metadata parsing
const (
	// Batch header offsets
	BatchHeaderSize     = 12
	BatchLengthOffset   = 8
	RecordBatchOverhead = 45

	// Record type identifiers
	RecordTypeTopic     = 2
	RecordTypePartition = 3

	// UUID size
	UUIDSize = 16
)

// MetadataService handles reading and parsing SwiftQueue cluster metadata
type MetadataService struct {
	config    *Config
	logReader *LogReader
}

// NewMetadataService creates a new metadata service instance
func NewMetadataService(config *Config) (*MetadataService, error) {
	logReader, err := NewLogReader(config.LogDirectory)
	if err != nil {
		return nil, fmt.Errorf("failed to create log reader: %w", err)
	}

	return &MetadataService{
		config:    config,
		logReader: logReader,
	}, nil
}

// Close releases resources held by the metadata service
func (ms *MetadataService) Close() error {
	if ms.logReader != nil {
		return ms.logReader.Close()
	}
	return nil
}

// GetTopicsAndPartitions reads all topics and partitions from the cluster metadata
func (ms *MetadataService) GetTopicsAndPartitions() ([]Topic, []Partition, error) {
	logFile, err := ms.logReader.ReadLogFile("00000000000000000000.log")
	fmt.Printf("Log file data: % X\n", logFile.Data)

	if err != nil {
		return nil, nil, fmt.Errorf("failed to read log file: %w", err)
	}

	topics, partitions := ms.parseMetadataLog(logFile.Data)
	return topics, partitions, nil
}

// parseMetadataLog parses the metadata log file and extracts topics and partitions
func (ms *MetadataService) parseMetadataLog(data []byte) ([]Topic, []Partition) {
	topics := make([]Topic, 0)
	partitions := make([]Partition, 0)

	offset := 0
	for offset < len(data) {
		// Check if we have enough data for batch header
		if offset+BatchHeaderSize > len(data) {
			break
		}

		// Read batch length
		batchLength := binary.BigEndian.Uint32(data[offset+BatchLengthOffset : offset+BatchHeaderSize])
		recordOffset := offset + BatchHeaderSize + RecordBatchOverhead
		// Parse records in this batch
		batchTopics, batchPartitions := ms.parseRecordBatch(data[recordOffset:])
		topics = append(topics, batchTopics...)
		partitions = append(partitions, batchPartitions...)

		// Move to next batch
		totalRecordSize := BatchHeaderSize + int(batchLength)
		if offset+totalRecordSize > len(data)-1 {
			break
		}
		offset += totalRecordSize
	}
	fmt.Printf("topics: %+v\n partitions: %+v\n", topics, partitions)

	return topics, partitions
}

// parseRecordBatch parses a single record batch
func (ms *MetadataService) parseRecordBatch(batch []byte) ([]Topic, []Partition) {
	if len(batch) < 4 {
		return nil, nil
	}

	topics := make([]Topic, 0)
	partitions := make([]Partition, 0)

	offset := 0
	recordsLength := binary.BigEndian.Uint32(batch[offset : offset+4])
	offset += 4

	recordOffset := offset
	for i := 0; i < int(recordsLength); i++ {
		if recordOffset >= len(batch) {
			break
		}

		offset = recordOffset

		// Read record length
		var perRecordLengthInt int16
		if i == 0 {
			perRecordLength := uint16(batch[offset])
			perRecordLengthInt = ZigZagDecode16(perRecordLength)
			offset += 1
			recordOffset += int(perRecordLengthInt) + 1
		} else {
			perRecordLength := uint16(batch[offset])
			perRecordLengthInt = ZigZagDecode16(perRecordLength)
			offset += 2
			recordOffset += int(perRecordLengthInt) + 2
		}

		offset += 3 // Skip attributes, timestamp delta, and offset delta

		// Parse key
		if offset >= len(batch) {
			break
		}
		keyLength := int(batch[offset])
		offset += 1
		if keyLength > 1 {
			offset += keyLength - 1
		}

		// Parse value
		if offset >= len(batch) {
			break
		}

		var valueLengthInt int
		if i == 0 {
			valueLength := uint16(batch[offset])
			valueLengthInt = int(ZigZagDecode16(valueLength))
			offset += 1
		} else {
			valueLength := uint16(batch[offset])
			valueLengthInt = int(ZigZagDecode16(valueLength))
			offset += 2
		}

		offset += 1 // Skip headers count
		typeRecord := int(batch[offset])
		offset += 2 // Skip to record data

		// Parse based on record type
		if typeRecord == int(RecordTypeTopic) {
			topic, _ := ms.parseTopicRecord(batch, offset)
			if topic != nil {
				topics = append(topics, *topic)
			}
		} else if typeRecord == RecordTypePartition {
			partition, _ := ms.parsePartitionRecord(batch, offset)
			if partition != nil {
				partitions = append(partitions, *partition)
			}
		}

		offset += valueLengthInt
	}

	return topics, partitions
}

// ZigZagDecode8 decodes a ZigZag-encoded 8-bit integer
func ZigZagDecode8(n uint8) int8 {
	return int8((n >> 1) ^ uint8(-(int8(n) & 1)))
}

func ZigZagDecode16(n uint16) int16 {
	return int16((n >> 1) ^ uint16(-(int16(n) & 1)))
}

// parseTopicRecord parses a topic record
func (ms *MetadataService) parseTopicRecord(batch []byte, offset int) (*Topic, int) {
	if offset >= len(batch) {
		return nil, offset
	}

	nameLength := int(batch[offset])
	offset += 1

	if offset+nameLength-1 >= len(batch) {
		return nil, offset
	}

	topicName := batch[offset : offset+nameLength-1]
	offset += nameLength - 1

	if offset+UUIDSize >= len(batch) {
		return nil, offset
	}
	topicUUID := batch[offset : offset+UUIDSize]
	offset += UUIDSize

	return &Topic{
		Name: string(topicName),
		UUID: hex.EncodeToString(topicUUID),
	}, offset
}

// parsePartitionRecord parses a partition record
func (ms *MetadataService) parsePartitionRecord(batch []byte, offset int) (*Partition, int) {
	if offset+1 > len(batch) {
		return nil, offset
	}

	// Parse partition ID
	if offset+4 > len(batch) {
		return nil, offset
	}
	partitionID := binary.BigEndian.Uint32(batch[offset : offset+4])
	offset += 4

	// Parse topic UUID
	if offset+UUIDSize > len(batch) {
		return nil, offset
	}
	topicUUID := batch[offset : offset+UUIDSize]
	offset += UUIDSize

	// Parse replicas
	if offset+1 > len(batch) {
		return nil, offset
	}
	lengthReplica := int(batch[offset])
	offset += 1

	if offset+4 > len(batch) {
		return nil, offset
	}
	replicaID := binary.BigEndian.Uint32(batch[offset : offset+4])
	offset += 4

	// Parse in-sync replicas
	if offset+1 > len(batch) {
		return nil, offset
	}
	lengthSyncReplica := int(batch[offset])
	offset += 1

	if offset+4 > len(batch) {
		return nil, offset
	}
	syncReplicaID := binary.BigEndian.Uint32(batch[offset : offset+4])
	offset += 4

	// Parse removing and adding replicas
	if offset+2 > len(batch) {
		return nil, offset
	}
	lengthRemovingReplica := int(batch[offset])
	offset += 1
	lengthAddingReplica := int(batch[offset])
	offset += 1

	// Parse leader info
	if offset+8 > len(batch) {
		return nil, offset
	}
	leaderID := binary.BigEndian.Uint32(batch[offset : offset+4])
	offset += 4
	leaderEpoch := binary.BigEndian.Uint32(batch[offset : offset+4])
	offset += 4

	return &Partition{
		ID:                    partitionID,
		TopicUUID:             hex.EncodeToString(topicUUID),
		ReplicaLength:         lengthReplica,
		ReplicaID:             replicaID,
		LengthSyncReplica:     lengthSyncReplica,
		InSyncReplica:         syncReplicaID,
		LengthRemovingReplica: lengthRemovingReplica,
		LengthAddingReplica:   lengthAddingReplica,
		LeaderID:              leaderID,
		LeaderEpoch:           leaderEpoch,
	}, offset
}
