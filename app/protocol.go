package main

// SwiftQueue protocol constants
const (
	// API Keys
	APIKeyDescribeTopicPartitions = 75
	APIKeyFetch                   = 1
	APIKeyApiVersions             = 18
	APIKeyDescribeCluster         = 60

	// Error Codes
	ErrorCodeNone               = 0
	ErrorCodeUnknownTopicOrPart = 3
	ErrorCodeUnsupportedVersion = 35

	// Protocol sizes (in bytes)
	SizeInt16  = 2
	SizeInt32  = 4
	SizeUInt8  = 1
	SizeUInt16 = 2
	SizeUInt32 = 4

	// API Version ranges
	APIVersionsMinVersion = 0
	APIVersionsMaxVersion = 4

	FetchMinVersion = 0
	FetchMaxVersion = 16

	DescribeTopicPartitionsMinVersion = 0
	DescribeTopicPartitionsMaxVersion = 17

	DescribeClusterMinVersion = 0
	DescribeClusterMaxVersion = 0

	// Special response values
	CursorNoMoreData            = 0xFF    // 255 - indicates no more data in cursor
	TopicAuthorizedOperations   = 0x0D_F8 // Special value for topic authorized operations
	PartitionUnusedOperations   = 0       // Unused partition operations value
	EligibleLeaderReplicasCount = 1       // Default eligible leader replicas
	LastKnownLSRCount           = 1       // Default last known LSR count
)

// API version information
type APIVersion struct {
	APIKey     uint16
	MinVersion uint16
	MaxVersion uint16
}

// SupportedAPIs returns the list of supported SwiftQueue APIs
func SupportedAPIs() []APIVersion {
	return []APIVersion{
		{
			APIKey:     APIKeyDescribeTopicPartitions,
			MinVersion: DescribeTopicPartitionsMinVersion,
			MaxVersion: DescribeTopicPartitionsMaxVersion,
		},
		{
			APIKey:     APIKeyApiVersions,
			MinVersion: APIVersionsMinVersion,
			MaxVersion: APIVersionsMaxVersion,
		},
		{
			APIKey:     APIKeyDescribeCluster,
			MinVersion: DescribeClusterMinVersion,
			MaxVersion: DescribeClusterMaxVersion,
		},
		{
			APIKey:     APIKeyFetch,
			MinVersion: FetchMinVersion,
			MaxVersion: FetchMaxVersion,
		},
	}
}
