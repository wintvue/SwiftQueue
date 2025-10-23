package main

// Topic represents a SwiftQueue topic with its name and UUID
type Topic struct {
	Name string
	UUID string
}

// Partition represents a SwiftQueue topic partition with its metadata
type Partition struct {
	ID                    uint32
	TopicUUID             string
	ReplicaLength         int
	ReplicaID             uint32
	LengthSyncReplica     int
	InSyncReplica         uint32
	LengthRemovingReplica int
	LengthAddingReplica   int
	LeaderID              uint32
	LeaderEpoch           uint32
}
