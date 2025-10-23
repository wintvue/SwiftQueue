package main

import (
// "encoding/binary"
// "encoding/hex"
// "fmt"
)

func BuildFetchResponse(baseReq *SwiftQueueRequest, req *FetchRequest) []byte {
	rb := NewResponseBuilder()

	// Correlation ID
	rb.WriteInt32(baseReq.CorrelationID)

	// Error code
	errorCode := ErrorCodeNone
	if baseReq.APIVersion < FetchMinVersion || baseReq.APIVersion > FetchMaxVersion {
		errorCode = ErrorCodeUnsupportedVersion
	}
	rb.WriteInt16(int16(errorCode))

	rb.WriteInt32(0)
	rb.WriteUInt8(0)
	rb.WriteInt32(req.SessionID)
	rb.WriteUInt8(1)
	rb.WriteInt32(0)

	// rb.WriteInt32(req.MaxWaitMs)
	// rb.WriteUInt8(uint8(req.IsolationLevel))
	// rb.WriteInt32(req.SessionID)
	// rb.WriteInt32(req.SessionEpoch)

	// Error code
	// rb.WriteInt16(int16(ErrorCodeNone))

	rb.PrependMessageSize()

	return rb.Bytes()
}
