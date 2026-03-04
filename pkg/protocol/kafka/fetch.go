package kafka

import (
	"context"
	"encoding/binary"
	"fmt"

	"github.com/niubell/timq/pkg/broker"
)

// handleFetch handles Fetch request (API key 1)
func (h *RequestHandler) handleFetch(header *RequestHeader, body []byte) ([]byte, error) {
	if len(body) < 12 {
		return h.encodeFetchResponse(nil, 0, 0)
	}

	offset := 0

	// Replica ID
	replicaID := binary.BigEndian.Int32(body[offset : offset+4])
	_ = replicaID
	offset += 4

	// Max wait ms
	maxWait := binary.BigEndian.Uint32(body[offset : offset+4])
	_ = maxWait
	offset += 4

	// Min bytes
	minBytes := binary.BigEndian.Uint32(body[offset : offset+4])
	_ = minBytes
	offset += 4

	// Max bytes
	maxBytes := binary.BigEndian.Uint32(body[offset : offset+4])
	_ = maxBytes
	offset += 4

	// Isolation level
	isolationLevel := body[offset]
	_ = isolationLevel
	offset++

	// Session ID and epoch (for incremental fetch)
	sessionID := binary.BigEndian.Uint32(body[offset : offset+4])
	_ = sessionID
	offset += 4
	sessionEpoch := binary.BigEndian.Int32(body[offset : offset+4])
	_ = sessionEpoch
	offset += 4

	// Topic array length
	topicCount := int(binary.BigEndian.Uint32(body[offset:offset+4]) - 1)
	offset += 4

	responses := make([]fetchTopicResponse, 0, topicCount)

	for i := 0; i < topicCount; i++ {
		// Topic name
		topicLen := int(binary.BigEndian.Uint16(body[offset : offset+2]))
		offset += 2
		topicName := string(body[offset : offset+topicLen])
		offset += topicLen

		// Partition array length
		partitionCount := int(binary.BigEndian.Uint32(body[offset:offset+4]) - 1)
		offset += 4

		partitionResponses := make([]fetchPartitionResponse, 0, partitionCount)

		for j := 0; j < partitionCount; j++ {
			// Partition index
			partitionIndex := binary.BigEndian.Int32(body[offset : offset+4])
			offset += 4

			// Current leader epoch
			currentLeaderEpoch := binary.BigEndian.Int32(body[offset : offset+4])
			_ = currentLeaderEpoch
			offset += 4

			// Fetch offset
			fetchOffset := binary.BigEndian.Int64(body[offset : offset+8])
			offset += 8

			// Last fetched epoch
			lastFetchedEpoch := binary.BigEndian.Int32(body[offset : offset+4])
			_ = lastFetchedEpoch
			offset += 4

			// Log start offset
			logStartOffset := binary.BigEndian.Int64(body[offset : offset+8])
			_ = logStartOffset
			offset += 8

			// Partition max bytes
			partitionMaxBytes := binary.BigEndian.Uint32(body[offset : offset+4])
			offset += 4

			// Fetch from broker
			ctx := context.Background()
			messages, err := h.broker.Consume(ctx, topicName, partitionIndex, fetchOffset, int32(partitionMaxBytes))

			var errorCode int16 = 0
			var records []byte

			if err != nil {
				errorCode = 2 // UNKNOWN_TOPIC_OR_PARTITION
			} else {
				records = h.encodeRecords(messages)
			}

			partitionResponses = append(partitionResponses, fetchPartitionResponse{
				PartitionIndex:   partitionIndex,
				ErrorCode:        errorCode,
				HighWatermark:    fetchOffset + int64(len(messages)),
				LastStableOffset: fetchOffset + int64(len(messages)),
				LogStartOffset:   0,
				AbortedTransactions: nil,
				PreferredReadReplica: 1,
				Records:          records,
			})
		}

		responses = append(responses, fetchTopicResponse{
			Name:       topicName,
			Partitions: partitionResponses,
		})
	}

	return h.encodeFetchResponse(responses, 0, 0)
}

type fetchTopicResponse struct {
	Name       string
	Partitions []fetchPartitionResponse
}

type fetchPartitionResponse struct {
	PartitionIndex       int32
	ErrorCode            int16
	HighWatermark        int64
	LastStableOffset     int64
	LogStartOffset       int64
	AbortedTransactions  []abortedTransaction
	PreferredReadReplica int32
	Records              []byte
}

type abortedTransaction struct {
	ProducerID  int64
	FirstOffset int64
}

func (h *RequestHandler) encodeRecords(messages []*broker.Message) []byte {
	// Simplified record batch encoding
	// Real implementation would encode proper Kafka record batch format
	if len(messages) == 0 {
		return nil
	}

	// Return the value of the first message as records
	return messages[0].Value
}

func (h *RequestHandler) encodeFetchResponse(topics []fetchTopicResponse, throttleTime int32, errorCode int16) ([]byte, error) {
	size := 4 + 2 + 4 + 1 // throttle_time + error_code + session_id + tagged_fields
	size += 4             // topics array length
	for _, t := range topics {
		size += 2 + len(t.Name) + 4 // name_len + name + partitions_len
		for _, p := range t.Partitions {
			size += 2 + 4 + 8 + 8 + 8 + 4 + 4 + 4 // error_code + partition + high_watermark + last_stable_offset + log_start_offset + aborted_txns_len + preferred_replica + records_len
			size += len(p.Records)
			size += 1 // partition tagged fields
		}
		size += 1 // topic tagged fields
	}

	response := make([]byte, size)
	offset := 0

	// Throttle time
	binary.BigEndian.PutUint32(response[offset:offset+4], uint32(throttleTime))
	offset += 4

	// Error code
	binary.BigEndian.PutUint16(response[offset:offset+2], errorCode)
	offset += 2

	// Session ID
	binary.BigEndian.PutUint32(response[offset:offset+4], 0)
	offset += 4

	// Topics array
	binary.BigEndian.PutUint32(response[offset:offset+4], uint32(len(topics)+1))
	offset += 4
	for _, t := range topics {
		binary.BigEndian.PutUint16(response[offset:offset+2], uint16(len(t.Name)))
		offset += 2
		copy(response[offset:], t.Name)
		offset += len(t.Name)

		// Partitions array
		binary.BigEndian.PutUint32(response[offset:offset+4], uint32(len(t.Partitions)+1))
		offset += 4
		for _, p := range t.Partitions {
			binary.BigEndian.PutUint16(response[offset:offset+2], p.ErrorCode)
			offset += 2
			binary.BigEndian.PutUint32(response[offset:offset+4], uint32(p.PartitionIndex))
			offset += 4
			binary.BigEndian.PutUint64(response[offset:offset+8], uint64(p.HighWatermark))
			offset += 8
			binary.BigEndian.PutUint64(response[offset:offset+8], uint64(p.LastStableOffset))
			offset += 8
			binary.BigEndian.PutUint64(response[offset:offset+8], uint64(p.LogStartOffset))
			offset += 8
			binary.BigEndian.PutUint32(response[offset:offset+4], 1) // aborted transactions length
			offset += 4
			binary.BigEndian.PutUint32(response[offset:offset+4], uint32(p.PreferredReadReplica))
			offset += 4

			// Records
			if p.Records == nil {
				binary.BigEndian.PutUint32(response[offset:offset+4], 0xFFFFFFFF) // null
				offset += 4
			} else {
				binary.BigEndian.PutUint32(response[offset:offset+4], uint32(len(p.Records)))
				offset += 4
				copy(response[offset:], p.Records)
				offset += len(p.Records)
			}

			response[offset] = 0 // partition tagged fields
			offset++
		}

		response[offset] = 0 // topic tagged fields
		offset++
	}

	response[offset] = 0 // response tagged fields

	return response, nil
}

// handleListOffsets handles ListOffsets request (API key 2)
func (h *RequestHandler) handleListOffsets(header *RequestHeader, body []byte) ([]byte, error) {
	if len(body) < 8 {
		return nil, fmt.Errorf("request too short")
	}

	offset := 0

	// Replica ID
	replicaID := binary.BigEndian.Int32(body[offset : offset+4])
	_ = replicaID
	offset += 4

	// Isolation level
	isolationLevel := body[offset]
	_ = isolationLevel
	offset++

	// Topic array length
	topicCount := int(binary.BigEndian.Uint32(body[offset:offset+4]) - 1)
	offset += 4

	responses := make([]listOffsetsTopicResponse, 0, topicCount)

	for i := 0; i < topicCount; i++ {
		// Topic name
		topicLen := int(binary.BigEndian.Uint16(body[offset : offset+2]))
		offset += 2
		topicName := string(body[offset : offset+topicLen])
		offset += topicLen

		// Partition array length
		partitionCount := int(binary.BigEndian.Uint32(body[offset:offset+4]) - 1)
		offset += 4

		partitionResponses := make([]listOffsetsPartitionResponse, 0, partitionCount)

		for j := 0; j < partitionCount; j++ {
			// Partition index
			partitionIndex := binary.BigEndian.Int32(body[offset : offset+4])
			offset += 4

			// Current leader epoch
			currentLeaderEpoch := binary.BigEndian.Int32(body[offset : offset+4])
			_ = currentLeaderEpoch
			offset += 4

			// Timestamp
			timestamp := binary.BigEndian.Int64(body[offset : offset+8])
			offset += 8

			// Get offset from broker
			offsetValue, err := h.broker.GetOffset(topicName, partitionIndex, timestamp)

			var errorCode int16 = 0
			if err != nil {
				errorCode = 2 // UNKNOWN_TOPIC_OR_PARTITION
				offsetValue = -1
			}

			partitionResponses = append(partitionResponses, listOffsetsPartitionResponse{
				PartitionIndex: partitionIndex,
				ErrorCode:      errorCode,
				Timestamp:      timestamp,
				Offset:         offsetValue,
			})
		}

		responses = append(responses, listOffsetsTopicResponse{
			Name:       topicName,
			Partitions: partitionResponses,
		})
	}

	return h.encodeListOffsetsResponse(responses, 0)
}

type listOffsetsTopicResponse struct {
	Name       string
	Partitions []listOffsetsPartitionResponse
}

type listOffsetsPartitionResponse struct {
	PartitionIndex int32
	ErrorCode      int16
	Timestamp      int64
	Offset         int64
}

func (h *RequestHandler) encodeListOffsetsResponse(topics []listOffsetsTopicResponse, throttleTime int32) ([]byte, error) {
	size := 4 + 1 // throttle_time + tagged_fields
	size += 4     // topics array length
	for _, t := range topics {
		size += 2 + len(t.Name) + 4 // name_len + name + partitions_len
		for _, p := range t.Partitions {
			size += 4 + 2 + 8 + 8 + 1 // partition + error_code + timestamp + offset + tagged_fields
		}
		size += 1 // topic tagged fields
	}

	response := make([]byte, size)
	offset := 0

	// Throttle time
	binary.BigEndian.PutUint32(response[offset:offset+4], uint32(throttleTime))
	offset += 4

	// Topics array
	binary.BigEndian.PutUint32(response[offset:offset+4], uint32(len(topics)+1))
	offset += 4
	for _, t := range topics {
		binary.BigEndian.PutUint16(response[offset:offset+2], uint16(len(t.Name)))
		offset += 2
		copy(response[offset:], t.Name)
		offset += len(t.Name)

		// Partitions array
		binary.BigEndian.PutUint32(response[offset:offset+4], uint32(len(t.Partitions)+1))
		offset += 4
		for _, p := range t.Partitions {
			binary.BigEndian.PutUint32(response[offset:offset+4], uint32(p.PartitionIndex))
			offset += 4
			binary.BigEndian.PutUint16(response[offset:offset+2], p.ErrorCode)
			offset += 2
			binary.BigEndian.PutUint64(response[offset:offset+8], uint64(p.Timestamp))
			offset += 8
			binary.BigEndian.PutUint64(response[offset:offset+8], uint64(p.Offset))
			offset += 8
			response[offset] = 0 // tagged fields
			offset++
		}

		response[offset] = 0 // topic tagged fields
		offset++
	}

	response[offset] = 0 // response tagged fields

	return response, nil
}
