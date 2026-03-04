package kafka

import (
	"context"
	"encoding/binary"

	"github.com/niubell/timq/pkg/broker"
)

// handleProduce handles Produce request (API key 0)
func (h *RequestHandler) handleProduce(header *RequestHeader, body []byte) ([]byte, error) {
	if len(body) < 8 {
		return h.encodeProduceResponse(nil, 0)
	}

	offset := 0

	// Transactional ID (nullable string, compact)
	txnIDLen := binary.BigEndian.Uint16(body[offset : offset+2])
	offset += 2
	if txnIDLen != 0xFFFF { // not null
		offset += int(txnIDLen)
	}

	// Acks
	acks := binary.BigEndian.Int16(body[offset : offset+2])
	_ = acks // Currently we always wait for write
	offset += 2

	// Timeout
	timeout := binary.BigEndian.Uint32(body[offset : offset+4])
	_ = timeout
	offset += 4

	// Topic data array length (compact)
	topicCount := int(binary.BigEndian.Uint32(body[offset:offset+4]) - 1)
	offset += 4

	responses := make([]produceTopicResponse, 0, topicCount)

	for i := 0; i < topicCount; i++ {
		// Topic name
		topicLen := int(binary.BigEndian.Uint16(body[offset : offset+2]))
		offset += 2
		topicName := string(body[offset : offset+topicLen])
		offset += topicLen

		// Partition data array length (compact)
		partitionCount := int(binary.BigEndian.Uint32(body[offset:offset+4]) - 1)
		offset += 4

		partitionResponses := make([]producePartitionResponse, 0, partitionCount)

		for j := 0; j < partitionCount; j++ {
			// Partition index
			partitionIndex := binary.BigEndian.Int32(body[offset : offset+4])
			offset += 4

			// Records (we need to parse the record batch)
			recordsLen := int(binary.BigEndian.Uint32(body[offset : offset+4]))
			offset += 4
			recordsData := body[offset : offset+recordsLen]
			offset += recordsLen

			// Parse records and produce messages
			messages := h.parseRecords(recordsData)

			// Produce to broker
			ctx := context.Background()
			_, err := h.broker.Produce(ctx, topicName, partitionIndex, messages)

			var errorCode int16 = 0
			if err != nil {
				errorCode = 2 // UNKNOWN_TOPIC_OR_PARTITION
			}

			partitionResponses = append(partitionResponses, producePartitionResponse{
				PartitionIndex: partitionIndex,
				ErrorCode:      errorCode,
				BaseOffset:     0, // Would be set to actual offset
				LogAppendTime:  0,
				LogStartOffset: 0,
			})
		}

		responses = append(responses, produceTopicResponse{
			Name:       topicName,
			Partitions: partitionResponses,
		})
	}

	return h.encodeProduceResponse(responses, 0)
}

type produceTopicResponse struct {
	Name       string
	Partitions []producePartitionResponse
}

type producePartitionResponse struct {
	PartitionIndex int32
	ErrorCode      int16
	BaseOffset     int64
	LogAppendTime  int64
	LogStartOffset int64
}

func (h *RequestHandler) parseRecords(data []byte) []*broker.Message {
	// Simplified record batch parsing
	// Real implementation would parse Kafka record batch format
	messages := make([]*broker.Message, 0)

	if len(data) < 12 {
		return messages
	}

	// For now, create a single message from the data
	messages = append(messages, &broker.Message{
		Key:   nil,
		Value: data,
	})

	return messages
}

func (h *RequestHandler) encodeProduceResponse(topics []produceTopicResponse, throttleTime int32) ([]byte, error) {
	size := 4 + 1 // throttle_time + tagged_fields
	size += 4     // topics array length
	for _, t := range topics {
		size += 2 + len(t.Name) + 4 // name_len + name + partitions_len
		for _, p := range t.Partitions {
			size += 2 + 4 + 8 + 8 + 8 + 1 // error_code + partition + base_offset + log_append_time + log_start_offset + tagged_fields
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
			binary.BigEndian.PutUint16(response[offset:offset+2], p.ErrorCode)
			offset += 2
			binary.BigEndian.PutUint32(response[offset:offset+4], uint32(p.PartitionIndex))
			offset += 4
			binary.BigEndian.PutUint64(response[offset:offset+8], uint64(p.BaseOffset))
			offset += 8
			binary.BigEndian.PutUint64(response[offset:offset+8], uint64(p.LogAppendTime))
			offset += 8
			binary.BigEndian.PutUint64(response[offset:offset+8], uint64(p.LogStartOffset))
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
