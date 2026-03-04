package kafka

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/niubell/timq/pkg/broker"
	"github.com/niubell/timq/pkg/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandleProduce(t *testing.T) {
	cfg := config.DefaultConfig()
	mockStorage := &mockStorageForProtocol{data: make(map[string][]byte)}
	brk := broker.NewBroker(mockStorage, cfg)
	
	// Create a topic first
	ctx := context.Background()
	_ = brk.CreateTopic(ctx, "test-topic", 1, 1)
	
	handler := &RequestHandler{broker: brk}
	
	// Build a produce request
	body := buildProduceRequest("test-topic", 0, []byte("test-data"))
	
	response, err := handler.handleProduce(&RequestHeader{}, body)
	require.NoError(t, err)
	require.NotEmpty(t, response)
}

func TestHandleProduce_EmptyBody(t *testing.T) {
	handler := &RequestHandler{}
	
	response, err := handler.handleProduce(&RequestHeader{}, []byte{})
	require.NoError(t, err)
	require.NotEmpty(t, response)
}

func TestParseRecords(t *testing.T) {
	handler := &RequestHandler{}
	
	tests := []struct {
		name     string
		data     []byte
		expected int
	}{
		{
			name:     "valid data",
			data:     []byte("valid test data that is longer than 12 bytes"),
			expected: 1,
		},
		{
			name:     "too short",
			data:     []byte("short"),
			expected: 0,
		},
		{
			name:     "exactly 12 bytes",
			data:     make([]byte, 12),
			expected: 1,
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			messages := handler.parseRecords(tt.data)
			assert.Len(t, messages, tt.expected)
		})
	}
}

func TestEncodeProduceResponse(t *testing.T) {
	handler := &RequestHandler{}
	
	tests := []struct {
		name         string
		topics       []produceTopicResponse
		throttleTime int32
	}{
		{
			name:         "empty response",
			topics:       []produceTopicResponse{},
			throttleTime: 0,
		},
		{
			name: "with responses",
			topics: []produceTopicResponse{
				{
					Name: "test-topic",
					Partitions: []producePartitionResponse{
						{
							PartitionIndex: 0,
							ErrorCode:      0,
							BaseOffset:     100,
							LogAppendTime:  0,
							LogStartOffset: 0,
						},
					},
				},
			},
			throttleTime: 0,
		},
		{
			name: "with error",
			topics: []produceTopicResponse{
				{
					Name: "test-topic",
					Partitions: []producePartitionResponse{
						{
							PartitionIndex: 0,
							ErrorCode:      2, // UNKNOWN_TOPIC_OR_PARTITION
							BaseOffset:     0,
							LogAppendTime:  0,
							LogStartOffset: 0,
						},
					},
				},
			},
			throttleTime: 100,
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			response, err := handler.encodeProduceResponse(tt.topics, tt.throttleTime)
			require.NoError(t, err)
			require.NotEmpty(t, response)
			
			// Verify throttle time
			throttle := binary.BigEndian.Uint32(response[0:4])
			assert.Equal(t, uint32(tt.throttleTime), throttle)
		})
	}
}

func buildProduceRequest(topic string, partition int32, data []byte) []byte {
	buf := &bytes.Buffer{}
	
	// Transactional ID (null)
	binary.Write(buf, binary.BigEndian, uint16(0xFFFF))
	
	// Acks
	binary.Write(buf, binary.BigEndian, int16(1))
	
	// Timeout
	binary.Write(buf, binary.BigEndian, uint32(30000))
	
	// Topic array length (compact: 1+1=2)
	binary.Write(buf, binary.BigEndian, uint32(2))
	
	// Topic name
	binary.Write(buf, binary.BigEndian, uint16(len(topic)))
	buf.WriteString(topic)
	
	// Partition array length (compact: 1+1=2)
	binary.Write(buf, binary.BigEndian, uint32(2))
	
	// Partition index
	binary.Write(buf, binary.BigEndian, partition)
	
	// Records length
	binary.Write(buf, binary.BigEndian, uint32(len(data)))
	buf.Write(data)
	
	return buf.Bytes()
}
