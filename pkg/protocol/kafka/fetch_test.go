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

func TestHandleFetch(t *testing.T) {
	cfg := config.DefaultConfig()
	mockStorage := &mockStorageForProtocol{data: make(map[string][]byte)}
	brk := broker.NewBroker(mockStorage, cfg)
	
	// Create topic and produce message
	ctx := context.Background()
	_ = brk.CreateTopic(ctx, "test-topic", 1, 1)
	msgs := []*broker.Message{{Key: []byte("k"), Value: []byte("v")}}
	_, _ = brk.Produce(ctx, "test-topic", 0, msgs)
	
	handler := &RequestHandler{broker: brk}
	
	// Build fetch request
	body := buildFetchRequest("test-topic", 0, 0, 1000)
	
	response, err := handler.handleFetch(&RequestHeader{}, body)
	require.NoError(t, err)
	require.NotEmpty(t, response)
}

func TestHandleFetch_EmptyBody(t *testing.T) {
	handler := &RequestHandler{}
	
	response, err := handler.handleFetch(&RequestHeader{}, []byte{})
	require.NoError(t, err)
	require.NotEmpty(t, response)
}

func TestEncodeRecords(t *testing.T) {
	handler := &RequestHandler{}
	
	tests := []struct {
		name     string
		messages []*broker.Message
		expected []byte
	}{
		{
			name:     "nil messages",
			messages: nil,
			expected: nil,
		},
		{
			name:     "empty messages",
			messages: []*broker.Message{},
			expected: nil,
		},
		{
			name: "single message",
			messages: []*broker.Message{
				{Value: []byte("test-data")},
			},
			expected: []byte("test-data"),
		},
		{
			name: "multiple messages - returns first",
			messages: []*broker.Message{
				{Value: []byte("first")},
				{Value: []byte("second")},
			},
			expected: []byte("first"),
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := handler.encodeRecords(tt.messages)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestEncodeFetchResponse(t *testing.T) {
	handler := &RequestHandler{}
	
	tests := []struct {
		name         string
		topics       []fetchTopicResponse
		throttleTime int32
		errorCode    int16
	}{
		{
			name:         "empty response",
			topics:       []fetchTopicResponse{},
			throttleTime: 0,
			errorCode:    0,
		},
		{
			name: "with topics",
			topics: []fetchTopicResponse{
				{
					Name: "test-topic",
					Partitions: []fetchPartitionResponse{
						{
							PartitionIndex:       0,
							ErrorCode:            0,
							HighWatermark:        100,
							LastStableOffset:     100,
							LogStartOffset:       0,
							PreferredReadReplica: 1,
							Records:              []byte("test-data"),
						},
					},
				},
			},
			throttleTime: 0,
			errorCode:    0,
		},
		{
			name: "with error",
			topics: []fetchTopicResponse{
				{
					Name: "test-topic",
					Partitions: []fetchPartitionResponse{
						{
							PartitionIndex:       0,
							ErrorCode:            2, // UNKNOWN_TOPIC_OR_PARTITION
							HighWatermark:        0,
							LastStableOffset:     0,
							LogStartOffset:       0,
							PreferredReadReplica: 1,
							Records:              nil,
						},
					},
				},
			},
			throttleTime: 0,
			errorCode:    0,
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			response, err := handler.encodeFetchResponse(tt.topics, tt.throttleTime, tt.errorCode)
			require.NoError(t, err)
			require.NotEmpty(t, response)
			
			// Verify throttle time
			throttle := binary.BigEndian.Uint32(response[0:4])
			assert.Equal(t, uint32(tt.throttleTime), throttle)
			
			// Verify error code
			errCode := binary.BigEndian.Uint16(response[4:6])
			assert.Equal(t, uint16(tt.errorCode), errCode)
		})
	}
}

func TestHandleListOffsets(t *testing.T) {
	cfg := config.DefaultConfig()
	mockStorage := &mockStorageForProtocol{data: make(map[string][]byte)}
	brk := broker.NewBroker(mockStorage, cfg)
	
	// Create topic and produce messages
	ctx := context.Background()
	_ = brk.CreateTopic(ctx, "test-topic", 1, 1)
	msgs := []*broker.Message{
		{Key: []byte("k1"), Value: []byte("v1")},
		{Key: []byte("k2"), Value: []byte("v2")},
	}
	_, _ = brk.Produce(ctx, "test-topic", 0, msgs)
	
	handler := &RequestHandler{broker: brk}
	
	// Build list offsets request
	body := buildListOffsetsRequest("test-topic", 0, -1) // latest
	
	response, err := handler.handleListOffsets(&RequestHeader{}, body)
	require.NoError(t, err)
	require.NotEmpty(t, response)
}

func TestHandleListOffsets_EmptyBody(t *testing.T) {
	handler := &RequestHandler{}
	
	_, err := handler.handleListOffsets(&RequestHeader{}, []byte{})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "too short")
}

func TestEncodeListOffsetsResponse(t *testing.T) {
	handler := &RequestHandler{}
	
	tests := []struct {
		name         string
		topics       []listOffsetsTopicResponse
		throttleTime int32
	}{
		{
			name:         "empty response",
			topics:       []listOffsetsTopicResponse{},
			throttleTime: 0,
		},
		{
			name: "with offsets",
			topics: []listOffsetsTopicResponse{
				{
					Name: "test-topic",
					Partitions: []listOffsetsPartitionResponse{
						{
							PartitionIndex: 0,
							ErrorCode:      0,
							Timestamp:      1234567890,
							Offset:         100,
						},
					},
				},
			},
			throttleTime: 0,
		},
		{
			name: "with error",
			topics: []listOffsetsTopicResponse{
				{
					Name: "test-topic",
					Partitions: []listOffsetsPartitionResponse{
						{
							PartitionIndex: 0,
							ErrorCode:      2,
							Timestamp:      -1,
							Offset:         -1,
						},
					},
				},
			},
			throttleTime: 100,
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			response, err := handler.encodeListOffsetsResponse(tt.topics, tt.throttleTime)
			require.NoError(t, err)
			require.NotEmpty(t, response)
			
			// Verify throttle time
			throttle := binary.BigEndian.Uint32(response[0:4])
			assert.Equal(t, uint32(tt.throttleTime), throttle)
		})
	}
}

func buildFetchRequest(topic string, partition int32, offset int64, maxBytes int32) []byte {
	buf := &bytes.Buffer{}
	
	// Replica ID
	binary.Write(buf, binary.BigEndian, int32(-1))
	
	// Max wait ms
	binary.Write(buf, binary.BigEndian, uint32(5000))
	
	// Min bytes
	binary.Write(buf, binary.BigEndian, uint32(1))
	
	// Max bytes
	binary.Write(buf, binary.BigEndian, uint32(maxBytes))
	
	// Isolation level
	buf.WriteByte(0)
	
	// Session ID
	binary.Write(buf, binary.BigEndian, uint32(0))
	
	// Session epoch
	binary.Write(buf, binary.BigEndian, int32(-1))
	
	// Topic array length (compact)
	binary.Write(buf, binary.BigEndian, uint32(2))
	
	// Topic name
	binary.Write(buf, binary.BigEndian, uint16(len(topic)))
	buf.WriteString(topic)
	
	// Partition array length (compact)
	binary.Write(buf, binary.BigEndian, uint32(2))
	
	// Partition index
	binary.Write(buf, binary.BigEndian, partition)
	
	// Current leader epoch
	binary.Write(buf, binary.BigEndian, int32(-1))
	
	// Fetch offset
	binary.Write(buf, binary.BigEndian, offset)
	
	// Last fetched epoch
	binary.Write(buf, binary.BigEndian, int32(-1))
	
	// Log start offset
	binary.Write(buf, binary.BigEndian, int64(-1))
	
	// Partition max bytes
	binary.Write(buf, binary.BigEndian, uint32(maxBytes))
	
	return buf.Bytes()
}

func buildListOffsetsRequest(topic string, partition int32, timestamp int64) []byte {
	buf := &bytes.Buffer{}
	
	// Replica ID
	binary.Write(buf, binary.BigEndian, int32(-1))
	
	// Isolation level
	buf.WriteByte(0)
	
	// Topic array length (compact)
	binary.Write(buf, binary.BigEndian, uint32(2))
	
	// Topic name
	binary.Write(buf, binary.BigEndian, uint16(len(topic)))
	buf.WriteString(topic)
	
	// Partition array length (compact)
	binary.Write(buf, binary.BigEndian, uint32(2))
	
	// Partition index
	binary.Write(buf, binary.BigEndian, partition)
	
	// Current leader epoch
	binary.Write(buf, binary.BigEndian, int32(-1))
	
	// Timestamp
	binary.Write(buf, binary.BigEndian, timestamp)
	
	return buf.Bytes()
}
