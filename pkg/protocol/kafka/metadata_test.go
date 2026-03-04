package kafka

import (
	"context"
	"encoding/binary"
	"testing"

	"github.com/niubell/timq/pkg/broker"
	"github.com/niubell/timq/pkg/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandleApiVersions(t *testing.T) {
	handler := &RequestHandler{}
	
	response, err := handler.handleApiVersions(&RequestHeader{}, []byte{})
	require.NoError(t, err)
	require.NotEmpty(t, response)
	
	// Parse response
	// error_code(2) + api_keys_length(4) + ... + throttle_time(4)
	require.GreaterOrEqual(t, len(response), 10)
	
	// Error code should be 0
	errorCode := binary.BigEndian.Uint16(response[0:2])
	assert.Equal(t, uint16(0), errorCode)
	
	// API keys length (compact array, so actual length is value - 1)
	apiKeysLen := binary.BigEndian.Uint32(response[2:6])
	assert.Greater(t, apiKeysLen, uint32(0))
	
	// Throttle time should be 0
	throttleTime := binary.BigEndian.Uint32(response[len(response)-4:])
	assert.Equal(t, uint32(0), throttleTime)
}

func TestHandleMetadata(t *testing.T) {
	cfg := config.DefaultConfig()
	mockStorage := &mockStorageForProtocol{data: make(map[string][]byte)}
	brk := broker.NewBroker(mockStorage, cfg)
	
	// Create some topics
	ctx := context.Background()
	_ = brk.CreateTopic(ctx, "topic1", 2, 1)
	_ = brk.CreateTopic(ctx, "topic2", 1, 1)
	
	handler := &RequestHandler{broker: brk}
	response, err := handler.handleMetadata(&RequestHeader{}, []byte{})
	require.NoError(t, err)
	require.NotEmpty(t, response)
	
	// Verify response contains broker info (at minimum)
	require.GreaterOrEqual(t, len(response), 4) // at least throttle_time
}

func TestEncodeMetadataResponse(t *testing.T) {
	handler := &RequestHandler{}
	
	tests := []struct {
		name    string
		brokers []MetadataResponseBroker
		topics  []MetadataResponseTopic
	}{
		{
			name: "empty response",
			brokers: []MetadataResponseBroker{
				{NodeID: 1, Host: "localhost", Port: 9092},
			},
			topics: []MetadataResponseTopic{},
		},
		{
			name: "with topics",
			brokers: []MetadataResponseBroker{
				{NodeID: 1, Host: "localhost", Port: 9092},
			},
			topics: []MetadataResponseTopic{
				{
					ErrorCode:  0,
					Name:       "test-topic",
					IsInternal: false,
					Partitions: []MetadataResponsePartition{
						{
							ErrorCode:      0,
							PartitionIndex: 0,
							LeaderID:       1,
							LeaderEpoch:    0,
							ReplicaNodes:   []int32{1},
							ISRNodes:       []int32{1},
						},
					},
				},
			},
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			response := handler.encodeMetadataResponse(tt.brokers, tt.topics)
			require.NotEmpty(t, response)
			
			// Verify throttle time is 0
			throttleTime := binary.BigEndian.Uint32(response[0:4])
			assert.Equal(t, uint32(0), throttleTime)
		})
	}
}

// mockStorageForProtocol implements minimal storage for protocol tests
type mockStorageForProtocol struct {
	data map[string][]byte
}

func (m *mockStorageForProtocol) Put(ctx context.Context, key, value []byte) error {
	m.data[string(key)] = value
	return nil
}

func (m *mockStorageForProtocol) Get(ctx context.Context, key []byte) ([]byte, error) {
	return m.data[string(key)], nil
}

func (m *mockStorageForProtocol) Delete(ctx context.Context, key []byte) error {
	delete(m.data, string(key))
	return nil
}

func (m *mockStorageForProtocol) Scan(ctx context.Context, startKey, endKey []byte, limit int) ([][]byte, [][]byte, error) {
	return nil, nil, nil
}

func (m *mockStorageForProtocol) BatchPut(ctx context.Context, keys, values [][]byte) error {
	for i := range keys {
		m.data[string(keys[i])] = values[i]
	}
	return nil
}

func (m *mockStorageForProtocol) BatchGet(ctx context.Context, keys [][]byte) ([][]byte, error) {
	return nil, nil
}

func (m *mockStorageForProtocol) BatchDelete(ctx context.Context, keys [][]byte) error {
	return nil
}

func (m *mockStorageForProtocol) Close() error {
	return nil
}

func (m *mockStorageForProtocol) ClusterID() uint64 {
	return 1
}
