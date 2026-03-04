package broker

import (
	"context"
	"errors"
	"testing"

	"github.com/niubell/timq/pkg/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockStorage is a test double for TiKVStorage
type mockStorage struct {
	data map[string][]byte
	batchPutFunc func(ctx context.Context, keys, values [][]byte) error
	scanFunc func(ctx context.Context, startKey, endKey []byte, limit int) ([][]byte, [][]byte, error)
}

func newMockStorage() *mockStorage {
	return &mockStorage{
		data: make(map[string][]byte),
	}
}

func (m *mockStorage) Put(ctx context.Context, key, value []byte) error {
	m.data[string(key)] = value
	return nil
}

func (m *mockStorage) Get(ctx context.Context, key []byte) ([]byte, error) {
	return m.data[string(key)], nil
}

func (m *mockStorage) Delete(ctx context.Context, key []byte) error {
	delete(m.data, string(key))
	return nil
}

func (m *mockStorage) Scan(ctx context.Context, startKey, endKey []byte, limit int) ([][]byte, [][]byte, error) {
	if m.scanFunc != nil {
		return m.scanFunc(ctx, startKey, endKey, limit)
	}
	var keys, values [][]byte
	for k, v := range m.data {
		if k >= string(startKey) && k < string(endKey) {
			keys = append(keys, []byte(k))
			values = append(values, v)
			if len(keys) >= limit {
				break
			}
		}
	}
	return keys, values, nil
}

func (m *mockStorage) BatchPut(ctx context.Context, keys, values [][]byte) error {
	if m.batchPutFunc != nil {
		return m.batchPutFunc(ctx, keys, values)
	}
	for i := range keys {
		m.data[string(keys[i])] = values[i]
	}
	return nil
}

func (m *mockStorage) BatchGet(ctx context.Context, keys [][]byte) ([][]byte, error) {
	values := make([][]byte, len(keys))
	for i, k := range keys {
		values[i] = m.data[string(k)]
	}
	return values, nil
}

func (m *mockStorage) BatchDelete(ctx context.Context, keys [][]byte) error {
	for _, k := range keys {
		delete(m.data, string(k))
	}
	return nil
}

func (m *mockStorage) Close() error {
	return nil
}

func (m *mockStorage) ClusterID() uint64 {
	return 1
}

func setupTestBroker(t *testing.T) (*Broker, *mockStorage) {
	store := newMockStorage()
	cfg := config.DefaultConfig()
	broker := NewBroker(store, cfg)
	return broker, store
}

func TestBroker_StartStop(t *testing.T) {
	b, _ := setupTestBroker(t)
	
	err := b.Start()
	assert.NoError(t, err)
	
	err = b.Stop()
	assert.NoError(t, err)
}

func TestBroker_CreateTopic(t *testing.T) {
	b, _ := setupTestBroker(t)
	ctx := context.Background()
	
	err := b.CreateTopic(ctx, "test-topic", 3, 1)
	require.NoError(t, err)
	
	// Verify topic exists
	topic, err := b.GetTopic("test-topic")
	require.NoError(t, err)
	assert.Equal(t, "test-topic", topic.Name)
	assert.Len(t, topic.Partitions, 3)
	
	// Duplicate should fail
	err = b.CreateTopic(ctx, "test-topic", 1, 1)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "already exists")
}

func TestBroker_GetTopic_NotFound(t *testing.T) {
	b, _ := setupTestBroker(t)
	
	_, err := b.GetTopic("nonexistent")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestBroker_ListTopics(t *testing.T) {
	b, _ := setupTestBroker(t)
	ctx := context.Background()
	
	// Empty initially
	topics := b.ListTopics()
	assert.Empty(t, topics)
	
	// Add topics
	_ = b.CreateTopic(ctx, "topic1", 1, 1)
	_ = b.CreateTopic(ctx, "topic2", 2, 1)
	_ = b.CreateTopic(ctx, "topic3", 3, 1)
	
	topics = b.ListTopics()
	assert.Len(t, topics, 3)
	assert.Contains(t, topics, "topic1")
	assert.Contains(t, topics, "topic2")
	assert.Contains(t, topics, "topic3")
}

func TestBroker_DeleteTopic(t *testing.T) {
	b, _ := setupTestBroker(t)
	ctx := context.Background()
	
	_ = b.CreateTopic(ctx, "to-delete", 1, 1)
	
	// Verify exists
	_, err := b.GetTopic("to-delete")
	require.NoError(t, err)
	
	// Delete
	err = b.DeleteTopic(ctx, "to-delete")
	require.NoError(t, err)
	
	// Verify deleted
	_, err = b.GetTopic("to-delete")
	assert.Error(t, err)
	
	// Delete non-existent should error
	err = b.DeleteTopic(ctx, "nonexistent")
	assert.Error(t, err)
}

func TestBroker_Produce(t *testing.T) {
	b, store := setupTestBroker(t)
	ctx := context.Background()
	
	_ = b.CreateTopic(ctx, "events", 2, 1)
	
	msgs := []*Message{
		{Key: []byte("k1"), Value: []byte("v1")},
		{Key: []byte("k2"), Value: []byte("v2")},
		{Key: []byte("k3"), Value: []byte("v3")},
	}
	
	offsets, err := b.Produce(ctx, "events", 0, msgs)
	require.NoError(t, err)
	assert.Equal(t, []int64{0, 1, 2}, offsets)
	
	// Verify messages in storage
	assert.Len(t, store.data, 3)
	
	// Produce to non-existent topic
	_, err = b.Produce(ctx, "nonexistent", 0, msgs)
	assert.Error(t, err)
	
	// Produce to invalid partition
	_, err = b.Produce(ctx, "events", 10, msgs)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid partition")
}

func TestBroker_Produce_BatchPutError(t *testing.T) {
	b, store := setupTestBroker(t)
	ctx := context.Background()
	
	_ = b.CreateTopic(ctx, "events", 1, 1)
	
	store.batchPutFunc = func(ctx context.Context, keys, values [][]byte) error {
		return errors.New("storage error")
	}
	
	msgs := []*Message{{Key: []byte("k1"), Value: []byte("v1")}}
	_, err := b.Produce(ctx, "events", 0, msgs)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "storage error")
}

func TestBroker_Consume(t *testing.T) {
	b, _ := setupTestBroker(t)
	ctx := context.Background()
	
	_ = b.CreateTopic(ctx, "events", 1, 1)
	
	// Produce messages
	msgs := []*Message{
		{Key: []byte("k1"), Value: []byte("v1")},
		{Key: []byte("k2"), Value: []byte("v2")},
		{Key: []byte("k3"), Value: []byte("v3")},
	}
	_, err := b.Produce(ctx, "events", 0, msgs)
	require.NoError(t, err)
	
	// Consume from beginning
	consumed, err := b.Consume(ctx, "events", 0, 0, 1000)
	require.NoError(t, err)
	assert.Len(t, consumed, 3)
	assert.Equal(t, []byte("v1"), consumed[0].Value)
	assert.Equal(t, []byte("v2"), consumed[1].Value)
	assert.Equal(t, []byte("v3"), consumed[2].Value)
	
	// Consume from offset 1
	consumed, err = b.Consume(ctx, "events", 0, 1, 1000)
	require.NoError(t, err)
	assert.Len(t, consumed, 2)
	assert.Equal(t, []byte("v2"), consumed[0].Value)
	
	// Consume with maxBytes limit
	consumed, err = b.Consume(ctx, "events", 0, 0, 1) // very small limit
	require.NoError(t, err)
	assert.Len(t, consumed, 1)
	
	// Consume from non-existent topic
	_, err = b.Consume(ctx, "nonexistent", 0, 0, 1000)
	assert.Error(t, err)
}

func TestBroker_Consume_ScanError(t *testing.T) {
	b, store := setupTestBroker(t)
	ctx := context.Background()
	
	_ = b.CreateTopic(ctx, "events", 1, 1)
	
	store.scanFunc = func(ctx context.Context, startKey, endKey []byte, limit int) ([][]byte, [][]byte, error) {
		return nil, nil, errors.New("scan error")
	}
	
	_, err := b.Consume(ctx, "events", 0, 0, 1000)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "scan error")
}

func TestBroker_Consume_CorruptedMessage(t *testing.T) {
	b, store := setupTestBroker(t)
	ctx := context.Background()
	
	_ = b.CreateTopic(ctx, "events", 1, 1)
	
	// Insert corrupted message directly
	key := "/timq/topics/events/partitions/0/messages/00000000000000000000"
	store.data[key] = []byte("corrupted")
	
	// Should skip corrupted message without error
	consumed, err := b.Consume(ctx, "events", 0, 0, 1000)
	require.NoError(t, err)
	assert.Empty(t, consumed)
}

func TestBroker_GetOffset(t *testing.T) {
	b, _ := setupTestBroker(t)
	ctx := context.Background()
	
	_ = b.CreateTopic(ctx, "events", 2, 1)
	
	// Produce some messages
	msgs := []*Message{
		{Key: []byte("k1"), Value: []byte("v1")},
		{Key: []byte("k2"), Value: []byte("v2")},
	}
	_, err := b.Produce(ctx, "events", 0, msgs)
	require.NoError(t, err)
	
	// Get latest offset
	offset, err := b.GetOffset("events", 0, -1)
	require.NoError(t, err)
	assert.Equal(t, int64(2), offset)
	
	// Get earliest offset
	offset, err = b.GetOffset("events", 0, -2)
	require.NoError(t, err)
	assert.Equal(t, int64(0), offset)
	
	// Get offset for timestamp (TODO)
	offset, err = b.GetOffset("events", 0, 1234567890)
	require.NoError(t, err)
	assert.Equal(t, int64(0), offset)
	
	// Invalid topic
	_, err = b.GetOffset("nonexistent", 0, -1)
	assert.Error(t, err)
	
	// Invalid partition
	_, err = b.GetOffset("events", 10, -1)
	assert.Error(t, err)
}

func TestBroker_messageKey(t *testing.T) {
	b, _ := setupTestBroker(t)
	
	key := b.messageKey("test-topic", 0, 123)
	assert.Equal(t, "/timq/topics/test-topic/partitions/0/messages/00000000000000000123", string(key))
	
	key = b.messageKey("test-topic", 1, 0)
	assert.Equal(t, "/timq/topics/test-topic/partitions/1/messages/00000000000000000000", string(key))
}

func TestBroker_encodeMessage(t *testing.T) {
	b, _ := setupTestBroker(t)
	
	msg := &Message{
		Key:   []byte("mykey"),
		Value: []byte("myvalue"),
	}
	
	encoded := b.encodeMessage(msg)
	
	// Check minimum length (8+4+4 + len(key) + len(value))
	assert.GreaterOrEqual(t, len(encoded), 24)
}

func TestBroker_decodeMessage(t *testing.T) {
	b, _ := setupTestBroker(t)
	
	tests := []struct {
		name    string
		data    []byte
		wantErr bool
	}{
		{
			name:    "too short",
			data:    []byte("short"),
			wantErr: true,
		},
		{
			name:    "corrupted key length",
			data:    make([]byte, 16),
			wantErr: true,
		},
		{
			name:    "corrupted value length",
			data:    []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 5, 0, 0, 0, 10}, // key_len=5, value_len=10
			wantErr: true,
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := b.decodeMessage(tt.data)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestBroker_ConcurrentAccess(t *testing.T) {
	b, _ := setupTestBroker(t)
	ctx := context.Background()
	
	_ = b.CreateTopic(ctx, "events", 1, 1)
	
	// Concurrent produces
	done := make(chan bool, 10)
	for i := 0; i < 10; i++ {
		go func(id int) {
			msgs := []*Message{{Key: []byte("k"), Value: []byte("v")}}
			_, err := b.Produce(ctx, "events", 0, msgs)
			assert.NoError(t, err)
			done <- true
		}(i)
	}
	
	for i := 0; i < 10; i++ {
		<-done
	}
	
	// Verify all messages produced
	topic, _ := b.GetTopic("events")
	assert.Equal(t, int64(10), topic.Partitions[0].LogEnd)
}
