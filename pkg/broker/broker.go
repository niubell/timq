package broker

import (
	"context"
	"encoding/binary"
	"fmt"
	"sync"
	"time"

	"github.com/niubell/timq/pkg/config"
	"github.com/niubell/timq/pkg/storage"
)

// Broker manages topics, partitions, and message routing
type Broker struct {
	storage *storage.TiKVStorage
	config  *config.Config
	
	mu     sync.RWMutex
	topics map[string]*Topic
}

// Topic represents a message topic
type Topic struct {
	Name       string
	Partitions []*Partition
	CreatedAt  time.Time
}

// Partition represents a topic partition
type Partition struct {
	ID         int32
	Topic      string
	Leader     int32
	Replicas   []int32
	ISR        []int32
	HighWater  int64 // Highest committed offset
	LogEnd     int64 // Next offset to be written
}

// Message represents a single message
type Message struct {
	Key       []byte
	Value     []byte
	Offset    int64
	Timestamp time.Time
	Headers   map[string]string
}

// NewBroker creates a new message broker
func NewBroker(store *storage.TiKVStorage, cfg *config.Config) *Broker {
	return &Broker{
		storage: store,
		config:  cfg,
		topics:  make(map[string]*Topic),
	}
}

// Start initializes the broker
func (b *Broker) Start() error {
	// Load existing topics from storage
	return b.loadTopics()
}

// Stop shuts down the broker
func (b *Broker) Stop() error {
	return nil
}

// CreateTopic creates a new topic with specified partition count
func (b *Broker) CreateTopic(ctx context.Context, name string, partitionCount int, replicaFactor int) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.topics[name]; exists {
		return fmt.Errorf("topic %s already exists", name)
	}

	topic := &Topic{
		Name:       name,
		Partitions: make([]*Partition, partitionCount),
		CreatedAt:  time.Now(),
	}

	for i := 0; i < partitionCount; i++ {
		topic.Partitions[i] = &Partition{
			ID:       int32(i),
			Topic:    name,
			Leader:   1, // Default broker ID
			Replicas: []int32{1},
			ISR:      []int32{1},
			LogEnd:   0,
		}
	}

	b.topics[name] = topic
	
	// Persist topic metadata
	if err := b.persistTopic(ctx, topic); err != nil {
		return err
	}

	return nil
}

// GetTopic retrieves a topic by name
func (b *Broker) GetTopic(name string) (*Topic, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	topic, exists := b.topics[name]
	if !exists {
		return nil, fmt.Errorf("topic %s not found", name)
	}
	return topic, nil
}

// ListTopics returns all topic names
func (b *Broker) ListTopics() []string {
	b.mu.RLock()
	defer b.mu.RUnlock()

	names := make([]string, 0, len(b.topics))
	for name := range b.topics {
		names = append(names, name)
	}
	return names
}

// DeleteTopic removes a topic
func (b *Broker) DeleteTopic(ctx context.Context, name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.topics[name]; !exists {
		return fmt.Errorf("topic %s not found", name)
	}

	delete(b.topics, name)
	
	// Delete topic metadata and data from storage
	return b.deleteTopicData(ctx, name)
}

// Produce writes messages to a topic partition
func (b *Broker) Produce(ctx context.Context, topic string, partition int32, messages []*Message) ([]int64, error) {
	t, err := b.GetTopic(topic)
	if err != nil {
		return nil, err
	}

	if int(partition) >= len(t.Partitions) {
		return nil, fmt.Errorf("invalid partition %d for topic %s", partition, topic)
	}

	p := t.Partitions[partition]
	offsets := make([]int64, len(messages))

	keys := make([][]byte, len(messages))
	values := make([][]byte, len(messages))

	for i, msg := range messages {
		offset := p.LogEnd + int64(i)
		msg.Offset = offset
		msg.Timestamp = time.Now()
		offsets[i] = offset

		// Encode message
		keys[i] = b.messageKey(topic, partition, offset)
		values[i] = b.encodeMessage(msg)
	}

	// Batch write to TiKV
	if err := b.storage.BatchPut(ctx, keys, values); err != nil {
		return nil, fmt.Errorf("failed to write messages: %w", err)
	}

	// Update partition log end offset
	p.LogEnd += int64(len(messages))

	return offsets, nil
}

// Consume reads messages from a topic partition starting at offset
func (b *Broker) Consume(ctx context.Context, topic string, partition int32, offset int64, maxBytes int32) ([]*Message, error) {
	_, err := b.GetTopic(topic)
	if err != nil {
		return nil, err
	}

	startKey := b.messageKey(topic, partition, offset)
	endKey := b.messageKey(topic, partition, offset+10000) // Max 10000 messages

	keys, values, err := b.storage.Scan(ctx, startKey, endKey, 1000)
	if err != nil {
		return nil, fmt.Errorf("failed to scan messages: %w", err)
	}

	messages := make([]*Message, 0, len(values))
	totalBytes := int32(0)

	for i, value := range values {
		_ = keys[i] // ignore key for now
		
		msg, err := b.decodeMessage(value)
		if err != nil {
			continue // Skip corrupted messages
		}

		totalBytes += int32(len(value))
		if totalBytes > maxBytes {
			break
		}

		messages = append(messages, msg)
	}

	return messages, nil
}

// GetOffset retrieves the latest or earliest offset for a partition
func (b *Broker) GetOffset(topic string, partition int32, timestamp int64) (int64, error) {
	t, err := b.GetTopic(topic)
	if err != nil {
		return 0, err
	}

	if int(partition) >= len(t.Partitions) {
		return 0, fmt.Errorf("invalid partition %d", partition)
	}

	p := t.Partitions[partition]

	if timestamp == -1 { // Latest
		return p.LogEnd, nil
	} else if timestamp == -2 { // Earliest
		return 0, nil
	}

	// TODO: Implement timestamp-based offset lookup
	return 0, nil
}

// Helper methods

func (b *Broker) messageKey(topic string, partition int32, offset int64) []byte {
	key := fmt.Sprintf("/timq/topics/%s/partitions/%d/messages/%020d", topic, partition, offset)
	return []byte(key)
}

func (b *Broker) encodeMessage(msg *Message) []byte {
	// Simple encoding: timestamp(8) + key_len(4) + key + value_len(4) + value
	keyLen := len(msg.Key)
	valueLen := len(msg.Value)
	
	buf := make([]byte, 16+keyLen+valueLen)
	binary.BigEndian.PutUint64(buf[0:8], uint64(msg.Timestamp.UnixMilli()))
	binary.BigEndian.PutUint32(buf[8:12], uint32(keyLen))
	copy(buf[12:12+keyLen], msg.Key)
	binary.BigEndian.PutUint32(buf[12+keyLen:16+keyLen], uint32(valueLen))
	copy(buf[16+keyLen:], msg.Value)
	
	return buf
}

func (b *Broker) decodeMessage(data []byte) (*Message, error) {
	if len(data) < 16 {
		return nil, fmt.Errorf("message too short")
	}

	timestamp := binary.BigEndian.Uint64(data[0:8])
	keyLen := binary.BigEndian.Uint32(data[8:12])
	
	if len(data) < int(16+keyLen) {
		return nil, fmt.Errorf("message data corrupted")
	}
	
	key := make([]byte, keyLen)
	copy(key, data[12:12+keyLen])
	
	valueLen := binary.BigEndian.Uint32(data[12+keyLen : 16+keyLen])
	
	if len(data) < int(16+keyLen+valueLen) {
		return nil, fmt.Errorf("message data corrupted")
	}
	
	value := make([]byte, valueLen)
	copy(value, data[16+keyLen:])

	return &Message{
		Key:       key,
		Value:     value,
		Timestamp: time.UnixMilli(int64(timestamp)),
	}, nil
}

func (b *Broker) loadTopics() error {
	// TODO: Load topic metadata from TiKV on startup
	return nil
}

func (b *Broker) persistTopic(ctx context.Context, topic *Topic) error {
	// TODO: Persist topic metadata to TiKV
	return nil
}

func (b *Broker) deleteTopicData(ctx context.Context, name string) error {
	// TODO: Delete all topic data from TiKV
	return nil
}
