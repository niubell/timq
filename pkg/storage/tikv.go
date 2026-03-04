package storage

import (
	"context"
	"fmt"
	"time"

	"github.com/tikv/client-go/v2/rawkv"
)

// TiKVStorage implements message storage using TiKV RawKV API
type TiKVStorage struct {
	client *rawkv.Client
}

// NewTiKVStorage creates a new TiKV storage client
func NewTiKVStorage(pdEndpoints string) (*TiKVStorage, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client, err := rawkv.NewClientWithOpts(ctx, []string{pdEndpoints})
	if err != nil {
		return nil, fmt.Errorf("failed to create TiKV client: %w", err)
	}

	return &TiKVStorage{
		client: client,
	}, nil
}

// Close closes the TiKV client
func (s *TiKVStorage) Close() error {
	return s.client.Close()
}

// Put stores a key-value pair
func (s *TiKVStorage) Put(ctx context.Context, key, value []byte) error {
	return s.client.Put(ctx, key, value)
}

// Get retrieves a value by key
func (s *TiKVStorage) Get(ctx context.Context, key []byte) ([]byte, error) {
	return s.client.Get(ctx, key)
}

// Delete removes a key-value pair
func (s *TiKVStorage) Delete(ctx context.Context, key []byte) error {
	return s.client.Delete(ctx, key)
}

// Scan retrieves keys and values in a range [startKey, endKey)
func (s *TiKVStorage) Scan(ctx context.Context, startKey, endKey []byte, limit int) ([][]byte, [][]byte, error) {
	return s.client.Scan(ctx, startKey, endKey, limit)
}

// BatchPut stores multiple key-value pairs
func (s *TiKVStorage) BatchPut(ctx context.Context, keys, values [][]byte) error {
	return s.client.BatchPut(ctx, keys, values)
}

// BatchGet retrieves multiple values by keys
func (s *TiKVStorage) BatchGet(ctx context.Context, keys [][]byte) ([][]byte, error) {
	return s.client.BatchGet(ctx, keys)
}

// BatchDelete removes multiple key-value pairs
func (s *TiKVStorage) BatchDelete(ctx context.Context, keys [][]byte) error {
	return s.client.BatchDelete(ctx, keys)
}

// ClusterID returns the TiKV cluster ID
func (s *TiKVStorage) ClusterID() uint64 {
	return s.client.ClusterID()
}
