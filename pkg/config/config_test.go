package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()
	
	assert.Equal(t, "127.0.0.1:2379", cfg.PDEndpoints)
	assert.Equal(t, ":9092", cfg.ListenAddr)
	assert.Equal(t, "./data", cfg.DataDir)
	assert.Equal(t, 1, cfg.DefaultPartitionCount)
	assert.Equal(t, 1, cfg.DefaultReplicaFactor)
	assert.Equal(t, int64(1<<20), cfg.MaxMessageSize)
	assert.Equal(t, 168, cfg.RetentionHours)
	assert.Equal(t, 512, cfg.SegmentSizeMB)
}

func TestConfig_Values(t *testing.T) {
	cfg := &Config{
		PDEndpoints:           "192.168.1.1:2379,192.168.1.2:2379",
		ListenAddr:            ":9093",
		DataDir:               "/var/lib/timq",
		DefaultPartitionCount: 3,
		DefaultReplicaFactor:  2,
		MaxMessageSize:        10 << 20, // 10MB
		RetentionHours:        72,
		SegmentSizeMB:         1024,
	}
	
	assert.Equal(t, "192.168.1.1:2379,192.168.1.2:2379", cfg.PDEndpoints)
	assert.Equal(t, ":9093", cfg.ListenAddr)
	assert.Equal(t, "/var/lib/timq", cfg.DataDir)
	assert.Equal(t, 3, cfg.DefaultPartitionCount)
	assert.Equal(t, 2, cfg.DefaultReplicaFactor)
	assert.Equal(t, int64(10<<20), cfg.MaxMessageSize)
	assert.Equal(t, 72, cfg.RetentionHours)
	assert.Equal(t, 1024, cfg.SegmentSizeMB)
}
