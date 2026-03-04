package config

// Config holds the server configuration
type Config struct {
	PDEndpoints string // TiKV PD endpoints, comma separated
	ListenAddr  string // Kafka protocol listen address
	DataDir     string // Local data directory
	
	// Topic settings
	DefaultPartitionCount int
	DefaultReplicaFactor  int
	
	// Storage settings
	MaxMessageSize    int64 // Maximum message size in bytes
	RetentionHours    int   // Message retention time in hours
	SegmentSizeMB     int   // Log segment size in MB
}

// DefaultConfig returns a default configuration
func DefaultConfig() *Config {
	return &Config{
		PDEndpoints:           "127.0.0.1:2379",
		ListenAddr:            ":9092",
		DataDir:               "./data",
		DefaultPartitionCount: 1,
		DefaultReplicaFactor:  1,
		MaxMessageSize:        1 << 20, // 1MB
		RetentionHours:        168,     // 7 days
		SegmentSizeMB:         512,     // 512MB
	}
}
