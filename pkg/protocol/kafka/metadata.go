package kafka

import (
	"encoding/binary"
)

// MetadataResponseBroker represents a broker in metadata response
type MetadataResponseBroker struct {
	NodeID int32
	Host   string
	Port   int32
	Rack   *string
}

// MetadataResponsePartition represents a partition in metadata response
type MetadataResponsePartition struct {
	ErrorCode      int16
	PartitionIndex int32
	LeaderID       int32
	LeaderEpoch    int32
	ReplicaNodes   []int32
	ISRNodes       []int32
	OfflineReplicas []int32
}

// MetadataResponseTopic represents a topic in metadata response
type MetadataResponseTopic struct {
	ErrorCode  int16
	Name       string
	IsInternal bool
	Partitions []MetadataResponsePartition
}

// handleMetadata handles Metadata request (API key 3)
func (h *RequestHandler) handleMetadata(header *RequestHeader, body []byte) ([]byte, error) {
	// Parse request to get topic names (simplified)
	// For now, return metadata for all topics

	// Get all topics from broker
	topicNames := h.broker.ListTopics()

	// Build response
	brokers := []MetadataResponseBroker{
		{NodeID: 1, Host: "localhost", Port: 9092},
	}

	topics := make([]MetadataResponseTopic, 0, len(topicNames))
	for _, name := range topicNames {
		topic, err := h.broker.GetTopic(name)
		if err != nil {
			continue
		}

		partitions := make([]MetadataResponsePartition, len(topic.Partitions))
		for i, p := range topic.Partitions {
			partitions[i] = MetadataResponsePartition{
				ErrorCode:       0,
				PartitionIndex:  p.ID,
				LeaderID:        p.Leader,
				LeaderEpoch:     0,
				ReplicaNodes:    p.Replicas,
				ISRNodes:        p.ISR,
				OfflineReplicas: []int32{},
			}
		}

		topics = append(topics, MetadataResponseTopic{
			ErrorCode:  0,
			Name:       name,
			IsInternal: false,
			Partitions: partitions,
		})
	}

	return h.encodeMetadataResponse(brokers, topics), nil
}

func (h *RequestHandler) encodeMetadataResponse(brokers []MetadataResponseBroker, topics []MetadataResponseTopic) []byte {
	// Calculate response size
	size := 4 // throttle_time_ms
	size += 4 // brokers array length
	for _, b := range brokers {
		size += 4 + 2 + len(b.Host) + 4 + 1 // node_id + host_len + host + port + tagged_fields
	}
	size += 2 // cluster_id (null)
	size += 4 // controller_id
	size += 4 // topics array length
	for _, t := range topics {
		size += 2 + 2 + len(t.Name) + 1 + 4 // error_code + name_len + name + is_internal + partitions_len
		for _, p := range t.Partitions {
			size += 2 + 4 + 4 + 4 + 4 + 4*len(p.ReplicaNodes) + 4 + 4*len(p.ISRNodes) + 4 + 1
		}
		size += 1 // topic tagged fields
	}
	size += 1 // response tagged fields

	response := make([]byte, size)
	offset := 0

	// Throttle time
	binary.BigEndian.PutUint32(response[offset:offset+4], 0)
	offset += 4

	// Brokers array
	binary.BigEndian.PutUint32(response[offset:offset+4], uint32(len(brokers)+1))
	offset += 4
	for _, b := range brokers {
		binary.BigEndian.PutUint32(response[offset:offset+4], uint32(b.NodeID))
		offset += 4
		binary.BigEndian.PutUint16(response[offset:offset+2], uint16(len(b.Host)))
		offset += 2
		copy(response[offset:], b.Host)
		offset += len(b.Host)
		binary.BigEndian.PutUint32(response[offset:offset+4], uint32(b.Port))
		offset += 4
		response[offset] = 0 // tagged fields
		offset++
	}

	// Cluster ID (null)
	binary.BigEndian.PutUint16(response[offset:offset+2], uint16(0xFFFF)) // -1 for null
	offset += 2

	// Controller ID
	binary.BigEndian.PutUint32(response[offset:offset+4], 1)
	offset += 4

	// Topics array
	binary.BigEndian.PutUint32(response[offset:offset+4], uint32(len(topics)+1))
	offset += 4
	for _, t := range topics {
		binary.BigEndian.PutUint16(response[offset:offset+2], t.ErrorCode)
		offset += 2
		binary.BigEndian.PutUint16(response[offset:offset+2], uint16(len(t.Name)))
		offset += 2
		copy(response[offset:], t.Name)
		offset += len(t.Name)
		if t.IsInternal {
			response[offset] = 1
		} else {
			response[offset] = 0
		}
		offset++

		// Partitions array
		binary.BigEndian.PutUint32(response[offset:offset+4], uint32(len(t.Partitions)+1))
		offset += 4
		for _, p := range t.Partitions {
			binary.BigEndian.PutUint16(response[offset:offset+2], p.ErrorCode)
			offset += 2
			binary.BigEndian.PutUint32(response[offset:offset+4], uint32(p.PartitionIndex))
			offset += 4
			binary.BigEndian.PutUint32(response[offset:offset+4], uint32(p.LeaderID))
			offset += 4
			binary.BigEndian.PutUint32(response[offset:offset+4], uint32(p.LeaderEpoch))
			offset += 4

			// Replica nodes
			binary.BigEndian.PutUint32(response[offset:offset+4], uint32(len(p.ReplicaNodes)+1))
			offset += 4
			for _, r := range p.ReplicaNodes {
				binary.BigEndian.PutUint32(response[offset:offset+4], uint32(r))
				offset += 4
			}

			// ISR nodes
			binary.BigEndian.PutUint32(response[offset:offset+4], uint32(len(p.ISRNodes)+1))
			offset += 4
			for _, r := range p.ISRNodes {
				binary.BigEndian.PutUint32(response[offset:offset+4], uint32(r))
				offset += 4
			}

			// Offline replicas
			binary.BigEndian.PutUint32(response[offset:offset+4], 1) // empty array
			offset += 4

			response[offset] = 0 // partition tagged fields
			offset++
		}

		response[offset] = 0 // topic tagged fields
		offset++
	}

	response[offset] = 0 // response tagged fields

	return response
}
