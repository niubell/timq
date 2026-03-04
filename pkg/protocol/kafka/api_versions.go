package kafka

import (
	"encoding/binary"
)

// handleApiVersions handles ApiVersions request (API key 18)
func (h *RequestHandler) handleApiVersions(header *RequestHeader, body []byte) ([]byte, error) {
	// Supported API versions
	apis := []struct {
		APIKey     int16
		MinVersion int16
		MaxVersion int16
	}{
		{0, 0, 7},   // Produce
		{1, 0, 11},  // Fetch
		{2, 0, 5},   // ListOffsets
		{3, 0, 9},   // Metadata
		{18, 0, 3},  // ApiVersions
	}

	// Build response
	// error_code(2) + [api_keys] + throttle_time_ms(4)
	responseLen := 2 + 4 + 4 // error_code + api_keys_length + throttle_time
	for range apis {
		responseLen += 2 + 2 + 2 + 1 // api_key + min_version + max_version + tagged_fields
	}

	response := make([]byte, responseLen)
	offset := 0

	// Error code: 0 (no error)
	binary.BigEndian.PutUint16(response[offset:offset+2], 0)
	offset += 2

	// API keys array length (compact array: length + 1)
	binary.BigEndian.PutUint32(response[offset:offset+4], uint32(len(apis)+1))
	offset += 4

	// API keys
	for _, api := range apis {
		binary.BigEndian.PutUint16(response[offset:offset+2], api.APIKey)
		offset += 2
		binary.BigEndian.PutUint16(response[offset:offset+2], api.MinVersion)
		offset += 2
		binary.BigEndian.PutUint16(response[offset:offset+2], api.MaxVersion)
		offset += 2
		// Empty tagged fields
		response[offset] = 0
		offset++
	}

	// Throttle time ms
	binary.BigEndian.PutUint32(response[offset:offset+4], 0)

	return response, nil
}
