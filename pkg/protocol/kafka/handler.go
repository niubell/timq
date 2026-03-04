package kafka

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"

	"github.com/niubell/timq/pkg/broker"
)

// RequestHandler handles Kafka protocol requests
type RequestHandler struct {
	broker *broker.Broker
	conn   net.Conn
}

// RequestHeader represents a Kafka request header
type RequestHeader struct {
	APIKey        int16
	APIVersion    int16
	CorrelationID int32
	ClientID      string
}

// ResponseHeader represents a Kafka response header
type ResponseHeader struct {
	CorrelationID int32
}

// NewRequestHandler creates a new request handler
func NewRequestHandler(broker *broker.Broker, conn net.Conn) *RequestHandler {
	return &RequestHandler{
		broker: broker,
		conn:   conn,
	}
}

// ProcessRequest reads and processes a single Kafka request
func (h *RequestHandler) ProcessRequest() error {
	// Read request length (4 bytes)
	lengthBuf := make([]byte, 4)
	if _, err := io.ReadFull(h.conn, lengthBuf); err != nil {
		return err
	}
	length := binary.BigEndian.Uint32(lengthBuf)

	if length < 4 {
		return fmt.Errorf("invalid request length: %d", length)
	}

	// Read request body
	body := make([]byte, length)
	if _, err := io.ReadFull(h.conn, body); err != nil {
		return err
	}

	// Parse request header
	header, offset, err := h.parseRequestHeader(body)
	if err != nil {
		return err
	}

	// Handle request based on API key
	response, err := h.handleRequest(header, body[offset:])
	if err != nil {
		return err
	}

	// Send response
	return h.sendResponse(header.CorrelationID, response)
}

func (h *RequestHandler) parseRequestHeader(data []byte) (*RequestHeader, int, error) {
	if len(data) < 10 {
		return nil, 0, fmt.Errorf("request too short")
	}

	header := &RequestHeader{
		APIKey:        binary.BigEndian.Int16(data[0:2]),
		APIVersion:    binary.BigEndian.Int16(data[2:4]),
		CorrelationID: binary.BigEndian.Int32(data[4:8]),
	}

	offset := 8

	// Read client ID length
	if len(data) < offset+2 {
		return nil, 0, fmt.Errorf("request too short for client ID length")
	}
	clientIDLen := binary.BigEndian.Int16(data[offset : offset+2])
	offset += 2

	if clientIDLen > 0 {
		if len(data) < offset+int(clientIDLen) {
			return nil, 0, fmt.Errorf("request too short for client ID")
		}
		header.ClientID = string(data[offset : offset+int(clientIDLen)])
		offset += int(clientIDLen)
	}

	return header, offset, nil
}

func (h *RequestHandler) handleRequest(header *RequestHeader, body []byte) ([]byte, error) {
	switch header.APIKey {
	case 1: // Fetch
		return h.handleFetch(header, body)
	case 18: // ApiVersions
		return h.handleApiVersions(header, body)
	case 3: // Metadata
		return h.handleMetadata(header, body)
	case 0: // Produce
		return h.handleProduce(header, body)
	case 2: // ListOffsets
		return h.handleListOffsets(header, body)
	default:
		// Return empty response for unsupported APIs
		return []byte{}, nil
	}
}

func (h *RequestHandler) sendResponse(correlationID int32, response []byte) error {
	// Response format: length(4) + correlationID(4) + response_body
	length := uint32(4 + len(response))
	
	buf := make([]byte, 4+4+len(response))
	binary.BigEndian.PutUint32(buf[0:4], length)
	binary.BigEndian.PutUint32(buf[4:8], correlationID)
	copy(buf[8:], response)

	_, err := h.conn.Write(buf)
	return err
}
