package kafka

import (
	"bytes"
	"encoding/binary"
	"io"
	"testing"

	"github.com/niubell/timq/pkg/broker"
	"github.com/niubell/timq/pkg/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockConn is a mock net.Conn for testing
type mockConn struct {
	readBuf  *bytes.Buffer
	writeBuf *bytes.Buffer
	closed   bool
}

func newMockConn() *mockConn {
	return &mockConn{
		readBuf:  &bytes.Buffer{},
		writeBuf: &bytes.Buffer{},
	}
}

func (m *mockConn) Read(b []byte) (n int, err error) {
	return m.readBuf.Read(b)
}

func (m *mockConn) Write(b []byte) (n int, err error) {
	return m.writeBuf.Write(b)
}

func (m *mockConn) Close() error {
	m.closed = true
	return nil
}

func (m *mockConn) LocalAddr() interface{}  { return nil }
func (m *mockConn) RemoteAddr() interface{} { return nil }
func (m *mockConn) SetDeadline(t interface{}) error      { return nil }
func (m *mockConn) SetReadDeadline(t interface{}) error  { return nil }
func (m *mockConn) SetWriteDeadline(t interface{}) error { return nil }

func TestNewRequestHandler(t *testing.T) {
	conn := newMockConn()
	brk := &broker.Broker{}
	
	handler := NewRequestHandler(brk, conn)
	
	assert.NotNil(t, handler)
	assert.Equal(t, brk, handler.broker)
	assert.Equal(t, conn, handler.conn)
}

func TestRequestHandler_parseRequestHeader(t *testing.T) {
	handler := &RequestHandler{}
	
	tests := []struct {
		name      string
		data      []byte
		wantErr   bool
		wantKey   int16
		wantVer   int16
		wantCorr  int32
		wantCID   string
		wantOff   int
	}{
		{
			name:    "valid header with client ID",
			data:    buildRequestHeader(0, 7, 12345, "test-client"),
			wantErr: false,
			wantKey: 0,
			wantVer: 7,
			wantCorr: 12345,
			wantCID: "test-client",
			wantOff: 13,
		},
		{
			name:    "valid header without client ID",
			data:    buildRequestHeader(18, 3, 99999, ""),
			wantErr: false,
			wantKey: 18,
			wantVer: 3,
			wantCorr: 99999,
			wantCID: "",
			wantOff: 10,
		},
		{
			name:    "too short",
			data:    []byte{0, 1, 0, 2, 0},
			wantErr: true,
		},
		{
			name:    "too short for client ID length",
			data:    []byte{0, 1, 0, 2, 0, 0, 0, 1},
			wantErr: true,
		},
		{
			name:    "too short for client ID",
			data:    buildRequestHeaderTruncated(),
			wantErr: true,
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			header, offset, err := handler.parseRequestHeader(tt.data)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantKey, header.APIKey)
			assert.Equal(t, tt.wantVer, header.APIVersion)
			assert.Equal(t, tt.wantCorr, header.CorrelationID)
			assert.Equal(t, tt.wantCID, header.ClientID)
			assert.Equal(t, tt.wantOff, offset)
		})
	}
}

func buildRequestHeader(apiKey, apiVer int16, corrID int32, clientID string) []byte {
	buf := make([]byte, 8+2+len(clientID))
	binary.BigEndian.PutUint16(buf[0:2], uint16(apiKey))
	binary.BigEndian.PutUint16(buf[2:4], uint16(apiVer))
	binary.BigEndian.PutUint32(buf[4:8], uint32(corrID))
	binary.BigEndian.PutUint16(buf[8:10], uint16(len(clientID)))
	copy(buf[10:], clientID)
	return buf
}

func buildRequestHeaderTruncated() []byte {
	buf := make([]byte, 10)
	binary.BigEndian.PutUint16(buf[0:2], 0)
	binary.BigEndian.PutUint16(buf[2:4], 0)
	binary.BigEndian.PutUint32(buf[4:8], 0)
	binary.BigEndian.PutUint16(buf[8:10], 100) // claim 100 bytes but don't provide them
	return buf
}

func TestRequestHandler_sendResponse(t *testing.T) {
	conn := newMockConn()
	handler := &RequestHandler{conn: conn}
	
	response := []byte{0, 1, 2, 3, 4}
	err := handler.sendResponse(12345, response)
	
	require.NoError(t, err)
	
	// Check written bytes
	written := conn.writeBuf.Bytes()
	require.GreaterOrEqual(t, len(written), 8)
	
	// Check length (4 + len(response))
	length := binary.BigEndian.Uint32(written[0:4])
	assert.Equal(t, uint32(4+5), length)
	
	// Check correlation ID
	corrID := binary.BigEndian.Uint32(written[4:8])
	assert.Equal(t, uint32(12345), corrID)
	
	// Check response body
	assert.Equal(t, response, written[8:])
}

func TestRequestHandler_handleRequest(t *testing.T) {
	cfg := config.DefaultConfig()
	mockStorage := &mockStorageForHandler{data: make(map[string][]byte)}
	brk := broker.NewBroker(mockStorage, cfg)
	handler := &RequestHandler{broker: brk}
	
	tests := []struct {
		name    string
		apiKey  int16
		wantErr bool
	}{
		{name: "Produce", apiKey: 0, wantErr: false},
		{name: "Fetch", apiKey: 1, wantErr: false},
		{name: "ListOffsets", apiKey: 2, wantErr: false},
		{name: "Metadata", apiKey: 3, wantErr: false},
		{name: "ApiVersions", apiKey: 18, wantErr: false},
		{name: "Unknown", apiKey: 999, wantErr: false},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			header := &RequestHeader{
				APIKey:        tt.apiKey,
				APIVersion:    1,
				CorrelationID: 1,
				ClientID:      "test",
			}
			_, err := handler.handleRequest(header, []byte{})
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestRequestHandler_ProcessRequest(t *testing.T) {
	tests := []struct {
		name     string
		setupConn func(*mockConn)
		wantErr  bool
	}{
		{
			name: "valid request",
			setupConn: func(c *mockConn) {
				// Build a valid ApiVersions request
				body := buildRequestHeader(18, 3, 1, "test")
				length := uint32(len(body))
				
				buf := make([]byte, 4+len(body))
				binary.BigEndian.PutUint32(buf[0:4], length)
				copy(buf[4:], body)
				c.readBuf.Write(buf)
			},
			wantErr: false,
		},
		{
			name: "invalid request length",
			setupConn: func(c *mockConn) {
				buf := make([]byte, 4)
				binary.BigEndian.PutUint32(buf, 2) // too short
				c.readBuf.Write(buf)
				c.readBuf.Write([]byte{0, 0}) // 2 bytes body
			},
			wantErr: true,
		},
		{
			name: "EOF on length read",
			setupConn: func(c *mockConn) {
				// Don't write anything
			},
			wantErr: true,
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := config.DefaultConfig()
			mockStorage := &mockStorageForHandler{data: make(map[string][]byte)}
			brk := broker.NewBroker(mockStorage, cfg)
			conn := newMockConn()
			tt.setupConn(conn)
			
			handler := NewRequestHandler(brk, conn)
			err := handler.ProcessRequest()
			
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				// Check that response was written
				assert.Greater(t, conn.writeBuf.Len(), 0)
			}
		})
	}
}

// mockStorageForHandler implements minimal storage interface for handler tests
type mockStorageForHandler struct {
	data map[string][]byte
}

func (m *mockStorageForHandler) Put(ctx interface{}, key, value []byte) error {
	m.data[string(key)] = value
	return nil
}

func (m *mockStorageForHandler) Get(ctx interface{}, key []byte) ([]byte, error) {
	return m.data[string(key)], nil
}

func (m *mockStorageForHandler) Delete(ctx interface{}, key []byte) error {
	delete(m.data, string(key))
	return nil
}

func (m *mockStorageForHandler) Scan(ctx interface{}, startKey, endKey []byte, limit int) ([][]byte, [][]byte, error) {
	return nil, nil, nil
}

func (m *mockStorageForHandler) BatchPut(ctx interface{}, keys, values [][]byte) error {
	for i := range keys {
		m.data[string(keys[i])] = values[i]
	}
	return nil
}

func (m *mockStorageForHandler) BatchGet(ctx interface{}, keys [][]byte) ([][]byte, error) {
	return nil, nil
}

func (m *mockStorageForHandler) BatchDelete(ctx interface{}, keys [][]byte) error {
	return nil
}

func (m *mockStorageForHandler) Close() error {
	return nil
}

func (m *mockStorageForHandler) ClusterID() uint64 {
	return 1
}
