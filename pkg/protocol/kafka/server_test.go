package kafka

import (
	"bytes"
	"context"
	"encoding/binary"
	"net"
	"testing"
	"time"

	"github.com/niubell/timq/pkg/broker"
	"github.com/niubell/timq/pkg/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewServer(t *testing.T) {
	cfg := config.DefaultConfig()
	mockStorage := &mockStorageForServer{data: make(map[string][]byte)}
	brk := broker.NewBroker(mockStorage, cfg)
	
	server := NewServer(":0", brk)
	
	assert.NotNil(t, server)
	assert.Equal(t, ":0", server.addr)
	assert.Equal(t, brk, server.broker)
}

func TestServer_StartStop(t *testing.T) {
	cfg := config.DefaultConfig()
	mockStorage := &mockStorageForServer{data: make(map[string][]byte)}
	brk := broker.NewBroker(mockStorage, cfg)
	
	server := NewServer("127.0.0.1:0", brk)
	
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	
	// Start server in background
	go func() {
		err := server.Start(ctx)
		assert.NoError(t, err)
	}()
	
	// Give server time to start
	time.Sleep(100 * time.Millisecond)
	
	// Stop server
	cancel()
	time.Sleep(100 * time.Millisecond)
}

func TestServer_handleConnection(t *testing.T) {
	cfg := config.DefaultConfig()
	mockStorage := &mockStorageForServer{data: make(map[string][]byte)}
	brk := broker.NewBroker(mockStorage, cfg)
	
	server := NewServer(":0", brk)
	
	// Create a mock connection with a valid request
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()
	
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	
	// Send an ApiVersions request
	go func() {
		body := buildRequestHeader(18, 3, 1, "test-client")
		length := uint32(len(body))
		
		buf := make([]byte, 4+len(body))
		binary.BigEndian.PutUint32(buf[0:4], length)
		copy(buf[4:], body)
		
		clientConn.Write(buf)
		
		// Read response
		respLen := make([]byte, 4)
		clientConn.Read(respLen)
		length = binary.BigEndian.Uint32(respLen)
		
		resp := make([]byte, length)
		clientConn.Read(resp)
		
		// Close to end the connection
		clientConn.Close()
	}()
	
	// Handle connection
	server.handleConnection(ctx, serverConn)
}

func TestServer_handleConnection_ContextCancel(t *testing.T) {
	cfg := config.DefaultConfig()
	mockStorage := &mockStorageForServer{data: make(map[string][]byte)}
	brk := broker.NewBroker(mockStorage, cfg)
	
	server := NewServer(":0", brk)
	
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()
	
	ctx, cancel := context.WithCancel(context.Background())
	
	// Cancel context immediately
	cancel()
	
	// Handle connection should return due to cancelled context
	server.handleConnection(ctx, serverConn)
}

func TestServer_handleConnection_EOF(t *testing.T) {
	cfg := config.DefaultConfig()
	mockStorage := &mockStorageForServer{data: make(map[string][]byte)}
	brk := broker.NewBroker(mockStorage, cfg)
	
	server := NewServer(":0", brk)
	
	// Use mock connection that returns EOF
	mockConn := &mockConnForServer{
		readBuf:  bytes.NewBuffer([]byte{}),
		writeBuf: &bytes.Buffer{},
	}
	
	ctx := context.Background()
	
	// Handle connection should return due to EOF
	server.handleConnection(ctx, mockConn)
	
	assert.True(t, mockConn.closed)
}

// mockStorageForServer implements storage for server tests
type mockStorageForServer struct {
	data map[string][]byte
}

func (m *mockStorageForServer) Put(ctx context.Context, key, value []byte) error {
	m.data[string(key)] = value
	return nil
}

func (m *mockStorageForServer) Get(ctx context.Context, key []byte) ([]byte, error) {
	return m.data[string(key)], nil
}

func (m *mockStorageForServer) Delete(ctx context.Context, key []byte) error {
	delete(m.data, string(key))
	return nil
}

func (m *mockStorageForServer) Scan(ctx context.Context, startKey, endKey []byte, limit int) ([][]byte, [][]byte, error) {
	return nil, nil, nil
}

func (m *mockStorageForServer) BatchPut(ctx context.Context, keys, values [][]byte) error {
	for i := range keys {
		m.data[string(keys[i])] = values[i]
	}
	return nil
}

func (m *mockStorageForServer) BatchGet(ctx context.Context, keys [][]byte) ([][]byte, error) {
	return nil, nil
}

func (m *mockStorageForServer) BatchDelete(ctx context.Context, keys [][]byte) error {
	return nil
}

func (m *mockStorageForServer) Close() error {
	return nil
}

func (m *mockStorageForServer) ClusterID() uint64 {
	return 1
}

// mockConnForServer implements net.Conn for testing
type mockConnForServer struct {
	readBuf  *bytes.Buffer
	writeBuf *bytes.Buffer
	closed   bool
}

func (m *mockConnForServer) Read(b []byte) (n int, err error) {
	return m.readBuf.Read(b)
}

func (m *mockConnForServer) Write(b []byte) (n int, err error) {
	return m.writeBuf.Write(b)
}

func (m *mockConnForServer) Close() error {
	m.closed = true
	return nil
}

func (m *mockConnForServer) LocalAddr() net.Addr  { return nil }
func (m *mockConnForServer) RemoteAddr() net.Addr { return nil }
func (m *mockConnForServer) SetDeadline(t time.Time) error      { return nil }
func (m *mockConnForServer) SetReadDeadline(t time.Time) error  { return nil }
func (m *mockConnForServer) SetWriteDeadline(t time.Time) error { return nil }
