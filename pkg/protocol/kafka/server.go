package kafka

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync"

	"github.com/niubell/timq/pkg/broker"
)

// Server implements the Kafka protocol server
type Server struct {
	addr     string
	broker   *broker.Broker
	listener net.Listener
	wg       sync.WaitGroup
}

// NewServer creates a new Kafka protocol server
func NewServer(addr string, brk *broker.Broker) *Server {
	return &Server{
		addr:   addr,
		broker: brk,
	}
}

// Start starts the Kafka server
func (s *Server) Start(ctx context.Context) error {
	listener, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", s.addr, err)
	}
	s.listener = listener

	go func() {
		<-ctx.Done()
		s.listener.Close()
	}()

	for {
		conn, err := listener.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				return nil
			default:
				continue
			}
		}

		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.handleConnection(ctx, conn)
		}()
	}
}

// Stop stops the server
func (s *Server) Stop() error {
	if s.listener != nil {
		s.listener.Close()
	}
	s.wg.Wait()
	return nil
}

// handleConnection handles a single client connection
func (s *Server) handleConnection(ctx context.Context, conn net.Conn) {
	defer conn.Close()

	handler := NewRequestHandler(s.broker, conn)
	
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if err := handler.ProcessRequest(); err != nil {
			if err != io.EOF {
				// Log error
			}
			return
		}
	}
}
