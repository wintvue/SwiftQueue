package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

// Server represents the SwiftQueue protocol server
type Server struct {
	config   *Config
	listener net.Listener
	logger   *log.Logger
	wg       sync.WaitGroup
	shutdown chan struct{}
}

// NewServer creates a new SwiftQueue server instance
func NewServer(config *Config) (*Server, error) {
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	logger := log.New(os.Stdout, "[swiftqueue-server] ", log.LstdFlags|log.Lmsgprefix)

	return &Server{
		config:   config,
		logger:   logger,
		shutdown: make(chan struct{}),
	}, nil
}

// Start begins listening for connections
func (s *Server) Start() error {
	listener, err := net.Listen("tcp", s.config.Address())
	if err != nil {
		return fmt.Errorf("failed to bind to %s: %w", s.config.Address(), err)
	}
	s.listener = listener

	s.logger.Printf("Server listening on %s", s.config.Address())

	return nil
}

// Serve accepts and handles incoming connections
func (s *Server) Serve(ctx context.Context) error {
	// Create a context that can be cancelled
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Handle graceful shutdown
	// go s.handleShutdown(cancel)

	for {
		// select {
		// case <-ctx.Done():
		// 	s.logger.Println("Server shutting down...")
		// 	return s.gracefulShutdown()
		// default:
		// }

		// Accept new connection
		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				// Shutdown in progress
				return nil
			default:
				s.logger.Printf("Error accepting connection: %v", err)
				continue
			}
		}

		// Handle connection in a goroutine
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			handler := NewConnectionHandler(conn, s.config, s.logger)
			if err := handler.Handle(ctx); err != nil {
				s.logger.Printf("Connection handler error: %v", err)
			}
		}()
	}
}

// handleShutdown listens for shutdown signals
func (s *Server) handleShutdown(cancel context.CancelFunc) {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	sig := <-sigChan
	s.logger.Printf("Received signal: %v", sig)
	cancel()
}

// gracefulShutdown performs a graceful shutdown
func (s *Server) gracefulShutdown() error {
	s.logger.Println("Starting graceful shutdown...")

	// Close the listener to stop accepting new connections
	if s.listener != nil {
		if err := s.listener.Close(); err != nil {
			s.logger.Printf("Error closing listener: %v", err)
		}
	}

	// Wait for existing connections to finish with timeout
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		s.logger.Println("All connections closed gracefully")
	case <-time.After(s.config.ShutdownTimeout):
		s.logger.Println("Shutdown timeout exceeded, forcing close")
	}

	return nil
}

// Run starts the server and blocks until shutdown
func (s *Server) Run() error {
	if err := s.Start(); err != nil {
		return err
	}

	return s.Serve(context.Background())
}
