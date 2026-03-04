package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/niubell/timq/pkg/broker"
	"github.com/niubell/timq/pkg/config"
	"github.com/niubell/timq/pkg/protocol/kafka"
	"github.com/niubell/timq/pkg/storage"
)

var (
	pdEndpoints = flag.String("pd-endpoints", "127.0.0.1:2379", "TiKV PD endpoints, comma separated")
	listenAddr  = flag.String("listen", ":9092", "Kafka protocol listen address")
	dataDir     = flag.String("data-dir", "./data", "Data directory for local storage")
)

func main() {
	flag.Parse()

	cfg := &config.Config{
		PDEndpoints: *pdEndpoints,
		ListenAddr:  *listenAddr,
		DataDir:     *dataDir,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize TiKV storage
	store, err := storage.NewTiKVStorage(cfg.PDEndpoints)
	if err != nil {
		log.Fatalf("Failed to connect to TiKV: %v", err)
	}
	defer store.Close()

	log.Println("Connected to TiKV cluster")

	// Initialize broker
	brk := broker.NewBroker(store, cfg)
	if err := brk.Start(); err != nil {
		log.Fatalf("Failed to start broker: %v", err)
	}
	defer brk.Stop()

	// Start Kafka protocol server
	server := kafka.NewServer(cfg.ListenAddr, brk)
	go func() {
		if err := server.Start(ctx); err != nil {
			log.Fatalf("Kafka server error: %v", err)
		}
	}()

	log.Printf("TiMQ server started, listening on %s", cfg.ListenAddr)

	// Wait for shutdown signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Println("Shutting down TiMQ server...")
}
