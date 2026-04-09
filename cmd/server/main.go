package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/AliSyed2006/distributed-kv/api/proto"
	"github.com/AliSyed2006/distributed-kv/internal/network"
	"github.com/AliSyed2006/distributed-kv/internal/storage"
	"google.golang.org/grpc"
)

func main() {
	// 1. Initialize StorageEngine
	// 64MB threshold as requested
	opts := storage.EngineOptions{
		Dir:        "./data",
		MaxMemSize: 64 * 1024 * 1024,
	}
	engine, err := storage.NewStorageEngine(opts)
	if err != nil {
		log.Fatalf("Failed to initialize storage engine: %v", err)
	}

	// 2. Setup gRPC Server
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	kvServer := network.NewServer(engine)
	proto.RegisterKVServiceServer(grpcServer, kvServer)

	// 3. Handle Graceful Shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		sig := <-sigChan
		fmt.Printf("\nReceived signal %v, shutting down gracefully...\n", sig)

		// Stop gRPC server first
		grpcServer.GracefulStop()

		// Close storage engine (this also stops background compaction)
		if err := engine.Close(); err != nil {
			log.Printf("Error closing storage engine: %v", err)
		}

		fmt.Println("Shutdown complete.")
		os.Exit(0)
	}()

	// 4. Start Server
	fmt.Println("DISTRIBUTED-KV LOG-STRUCTURED ENGINE")
	fmt.Printf("Starting gRPC server on :50051 (MaxMemSize: 64MB)\n")
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
