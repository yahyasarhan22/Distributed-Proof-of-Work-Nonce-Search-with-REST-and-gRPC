package main

import (
	"context"
	"flag"
	"google.golang.org/grpc"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"pow-distributed/internal/coordinator"
	pb "pow-distributed/proto"
	"syscall"
	"time"
)

func main() {
	if err := run(); err != nil {
		slog.Error("coordinator stopped", "error", err)
		os.Exit(1)
	}
}
func run() error {
	httpAddress := flag.String("http", "127.0.0.1:8080", "REST listen address")
	grpcAddress := flag.String("grpc", "127.0.0.1:50051", "gRPC listen address")
	chunk := flag.Uint64("chunk-size", 1000000, "nonces per half-open range")
	heartbeatTimeout := flag.Duration("heartbeat-timeout", 3*time.Second, "worker lease timeout")
	assignmentTimeout := flag.Duration("assignment-timeout", 2*time.Minute, "maximum duration of one Search RPC")
	flag.Parse()
	c, err := coordinator.New(coordinator.Config{ChunkSize: *chunk, HeartbeatTimeout: *heartbeatTimeout, AssignmentTimeout: *assignmentTimeout})
	if err != nil {
		return err
	}
	defer c.Close()
	gl, err := net.Listen("tcp", *grpcAddress)
	if err != nil {
		return err
	}
	defer gl.Close()
	hl, err := net.Listen("tcp", *httpAddress)
	if err != nil {
		return err
	}
	defer hl.Close()
	gs := grpc.NewServer()
	pb.RegisterCoordinatorServer(gs, c)
	defer gs.Stop()
	hs := &http.Server{Handler: c.Handler(), ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 60 * time.Second}
	errs := make(chan error, 2)
	go func() { errs <- gs.Serve(gl) }()
	go func() { errs <- hs.Serve(hl) }()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	slog.Info("coordinator ready", "http", *httpAddress, "grpc", *grpcAddress, "chunk_size", *chunk)
	select {
	case <-ctx.Done():
	case err = <-errs:
	}
	c.Close() // Cancel searches and publish terminal events before HTTP shutdown.
	shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = hs.Shutdown(shutdown)
	return err
}
