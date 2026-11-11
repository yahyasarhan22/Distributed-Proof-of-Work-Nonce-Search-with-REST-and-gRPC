package main

import (
	"context"
	"flag"
	"fmt"
	"google.golang.org/grpc"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"pow-distributed/internal/worker"
	pb "pow-distributed/proto"
	"syscall"
	"time"
)

func main() {
	if err := run(); err != nil {
		slog.Error("worker stopped", "error", err)
		os.Exit(1)
	}
}
func run() error {
	id := flag.String("id", "worker-1", "unique worker ID")
	port := flag.Int("port", 50052, "gRPC port")
	host := flag.String("host", "127.0.0.1", "gRPC bind host")
	advertise := flag.String("advertise", "", "reachable host:port (defaults to host:port)")
	coord := flag.String("coordinator", "127.0.0.1:50051", "coordinator gRPC address")
	heartbeat := flag.Duration("heartbeat-interval", 500*time.Millisecond, "heartbeat/re-registration interval")
	timeout := flag.Duration("rpc-timeout", time.Second, "registration/heartbeat timeout")
	progress := flag.Duration("progress-interval", 200*time.Millisecond, "progress streaming interval")
	flag.Parse()
	if *id == "" || *port < 1 || *port > 65535 || *heartbeat <= 0 || *timeout <= 0 || *progress <= 0 {
		return fmt.Errorf("invalid ID, port or interval")
	}
	address := net.JoinHostPort(*host, fmt.Sprint(*port))
	if *advertise == "" {
		*advertise = address
	}
	l, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}
	defer l.Close()
	s := grpc.NewServer()
	pb.RegisterWorkerServer(s, worker.New(*progress))
	defer s.Stop()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	errs := make(chan error, 2)
	go func() { errs <- s.Serve(l) }()
	go func() { errs <- worker.RunRegistration(ctx, *coord, *id, *advertise, *heartbeat, *timeout) }()
	slog.Info("worker ready", "worker", *id, "address", address)
	select {
	case <-ctx.Done():
		return nil
	case err = <-errs:
		return err
	}
}
