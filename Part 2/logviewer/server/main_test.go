package main

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	pb "logviewer/proto"
)

// Use a real local gRPC connection so serialization and streaming are tested too.
func testClient(t *testing.T) (pb.LogServiceClient, <-chan struct{}) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	service := &logServer{}
	finished := make(chan struct{}, 1)
	server := grpc.NewServer(grpc.StreamInterceptor(func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		err := handler(srv, stream)
		finished <- struct{}{}
		return err
	}))
	pb.RegisterLogServiceServer(server, service)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return pb.NewLogServiceClient(conn), finished
}

func TestServerInfo(t *testing.T) {
	client, _ := testClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	info, err := client.GetServerInfo(ctx, &pb.InfoRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if info.GetName() != "Log Server" || info.GetStatus() != "RUNNING" {
		t.Fatalf("unexpected info: %v", info)
	}
}

func TestWatchLogsArriveOverTimeAndFinish(t *testing.T) {
	client, _ := testClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	stream, err := client.WatchLogs(ctx, &pb.LogRequest{ServiceName: "payments"})
	if err != nil {
		t.Fatal(err)
	}
	var previous time.Time
	seen := make(map[string]bool)
	levels := make(map[string]bool)
	count := 0
	for {
		entry, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		now := time.Now()
		if !previous.IsZero() && now.Sub(previous) < 750*time.Millisecond {
			t.Fatal("logs arrived in a batch instead of one second apart")
		}
		previous = now
		if entry.GetMessage() == "" {
			t.Fatal("empty message")
		}
		if entry.GetLevel() != "INFO" && entry.GetLevel() != "WARNING" && entry.GetLevel() != "ERROR" {
			t.Fatalf("invalid level: %q", entry.GetLevel())
		}
		seen[entry.GetMessage()] = true
		levels[entry.GetLevel()] = true
		count++
		t.Logf("received at %s: [%s] %s", now.Format("15:04:05.000"), entry.GetLevel(), entry.GetMessage())
	}
	if count < 5 || len(seen) < 5 || len(levels) != 3 {
		t.Fatalf("got %d logs, %d distinct messages, levels %v", count, len(seen), levels)
	}
}

func TestWatchLogsCancellation(t *testing.T) {
	client, finished := testClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	stream, err := client.WatchLogs(ctx, &pb.LogRequest{ServiceName: "payments"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Recv(); err != nil {
		t.Fatal(err)
	}
	cancel()
	start := time.Now()
	if _, err := stream.Recv(); status.Code(err) != codes.Canceled {
		t.Fatalf("expected cancellation, got %v", err)
	}
	if time.Since(start) > 500*time.Millisecond {
		t.Fatal("cancellation was not prompt")
	}
	select {
	case <-finished:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("server continued running after stream cancellation")
	}
}

func TestWatchLogsDeadline(t *testing.T) {
	client, finished := testClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	stream, err := client.WatchLogs(ctx, &pb.LogRequest{ServiceName: "demo-app"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Recv(); err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Recv(); status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("expected deadline exceeded, got %v", err)
	}
	select {
	case <-finished:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("server continued running after deadline")
	}
}
