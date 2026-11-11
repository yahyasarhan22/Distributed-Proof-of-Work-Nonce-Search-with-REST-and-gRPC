package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	pb "logviewer/proto"
)

func run(ctx context.Context, service string) error {
	// Plaintext is sufficient for this local assignment demonstration.
	conn, err := grpc.NewClient("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("create connection: %w", err)
	}
	defer conn.Close()
	client := pb.NewLogServiceClient(conn)

	fmt.Println("===== Unary RPC =====")
	// NewClient connects lazily. This deadline also bounds connection setup.
	infoCtx, cancelInfo := context.WithTimeout(ctx, 5*time.Second)
	info, err := client.GetServerInfo(infoCtx, &pb.InfoRequest{})
	cancelInfo()
	if err != nil {
		if ctx.Err() == context.Canceled {
			fmt.Println("Request canceled.")
			return nil
		}
		return fmt.Errorf("GetServerInfo (is the server running on localhost:50051?): %w", err)
	}
	fmt.Printf("Server Name: %s\nStatus: %s\n\n", info.GetName(), info.GetStatus())

	fmt.Println("===== Server Streaming RPC =====")
	fmt.Printf("Watching logs for %s...\n\n", service)
	streamCtx, cancelStream := context.WithTimeout(ctx, 15*time.Second)
	defer cancelStream()
	stream, err := client.WatchLogs(streamCtx, &pb.LogRequest{ServiceName: service})
	if err != nil {
		return streamError(ctx, err)
	}
	for {
		entry, err := stream.Recv()
		if err == io.EOF {
			fmt.Println("\nStream completed successfully.")
			return nil
		}
		if err != nil {
			return streamError(ctx, err)
		}
		// Print now; do not collect the stream into a batch.
		fmt.Printf("[%s] %s\n", entry.GetLevel(), entry.GetMessage())
	}
}

func streamError(ctx context.Context, err error) error {
	if ctx.Err() == context.Canceled && status.Code(err) == codes.Canceled {
		fmt.Println("\nStream canceled.")
		return nil
	}
	return fmt.Errorf("WatchLogs: %w", err)
}

func main() {
	service := flag.String("service", "demo-app", "simulated service to watch")
	flag.Parse()
	if *service == "" {
		fmt.Fprintln(os.Stderr, "Service name must not be empty.")
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	err := run(ctx, *service)
	stop()
	if err != nil {
		fmt.Fprintln(os.Stderr, "Client error:", err)
		os.Exit(1)
	}
}
