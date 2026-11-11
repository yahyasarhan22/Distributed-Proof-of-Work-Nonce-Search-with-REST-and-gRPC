package main

import (
	"context"
	"errors"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	pb "logviewer/proto"
)

type logServer struct {
	pb.UnimplementedLogServiceServer
}

func (s *logServer) GetServerInfo(ctx context.Context, _ *pb.InfoRequest) (*pb.InfoResponse, error) {
	if client, ok := peer.FromContext(ctx); ok {
		log.Printf("GetServerInfo: client %s connected", client.Addr)
	}
	return &pb.InfoResponse{Name: "Log Server", Status: "RUNNING"}, nil
}

func (s *logServer) WatchLogs(req *pb.LogRequest, stream grpc.ServerStreamingServer[pb.LogMessage]) error {
	ctx := stream.Context()
	service := req.GetServiceName()
	if service == "" {
		service = "demo-app"
	}
	log.Printf("WatchLogs: watching service %q", service)

	// Each request gets the same simulated events for its selected service.
	entries := []pb.LogMessage{
		{Level: "INFO", Message: "Server started"},
		{Level: "INFO", Message: "User connected"},
		{Level: "WARNING", Message: "High memory usage"},
		{Level: "ERROR", Message: "Database connection failed"},
		{Level: "INFO", Message: "Connection restored"},
	}
	for i := range entries {
		// The first log is immediate. Wait between later logs, but allow
		// cancellation to interrupt the wait instead of using time.Sleep.
		if i > 0 {
			timer := time.NewTimer(time.Second)
			select {
			case <-ctx.Done():
				timer.Stop()
				log.Printf("WatchLogs: service %q stopped: %v", service, ctx.Err())
				return status.FromContextError(ctx.Err()).Err()
			case <-timer.C:
			}
		}
		if err := ctx.Err(); err != nil {
			log.Printf("WatchLogs: service %q stopped: %v", service, err)
			return status.FromContextError(err).Err()
		}
		if err := stream.Send(&entries[i]); err != nil {
			log.Printf("WatchLogs: service %q send failed: %v", service, err)
			return err
		}
		log.Printf("WatchLogs [%s]: [%s] %s", service, entries[i].Level, entries[i].Message)
	}
	log.Printf("WatchLogs: service %q stream completed", service)
	return nil // A successful return ends the stream normally.
}

func main() {
	listener, err := net.Listen("tcp", "localhost:50051")
	if err != nil {
		log.Fatalf("Listen failed: %v", err)
	}
	defer listener.Close()
	server := grpc.NewServer()
	pb.RegisterLogServiceServer(server, &logServer{})

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	shutdownDone := make(chan struct{})
	go func() {
		<-ctx.Done()
		log.Print("Shutting down Log Server...")
		// Give active streams time to finish; bound shutdown to five seconds.
		forceStop := time.AfterFunc(5*time.Second, server.Stop)
		server.GracefulStop()
		forceStop.Stop()
		close(shutdownDone)
	}()
	log.Print("Log Server listening on localhost:50051")
	if err := server.Serve(listener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
		log.Fatalf("Server failed: %v", err)
	}
	if ctx.Err() != nil {
		<-shutdownDone
	}
	log.Print("Log Server stopped.")
}
