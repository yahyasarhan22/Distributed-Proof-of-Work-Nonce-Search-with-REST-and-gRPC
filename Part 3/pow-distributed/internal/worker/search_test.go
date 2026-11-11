package worker

import (
	"context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"io"
	"net"
	"pow-distributed/internal/pow"
	pb "pow-distributed/proto"
	"testing"
	"time"
)

func TestSearchGRPC(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := grpc.NewServer()
	pb.RegisterWorkerServer(s, New(time.Millisecond))
	go s.Serve(listener)
	defer s.Stop()
	conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	client := pb.NewWorkerClient(conn)
	for _, tc := range []struct {
		start, end uint64
		kind       pb.UpdateKind
	}{{0, 1, pb.UpdateKind_EXHAUSTED}, {28, 29, pb.UpdateKind_FOUND}} {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		stream, err := client.Search(ctx, &pb.SearchRequest{JobId: "j", AssignmentId: "a", Data: "hello", Difficulty: 1, Start: tc.start, End: tc.end})
		if err != nil {
			t.Fatal(err)
		}
		progress := false
		var final *pb.SearchUpdate
		for {
			u, e := stream.Recv()
			if e == io.EOF {
				break
			}
			if e != nil {
				t.Fatal(e)
			}
			if u.Kind == pb.UpdateKind_PROGRESS {
				progress = true
			} else {
				final = u
			}
		}
		cancel()
		if !progress || final == nil || final.Kind != tc.kind || final.Checked != 1 {
			t.Fatalf("progress=%v final=%v", progress, final)
		}
		if final.Kind == pb.UpdateKind_FOUND && (final.Nonce != 28 || final.Hash != pow.Hash("hello", 28)) {
			t.Fatal(final)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	stream, err := client.Search(ctx, &pb.SearchRequest{JobId: "j", AssignmentId: "c", Data: "cancel", Difficulty: 8, End: 1 << 60})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = stream.Recv(); err != nil {
		t.Fatal(err)
	}
	cancel()
	if _, err = stream.Recv(); err == nil {
		t.Fatal("expected cancellation")
	}
}

func TestPeriodicProgressValidationAndBusyRejection(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := grpc.NewServer()
	pb.RegisterWorkerServer(s, New(time.Nanosecond))
	go s.Serve(listener)
	defer s.Stop()
	conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	client := pb.NewWorkerClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	invalid, err := client.Search(ctx, &pb.SearchRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = invalid.Recv(); status.Code(err) != codes.InvalidArgument {
		t.Fatal("invalid search accepted", err)
	}
	stream, err := client.Search(ctx, &pb.SearchRequest{JobId: "j", AssignmentId: "active", Data: "cancel", Difficulty: 8, End: 1 << 60})
	if err != nil {
		t.Fatal(err)
	}
	last := uint64(0)
	for {
		u, e := stream.Recv()
		if e != nil {
			t.Fatal(e)
		}
		if u.Kind != pb.UpdateKind_PROGRESS {
			t.Fatal("expected progress", u)
		}
		if u.Checked < last {
			t.Fatal("nonmonotonic progress")
		}
		last = u.Checked
		if last > 1 {
			break
		}
	}
	busy, err := client.Search(ctx, &pb.SearchRequest{JobId: "j", AssignmentId: "second", Data: "hello", Difficulty: 1, End: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = busy.Recv(); status.Code(err) != codes.ResourceExhausted {
		t.Fatal("simultaneous search accepted", err)
	}
	cancel()
	deadline := time.Now().Add(3 * time.Second)
	for {
		nextCtx, nextCancel := context.WithTimeout(context.Background(), time.Second)
		next, e := client.Search(nextCtx, &pb.SearchRequest{JobId: "j", AssignmentId: "next", Data: "hello", Difficulty: 1, End: 1})
		if e == nil {
			_, e = next.Recv()
		}
		if e == nil {
			u, e := next.Recv()
			nextCancel()
			if e != nil || u.Kind != pb.UpdateKind_EXHAUSTED {
				t.Fatal("worker did not resume", u, e)
			}
			break
		}
		nextCancel()
		if status.Code(e) != codes.ResourceExhausted || time.Now().After(deadline) {
			t.Fatal("cancel did not release worker", e)
		}
		time.Sleep(time.Millisecond)
	}
}
