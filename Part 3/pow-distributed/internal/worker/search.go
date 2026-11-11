// Package worker implements one cancellable range search at a time.
package worker

import (
	"encoding/hex"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"log/slog"
	"pow-distributed/internal/pow"
	pb "pow-distributed/proto"
	"sync/atomic"
	"time"
)

type Service struct {
	pb.UnimplementedWorkerServer
	busy             atomic.Bool
	progressInterval time.Duration
}

func New(progressInterval time.Duration) *Service {
	return &Service{progressInterval: progressInterval}
}
func (s *Service) Search(r *pb.SearchRequest, stream grpc.ServerStreamingServer[pb.SearchUpdate]) error {
	if r.JobId == "" || r.AssignmentId == "" || r.Data == "" || r.Difficulty < 1 || r.Difficulty > 8 || r.Start >= r.End {
		return status.Error(codes.InvalidArgument, "invalid range search")
	}
	if !s.busy.CompareAndSwap(false, true) {
		return status.Error(codes.ResourceExhausted, "worker already searching")
	}
	defer s.busy.Store(false)
	ctx := stream.Context()
	slog.Info("search started", "job", r.JobId, "assignment", r.AssignmentId, "start", r.Start, "end", r.End)
	defer func() {
		if ctx.Err() != nil {
			slog.Info("search cancelled", "job", r.JobId, "assignment", r.AssignmentId, "reason", ctx.Err())
		}
	}()
	send := func(kind pb.UpdateKind, checked, nonce uint64, hash string) error {
		return stream.Send(&pb.SearchUpdate{AssignmentId: r.AssignmentId, Kind: kind, Checked: checked, Nonce: nonce, Hash: hash})
	}
	if err := send(pb.UpdateKind_PROGRESS, 0, 0, ""); err != nil {
		return err
	}
	last := time.Now()
	for n := r.Start; n < r.End; n++ {
		checked := n - r.Start
		if checked%256 == 0 {
			select {
			case <-ctx.Done():
				return status.FromContextError(ctx.Err()).Err()
			default:
			}
		}
		h := pow.Sum(r.Data, n)
		if pow.LeadingZeros(h, int(r.Difficulty)) {
			slog.Info("solution found", "job", r.JobId, "nonce", n)
			return send(pb.UpdateKind_FOUND, checked+1, n, hex.EncodeToString(h[:]))
		}
		if checked%1024 == 0 && time.Since(last) >= s.progressInterval {
			if err := send(pb.UpdateKind_PROGRESS, checked+1, 0, ""); err != nil {
				return err
			}
			last = time.Now()
		}
	}
	return send(pb.UpdateKind_EXHAUSTED, r.End-r.Start, 0, "")
}
