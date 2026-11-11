package worker

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"log/slog"
	pb "pow-distributed/proto"
	"time"
)

// RunRegistration reconnects after coordinator restarts or a session is rejected.
// instance is stable for this process; a restart creates a new instance ID.
func RunRegistration(ctx context.Context, coordinator, id, address string, interval, rpcTimeout time.Duration) error {
	conn, err := grpc.NewClient(coordinator, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()
	var b [16]byte
	if _, err = rand.Read(b[:]); err != nil {
		return err
	}
	instance := hex.EncodeToString(b[:])
	client := pb.NewCoordinatorClient(conn)
	session := ""
	tick := time.NewTicker(interval)
	defer tick.Stop()
	for {
		callCtx, cancel := context.WithTimeout(ctx, rpcTimeout)
		if session == "" {
			result, e := client.RegisterWorker(callCtx, &pb.RegisterRequest{WorkerId: id, Address: address, InstanceId: instance})
			err = e
			if err == nil {
				session = result.SessionId
				slog.Info("registered with coordinator", "worker", id)
			}
		} else {
			_, err = client.Heartbeat(callCtx, &pb.HeartbeatRequest{WorkerId: id, SessionId: session})
			if err != nil {
				session = ""
			}
		}
		cancel()
		if err != nil && ctx.Err() == nil {
			slog.Warn("coordinator contact failed; retrying", "worker", id, "error", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-tick.C:
		}
	}
}
