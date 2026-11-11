package coordinator

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"pow-distributed/internal/pow"
	workerpkg "pow-distributed/internal/worker"
	pb "pow-distributed/proto"
)

func waitFor(t *testing.T, f func() bool) {
	t.Helper()
	deadline := time.NewTimer(8 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(5 * time.Millisecond)
	defer tick.Stop()
	for {
		if f() {
			return
		}
		select {
		case <-deadline.C:
			t.Fatal("condition timed out")
		case <-tick.C:
		}
	}
}
func receive[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(8 * time.Second):
		t.Fatal("channel timed out")
		var zero T
		return zero
	}
}
func serveGRPC(t *testing.T, register func(*grpc.Server)) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := grpc.NewServer()
	register(s)
	go s.Serve(l)
	t.Cleanup(s.Stop)
	return l.Addr().String()
}
func coordinatorClient(t *testing.T, c *Coordinator) pb.CoordinatorClient {
	t.Helper()
	addr := serveGRPC(t, func(s *grpc.Server) { pb.RegisterCoordinatorServer(s, c) })
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return pb.NewCoordinatorClient(conn)
}
func register(t *testing.T, client pb.CoordinatorClient, id, instance, addr string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	r, err := client.RegisterWorker(ctx, &pb.RegisterRequest{WorkerId: id, InstanceId: instance, Address: addr})
	if err != nil {
		t.Fatal(err)
	}
	return r.SessionId
}
func liveWorker(t *testing.T, c *Coordinator, id string) {
	t.Helper()
	ca := serveGRPC(t, func(s *grpc.Server) { pb.RegisterCoordinatorServer(s, c) })
	wa := serveGRPC(t, func(s *grpc.Server) { pb.RegisterWorkerServer(s, workerpkg.New(time.Millisecond)) })
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- workerpkg.RunRegistration(ctx, ca, id, wa, 20*time.Millisecond, time.Second) }()
	t.Cleanup(func() {
		cancel()
		if e := receive(t, done); e != nil {
			t.Error(e)
		}
	})
	waitFor(t, func() bool {
		for _, w := range c.Workers() {
			if w.WorkerID == id && w.Alive {
				return true
			}
		}
		return false
	})
}

type controlledWorker struct {
	pb.UnimplementedWorkerServer
	search func(*pb.SearchRequest, grpc.ServerStreamingServer[pb.SearchUpdate]) error
}

func (w *controlledWorker) Search(r *pb.SearchRequest, s grpc.ServerStreamingServer[pb.SearchUpdate]) error {
	return w.search(r, s)
}
func controlled(t *testing.T, fn func(*pb.SearchRequest, grpc.ServerStreamingServer[pb.SearchUpdate]) error) string {
	return serveGRPC(t, func(s *grpc.Server) { pb.RegisterWorkerServer(s, &controlledWorker{search: fn}) })
}
func assertProof(t *testing.T, j JobView) {
	t.Helper()
	if j.Status != "FOUND" || j.Nonce == nil {
		t.Fatalf("not found: %+v", j)
	}
	h := sha256.Sum256([]byte(j.Data + fmt.Sprint(*j.Nonce)))
	hash := hex.EncodeToString(h[:])
	if j.Hash != hash || !strings.HasPrefix(hash, strings.Repeat("0", j.Difficulty)) {
		t.Fatalf("invalid independent proof %+v", j)
	}
}

func TestRealWorkerSolvesConcurrentJobs(t *testing.T) {
	c := newTestCoordinator(t)
	liveWorker(t, c, "real-1")
	liveWorker(t, c, "real-2")
	var ids []string
	for i := 0; i < 3; i++ {
		j, err := c.Submit(fmt.Sprintf("hello-%d", i), 3)
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, j.JobID)
	}
	if ids[0] == ids[1] || ids[1] == ids[2] {
		t.Fatal("duplicate IDs")
	}
	for _, id := range ids {
		waitFor(t, func() bool { j, _ := c.Get(id); return j.Status == "FOUND" })
		j, _ := c.Get(id)
		assertProof(t, j)
		if j.NoncesChecked == 0 || j.ElapsedSeconds <= 0 {
			t.Fatal(j)
		}
		if c.Cancel(id) != ErrFinished {
			t.Fatal("finished cancellation")
		}
	}
}

func TestDistinctRangesFairSchedulingAndHTTPCancellation(t *testing.T) {
	c := newTestCoordinator(t)
	client := coordinatorClient(t, c)
	requests := make(chan *pb.SearchRequest, 16)
	stopped := make(chan string, 16)
	addr := controlled(t, func(r *pb.SearchRequest, s grpc.ServerStreamingServer[pb.SearchUpdate]) error {
		requests <- r
		_ = s.Send(&pb.SearchUpdate{AssignmentId: r.AssignmentId, Checked: 1})
		<-s.Context().Done()
		stopped <- r.JobId
		return s.Context().Err()
	})
	a, _ := c.Submit("a", 8)
	b, _ := c.Submit("b", 8)
	for i := 0; i < 4; i++ {
		register(t, client, fmt.Sprint(i), "instance", addr)
	}
	ranges := map[string][]Range{}
	for i := 0; i < 4; i++ {
		r := receive(t, requests)
		ranges[r.JobId] = append(ranges[r.JobId], Range{r.Start, r.End})
	}
	if len(ranges[a.JobID]) != 2 || len(ranges[b.JobID]) != 2 {
		t.Fatalf("unfair assignment: %v", ranges)
	}
	for _, rs := range ranges {
		if rs[0].Start == rs[1].Start || rs[0].End-rs[0].Start != 1000 {
			t.Fatal(rs)
		}
	}
	server := httptest.NewServer(c.Handler())
	defer server.Close()
	req, _ := http.NewRequest("DELETE", server.URL+"/jobs/"+a.JobID, nil)
	response, err := server.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != 200 {
		t.Fatal(response.Status)
	}
	for i := 0; i < 2; i++ {
		if receive(t, stopped) != a.JobID {
			t.Fatal("wrong cancellation")
		}
	}
	j, _ := c.Get(a.JobID)
	if j.Status != "CANCELLED" {
		t.Fatal(j)
	}
	c.Cancel(b.JobID)
}

func TestRPCFailureRequeuesRangeAndReplacementResumes(t *testing.T) {
	c := newTestCoordinator(t)
	client := coordinatorClient(t, c)
	started := make(chan *pb.SearchRequest, 1)
	crash := make(chan struct{})
	bad := controlled(t, func(r *pb.SearchRequest, s grpc.ServerStreamingServer[pb.SearchUpdate]) error {
		if e := s.Send(&pb.SearchUpdate{AssignmentId: r.AssignmentId, Checked: 7}); e != nil {
			return e
		}
		started <- r
		select {
		case <-crash:
			return status.Error(codes.Unavailable, "simulated process crash")
		case <-s.Context().Done():
			return s.Context().Err()
		}
	})
	register(t, client, "replace-me", "old", bad)
	j, _ := c.Submit("hello", 2)
	first := receive(t, started)
	close(crash)
	waitFor(t, func() bool { ws := c.Workers(); return len(ws) == 1 && !ws[0].Alive })
	replacementRanges := make(chan *pb.SearchRequest, 16)
	service := workerpkg.New(time.Millisecond)
	good := controlled(t, func(r *pb.SearchRequest, s grpc.ServerStreamingServer[pb.SearchUpdate]) error {
		replacementRanges <- r
		return service.Search(r, s)
	})
	register(t, client, "replace-me", "new", good)
	retry := receive(t, replacementRanges)
	if retry.Start != first.Start || retry.End != first.End || retry.AssignmentId == first.AssignmentId {
		t.Fatalf("range was skipped: %v %v", first, retry)
	}
	waitFor(t, func() bool { v, _ := c.Get(j.JobID); return v.Status == "FOUND" })
	v, _ := c.Get(j.JobID)
	assertProof(t, v)
	if v.NoncesChecked < 8 {
		t.Fatal("retry attempts not counted")
	}
}

func TestHeartbeatTimeoutAndSessionFencing(t *testing.T) {
	c, err := New(Config{ChunkSize: 1000, HeartbeatTimeout: 150 * time.Millisecond, AssignmentTimeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	client := coordinatorClient(t, c)
	started := make(chan *pb.SearchRequest, 2)
	stopped := make(chan struct{}, 2)
	addr := controlled(t, func(r *pb.SearchRequest, s grpc.ServerStreamingServer[pb.SearchUpdate]) error {
		started <- r
		<-s.Context().Done()
		stopped <- struct{}{}
		return s.Context().Err()
	})
	session := register(t, client, "w", "old", addr)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err = client.Heartbeat(ctx, &pb.HeartbeatRequest{WorkerId: "w", SessionId: session}); err != nil {
		t.Fatal(err)
	}
	if _, err = client.RegisterWorker(ctx, &pb.RegisterRequest{WorkerId: "w", InstanceId: "intruder", Address: addr}); status.Code(err) != codes.AlreadyExists {
		t.Fatal(err)
	}
	j, _ := c.Submit("lease", 8)
	first := receive(t, started)
	receive(t, stopped)
	waitFor(t, func() bool { return !c.Workers()[0].Alive })
	newer := register(t, client, "w", "new", addr)
	if newer == session {
		t.Fatal("session reused")
	}
	if _, err = client.Heartbeat(ctx, &pb.HeartbeatRequest{WorkerId: "w", SessionId: session}); status.Code(err) != codes.FailedPrecondition {
		t.Fatal("stale heartbeat accepted", err)
	}
	retry := receive(t, started)
	if retry.Start != first.Start || retry.End != first.End {
		t.Fatal("lost range")
	}
	c.Cancel(j.JobID)
}

func TestInvalidProofAndPrematureEOFDoNotCompleteRange(t *testing.T) {
	for _, invalid := range []bool{true, false} {
		t.Run(fmt.Sprint(invalid), func(t *testing.T) {
			c := newTestCoordinator(t)
			client := coordinatorClient(t, c)
			addr := controlled(t, func(r *pb.SearchRequest, s grpc.ServerStreamingServer[pb.SearchUpdate]) error {
				if invalid {
					return s.Send(&pb.SearchUpdate{AssignmentId: r.AssignmentId, Kind: pb.UpdateKind_FOUND, Nonce: r.Start, Checked: 1, Hash: "fabricated"})
				}
				return nil
			})
			register(t, client, "bad", "one", addr)
			j, _ := c.Submit("hello", 2)
			waitFor(t, func() bool { return !c.Workers()[0].Alive })
			v, _ := c.Get(j.JobID)
			if v.Status != "RUNNING" || v.Nonce != nil {
				t.Fatal(v)
			}
			c.mu.Lock()
			retry := append([]Range(nil), c.jobs[j.JobID].retry...)
			c.mu.Unlock()
			if len(retry) != 1 || retry[0].Start != 0 || retry[0].End != 1000 {
				t.Fatal(retry)
			}
		})
	}
}

func TestSSEMultipleSubscribersLateSubscriptionAndDisconnect(t *testing.T) {
	c := newTestCoordinator(t)
	j, _ := c.Submit("sse", 8)
	server := httptest.NewServer(c.Handler())
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "GET", server.URL+"/jobs/"+j.JobID+"/events", nil)
	a, err := server.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Body.Close()
	if a.Header.Get("Content-Type") != "text/event-stream" {
		t.Fatal(a.Header)
	}
	b, err := server.Client().Get(server.URL + "/jobs/" + j.JobID + "/events")
	if err != nil {
		t.Fatal(err)
	}
	defer b.Body.Close()
	// A deliberately unread subscriber must not prevent publication or cancellation.
	slow, unsub, _ := c.Subscribe(j.JobID)
	defer unsub()
	_ = slow
	c.mu.Lock()
	for i := 0; i < 1000; i++ {
		c.publishLocked(c.jobs[j.JobID])
	}
	c.mu.Unlock()
	if err = c.Cancel(j.JobID); err != nil {
		t.Fatal(err)
	}
	for _, body := range []io.Reader{a.Body, b.Body} {
		scanner := bufio.NewScanner(body)
		terminal := false
		for scanner.Scan() {
			if scanner.Text() == "event: cancelled" {
				terminal = true
			}
		}
		if scanner.Err() != nil {
			t.Fatal(scanner.Err())
		}
		if !terminal {
			t.Fatal("missing final event")
		}
	}
	late, err := server.Client().Get(server.URL + "/jobs/" + j.JobID + "/events")
	if err != nil {
		t.Fatal(err)
	}
	bytes, _ := io.ReadAll(late.Body)
	late.Body.Close()
	if !strings.Contains(string(bytes), "event: cancelled") {
		t.Fatal(string(bytes))
	}
	another, _ := c.Submit("disconnect", 8)
	req2, _ := http.NewRequestWithContext(ctx, "GET", server.URL+"/jobs/"+another.JobID+"/events", nil)
	r, err := server.Client().Do(req2)
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	r.Body.Close()
	waitFor(t, func() bool { c.mu.Lock(); defer c.mu.Unlock(); return len(c.jobs[another.JobID].subscribers) == 0 })
}

func TestSimultaneousSolutionsZeroNonceAndFrozenTerminalState(t *testing.T) {
	c := newTestCoordinator(t)
	client := coordinatorClient(t, c)
	ready := make(chan *pb.SearchRequest, 2)
	gate := make(chan struct{})
	data := ""
	for i := 0; ; i++ {
		data = fmt.Sprintf("zero%d", i)
		if pow.Valid(data, 0, 1) {
			break
		}
	}
	addr := controlled(t, func(r *pb.SearchRequest, s grpc.ServerStreamingServer[pb.SearchUpdate]) error {
		ready <- r
		select {
		case <-gate:
		case <-s.Context().Done():
			return s.Context().Err()
		}
		for n := r.Start; n < r.End; n++ {
			if pow.Valid(r.Data, n, int(r.Difficulty)) {
				return s.Send(&pb.SearchUpdate{AssignmentId: r.AssignmentId, Kind: pb.UpdateKind_FOUND, Nonce: n, Hash: pow.Hash(r.Data, n), Checked: n - r.Start + 1})
			}
		}
		return nil
	})
	register(t, client, "a", "a", addr)
	register(t, client, "b", "b", addr)
	j, _ := c.Submit(data, 1)
	receive(t, ready)
	receive(t, ready)
	close(gate)
	waitFor(t, func() bool { v, _ := c.Get(j.JobID); return v.Status == "FOUND" })
	waitFor(t, func() bool {
		for _, w := range c.Workers() {
			if w.Current != nil {
				return false
			}
		}
		return true
	})
	v, _ := c.Get(j.JobID)
	assertProof(t, v)
	again, _ := c.Get(j.JobID)
	if v.ElapsedSeconds != again.ElapsedSeconds || v.NoncesChecked != again.NoncesChecked {
		t.Fatal("terminal snapshot changed")
	}
	// Verify REST represents a valid nonce zero rather than omitting it.
	c2 := newTestCoordinator(t)
	liveWorker(t, c2, "zero")
	zero, _ := c2.Submit(data, 1)
	waitFor(t, func() bool { x, _ := c2.Get(zero.JobID); return x.Status == "FOUND" })
	response := httptest.NewRecorder()
	c2.Handler().ServeHTTP(response, httptest.NewRequest("GET", "/jobs/"+zero.JobID, nil))
	var obj map[string]any
	json.Unmarshal(response.Body.Bytes(), &obj)
	if obj["nonce"] != float64(0) {
		t.Fatal(response.Body)
	}
}

func TestOverflowAndCancelledLateResult(t *testing.T) {
	c := newTestCoordinator(t)
	j, _ := c.Submit("overflow", 8)
	c.mu.Lock()
	c.jobs[j.JobID].next = math.MaxUint64 - 4
	c.mu.Unlock()
	client := coordinatorClient(t, c)
	started := make(chan *pb.SearchRequest, 1)
	addr := controlled(t, func(r *pb.SearchRequest, s grpc.ServerStreamingServer[pb.SearchUpdate]) error {
		started <- r
		<-s.Context().Done()
		return s.Context().Err()
	})
	register(t, client, "edge", "edge", addr)
	r := receive(t, started)
	if r.End != math.MaxUint64 || r.Start != math.MaxUint64-4 {
		t.Fatal(r)
	}
	c.mu.Lock()
	a := c.workers["edge"].active
	c.mu.Unlock()
	c.Cancel(j.JobID)
	if done, e := c.accept(a, &pb.SearchUpdate{AssignmentId: r.AssignmentId, Kind: pb.UpdateKind_FOUND, Checked: 1}); !done || e != nil {
		t.Fatal(done, e)
	}
	v, _ := c.Get(j.JobID)
	if v.Status != "CANCELLED" {
		t.Fatal(v)
	}
}

func TestConcurrentRESTReads(t *testing.T) {
	c := newTestCoordinator(t)
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			j, e := c.Submit("reads", 8)
			if e != nil {
				t.Error(e)
				return
			}
			c.Jobs()
			c.Workers()
			c.Get(j.JobID)
			c.Cancel(j.JobID)
		}()
	}
	wg.Wait()
}

func TestVerifiedProofCancelsSiblingSearch(t *testing.T) {
	c := newTestCoordinator(t)
	client := coordinatorClient(t, c)
	started := make(chan struct{}, 1)
	stopped := make(chan struct{}, 1)
	addr := controlled(t, func(r *pb.SearchRequest, s grpc.ServerStreamingServer[pb.SearchUpdate]) error {
		started <- struct{}{}
		<-s.Context().Done()
		stopped <- struct{}{}
		return s.Context().Err()
	})
	register(t, client, "blocked", "one", addr)
	j, _ := c.Submit("hello", 2)
	receive(t, started)
	liveWorker(t, c, "solver")
	receive(t, stopped)
	v, _ := c.Get(j.JobID)
	assertProof(t, v)
	if *v.Nonce < 1000 {
		t.Fatal("proof did not come from sibling range")
	}
	w := httptest.NewRecorder()
	c.Handler().ServeHTTP(w, httptest.NewRequest("DELETE", "/jobs/"+j.JobID, nil))
	if w.Code != 409 {
		t.Fatal(w.Code)
	}
}

func TestAssignmentDeadlineRequeuesUnfinishedWork(t *testing.T) {
	c, err := New(Config{ChunkSize: 1000, HeartbeatTimeout: time.Second, AssignmentTimeout: 100 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	client := coordinatorClient(t, c)
	started := make(chan *pb.SearchRequest, 1)
	stopped := make(chan struct{}, 1)
	addr := controlled(t, func(r *pb.SearchRequest, s grpc.ServerStreamingServer[pb.SearchUpdate]) error {
		started <- r
		<-s.Context().Done()
		stopped <- struct{}{}
		return s.Context().Err()
	})
	session := register(t, client, "slow", "one", addr)
	j, _ := c.Submit("hello", 2)
	first := receive(t, started)
	c.mu.Lock()
	old := c.workers["slow"].active
	c.mu.Unlock()
	heartbeatCtx, heartbeatCancel := context.WithTimeout(context.Background(), time.Second)
	defer heartbeatCancel()
	if _, err := client.Heartbeat(heartbeatCtx, &pb.HeartbeatRequest{WorkerId: "slow", SessionId: session}); err != nil {
		t.Fatal("active assignment did not have a healthy lease", err)
	}
	receive(t, stopped)
	waitFor(t, func() bool { return !c.Workers()[0].Alive })
	c.mu.Lock()
	retry := append([]Range(nil), c.jobs[j.JobID].retry...)
	c.mu.Unlock()
	if len(retry) != 1 || retry[0].Start != first.Start || retry[0].End != first.End {
		t.Fatal("deadline skipped work", retry)
	}
	// With all workers gone the same job remains runnable, then resumes on registration.
	v, _ := c.Get(j.JobID)
	if v.Status != "RUNNING" {
		t.Fatal(v)
	}
	replacementStarted := make(chan *pb.SearchRequest, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	service := workerpkg.New(time.Millisecond)
	replacement := controlled(t, func(r *pb.SearchRequest, s grpc.ServerStreamingServer[pb.SearchUpdate]) error {
		replacementStarted <- r
		select {
		case <-release:
		case <-s.Context().Done():
			return s.Context().Err()
		}
		return service.Search(r, s)
	})
	register(t, client, "slow", "replacement", replacement)
	second := receive(t, replacementStarted)
	if second.Start != first.Start || second.End != first.End || second.AssignmentId == first.AssignmentId {
		t.Fatal("timeout reassignment was not fresh", first, second)
	}
	if done, err := c.accept(old, &pb.SearchUpdate{AssignmentId: first.AssignmentId, Checked: 1}); !done || err != nil {
		t.Fatal("stale assignment was not ignored", done, err)
	}
	if _, err := client.Heartbeat(heartbeatCtx, &pb.HeartbeatRequest{WorkerId: "slow", SessionId: session}); status.Code(err) != codes.FailedPrecondition {
		t.Fatal("stale timeout session accepted", err)
	}
	v, _ = c.Get(j.JobID)
	if v.NoncesChecked != 0 {
		t.Fatal("stale progress corrupted count", v)
	}
	releaseOnce.Do(func() { close(release) })
	waitFor(t, func() bool { v, _ := c.Get(j.JobID); return v.Status == "FOUND" })
	v, _ = c.Get(j.JobID)
	assertProof(t, v)
}

func TestShutdownCancelsActiveSearch(t *testing.T) {
	c := newTestCoordinator(t)
	client := coordinatorClient(t, c)
	started := make(chan struct{}, 1)
	stopped := make(chan struct{}, 1)
	addr := controlled(t, func(r *pb.SearchRequest, s grpc.ServerStreamingServer[pb.SearchUpdate]) error {
		started <- struct{}{}
		<-s.Context().Done()
		stopped <- struct{}{}
		return s.Context().Err()
	})
	register(t, client, "shutdown", "one", addr)
	j, _ := c.Submit("shutdown", 8)
	receive(t, started)
	c.Close()
	receive(t, stopped)
	v, _ := c.Get(j.JobID)
	if v.Status != "CANCELLED" {
		t.Fatal(v)
	}
}

func TestBusyHandoffKeepsHealthyWorkerSession(t *testing.T) {
	c := newTestCoordinator(t)
	client := coordinatorClient(t, c)
	started := make(chan struct{}, 1)
	rejected := make(chan struct{}, 4)
	release := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	var busy atomic.Bool
	service := workerpkg.New(time.Millisecond)
	addr := controlled(t, func(r *pb.SearchRequest, s grpc.ServerStreamingServer[pb.SearchUpdate]) error {
		if !busy.CompareAndSwap(false, true) {
			select {
			case rejected <- struct{}{}:
			default:
			}
			return status.Error(codes.ResourceExhausted, "previous handler still cleaning up")
		}
		defer busy.Store(false)
		if r.Data == "slow-cleanup" {
			started <- struct{}{}
			<-s.Context().Done()
			<-release
			return s.Context().Err()
		}
		return service.Search(r, s)
	})
	session := register(t, client, "healthy", "one", addr)
	a, _ := c.Submit("slow-cleanup", 8)
	receive(t, started)
	b, _ := c.Submit("hello", 1)
	c.Cancel(a.JobID)
	receive(t, rejected)
	waitFor(t, func() bool { return c.Workers()[0].Current == nil })
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := client.Heartbeat(ctx, &pb.HeartbeatRequest{WorkerId: "healthy", SessionId: session}); err != nil {
		t.Fatal("busy handoff invalidated a healthy session", err)
	}
	releaseOnce.Do(func() { close(release) })
	waitFor(t, func() bool { v, _ := c.Get(b.JobID); return v.Status == "FOUND" })
	v, _ := c.Get(b.JobID)
	assertProof(t, v)
}
