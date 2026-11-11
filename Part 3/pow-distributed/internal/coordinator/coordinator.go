// Package coordinator owns in-memory jobs, worker leases and nonce scheduling.
package coordinator

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net"
	"sort"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"pow-distributed/internal/pow"
	pb "pow-distributed/proto"
)

type Config struct {
	ChunkSize         uint64
	HeartbeatTimeout  time.Duration
	AssignmentTimeout time.Duration
}
type JobView struct {
	JobID          string  `json:"job_id"`
	Data           string  `json:"data"`
	Difficulty     int     `json:"difficulty"`
	Status         string  `json:"status"`
	NoncesChecked  uint64  `json:"nonces_checked"`
	ElapsedSeconds float64 `json:"elapsed_seconds"`
	Nonce          *uint64 `json:"nonce,omitempty"`
	Hash           string  `json:"hash,omitempty"`
}
type Range struct {
	Start uint64 `json:"start"`
	End   uint64 `json:"end"`
}
type AssignmentView struct {
	AssignmentID string `json:"assignment_id"`
	JobID        string `json:"job_id"`
	Range
}
type WorkerView struct {
	WorkerID      string          `json:"worker_id"`
	Address       string          `json:"address"`
	Alive         bool            `json:"alive"`
	LastHeartbeat time.Time       `json:"last_heartbeat"`
	Current       *AssignmentView `json:"current_assignment,omitempty"`
}
type job struct {
	view              JobView
	started, finished time.Time
	next              uint64
	retry             []Range
	subscribers       map[chan JobView]struct{}
}
type worker struct {
	view              WorkerView
	instance, session string
	active            *assignment
	availableAfter    time.Time
}
type assignment struct {
	view    AssignmentView
	worker  *worker
	job     *job
	checked uint64
	cancel  context.CancelFunc
}
type Coordinator struct {
	pb.UnimplementedCoordinatorServer
	mu      sync.Mutex
	cfg     Config
	jobs    map[string]*job
	order   []string
	cursor  int
	workers map[string]*worker
	ctx     context.Context
	cancel  context.CancelFunc
	wake    chan struct{}
	wg      sync.WaitGroup
	closed  bool
}

func New(cfg Config) (*Coordinator, error) {
	if cfg.ChunkSize == 0 || cfg.HeartbeatTimeout <= 0 || cfg.AssignmentTimeout <= 0 {
		return nil, errors.New("chunk size and timeouts must be positive")
	}
	ctx, cancel := context.WithCancel(context.Background())
	c := &Coordinator{cfg: cfg, jobs: make(map[string]*job), workers: make(map[string]*worker), ctx: ctx, cancel: cancel, wake: make(chan struct{}, 1)}
	c.wg.Add(1)
	go c.loop()
	return c, nil
}
func identifier() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b[:])
}
func (c *Coordinator) signal() {
	select {
	case c.wake <- struct{}{}:
	default:
	}
}
func (c *Coordinator) Close() {
	c.mu.Lock()
	if !c.closed {
		c.closed = true
		c.cancel()
		for _, j := range c.jobs {
			if j.view.Status == "RUNNING" {
				c.finishLocked(j, "CANCELLED")
			}
		}
	}
	c.mu.Unlock()
	c.wg.Wait()
}
func (c *Coordinator) RegisterWorker(_ context.Context, r *pb.RegisterRequest) (*pb.RegisterResponse, error) {
	if r.WorkerId == "" || r.InstanceId == "" {
		return nil, status.Error(codes.InvalidArgument, "worker and instance IDs required")
	}
	host, port, err := net.SplitHostPort(r.Address)
	if err != nil || host == "" || port == "" {
		return nil, status.Error(codes.InvalidArgument, "address must be host:port")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, status.Error(codes.Unavailable, "coordinator shutting down")
	}
	if old := c.workers[r.WorkerId]; old != nil {
		if old.view.Alive && time.Since(old.view.LastHeartbeat) > c.cfg.HeartbeatTimeout {
			c.deadLocked(old, "registration after lease expiry")
		}
		if old.view.Alive {
			if old.instance == r.InstanceId && old.view.Address == r.Address {
				return &pb.RegisterResponse{SessionId: old.session}, nil
			}
			return nil, status.Error(codes.AlreadyExists, "worker ID already has a live session")
		}
	}
	w := &worker{view: WorkerView{WorkerID: r.WorkerId, Address: r.Address, Alive: true, LastHeartbeat: time.Now()}, instance: r.InstanceId, session: identifier()}
	c.workers[r.WorkerId] = w
	c.signal()
	slog.Info("worker registered", "worker", r.WorkerId, "address", r.Address)
	return &pb.RegisterResponse{SessionId: w.session}, nil
}
func (c *Coordinator) Heartbeat(_ context.Context, r *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	w := c.workers[r.WorkerId]
	if c.closed || w == nil || !w.view.Alive || w.session != r.SessionId {
		return nil, status.Error(codes.FailedPrecondition, "register a new session")
	}
	if time.Since(w.view.LastHeartbeat) > c.cfg.HeartbeatTimeout {
		c.deadLocked(w, "heartbeat timeout")
		return nil, status.Error(codes.FailedPrecondition, "lease expired; register again")
	}
	w.view.LastHeartbeat = time.Now()
	return &pb.HeartbeatResponse{}, nil
}
func (c *Coordinator) Submit(data string, difficulty int) (JobView, error) {
	if data == "" || difficulty < 1 || difficulty > 8 {
		return JobView{}, errors.New("nonempty data and integer difficulty 1..8 required")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return JobView{}, errors.New("coordinator closed")
	}
	j := &job{view: JobView{JobID: identifier(), Data: data, Difficulty: difficulty, Status: "RUNNING"}, started: time.Now(), subscribers: make(map[chan JobView]struct{})}
	c.jobs[j.view.JobID] = j
	c.order = append(c.order, j.view.JobID)
	c.signal()
	return c.snapshotLocked(j), nil
}
func (c *Coordinator) snapshotLocked(j *job) JobView {
	v := j.view
	end := j.finished
	if end.IsZero() {
		end = time.Now()
	}
	v.ElapsedSeconds = end.Sub(j.started).Seconds()
	return v
}
func (c *Coordinator) Get(id string) (JobView, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	j, ok := c.jobs[id]
	if !ok {
		return JobView{}, false
	}
	return c.snapshotLocked(j), true
}
func (c *Coordinator) Jobs() []JobView {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]JobView, 0, len(c.jobs))
	for _, id := range c.order {
		out = append(out, c.snapshotLocked(c.jobs[id]))
	}
	return out
}
func (c *Coordinator) Workers() []WorkerView {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]WorkerView, 0, len(c.workers))
	for _, w := range c.workers {
		v := w.view
		if w.active != nil {
			a := w.active.view
			v.Current = &a
		}
		out = append(out, v)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].WorkerID < out[j].WorkerID })
	return out
}

var ErrNotFound = errors.New("job not found")
var ErrFinished = errors.New("job already terminal")

func (c *Coordinator) Cancel(id string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	j := c.jobs[id]
	if j == nil {
		return ErrNotFound
	}
	if j.view.Status != "RUNNING" {
		return ErrFinished
	}
	slog.Info("client cancelled job", "job", id)
	c.finishLocked(j, "CANCELLED")
	return nil
}
func (c *Coordinator) publishLocked(j *job) {
	v := c.snapshotLocked(j)
	for ch := range j.subscribers {
		select {
		case ch <- v:
		default:
			select {
			case <-ch:
			default:
			}
			select {
			case ch <- v:
			default:
			}
		}
	}
}
func (c *Coordinator) Subscribe(id string) (<-chan JobView, func(), bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	j := c.jobs[id]
	if j == nil {
		return nil, nil, false
	}
	ch := make(chan JobView, 1)
	j.subscribers[ch] = struct{}{}
	ch <- c.snapshotLocked(j)
	return ch, func() { c.mu.Lock(); delete(j.subscribers, ch); c.mu.Unlock() }, true
}
func (c *Coordinator) finishLocked(j *job, state string) {
	j.view.Status = state
	j.finished = time.Now()
	for _, w := range c.workers {
		if a := w.active; a != nil && a.job == j {
			a.cancel()
			slog.Info("cancel search", "job", j.view.JobID, "worker", w.view.WorkerID, "assignment", a.view.AssignmentID)
		}
	}
	c.publishLocked(j)
	c.signal()
}
func (c *Coordinator) deadLocked(w *worker, reason string) {
	w.view.Alive = false
	if a := w.active; a != nil {
		a.cancel()
		if a.job.view.Status == "RUNNING" {
			a.job.retry = append(a.job.retry, a.view.Range)
			slog.Warn("range requeued", "worker", w.view.WorkerID, "job", a.view.JobID, "start", a.view.Start, "end", a.view.End, "reason", reason)
		}
		w.active = nil
	}
	slog.Warn("worker unavailable", "worker", w.view.WorkerID, "reason", reason)
	c.signal()
}
func (c *Coordinator) loop() {
	defer c.wg.Done()
	tick := time.NewTicker(max(time.Millisecond, min(100*time.Millisecond, c.cfg.HeartbeatTimeout/3)))
	defer tick.Stop()
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-c.wake:
		case <-tick.C:
		}
		c.mu.Lock()
		if c.closed {
			c.mu.Unlock()
			return
		}
		for _, w := range c.workers {
			if w.view.Alive && time.Since(w.view.LastHeartbeat) > c.cfg.HeartbeatTimeout {
				c.deadLocked(w, "heartbeat timeout")
			}
		}
		for _, w := range c.workers {
			if !w.view.Alive || w.active != nil || time.Now().Before(w.availableAfter) {
				continue
			}
			for k := 0; k < len(c.order); k++ {
				j := c.jobs[c.order[c.cursor%len(c.order)]]
				c.cursor = (c.cursor + 1) % len(c.order)
				if j.view.Status != "RUNNING" || (len(j.retry) == 0 && j.next == math.MaxUint64) {
					continue
				}
				var r Range
				if len(j.retry) > 0 {
					r = j.retry[0]
					j.retry = j.retry[1:]
				} else {
					size := min(c.cfg.ChunkSize, math.MaxUint64-j.next)
					r = Range{Start: j.next, End: j.next + size}
					j.next = r.End
				}
				ctx, cancel := context.WithTimeout(c.ctx, c.cfg.AssignmentTimeout)
				a := &assignment{view: AssignmentView{AssignmentID: identifier(), JobID: j.view.JobID, Range: r}, worker: w, job: j, cancel: cancel}
				w.active = a
				c.wg.Add(1)
				go c.search(ctx, a)
				slog.Info("range assigned", "worker", w.view.WorkerID, "job", j.view.JobID, "assignment", a.view.AssignmentID, "start", r.Start, "end", r.End)
				break
			}
		}
		c.mu.Unlock()
	}
}
func (c *Coordinator) search(ctx context.Context, a *assignment) {
	defer c.wg.Done()
	defer a.cancel()
	conn, err := grpc.NewClient(a.worker.view.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		c.searchEnded(a, err)
		return
	}
	defer conn.Close()
	stream, err := pb.NewWorkerClient(conn).Search(ctx, &pb.SearchRequest{JobId: a.view.JobID, AssignmentId: a.view.AssignmentID, Data: a.job.view.Data, Difficulty: uint32(a.job.view.Difficulty), Start: a.view.Start, End: a.view.End})
	if err != nil {
		c.searchEnded(a, err)
		return
	}
	for {
		update, e := stream.Recv()
		if e != nil {
			c.searchEnded(a, e)
			return
		}
		done, e := c.accept(a, update)
		if e != nil {
			c.searchEnded(a, e)
			return
		}
		if done {
			c.searchEnded(a, nil)
			return
		}
	}
}
func (c *Coordinator) accept(a *assignment, u *pb.SearchUpdate) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	j, w := a.job, a.worker
	if w.active != a || c.workers[w.view.WorkerID] != w || j.view.Status != "RUNNING" {
		return true, nil
	}
	if u.AssignmentId != a.view.AssignmentID || u.Checked < a.checked || u.Checked > a.view.End-a.view.Start {
		return false, errors.New("invalid assignment/progress")
	}
	switch u.Kind {
	case pb.UpdateKind_PROGRESS:
	case pb.UpdateKind_EXHAUSTED:
		if u.Checked != a.view.End-a.view.Start {
			return false, errors.New("incomplete exhaustion")
		}
	case pb.UpdateKind_FOUND:
		if u.Nonce < a.view.Start || u.Nonce >= a.view.End || u.Checked != u.Nonce-a.view.Start+1 || !pow.Valid(j.view.Data, u.Nonce, j.view.Difficulty) || pow.Hash(j.view.Data, u.Nonce) != u.Hash {
			return false, errors.New("invalid proof")
		}
	default:
		return false, errors.New("unknown update kind")
	}
	delta := u.Checked - a.checked
	a.checked = u.Checked
	j.view.NoncesChecked += min(delta, math.MaxUint64-j.view.NoncesChecked)
	if u.Kind == pb.UpdateKind_FOUND {
		nonce := u.Nonce
		j.view.Nonce = &nonce
		j.view.Hash = u.Hash
		slog.Info("proof verified", "job", j.view.JobID, "worker", w.view.WorkerID, "nonce", nonce, "hash", u.Hash)
		c.finishLocked(j, "FOUND")
		return true, nil
	}
	c.publishLocked(j)
	return u.Kind == pb.UpdateKind_EXHAUSTED, nil
}
func (c *Coordinator) searchEnded(a *assignment, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	w := a.worker
	if w.active != a || c.workers[w.view.WorkerID] != w {
		return
	}
	if err != nil && a.job.view.Status == "RUNNING" {
		if status.Code(err) == codes.ResourceExhausted {
			// Client cancellation/final delivery can precede remote handler cleanup.
			// Busy is a bounded handoff retry, not evidence that the lease is dead.
			a.job.retry = append(a.job.retry, a.view.Range)
			w.active = nil
			w.availableAfter = time.Now().Add(100 * time.Millisecond)
			slog.Info("busy worker; range requeued", "worker", w.view.WorkerID, "job", a.view.JobID, "start", a.view.Start, "end", a.view.End)
			c.signal()
		} else {
			c.deadLocked(w, fmt.Sprintf("Search RPC: %v", err))
		}
	} else {
		w.active = nil
		c.signal()
	}
}
