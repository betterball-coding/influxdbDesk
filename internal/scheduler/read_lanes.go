package scheduler

import (
	"context"
	"errors"
	"sync"
)

type ReadLane string

const (
	InteractiveLane ReadLane = "INTERACTIVE"
	TransferLane    ReadLane = "TRANSFER"
)

var (
	ErrQueryQueueFull = errors.New("QUERY_QUEUE_FULL")
	ErrInvalidLane    = errors.New("INVALID_READ_LANE")
)

const (
	interactiveLimit = 4
	interactiveQueue = 32
	transferLimit    = 1
)

type waiter struct {
	ready chan struct{}
	owned bool
}

type laneState struct {
	limit      int
	queueLimit int
	inFlight   int
	waiters    []*waiter
}

// ReadLanes owns the independent per-generation interactive and transfer budgets.
type ReadLanes struct {
	mu          sync.Mutex
	interactive laneState
	transfer    laneState
}

type Lease struct {
	once  sync.Once
	owner *ReadLanes
	lane  ReadLane
}

type Snapshot struct {
	InteractiveInFlight int
	InteractiveWaiting  int
	TransferInFlight    int
	TransferWaiting     int
}

func NewReadLanes() *ReadLanes {
	return &ReadLanes{
		interactive: laneState{limit: interactiveLimit, queueLimit: interactiveQueue},
		transfer:    laneState{limit: transferLimit, queueLimit: -1},
	}
}

func (l *ReadLanes) Acquire(ctx context.Context, lane ReadLane) (*Lease, error) {
	if ctx == nil {
		return nil, context.Canceled
	}
	l.mu.Lock()
	state, err := l.state(lane)
	if err != nil {
		l.mu.Unlock()
		return nil, err
	}
	if len(state.waiters) == 0 && state.inFlight < state.limit {
		state.inFlight++
		l.mu.Unlock()
		return &Lease{owner: l, lane: lane}, nil
	}
	if state.queueLimit >= 0 && len(state.waiters) >= state.queueLimit {
		l.mu.Unlock()
		return nil, ErrQueryQueueFull
	}
	w := &waiter{ready: make(chan struct{})}
	state.waiters = append(state.waiters, w)
	l.mu.Unlock()

	select {
	case <-w.ready:
		return &Lease{owner: l, lane: lane}, nil
	case <-ctx.Done():
		l.mu.Lock()
		if !w.owned {
			state, _ = l.state(lane)
			for i, candidate := range state.waiters {
				if candidate == w {
					state.waiters = append(state.waiters[:i], state.waiters[i+1:]...)
					break
				}
			}
			l.mu.Unlock()
			return nil, ctx.Err()
		}
		l.mu.Unlock()
		// Ownership won the race with cancellation. Return a lease so its caller
		// can release the slot through the normal worker cleanup path.
		return &Lease{owner: l, lane: lane}, nil
	}
}

func (l *Lease) Release() {
	if l == nil || l.owner == nil {
		return
	}
	l.once.Do(func() { l.owner.release(l.lane) })
}

func (l *ReadLanes) Snapshot() Snapshot {
	l.mu.Lock()
	defer l.mu.Unlock()
	return Snapshot{
		InteractiveInFlight: l.interactive.inFlight,
		InteractiveWaiting:  len(l.interactive.waiters),
		TransferInFlight:    l.transfer.inFlight,
		TransferWaiting:     len(l.transfer.waiters),
	}
}

func (l *ReadLanes) release(lane ReadLane) {
	l.mu.Lock()
	defer l.mu.Unlock()
	state, err := l.state(lane)
	if err != nil || state.inFlight == 0 {
		return
	}
	if len(state.waiters) == 0 {
		state.inFlight--
		return
	}
	next := state.waiters[0]
	state.waiters = state.waiters[1:]
	next.owned = true
	close(next.ready)
}

func (l *ReadLanes) state(lane ReadLane) (*laneState, error) {
	switch lane {
	case InteractiveLane:
		return &l.interactive, nil
	case TransferLane:
		return &l.transfer, nil
	default:
		return nil, ErrInvalidLane
	}
}
