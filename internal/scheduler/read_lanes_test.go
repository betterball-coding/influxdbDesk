package scheduler

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestInteractiveQueueLimitAndIsolation(t *testing.T) {
	lanes := NewReadLanes()
	active := make([]*Lease, 0, 4)
	for i := 0; i < 4; i++ {
		lease, err := lanes.Acquire(context.Background(), InteractiveLane)
		if err != nil {
			t.Fatal(err)
		}
		active = append(active, lease)
	}

	contexts := make([]context.CancelFunc, 0, 32)
	for i := 0; i < 32; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		contexts = append(contexts, cancel)
		go func() {
			lease, _ := lanes.Acquire(ctx, InteractiveLane)
			if lease != nil {
				lease.Release()
			}
		}()
	}
	waitFor(t, func() bool { return lanes.Snapshot().InteractiveWaiting == 32 })
	if _, err := lanes.Acquire(context.Background(), InteractiveLane); !errors.Is(err, ErrQueryQueueFull) {
		t.Fatalf("expected queue full, got %v", err)
	}

	transfer, err := lanes.Acquire(context.Background(), TransferLane)
	if err != nil {
		t.Fatalf("interactive load borrowed transfer capacity: %v", err)
	}
	if snapshot := lanes.Snapshot(); snapshot.InteractiveInFlight+snapshot.TransferInFlight != 5 {
		t.Fatalf("unexpected total in flight: %+v", snapshot)
	}
	transfer.Release()
	for _, cancel := range contexts {
		cancel()
	}
	for _, lease := range active {
		lease.Release()
	}
}

func TestWaitingCancellationRemovesQueueEntry(t *testing.T) {
	lanes := NewReadLanes()
	lease, err := lanes.Acquire(context.Background(), TransferLane)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := lanes.Acquire(ctx, TransferLane)
		done <- err
	}()
	waitFor(t, func() bool { return lanes.Snapshot().TransferWaiting == 1 })
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("expected cancellation, got %v", err)
	}
	if snapshot := lanes.Snapshot(); snapshot.TransferWaiting != 0 || snapshot.TransferInFlight != 1 {
		t.Fatalf("unexpected snapshot after cancel: %+v", snapshot)
	}
	lease.Release()
}

func waitFor(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("condition did not become true")
		}
		time.Sleep(time.Millisecond)
	}
}
