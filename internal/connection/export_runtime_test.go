package connection

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/influxdesk/influxdesk/internal/exportlane"
	"github.com/influxdesk/influxdesk/internal/profile"
	"github.com/influxdesk/influxdesk/internal/protection"
	"github.com/influxdesk/influxdesk/internal/scheduler"
	"github.com/influxdesk/influxdesk/internal/store"
	"github.com/influxdesk/influxdesk/internal/transport"
)

func TestExportRuntimeIsGenerationScopedAndInvalidatedByClose(t *testing.T) {
	manager, profileID := newExportRuntimeManager(t)
	ctx := context.Background()

	first, err := manager.Open(ctx, profileID)
	if err != nil {
		t.Fatal(err)
	}
	firstKey := exportlane.GenerationKey{
		ConnectionID: first.ConnectionID,
		Generation:   first.ConnectionGeneration,
	}
	firstDispatcher, err := manager.ExportDispatcher(firstKey)
	if err != nil {
		t.Fatal(err)
	}
	firstLanes, err := manager.ExportReadLanes(firstKey)
	if err != nil {
		t.Fatal(err)
	}
	if firstDispatcher == nil || firstLanes == nil {
		t.Fatal("active generation returned a nil export runtime")
	}
	if again, err := manager.ExportDispatcher(firstKey); err != nil || again != firstDispatcher {
		t.Fatalf("dispatcher was not stable within the generation: dispatcher=%p err=%v", again, err)
	}
	if again, err := manager.ExportReadLanes(firstKey); err != nil || again != firstLanes {
		t.Fatalf("read lanes were not stable within the generation: lanes=%p err=%v", again, err)
	}

	invalidKeys := []exportlane.GenerationKey{
		{},
		{ConnectionID: first.ConnectionID},
		{Generation: first.ConnectionGeneration},
		{ConnectionID: "another-connection", Generation: first.ConnectionGeneration},
		{ConnectionID: first.ConnectionID, Generation: "999"},
	}
	for _, key := range invalidKeys {
		if dispatcher, err := manager.ExportDispatcher(key); !errors.Is(err, ErrNotOpen) || dispatcher != nil {
			t.Fatalf("dispatcher accepted mismatched generation %+v: dispatcher=%p err=%v", key, dispatcher, err)
		}
		if lanes, err := manager.ExportReadLanes(key); !errors.Is(err, ErrNotOpen) || lanes != nil {
			t.Fatalf("lanes accepted mismatched generation %+v: lanes=%p err=%v", key, lanes, err)
		}
	}

	if err := manager.Close(ctx, profileID); err != nil {
		t.Fatal(err)
	}
	if dispatcher, err := manager.ExportDispatcher(firstKey); !errors.Is(err, ErrNotOpen) || dispatcher != nil {
		t.Fatalf("closed dispatcher provider remained live: dispatcher=%p err=%v", dispatcher, err)
	}
	if lanes, err := manager.ExportReadLanes(firstKey); !errors.Is(err, ErrNotOpen) || lanes != nil {
		t.Fatalf("closed lane provider remained live: lanes=%p err=%v", lanes, err)
	}

	second, err := manager.Open(ctx, profileID)
	if err != nil {
		t.Fatal(err)
	}
	secondKey := exportlane.GenerationKey{
		ConnectionID: second.ConnectionID,
		Generation:   second.ConnectionGeneration,
	}
	if secondKey == firstKey {
		t.Fatalf("connection generation was reused: %+v", secondKey)
	}
	if dispatcher, err := manager.ExportDispatcher(firstKey); !errors.Is(err, ErrNotOpen) || dispatcher != nil {
		t.Fatalf("old generation resolved through the reopened connection: dispatcher=%p err=%v", dispatcher, err)
	}
	if lanes, err := manager.ExportReadLanes(firstKey); !errors.Is(err, ErrNotOpen) || lanes != nil {
		t.Fatalf("old generation lanes resolved through the reopened connection: lanes=%p err=%v", lanes, err)
	}
	secondDispatcher, err := manager.ExportDispatcher(secondKey)
	if err != nil {
		t.Fatal(err)
	}
	secondLanes, err := manager.ExportReadLanes(secondKey)
	if err != nil {
		t.Fatal(err)
	}
	if secondDispatcher == firstDispatcher || secondLanes == firstLanes {
		t.Fatalf("reopened generation reused runtime: dispatcher=%t lanes=%t",
			secondDispatcher == firstDispatcher, secondLanes == firstLanes)
	}
}

func TestExportRuntimeReadBudgetIsFourPlusOnePerGeneration(t *testing.T) {
	manager, profileID := newExportRuntimeManager(t)
	opened, err := manager.Open(context.Background(), profileID)
	if err != nil {
		t.Fatal(err)
	}
	key := exportlane.GenerationKey{
		ConnectionID: opened.ConnectionID,
		Generation:   opened.ConnectionGeneration,
	}
	lanes, err := manager.ExportReadLanes(key)
	if err != nil {
		t.Fatal(err)
	}

	interactive := make([]*scheduler.Lease, 0, 4)
	for index := 0; index < 4; index++ {
		lease, err := lanes.Acquire(context.Background(), scheduler.InteractiveLane)
		if err != nil {
			t.Fatal(err)
		}
		interactive = append(interactive, lease)
	}
	transfer, err := lanes.Acquire(context.Background(), scheduler.TransferLane)
	if err != nil {
		t.Fatalf("interactive capacity borrowed the transfer slot: %v", err)
	}
	defer transfer.Release()
	for _, lease := range interactive {
		defer lease.Release()
	}

	snapshot := lanes.Snapshot()
	if snapshot.InteractiveInFlight != 4 || snapshot.TransferInFlight != 1 ||
		snapshot.InteractiveInFlight+snapshot.TransferInFlight != 5 {
		t.Fatalf("unexpected per-generation 4+1 budget: %+v", snapshot)
	}

	interactiveContext, cancelInteractive := context.WithCancel(context.Background())
	cancelInteractive()
	if lease, err := lanes.Acquire(interactiveContext, scheduler.InteractiveLane); !errors.Is(err, context.Canceled) || lease != nil {
		t.Fatalf("fifth interactive request exceeded its lane: lease=%p err=%v", lease, err)
	}
	transferContext, cancelTransfer := context.WithCancel(context.Background())
	cancelTransfer()
	if lease, err := lanes.Acquire(transferContext, scheduler.TransferLane); !errors.Is(err, context.Canceled) || lease != nil {
		t.Fatalf("second export request exceeded TransferLane=1: lease=%p err=%v", lease, err)
	}
	if got := lanes.Snapshot(); got.InteractiveInFlight != 4 || got.TransferInFlight != 1 ||
		got.InteractiveWaiting != 0 || got.TransferWaiting != 0 {
		t.Fatalf("canceled overflow requests changed the active budget: %+v", got)
	}
}

func newExportRuntimeManager(t *testing.T) (*Manager, string) {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("X-Influxdb-Version", "1.12.4")
		switch request.URL.Path {
		case "/ping":
			writer.WriteHeader(http.StatusNoContent)
		case "/query":
			writer.Header().Set("Content-Type", "application/json")
			_, _ = writer.Write([]byte(`{"results":[{"statement_id":0,"series":[{"name":"databases","columns":["name"],"values":[["db"]]}]}]}`))
		default:
			writer.WriteHeader(http.StatusNotFound)
		}
	}))
	database, err := store.Open(context.Background(), filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		server.Close()
		t.Fatal(err)
	}
	profiles := profile.NewRepository(database, time.Now)
	created, err := profiles.Save(context.Background(), profile.SaveRequest{
		Name: "export-runtime", BaseURL: server.URL,
		Environment: profile.EnvironmentDevelopment, AuthMode: transport.AuthNone,
		ProtectionMode: protection.ProtectedLocked,
	})
	if err != nil {
		_ = database.Close()
		server.Close()
		t.Fatal(err)
	}
	manager := NewManager(profiles, fakeCredentials{}, protection.NewManager(database, time.Now, nil), time.Now)
	t.Cleanup(func() {
		_ = manager.CloseAll(context.Background())
		_ = database.Close()
		server.Close()
	})
	return manager, created.ID
}
