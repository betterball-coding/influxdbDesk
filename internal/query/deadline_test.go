package query

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/influxdesk/influxdesk/internal/transport"
)

type delayedBarrierDispatcher struct {
	inner *transport.Dispatcher
	delay time.Duration
}

func (d *delayedBarrierDispatcher) Dispatch(ctx context.Context, request transport.Request) (*transport.Response, error) {
	return d.inner.Dispatch(ctx, request)
}

func (d *delayedBarrierDispatcher) BeginRoundTrip(ctx context.Context, request transport.Request) (*transport.Attempt, error) {
	time.Sleep(d.delay)
	return d.inner.BeginRoundTrip(ctx, request)
}

func TestQueryDeadlineStartsAfterRoundTripBarrier(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{"results":[{"statement_id":0}]}`))
	}))
	defer server.Close()
	dispatcher, err := transport.NewDispatcher(transport.Config{BaseURL: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	defer dispatcher.CloseIdleConnections()
	service, err := NewService(&delayedBarrierDispatcher{inner: dispatcher, delay: 80 * time.Millisecond},
		ServiceOptions{QueryTimeout: 20 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	started, err := service.StartReadQuery(context.Background(), StartRequest{
		ProfileID: "profile", ProfileRevision: "1", ConnectionID: "connection", Generation: "1",
		Query: "SHOW DATABASES", ClientRequestID: uuid.NewString(),
	})
	if err != nil {
		t.Fatal(err)
	}
	terminal := waitTerminal(t, service, started.ID)
	if terminal.State != SessionSucceeded {
		t.Fatalf("pre-barrier wait consumed execution deadline: %+v", terminal)
	}
}

func TestQueryDeadlineCoversResponseBodyDecode(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		writer.WriteHeader(http.StatusOK)
		if flusher, ok := writer.(http.Flusher); ok {
			flusher.Flush()
		}
		time.Sleep(100 * time.Millisecond)
		_, _ = writer.Write([]byte(`{"results":[{"statement_id":0}]}`))
	}))
	defer server.Close()
	dispatcher, err := transport.NewDispatcher(transport.Config{BaseURL: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	defer dispatcher.CloseIdleConnections()
	service, err := NewService(dispatcher, ServiceOptions{QueryTimeout: 20 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	started, err := service.StartReadQuery(context.Background(), StartRequest{
		ProfileID: "profile", ProfileRevision: "1", ConnectionID: "connection", Generation: "1",
		Query: "SHOW DATABASES", ClientRequestID: uuid.NewString(),
	})
	if err != nil {
		t.Fatal(err)
	}
	terminal := waitTerminal(t, service, started.ID)
	if terminal.State != SessionTimedOut || terminal.PublicErrorCode != "QUERY_TIMEOUT" {
		t.Fatalf("body deadline was not enforced: %+v", terminal)
	}
}
