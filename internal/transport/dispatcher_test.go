package transport

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestDispatcherBuildsFixedReadQuery(t *testing.T) {
	t.Parallel()

	requestSeen := make(chan *http.Request, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		captured := r.Clone(r.Context())
		captured.Body = io.NopCloser(strings.NewReader(string(body)))
		requestSeen <- captured
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"results":[{"statement_id":0}]}`)
	}))
	defer server.Close()

	dispatcher, err := NewDispatcher(Config{
		BaseURL: server.URL + "/influx",
		Auth:    AuthConfig{Mode: AuthBearer, Token: "secret"},
	})
	if err != nil {
		t.Fatalf("NewDispatcher() error = %v", err)
	}
	response, err := dispatcher.Dispatch(context.Background(), ReadQueryRequest{
		Database:        "metrics",
		RetentionPolicy: "autogen",
		Query:           `SELECT value FROM cpu`,
	})
	if err != nil {
		t.Fatalf("Dispatch() error = %v", err)
	}
	defer response.Close()

	request := <-requestSeen
	if request.Method != http.MethodPost || request.URL.Path != "/influx/query" {
		t.Fatalf("request = %s %s", request.Method, request.URL.Path)
	}
	if request.URL.RawQuery != "" {
		t.Fatalf("query text leaked into URL: %q", request.URL.RawQuery)
	}
	if got := request.Header.Get("Authorization"); got != "Bearer secret" {
		t.Fatalf("authorization = %q", got)
	}
	body, _ := io.ReadAll(request.Body)
	values, err := url.ParseQuery(string(body))
	if err != nil {
		t.Fatalf("ParseQuery() error = %v", err)
	}
	want := map[string]string{
		"db": "metrics", "rp": "autogen", "q": `SELECT value FROM cpu`,
		"chunked": "true", "chunk_size": "5000", "epoch": "ns", "pretty": "false",
	}
	for key, value := range want {
		if got := values.Get(key); got != value {
			t.Errorf("form[%s] = %q, want %q", key, got, value)
		}
	}
}

func TestDispatcherNeverFollowsRedirect(t *testing.T) {
	t.Parallel()

	var targetHits atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		targetHits.Add(1)
	}))
	defer target.Close()
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Location", target.URL+"/query")
		w.WriteHeader(http.StatusTemporaryRedirect)
	}))
	defer source.Close()

	dispatcher, err := NewDispatcher(Config{
		BaseURL: source.URL,
		Auth:    AuthConfig{Mode: AuthBasic, Username: "user", Password: "pass"},
	})
	if err != nil {
		t.Fatalf("NewDispatcher() error = %v", err)
	}
	response, err := dispatcher.Dispatch(context.Background(), ShowDatabasesRequest{})
	if err != nil {
		t.Fatalf("Dispatch() error = %v", err)
	}
	defer response.Close()
	if response.StatusCode != http.StatusTemporaryRedirect {
		t.Fatalf("status = %d, want %d", response.StatusCode, http.StatusTemporaryRedirect)
	}
	if targetHits.Load() != 0 {
		t.Fatal("redirect target was contacted")
	}
}

func TestDispatcherRechecksReadQueryClassification(t *testing.T) {
	t.Parallel()

	var hits atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		hits.Add(1)
	}))
	defer server.Close()
	dispatcher, err := NewDispatcher(Config{BaseURL: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	_, err = dispatcher.Dispatch(context.Background(), ReadQueryRequest{Query: `SELECT value INTO archive FROM cpu`})
	if !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("Dispatch() error = %v, want ErrInvalidRequest", err)
	}
	if hits.Load() != 0 {
		t.Fatal("misclassified read request reached the network")
	}
}

func TestDispatcherWritesNanosecondPrecision(t *testing.T) {
	t.Parallel()

	requestSeen := make(chan *http.Request, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestSeen <- r.Clone(r.Context())
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	dispatcher, err := NewDispatcher(Config{BaseURL: server.URL})
	if err != nil {
		t.Fatalf("NewDispatcher() error = %v", err)
	}
	response, err := dispatcher.Dispatch(context.Background(), WriteBatchRequest{
		Database: "metrics",
		Payload:  []byte("cpu value=9007199254740993i 1700000000000000001\n"),
	})
	if err != nil {
		t.Fatalf("Dispatch() error = %v", err)
	}
	defer response.Close()
	request := <-requestSeen
	if request.URL.Path != "/write" || request.URL.Query().Get("precision") != "ns" {
		t.Fatalf("write URL = %s", request.URL.String())
	}
}

func TestNewDispatcherRejectsUnsafeURLs(t *testing.T) {
	t.Parallel()

	for _, raw := range []string{
		"https://user:pass@example.com",
		"ftp://example.com",
		"https://example.com/base/../admin",
		"https://example.com/base?token=x",
		" https://example.com",
	} {
		t.Run(raw, func(t *testing.T) {
			if _, err := NewDispatcher(Config{BaseURL: raw}); err == nil {
				t.Fatalf("NewDispatcher(%q) unexpectedly succeeded", raw)
			}
		})
	}
}

func TestNewDispatcherRequiresConsentForAuthenticatedRemoteHTTP(t *testing.T) {
	t.Parallel()

	auth := AuthConfig{Mode: AuthBasic, Username: "operator", Password: "secret"}
	if _, err := NewDispatcher(Config{BaseURL: "http://192.0.2.10:8086", Auth: auth}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("authenticated remote HTTP error = %v, want ErrInvalidConfig", err)
	}
	allowed, err := NewDispatcher(Config{
		BaseURL: "http://192.0.2.10:8086", Auth: auth, AllowInsecureAuth: true,
	})
	if err != nil {
		t.Fatalf("explicitly allowed authenticated HTTP error = %v", err)
	}
	allowed.CloseIdleConnections()

	for _, baseURL := range []string{
		"http://127.0.0.1:8086", "http://[::1]:8086", "http://localhost:8086", "https://influx.example.test:8086",
	} {
		dispatcher, err := NewDispatcher(Config{BaseURL: baseURL, Auth: auth})
		if err != nil {
			t.Errorf("safe authenticated endpoint %q error = %v", baseURL, err)
			continue
		}
		dispatcher.CloseIdleConnections()
	}

	unauthenticated, err := NewDispatcher(Config{BaseURL: "http://192.0.2.10:8086"})
	if err != nil {
		t.Fatalf("unauthenticated HTTP error = %v", err)
	}
	unauthenticated.CloseIdleConnections()
}

func TestBeginRoundTripCrossesBarrierBeforeResponseHeaders(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		close(entered)
		<-release
		writer.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	dispatcher, err := NewDispatcher(Config{BaseURL: server.URL, Auth: AuthConfig{Mode: AuthNone}})
	if err != nil {
		t.Fatal(err)
	}
	attempt, err := dispatcher.BeginRoundTrip(context.Background(), PingRequest{})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("server did not observe RoundTrip entry")
	}
	close(release)
	response, err := attempt.Wait()
	if err != nil {
		t.Fatal(err)
	}
	defer response.Close()
	if response.StatusCode != http.StatusNoContent {
		t.Fatalf("status=%d", response.StatusCode)
	}
}

func TestBeginRoundTripCancellationCannotLateStartTransport(t *testing.T) {
	beforeStart := make(chan struct{})
	releaseStart := make(chan struct{})
	var baseCalls atomic.Int32

	dispatcher, err := NewDispatcher(Config{BaseURL: "http://127.0.0.1:1"})
	if err != nil {
		t.Fatal(err)
	}
	dispatcher.client.Transport = &startSignalingRoundTripper{
		beforeStart: func() {
			close(beforeStart)
			<-releaseStart
		},
		base: roundTripperFunc(func(*http.Request) (*http.Response, error) {
			baseCalls.Add(1)
			return nil, errors.New("base transport must not be called")
		}),
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := dispatcher.BeginRoundTrip(ctx, PingRequest{})
		done <- err
	}()
	select {
	case <-beforeStart:
	case <-time.After(time.Second):
		t.Fatal("transport did not reach the pre-start hook")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("BeginRoundTrip() error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("BeginRoundTrip did not return after cancellation")
	}
	close(releaseStart)
	time.Sleep(10 * time.Millisecond)
	if baseCalls.Load() != 0 {
		t.Fatalf("base RoundTrip calls = %d, want 0", baseCalls.Load())
	}
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}
