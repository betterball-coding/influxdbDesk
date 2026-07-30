package transport

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"path"
	"strings"
	"sync/atomic"
	"time"

	policy "github.com/influxdesk/influxdesk/internal/influxql"
)

const (
	queryPath = "/query"
	writePath = "/write"
	pingPath  = "/ping"
)

var (
	ErrInvalidConfig  = errors.New("invalid transport configuration")
	ErrInvalidRequest = errors.New("invalid transport request")
)

// Dispatcher is the sole HTTP network exit for InfluxDB traffic.
type Dispatcher struct {
	baseURL *url.URL
	auth    AuthConfig
	client  *http.Client
}

type startBarrierKey struct{}

const (
	startPending int32 = iota
	startEntered
	startAborted
)

type startBarrier struct {
	entered chan struct{}
	state   atomic.Int32
}

func (b *startBarrier) begin() bool {
	if !b.state.CompareAndSwap(startPending, startEntered) {
		return false
	}
	close(b.entered)
	return true
}

func (b *startBarrier) abort() bool {
	return b.state.CompareAndSwap(startPending, startAborted)
}

func (b *startBarrier) started() bool { return b.state.Load() == startEntered }

type startSignalingRoundTripper struct {
	base        http.RoundTripper
	beforeStart func()
}

func (r *startSignalingRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	if r.beforeStart != nil {
		r.beforeStart()
	}
	if barrier, ok := request.Context().Value(startBarrierKey{}).(*startBarrier); ok {
		if !barrier.begin() {
			return nil, request.Context().Err()
		}
	}
	return r.base.RoundTrip(request)
}

func NewDispatcher(config Config) (*Dispatcher, error) {
	baseURL, err := normalizeBaseURL(config.BaseURL)
	if err != nil {
		return nil, err
	}
	if err := validateAuth(config.Auth); err != nil {
		return nil, err
	}

	connectTimeout := config.ConnectTimeout
	if connectTimeout <= 0 {
		connectTimeout = 10 * time.Second
	}
	keepAlive := config.KeepAlive
	if keepAlive <= 0 {
		keepAlive = 30 * time.Second
	}
	dialContext := config.DialContext
	if dialContext == nil {
		dialer := &net.Dialer{Timeout: connectTimeout, KeepAlive: keepAlive}
		dialContext = dialer.DialContext
	}

	tlsConfig := &tls.Config{
		MinVersion: tls.VersionTLS12,
		ServerName: config.ServerName,
	}
	if config.RootCAs != nil {
		tlsConfig.RootCAs = config.RootCAs.Clone()
	}
	if config.ClientCertificate != nil {
		tlsConfig.Certificates = []tls.Certificate{cloneTLSCertificate(*config.ClientCertificate)}
	}

	maxIdle := config.MaxIdleConns
	if maxIdle <= 0 {
		maxIdle = 32
	}
	maxIdleHost := config.MaxIdleConnsHost
	if maxIdleHost <= 0 {
		maxIdleHost = 8
	}
	roundTripper := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           dialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          maxIdle,
		MaxIdleConnsPerHost:   maxIdleHost,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   connectTimeout,
		ExpectContinueTimeout: time.Second,
		ResponseHeaderTimeout: 0,
		TLSClientConfig:       tlsConfig,
	}
	if config.Proxy != nil {
		proxyURL, err := normalizeProxy(*config.Proxy)
		if err != nil {
			return nil, err
		}
		roundTripper.Proxy = http.ProxyURL(proxyURL)
	}

	return &Dispatcher{
		baseURL: baseURL,
		auth:    config.Auth,
		client: &http.Client{
			Transport: &startSignalingRoundTripper{base: roundTripper},
			Timeout:   0,
			CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}, nil
}

// Dispatch maps a sealed request to one of /ping, /query or /write.
func (d *Dispatcher) Dispatch(ctx context.Context, operation Request) (*Response, error) {
	attempt, err := d.BeginRoundTrip(ctx, operation)
	if err != nil {
		return nil, err
	}
	return attempt.Wait()
}

// BeginRoundTrip returns only after the configured transport's unique
// RoundTrip method has been entered. It does not wait for response headers.
func (d *Dispatcher) BeginRoundTrip(ctx context.Context, operation Request) (*Attempt, error) {
	if d == nil || d.client == nil || d.baseURL == nil {
		return nil, fmt.Errorf("%w: dispatcher is not initialized", ErrInvalidConfig)
	}
	if ctx == nil {
		return nil, fmt.Errorf("%w: nil context", ErrInvalidRequest)
	}

	barrier := &startBarrier{entered: make(chan struct{})}
	requestCtx := context.WithValue(ctx, startBarrierKey{}, barrier)
	request, err := d.buildRequest(requestCtx, operation)
	if err != nil {
		return nil, err
	}
	result := make(chan attemptResult, 1)
	go func() {
		response, err := d.client.Do(request)
		if err != nil {
			result <- attemptResult{err: err}
			return
		}
		result <- attemptResult{response: &Response{
			StatusCode: response.StatusCode, Header: map[string][]string(response.Header.Clone()),
			Body: response.Body, ContentLength: response.ContentLength,
		}}
	}()
	select {
	case <-barrier.entered:
		return &Attempt{result: result}, nil
	case completed := <-result:
		if barrier.started() {
			ready := make(chan attemptResult, 1)
			ready <- completed
			return &Attempt{result: ready}, nil
		}
		barrier.abort()
		if completed.response != nil {
			_ = completed.response.Close()
		}
		if completed.err == nil {
			completed.err = fmt.Errorf("%w: RoundTrip did not cross start barrier", ErrInvalidRequest)
		}
		return nil, completed.err
	case <-ctx.Done():
		if !barrier.abort() {
			return &Attempt{result: result}, nil
		}
		return nil, ctx.Err()
	}
}

func (d *Dispatcher) CloseIdleConnections() {
	if d != nil && d.client != nil {
		d.client.CloseIdleConnections()
	}
}

func (d *Dispatcher) buildRequest(ctx context.Context, operation Request) (*http.Request, error) {
	var (
		method      string
		endpoint    string
		contentType string
		body        *bytes.Reader
	)
	values := make(url.Values)

	switch request := operation.(type) {
	case PingRequest:
		method, endpoint, body = http.MethodGet, pingPath, bytes.NewReader(nil)
	case ShowDatabasesRequest:
		method, endpoint = http.MethodPost, queryPath
		values.Set("q", "SHOW DATABASES")
		values.Set("epoch", "ns")
		values.Set("pretty", "false")
		body = bytes.NewReader([]byte(values.Encode()))
		contentType = "application/x-www-form-urlencoded"
	case AuthorizedReadQuery:
		if strings.TrimSpace(request.Query) == "" {
			return nil, fmt.Errorf("%w: empty read query", ErrInvalidRequest)
		}
		analysis, err := policy.ParseAndClassify(request.Query)
		if err != nil || analysis.Classification != policy.ReadOnly {
			return nil, fmt.Errorf("%w: read query did not pass the AST allowlist", ErrInvalidRequest)
		}
		method, endpoint = http.MethodPost, queryPath
		setQueryValues(values, request.Database, request.RetentionPolicy, analysis.Canonical)
		values.Set("chunked", "true")
		values.Set("chunk_size", "5000")
		body = bytes.NewReader([]byte(values.Encode()))
		contentType = "application/x-www-form-urlencoded"
	case AuthorizedMutationQuery:
		if strings.TrimSpace(request.Query) == "" {
			return nil, fmt.Errorf("%w: empty mutation query", ErrInvalidRequest)
		}
		analysis, err := policy.ParseAndClassify(request.Query)
		if err != nil || analysis.Classification != policy.MayMutate {
			return nil, fmt.Errorf("%w: mutation query did not pass the AST policy", ErrInvalidRequest)
		}
		method, endpoint = http.MethodPost, queryPath
		setQueryValues(values, request.Database, request.RetentionPolicy, analysis.Canonical)
		body = bytes.NewReader([]byte(values.Encode()))
		contentType = "application/x-www-form-urlencoded"
	case AuthorizedWriteBatch:
		if request.Database == "" || len(request.Payload) == 0 {
			return nil, fmt.Errorf("%w: write database and payload are required", ErrInvalidRequest)
		}
		method, endpoint = http.MethodPost, writePath
		values.Set("db", request.Database)
		if request.RetentionPolicy != "" {
			values.Set("rp", request.RetentionPolicy)
		}
		values.Set("precision", "ns")
		body = bytes.NewReader(bytes.Clone(request.Payload))
		contentType = "text/plain; charset=utf-8"
	default:
		return nil, fmt.Errorf("%w: unsupported request type", ErrInvalidRequest)
	}

	target := d.endpointURL(endpoint)
	if endpoint == writePath {
		target.RawQuery = values.Encode()
	}
	httpRequest, err := http.NewRequestWithContext(ctx, method, target.String(), body)
	if err != nil {
		return nil, fmt.Errorf("%w: cannot construct request", ErrInvalidRequest)
	}
	if contentType != "" {
		httpRequest.Header.Set("Content-Type", contentType)
	}
	httpRequest.Header.Set("Accept", "application/json")
	d.applyAuth(httpRequest)
	return httpRequest, nil
}

func setQueryValues(values url.Values, database, retentionPolicy, query string) {
	if database != "" {
		values.Set("db", database)
	}
	if retentionPolicy != "" {
		values.Set("rp", retentionPolicy)
	}
	values.Set("q", query)
	values.Set("epoch", "ns")
	values.Set("pretty", "false")
}

func (d *Dispatcher) endpointURL(endpoint string) *url.URL {
	target := *d.baseURL
	target.Path = strings.TrimSuffix(d.baseURL.Path, "/") + endpoint
	target.RawPath = ""
	target.RawQuery = ""
	target.Fragment = ""
	return &target
}

func (d *Dispatcher) applyAuth(request *http.Request) {
	switch d.auth.Mode {
	case AuthBasic:
		request.SetBasicAuth(d.auth.Username, d.auth.Password)
	case AuthBearer:
		request.Header.Set("Authorization", "Bearer "+d.auth.Token)
	}
}

func normalizeBaseURL(raw string) (*url.URL, error) {
	if raw == "" || strings.TrimSpace(raw) != raw {
		return nil, fmt.Errorf("%w: base URL is empty or has surrounding whitespace", ErrInvalidConfig)
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("%w: malformed base URL", ErrInvalidConfig)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return nil, fmt.Errorf("%w: base URL scheme must be http or https", ErrInvalidConfig)
	}
	if parsed.Host == "" || parsed.Hostname() == "" {
		return nil, fmt.Errorf("%w: base URL host is required", ErrInvalidConfig)
	}
	if parsed.User != nil {
		return nil, fmt.Errorf("%w: URL userinfo is forbidden", ErrInvalidConfig)
	}
	if parsed.Opaque != "" || parsed.RawQuery != "" || parsed.Fragment != "" || parsed.RawPath != "" {
		return nil, fmt.Errorf("%w: base URL contains unsupported components", ErrInvalidConfig)
	}
	if strings.Contains(parsed.Path, "\\") {
		return nil, fmt.Errorf("%w: ambiguous base URL path", ErrInvalidConfig)
	}
	if parsed.Path == "" {
		return parsed, nil
	}
	trimmedPath := strings.TrimSuffix(parsed.Path, "/")
	if trimmedPath == "" {
		trimmedPath = "/"
	}
	if path.Clean(parsed.Path) != trimmedPath {
		return nil, fmt.Errorf("%w: base URL path must be canonical", ErrInvalidConfig)
	}
	if trimmedPath == "/" {
		parsed.Path = ""
	} else {
		parsed.Path = trimmedPath
	}
	return parsed, nil
}

func validateAuth(auth AuthConfig) error {
	if containsNewline(auth.Username) || containsNewline(auth.Password) || containsNewline(auth.Token) {
		return fmt.Errorf("%w: credential contains a newline", ErrInvalidConfig)
	}
	switch auth.Mode {
	case "", AuthNone:
		return nil
	case AuthBasic:
		if auth.Username == "" {
			return fmt.Errorf("%w: basic username is required", ErrInvalidConfig)
		}
	case AuthBearer:
		if auth.Token == "" {
			return fmt.Errorf("%w: bearer token is required", ErrInvalidConfig)
		}
	default:
		return fmt.Errorf("%w: unsupported authentication mode", ErrInvalidConfig)
	}
	return nil
}

func normalizeProxy(config ProxyConfig) (*url.URL, error) {
	parsed, err := url.Parse(config.URL)
	if err != nil || parsed.Host == "" || parsed.User != nil {
		return nil, fmt.Errorf("%w: malformed proxy URL or forbidden userinfo", ErrInvalidConfig)
	}
	switch parsed.Scheme {
	case "http", "https", "socks5", "socks5h":
	default:
		return nil, fmt.Errorf("%w: unsupported proxy scheme", ErrInvalidConfig)
	}
	if parsed.Path != "" || parsed.RawQuery != "" || parsed.Fragment != "" || parsed.Opaque != "" {
		return nil, fmt.Errorf("%w: proxy URL contains unsupported components", ErrInvalidConfig)
	}
	if containsNewline(config.Username) || containsNewline(config.Password) {
		return nil, fmt.Errorf("%w: proxy credential contains a newline", ErrInvalidConfig)
	}
	if config.Username == "" && config.Password != "" {
		return nil, fmt.Errorf("%w: proxy username is required", ErrInvalidConfig)
	}
	if config.Username == "" {
		return parsed, nil
	}
	// net/http converts proxy userinfo into Proxy-Authorization on the wire;
	// callers can never place credentials in the configured proxy URL.
	parsed.User = url.UserPassword(config.Username, config.Password)
	return parsed, nil
}

func containsNewline(value string) bool {
	return strings.ContainsAny(value, "\r\n")
}

func cloneTLSCertificate(source tls.Certificate) tls.Certificate {
	clone := source
	clone.Certificate = make([][]byte, len(source.Certificate))
	for i, certificate := range source.Certificate {
		clone.Certificate[i] = bytes.Clone(certificate)
	}
	clone.OCSPStaple = bytes.Clone(source.OCSPStaple)
	clone.SignedCertificateTimestamps = make([][]byte, len(source.SignedCertificateTimestamps))
	for i, timestamp := range source.SignedCertificateTimestamps {
		clone.SignedCertificateTimestamps[i] = bytes.Clone(timestamp)
	}
	return clone
}
