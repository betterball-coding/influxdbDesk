package transport

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"io"
	"net"
	"time"
)

// Request is sealed so callers cannot choose arbitrary methods or endpoint
// paths. New network operations must be added explicitly in this package.
type Request interface {
	transportRequest()
}

type PingRequest struct{}

func (PingRequest) transportRequest() {}

// ShowDatabasesRequest is the only query accepted by a connection probe.
type ShowDatabasesRequest struct{}

func (ShowDatabasesRequest) transportRequest() {}

// AuthorizedReadQuery always uses chunked=true, chunk_size=5000, epoch=ns and
// pretty=false. Query authorization happens before constructing this value.
type AuthorizedReadQuery struct {
	Database        string
	RetentionPolicy string
	Query           string
}

func (AuthorizedReadQuery) transportRequest() {}

// ReadQueryRequest is retained as a source-compatible name for callers.
type ReadQueryRequest = AuthorizedReadQuery

// AuthorizedMutationQuery represents one authorized mutating statement.
type AuthorizedMutationQuery struct {
	Database        string
	RetentionPolicy string
	Query           string
}

func (AuthorizedMutationQuery) transportRequest() {}

type MutationQueryRequest = AuthorizedMutationQuery

// AuthorizedWriteBatch writes canonical line protocol with precision=ns.
type AuthorizedWriteBatch struct {
	Database        string
	RetentionPolicy string
	Payload         []byte
}

func (AuthorizedWriteBatch) transportRequest() {}

type WriteBatchRequest = AuthorizedWriteBatch

type AuthMode string

const (
	AuthNone   AuthMode = "NONE"
	AuthBasic  AuthMode = "BASIC"
	AuthBearer AuthMode = "BEARER"
)

type AuthConfig struct {
	Mode     AuthMode
	Username string
	Password string
	Token    string
}

type ProxyConfig struct {
	URL      string
	Username string
	Password string
}

// Config is an immutable connection snapshot used to construct a Dispatcher.
// DialContext is the integration point for a single-hop SSH tunnel.
type Config struct {
	BaseURL           string
	Auth              AuthConfig
	Proxy             *ProxyConfig
	RootCAs           *x509.CertPool
	ClientCertificate *tls.Certificate
	ServerName        string
	DialContext       func(context.Context, string, string) (net.Conn, error)
	ConnectTimeout    time.Duration
	KeepAlive         time.Duration
	MaxIdleConns      int
	MaxIdleConnsHost  int
}

// Response intentionally does not expose http.Client or http.Response.
// Callers must close Body on every successful Dispatch.
type Response struct {
	StatusCode    int
	Header        map[string][]string
	Body          io.ReadCloser
	ContentLength int64
}

type Attempt struct {
	result <-chan attemptResult
}

type attemptResult struct {
	response *Response
	err      error
}

func (a *Attempt) Wait() (*Response, error) {
	if a == nil || a.result == nil {
		return nil, ErrInvalidRequest
	}
	result := <-a.result
	return result.response, result.err
}

func (r *Response) Close() error {
	if r == nil || r.Body == nil {
		return nil
	}
	return r.Body.Close()
}
