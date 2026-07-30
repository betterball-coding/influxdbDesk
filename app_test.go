package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	influxql "github.com/influxdata/influxql"
	"github.com/influxdesk/influxdesk/internal/credential"
	"github.com/influxdesk/influxdesk/internal/exportjob"
	"github.com/influxdesk/influxdesk/internal/profile"
	"github.com/influxdesk/influxdesk/internal/protection"
	"github.com/influxdesk/influxdesk/internal/query"
	"github.com/influxdesk/influxdesk/internal/tasks"
	"github.com/influxdesk/influxdesk/internal/transfer"
	"github.com/influxdesk/influxdesk/internal/transport"
)

type emptyProfileCredentials struct{}

func (emptyProfileCredentials) Put(string, credential.Kind, []byte) error { return nil }
func (emptyProfileCredentials) Get(string, credential.Kind) ([]byte, error) {
	return nil, credential.ErrNotFound
}
func (emptyProfileCredentials) Delete(string, credential.Kind) error { return nil }
func (emptyProfileCredentials) DeleteProfile(string) error           { return nil }

func TestApplicationReadinessGateAndLocalInitialization(t *testing.T) {
	app := NewApp()
	if _, err := app.ListProfiles(); err != ErrApplicationNotReady {
		t.Fatalf("expected readiness gate, got %v", err)
	}
	ctx := context.Background()
	app.ctx = ctx
	if err := app.initialize(ctx, t.TempDir()); err != nil {
		t.Fatal(err)
	}
	if status := app.GetRuntimeStatus(); !status.Ready || status.StartupCode != "" {
		t.Fatalf("unexpected runtime status: %+v", status)
	}
	created, err := app.SaveProfile(SaveProfileInput{
		Name: "local", BaseURL: "http://127.0.0.1:8086",
		Environment: profile.EnvironmentDevelopment, AuthMode: transport.AuthNone,
		ProtectionMode: protection.PermanentReadOnly,
	})
	if err != nil || created.Revision != "1" {
		t.Fatalf("profile create failed: %+v %v", created, err)
	}
	app.shutdown(ctx)
	if status := app.GetRuntimeStatus(); status.Ready {
		t.Fatal("application remained ready after shutdown")
	}
}

func TestGetSchemaSnapshotLoadsAllMeasurementPages(t *testing.T) {
	var catalogQueries atomic.Int32
	catalogStarted := make(chan string, 2)
	releaseCatalog := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseCatalog) }) }
	defer release()
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("X-Influxdb-Version", "1.12.4")
		if request.URL.Path == "/ping" {
			writer.WriteHeader(http.StatusNoContent)
			return
		}
		if request.URL.Path != "/query" || request.ParseForm() != nil {
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		writer.Header().Set("Content-Type", "application/json")
		queryText := request.Form.Get("q")
		database := request.Form.Get("db")
		switch queryText {
		case "SHOW DATABASES":
			_ = json.NewEncoder(writer).Encode(map[string]any{
				"results": []any{map[string]any{
					"statement_id": 0,
					"series": []any{map[string]any{
						"name": "databases", "columns": []string{"name"},
						"values": [][]string{{"_internal"}, {"data_engine"}},
					}},
				}},
			})
		case "SHOW RETENTION POLICIES;\nSHOW MEASUREMENTS":
			catalogQueries.Add(1)
			catalogStarted <- database
			<-releaseCatalog
			measurementValues := [][]string{{"monitor"}}
			if database == "data_engine" {
				measurementValues = make([][]string, 5001)
				for index := range measurementValues {
					measurementValues[index] = []string{"measurement-" + strconv.Itoa(index)}
				}
			}
			encoder := json.NewEncoder(writer)
			_ = encoder.Encode(map[string]any{
				"results": []any{map[string]any{
					"statement_id": 0,
					"series": []any{map[string]any{
						"name": "retention policies", "columns": []string{"name"},
						"values": [][]string{{"autogen"}},
					}},
				}},
			})
			_ = encoder.Encode(map[string]any{
				"results": []any{map[string]any{
					"statement_id": 1,
					"series": []any{map[string]any{
						"name": "measurements", "columns": []string{"name"},
						"values": measurementValues,
					}},
				}},
			})
		default:
			writer.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer server.Close()

	app := NewApp()
	app.ctx = context.Background()
	if err := app.initialize(app.ctx, t.TempDir()); err != nil {
		t.Fatal(err)
	}
	defer app.shutdown(context.Background())
	created, err := app.SaveProfile(SaveProfileInput{
		Name: "schema", BaseURL: server.URL, Environment: profile.EnvironmentDevelopment,
		AuthMode: transport.AuthNone, ProtectionMode: protection.PermanentReadOnly,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := app.OpenConnection(created.ID); err != nil {
		t.Fatal(err)
	}

	type snapshotResult struct {
		snapshot []SchemaDatabase
		err      error
	}
	result := make(chan snapshotResult, 1)
	go func() {
		snapshot, err := app.GetSchemaSnapshot(created.ID)
		result <- snapshotResult{snapshot: snapshot, err: err}
	}()
	startedDatabases := make(map[string]struct{}, 2)
	for len(startedDatabases) < 2 {
		select {
		case database := <-catalogStarted:
			startedDatabases[database] = struct{}{}
		case <-time.After(5 * time.Second):
			release()
			t.Fatal("database catalog queries did not run concurrently")
		}
	}
	release()
	loaded := <-result
	if loaded.err != nil {
		t.Fatal(loaded.err)
	}
	snapshot := loaded.snapshot
	if len(snapshot) != 2 || catalogQueries.Load() != 2 {
		t.Fatalf("snapshot databases=%d catalog queries=%d", len(snapshot), catalogQueries.Load())
	}
	var dataEngine *SchemaDatabase
	for index := range snapshot {
		if snapshot[index].Name == "data_engine" {
			dataEngine = &snapshot[index]
			break
		}
	}
	if dataEngine == nil {
		t.Fatal("data_engine database is missing")
	}
	if len(dataEngine.RetentionPolicies) != 1 || dataEngine.RetentionPolicies[0] != "autogen" {
		t.Fatalf("retention policies = %v", dataEngine.RetentionPolicies)
	}
	if len(dataEngine.Measurements) != 5001 {
		t.Fatalf("measurements=%d, want 5001", len(dataEngine.Measurements))
	}
	foundLastPage := false
	for _, measurement := range dataEngine.Measurements {
		if measurement.Name == "measurement-5000" {
			foundLastPage = true
			break
		}
	}
	if !foundLastPage {
		t.Fatal("measurement from the second result page is missing")
	}
}

func TestSchemaQueriesShareApplicationWideConcurrencyLimit(t *testing.T) {
	var inFlight atomic.Int32
	var maxInFlight atomic.Int32
	started := make(chan struct{}, 3)
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("X-Influxdb-Version", "1.12.4")
		if request.URL.Path == "/ping" {
			writer.WriteHeader(http.StatusNoContent)
			return
		}
		if request.URL.Path != "/query" || request.ParseForm() != nil {
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		writer.Header().Set("Content-Type", "application/json")
		switch request.Form.Get("q") {
		case "SHOW DATABASES":
			_, _ = io.WriteString(writer, `{"results":[{"statement_id":0,"series":[{"name":"databases","columns":["name"],"values":[["metrics"]]}]}]}`)
		case "SHOW RETENTION POLICIES;\nSHOW MEASUREMENTS":
			current := inFlight.Add(1)
			for {
				observed := maxInFlight.Load()
				if current <= observed || maxInFlight.CompareAndSwap(observed, current) {
					break
				}
			}
			started <- struct{}{}
			<-release
			inFlight.Add(-1)
			_, _ = io.WriteString(writer, `{"results":[{"statement_id":0,"series":[{"name":"retention policies","columns":["name"],"values":[["autogen"]]}]}]}
{"results":[{"statement_id":1,"series":[{"name":"measurements","columns":["name"],"values":[["cpu"]]}]}]}`)
		default:
			writer.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer server.Close()

	app := NewApp()
	app.ctx = context.Background()
	if err := app.initialize(app.ctx, t.TempDir()); err != nil {
		t.Fatal(err)
	}
	defer app.shutdown(context.Background())
	created, err := app.SaveProfile(SaveProfileInput{
		Name: "schema-limit", BaseURL: server.URL, Environment: profile.EnvironmentDevelopment,
		AuthMode: transport.AuthNone, ProtectionMode: protection.PermanentReadOnly,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := app.OpenConnection(created.ID); err != nil {
		t.Fatal(err)
	}

	results := make(chan error, 3)
	for range 3 {
		go func() {
			_, err := app.GetSchemaSnapshot(created.ID)
			results <- err
		}()
	}
	for range 2 {
		select {
		case <-started:
		case <-time.After(5 * time.Second):
			close(release)
			t.Fatal("two schema queries did not start")
		}
	}
	select {
	case <-started:
		close(release)
		t.Fatal("a third schema query bypassed the shared two-slot limit")
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	for range 3 {
		select {
		case err := <-results:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("schema query did not finish after releasing slots")
		}
	}
	if maxInFlight.Load() != 2 {
		t.Fatalf("maximum concurrent schema queries=%d, want 2", maxInFlight.Load())
	}
}

func TestGetMeasurementSchemaUsesOnlyEscapedFieldAndTagKeyQueries(t *testing.T) {
	database := `metrics "west"\archive`
	measurement := `cpu"; SHOW SERIES\edge`
	type observedQuery struct {
		text     string
		database string
	}
	observed := make(chan observedQuery, 8)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("X-Influxdb-Version", "1.12.4")
		if request.URL.Path == "/ping" {
			writer.WriteHeader(http.StatusNoContent)
			return
		}
		if request.URL.Path != "/query" || request.ParseForm() != nil {
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		writer.Header().Set("Content-Type", "application/json")
		queryText := request.Form.Get("q")
		switch {
		case queryText == "SHOW DATABASES":
			_, _ = io.WriteString(writer, `{"results":[{"statement_id":0}]}`)
		case strings.HasPrefix(queryText, "SHOW FIELD KEYS") && strings.Contains(queryText, ";\nSHOW TAG KEYS"):
			observed <- observedQuery{text: queryText, database: request.Form.Get("db")}
			_, _ = io.WriteString(writer, `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["fieldKey","fieldType"],"values":[["zeta","float"],["alpha","integer"],["alpha","integer"]]}]}]}
{"results":[{"statement_id":1,"series":[{"name":"cpu","columns":["tagKey"],"values":[["zone"],["host"],["host"]]}]}]}`)
		default:
			writer.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer server.Close()

	app := NewApp()
	app.ctx = context.Background()
	if err := app.initialize(app.ctx, t.TempDir()); err != nil {
		t.Fatal(err)
	}
	defer app.shutdown(context.Background())
	created, err := app.SaveProfile(SaveProfileInput{
		Name: "measurement-schema", BaseURL: server.URL,
		Environment: profile.EnvironmentDevelopment, AuthMode: transport.AuthNone,
		ProtectionMode: protection.PermanentReadOnly,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := app.OpenConnection(created.ID); err != nil {
		t.Fatal(err)
	}

	detail, err := app.GetMeasurementSchema(created.ID, database, measurement)
	if err != nil {
		t.Fatal(err)
	}
	wantFields := []SchemaField{{Name: "alpha", Type: "integer"}, {Name: "zeta", Type: "float"}}
	if detail.Name != measurement || !reflect.DeepEqual(detail.Fields, wantFields) ||
		!reflect.DeepEqual(detail.Tags, []string{"host", "zone"}) {
		t.Fatalf("measurement detail = %+v", detail)
	}

	got := <-observed
	if got.database != database {
		t.Fatalf("query database=%q, want %q", got.database, database)
	}
	parsed, err := influxql.ParseQuery(got.text)
	if err != nil || len(parsed.Statements) != 2 {
		t.Fatalf("schema query did not contain exactly two statements: %q err=%v", got.text, err)
	}
	for index, statementType := range []any{
		(*influxql.ShowFieldKeysStatement)(nil), (*influxql.ShowTagKeysStatement)(nil),
	} {
		var statementDatabase string
		var sources influxql.Sources
		switch statement := parsed.Statements[index].(type) {
		case *influxql.ShowFieldKeysStatement:
			if _, ok := statementType.(*influxql.ShowFieldKeysStatement); !ok {
				t.Fatalf("query %d type=%T", index, statement)
			}
			statementDatabase, sources = statement.Database, statement.Sources
		case *influxql.ShowTagKeysStatement:
			if _, ok := statementType.(*influxql.ShowTagKeysStatement); !ok {
				t.Fatalf("query %d type=%T", index, statement)
			}
			statementDatabase, sources = statement.Database, statement.Sources
		default:
			t.Fatalf("query %d unexpectedly dispatched %T: %q", index, statement, got.text)
		}
		if statementDatabase != database || len(sources) != 1 {
			t.Fatalf("query %d database=%q sources=%v", index, statementDatabase, sources)
		}
		source, ok := sources[0].(*influxql.Measurement)
		if !ok || source.Name != measurement {
			t.Fatalf("query %d source=%#v, want measurement %q", index, sources[0], measurement)
		}
	}
	select {
	case extra := <-observed:
		t.Fatalf("unexpected second schema query: %q", extra.text)
	default:
	}
}

func TestSaveProfileRejectsChangesWhileConnectionGenerationIsOpen(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("X-Influxdb-Version", "1.12.4")
		if request.URL.Path == "/ping" {
			writer.WriteHeader(http.StatusNoContent)
			return
		}
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{"results":[{"statement_id":0}]}`))
	}))
	defer server.Close()
	app := NewApp()
	app.ctx = context.Background()
	if err := app.initialize(app.ctx, t.TempDir()); err != nil {
		t.Fatal(err)
	}
	defer app.shutdown(context.Background())
	created, err := app.SaveProfile(SaveProfileInput{
		Name: "active", BaseURL: server.URL, Environment: profile.EnvironmentDevelopment,
		AuthMode: transport.AuthNone, ProtectionMode: protection.ProtectedLocked,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := app.OpenConnection(created.ID); err != nil {
		t.Fatal(err)
	}
	_, err = app.SaveProfile(SaveProfileInput{
		ID: created.ID, ExpectedRevision: created.Revision, Name: "changed",
		BaseURL: server.URL, Environment: profile.EnvironmentDevelopment,
		AuthMode: transport.AuthNone, ProtectionMode: protection.ProtectedLocked,
	})
	if !errors.Is(err, profile.ErrProfileInUse) {
		t.Fatalf("SaveProfile error = %v", err)
	}
	unchanged, err := app.GetProfile(created.ID)
	if err != nil || unchanged.Name != "active" || unchanged.Revision != created.Revision {
		t.Fatalf("profile changed while open: %+v err=%v", unchanged, err)
	}
}

func TestAppCloseEditReconnectAndCloseDeleteProfileWorkflow(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("X-Influxdb-Version", "1.12.4")
		if request.URL.Path == "/ping" {
			writer.WriteHeader(http.StatusNoContent)
			return
		}
		if request.URL.Path != "/query" {
			writer.WriteHeader(http.StatusNotFound)
			return
		}
		writer.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(writer, `{"results":[{"statement_id":0}]}`)
	}))
	defer server.Close()

	app := NewApp()
	app.ctx = context.Background()
	if err := app.initialize(app.ctx, t.TempDir()); err != nil {
		t.Fatal(err)
	}
	defer app.shutdown(context.Background())
	created, err := app.SaveProfile(SaveProfileInput{
		Name: "editable", BaseURL: server.URL, DefaultDatabase: "metrics",
		Environment: profile.EnvironmentDevelopment, AuthMode: transport.AuthNone,
		ProtectionMode: protection.ProtectedLocked,
	})
	if err != nil || created.DefaultDatabase != "metrics" {
		t.Fatalf("created=%+v err=%v", created, err)
	}
	first, err := app.OpenConnection(created.ID)
	if err != nil || first.ConnectionGeneration != "1" || first.ProfileRevision != "1" {
		t.Fatalf("first connection=%+v err=%v", first, err)
	}

	edit := SaveProfileInput{
		ID: created.ID, ExpectedRevision: created.Revision, Name: "edited",
		BaseURL: server.URL, DefaultDatabase: "operations",
		Environment: profile.EnvironmentDevelopment, AuthMode: transport.AuthNone,
		ProtectionMode: protection.ProtectedLocked,
	}
	if _, err := app.SaveProfile(edit); !errors.Is(err, profile.ErrProfileInUse) {
		t.Fatalf("open profile edit error=%v", err)
	}
	if err := app.CloseConnection(created.ID); err != nil {
		t.Fatal(err)
	}
	edited, err := app.SaveProfile(edit)
	if err != nil || edited.Revision != "2" || edited.DefaultDatabase != "operations" {
		t.Fatalf("edited=%+v err=%v", edited, err)
	}
	second, err := app.OpenConnection(created.ID)
	if err != nil || second.ConnectionGeneration != "2" || second.ProfileRevision != "2" {
		t.Fatalf("second connection=%+v err=%v", second, err)
	}
	if err := app.DeleteProfile(created.ID, edited.Revision); !errors.Is(err, profile.ErrProfileInUse) {
		t.Fatalf("open profile delete error=%v", err)
	}
	if err := app.CloseConnection(created.ID); err != nil {
		t.Fatal(err)
	}
	if err := app.DeleteProfile(created.ID, created.Revision); !errors.Is(err, profile.ErrRevisionConflict) {
		t.Fatalf("stale delete error=%v", err)
	}
	if loaded, err := app.GetProfile(created.ID); err != nil || loaded.Revision != edited.Revision {
		t.Fatalf("failed delete removed profile: %+v err=%v", loaded, err)
	}

	// Non-Windows test builds do not expose Credential Manager. Substitute an
	// empty credential store only for the successful deletion boundary.
	app.mu.Lock()
	app.profiles = profile.NewService(profile.NewRepository(app.store, nil), emptyProfileCredentials{})
	app.mu.Unlock()
	if err := app.DeleteProfile(created.ID, edited.Revision); err != nil {
		t.Fatal(err)
	}
	if _, err := app.GetProfile(created.ID); !errors.Is(err, profile.ErrNotFound) {
		t.Fatalf("deleted profile lookup error=%v", err)
	}
}

func TestInitializeRecoversImportCleanupBeforeReady(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	first := NewApp()
	first.ctx = ctx
	if err := first.initialize(ctx, root); err != nil {
		t.Fatal(err)
	}
	job, _, err := first.transfers.CreateImport(ctx, transfer.CreateImportRequest{
		JobID: uuid.NewString(), ProfileID: uuid.NewString(),
		ClientScope: "PreflightImport\x1f" + uuid.NewString(), RequestDigest: "request-digest",
		LedgerExpiresAt: time.Now().Add(90 * 24 * time.Hour),
		InitialCheckpoint: transfer.Checkpoint{
			Sequence: "0", LogicalOffset: "0", AdaptiveMaxPoints: "5000", AdaptiveMaxBytes: "5242880",
			SourceSHA256: "source", StagingSHA256: "staging", NormalizationVersion: "canonical-lp-v1",
			SpecDigest: "spec", TargetDigest: "target",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	stagingPath := filepath.Join(root, "staging", "import-"+job.Task.ID+".canonical.lp")
	if err := os.WriteFile(stagingPath, []byte("cpu value=1i 1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	canceled, _, err := first.transfers.CancelImport(ctx, job.Task.ID, transfer.ImportCommandEnvelope{
		CommandRequestID: uuid.NewString(), ExpectedStateRevision: job.Task.StateRevision,
	})
	if err != nil {
		t.Fatal(err)
	}
	cleanupCommand := transfer.ImportCommandEnvelope{
		CommandRequestID: uuid.NewString(), ExpectedStateRevision: canceled.Task.StateRevision,
	}
	cleaning, _, err := first.transfers.CleanupImport(ctx, job.Task.ID, cleanupCommand,
		func(context.Context, string) error { return errors.New("injected cleanup failure") })
	if !errors.Is(err, transfer.ErrStageFileCleanup) || cleaning.Task.State != transfer.ImportCleaning {
		t.Fatalf("cleaning=%+v err=%v", cleaning, err)
	}
	first.shutdown(ctx)

	restarted := NewApp()
	restarted.ctx = ctx
	if err := restarted.initialize(ctx, root); err != nil {
		t.Fatal(err)
	}
	defer restarted.shutdown(ctx)
	if status := restarted.GetRuntimeStatus(); !status.Ready {
		t.Fatalf("application did not become ready after cleanup recovery: %+v", status)
	}
	recovered, err := restarted.GetImportJob(job.Task.ID)
	if err != nil || recovered.Task.State != transfer.ImportCanceled || !recovered.Task.Terminal {
		t.Fatalf("recovered=%+v err=%v", recovered, err)
	}
	if _, err := os.Stat(stagingPath); !os.IsNotExist(err) {
		t.Fatalf("startup cleanup did not remove staging file: %v", err)
	}
}

func TestStartExportCanonicalAdmissionAndReplayPrecedesTargetProbe(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("X-Influxdb-Version", "1.12.4")
		switch request.URL.Path {
		case "/ping":
			writer.WriteHeader(http.StatusNoContent)
		case "/query":
			writer.Header().Set("Content-Type", "application/json")
			_, _ = writer.Write([]byte(`{"results":[{"statement_id":0}]}`))
		default:
			writer.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	app := NewApp()
	app.ctx = context.Background()
	if err := app.initialize(app.ctx, t.TempDir()); err != nil {
		t.Fatal(err)
	}
	defer app.shutdown(context.Background())
	profileView, err := app.SaveProfile(SaveProfileInput{
		Name: "export", BaseURL: server.URL, Environment: profile.EnvironmentDevelopment,
		AuthMode: transport.AuthNone, ProtectionMode: protection.PermanentReadOnly,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := app.OpenConnection(profileView.ID); err != nil {
		t.Fatal(err)
	}
	target := t.TempDir()
	request := StartExportInput{
		ClientRequestID: "c30dc930-8fef-4bf8-9ae1-03bd21e34bdf", ProfileID: profileView.ID,
		TargetDirectory: target, Database: "metrics", RetentionPolicy: "autogen",
		Measurements: []string{"cpu"}, StartNS: "9007199254740992",
		EndNS: "9007199254740994", Strict: true,
	}
	created, err := app.StartExport(request)
	if err != nil || created.Task.State != "QUEUED" || created.TargetReservationID == nil {
		t.Fatalf("created=%+v err=%v", created, err)
	}
	if err := os.Remove(target); err != nil {
		t.Fatal(err)
	}
	replayed, err := app.StartExport(request)
	if err != nil || replayed.Task.ID != created.Task.ID {
		t.Fatalf("replayed=%+v err=%v", replayed, err)
	}
	request.EndNS = "9007199254740995"
	if _, err := app.StartExport(request); !errors.Is(err, tasks.ErrIdempotencyConflict) {
		t.Fatalf("different canonical request error=%v", err)
	}
	unsupported := request
	unsupported.ClientRequestID = uuid.NewString()
	unsupported.Measurements = []string{"cpu", "memory"}
	if _, err := app.StartExport(unsupported); err == nil || err.Error() != "UNSUPPORTED_EXPORT_SPEC" {
		t.Fatalf("multi-measurement export error=%v", err)
	}
}

func TestCanonicalSignedInt64RejectsNonCanonicalAndOutOfRange(t *testing.T) {
	for _, value := range []string{"0", "-9223372036854775808", "9223372036854775807"} {
		if got, err := canonicalSignedInt64(value); err != nil || got != value {
			t.Fatalf("value=%s got=%s err=%v", value, got, err)
		}
	}
	for _, value := range []string{"+1", "01", "-0", "9223372036854775808", "-9223372036854775809"} {
		if _, err := canonicalSignedInt64(value); err == nil {
			t.Fatalf("non-canonical value %s was accepted", value)
		}
	}
}

func TestAppExportRunsDispatcherTransferLaneToDurablePackage(t *testing.T) {
	var queryCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("X-Influxdb-Version", "1.12.4")
		switch request.URL.Path {
		case "/ping":
			writer.WriteHeader(http.StatusNoContent)
		case "/query":
			if err := request.ParseForm(); err != nil {
				writer.WriteHeader(http.StatusBadRequest)
				return
			}
			writer.Header().Set("Content-Type", "application/json")
			switch q := request.Form.Get("q"); {
			case q == "SHOW DATABASES":
				_, _ = io.WriteString(writer, `{"results":[{"statement_id":0}]}`)
			case strings.HasPrefix(q, "SHOW TAG KEYS"):
				queryCalls.Add(1)
				_, _ = io.WriteString(writer, `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["tagKey"],"values":[["host"]]}]}]}`)
			case strings.HasPrefix(q, "SHOW FIELD KEYS"):
				queryCalls.Add(1)
				_, _ = io.WriteString(writer, `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["fieldKey","fieldType"],"values":[["count","integer"]]}]}]}`)
			case strings.HasPrefix(q, "SELECT *"):
				queryCalls.Add(1)
				_, _ = io.WriteString(writer, `{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"host":"a"},"columns":["time","count"],"values":[[1,9007199254740993]]}]}]}`)
			default:
				writer.WriteHeader(http.StatusBadRequest)
			}
		default:
			writer.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	app := NewApp()
	app.ctx = context.Background()
	if err := app.initialize(app.ctx, t.TempDir()); err != nil {
		t.Fatal(err)
	}
	defer app.shutdown(context.Background())
	profileView, err := app.SaveProfile(SaveProfileInput{
		Name: "export-e2e", BaseURL: server.URL, Environment: profile.EnvironmentDevelopment,
		AuthMode: transport.AuthNone, ProtectionMode: protection.PermanentReadOnly,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := app.OpenConnection(profileView.ID); err != nil {
		t.Fatal(err)
	}
	target := t.TempDir()
	exportRequest := StartExportInput{
		ClientRequestID: uuid.NewString(), ProfileID: profileView.ID,
		TargetDirectory: target, Database: "metrics", RetentionPolicy: "autogen",
		Measurements: []string{"cpu"}, StartNS: "0", EndNS: "2", Strict: true,
	}
	created, err := app.StartExport(exportRequest)
	if err != nil {
		t.Fatal(err)
	}
	replayedStart, err := app.StartExport(exportRequest)
	if err != nil || replayedStart.Task.ID != created.Task.ID {
		t.Fatalf("start replay=%+v err=%v", replayedStart, err)
	}
	deadline := time.Now().Add(5 * time.Second)
	job := created
	for time.Now().Before(deadline) {
		job, err = app.GetExportJob(created.Task.ID)
		if err != nil {
			t.Fatal(err)
		}
		if job.Task.Terminal {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if job.Task.State != "SUCCEEDED" || !job.Task.Terminal || job.Retained ||
		job.TargetReservationID != nil {
		t.Fatalf("export job=%+v", job)
	}
	if queryCalls.Load() != 3 {
		t.Fatalf("query calls=%d, want metadata+metadata+slice", queryCalls.Load())
	}
	dataPath := filepath.Join(job.Plan.OutputDirectory, "data-000000.lp.gz")
	artifact, err := transfer.ValidateGzipArtifact(dataPath)
	if err != nil || artifact.SHA256 == "" {
		t.Fatalf("gzip artifact=%+v err=%v", artifact, err)
	}
	manifestBytes, err := os.ReadFile(filepath.Join(job.Plan.OutputDirectory, "manifest.json"))
	if err != nil || !strings.Contains(string(manifestBytes), `"snapshotConsistent":false`) ||
		!strings.Contains(string(manifestBytes), artifact.SHA256) {
		t.Fatalf("manifest=%s err=%v", manifestBytes, err)
	}
	cleaned, err := app.CleanupExport(job.Task.ID, exportjob.CommandEnvelope{
		CommandRequestID: uuid.NewString(), ExpectedStateRevision: job.Task.StateRevision,
	})
	if err != nil || cleaned.Task.State != exportjob.StateSucceeded {
		t.Fatalf("successful cleanup=%+v err=%v", cleaned, err)
	}
	if _, err := os.Stat(filepath.Join(job.Plan.OutputDirectory, "manifest.json")); err != nil {
		t.Fatalf("successful cleanup removed delivered package: %v", err)
	}
}

func TestCloseConnectionPausesRunningExportAfterWorkerCleanup(t *testing.T) {
	exportStarted := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("X-Influxdb-Version", "1.12.4")
		if request.URL.Path == "/ping" {
			writer.WriteHeader(http.StatusNoContent)
			return
		}
		if request.URL.Path != "/query" {
			writer.WriteHeader(http.StatusNotFound)
			return
		}
		_ = request.ParseForm()
		if request.Form.Get("q") == "SHOW DATABASES" {
			writer.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(writer, `{"results":[{"statement_id":0}]}`)
			return
		}
		select {
		case exportStarted <- struct{}{}:
		default:
		}
		<-request.Context().Done()
	}))
	defer server.Close()

	app := NewApp()
	app.ctx = context.Background()
	if err := app.initialize(app.ctx, t.TempDir()); err != nil {
		t.Fatal(err)
	}
	defer app.shutdown(context.Background())
	profileView, err := app.SaveProfile(SaveProfileInput{
		Name: "export-pause", BaseURL: server.URL, Environment: profile.EnvironmentDevelopment,
		AuthMode: transport.AuthNone, ProtectionMode: protection.PermanentReadOnly,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := app.OpenConnection(profileView.ID); err != nil {
		t.Fatal(err)
	}
	job, err := app.StartExport(StartExportInput{
		ClientRequestID: uuid.NewString(), ProfileID: profileView.ID,
		TargetDirectory: t.TempDir(), Database: "metrics", RetentionPolicy: "autogen",
		Measurements: []string{"cpu"}, StartNS: "0", EndNS: "2", Strict: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-exportStarted:
	case <-time.After(3 * time.Second):
		t.Fatal("export did not enter RoundTrip")
	}
	if err := app.CloseConnection(profileView.ID); err != nil {
		t.Fatal(err)
	}
	paused, err := app.GetExportJob(job.Task.ID)
	if err != nil || paused.Task.State != exportjob.StatePausedRestartable || paused.Task.Terminal {
		t.Fatalf("paused export=%+v err=%v", paused, err)
	}
	if matches, err := filepath.Glob(filepath.Join(paused.Plan.OutputDirectory, "*.part")); err != nil || len(matches) != 0 {
		t.Fatalf("part files after connection close=%v err=%v", matches, err)
	}
}

func TestCancelExportLedgerReplayConvergesRunningScheduler(t *testing.T) {
	exportStarted := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("X-Influxdb-Version", "1.12.4")
		if request.URL.Path == "/ping" {
			writer.WriteHeader(http.StatusNoContent)
			return
		}
		if request.URL.Path != "/query" {
			writer.WriteHeader(http.StatusNotFound)
			return
		}
		_ = request.ParseForm()
		if request.Form.Get("q") == "SHOW DATABASES" {
			writer.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(writer, `{"results":[{"statement_id":0}]}`)
			return
		}
		exportStarted <- struct{}{}
		<-request.Context().Done()
	}))
	defer server.Close()

	app := NewApp()
	app.ctx = context.Background()
	if err := app.initialize(app.ctx, t.TempDir()); err != nil {
		t.Fatal(err)
	}
	defer app.shutdown(context.Background())
	profileView, err := app.SaveProfile(SaveProfileInput{
		Name: "export-cancel", BaseURL: server.URL, Environment: profile.EnvironmentDevelopment,
		AuthMode: transport.AuthNone, ProtectionMode: protection.PermanentReadOnly,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := app.OpenConnection(profileView.ID); err != nil {
		t.Fatal(err)
	}
	job, err := app.StartExport(StartExportInput{
		ClientRequestID: uuid.NewString(), ProfileID: profileView.ID,
		TargetDirectory: t.TempDir(), Database: "metrics", RetentionPolicy: "autogen",
		Measurements: []string{"cpu"}, StartNS: "0", EndNS: "2", Strict: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-exportStarted:
	case <-time.After(3 * time.Second):
		t.Fatal("export did not enter RoundTrip")
	}
	running, err := app.GetExportJob(job.Task.ID)
	if err != nil || running.Task.State != exportjob.StateRunning {
		t.Fatalf("running export=%+v err=%v", running, err)
	}
	command := exportjob.CommandEnvelope{
		CommandRequestID: uuid.NewString(), ExpectedStateRevision: running.Task.StateRevision,
	}
	if _, err := app.CancelExport(job.Task.ID, command); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	var canceled exportjob.Job
	for time.Now().Before(deadline) {
		canceled, err = app.GetExportJob(job.Task.ID)
		if err == nil && canceled.Task.State == exportjob.StateCanceled {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if canceled.Task.State != exportjob.StateCanceled || !canceled.Task.Terminal {
		t.Fatalf("canceled export=%+v err=%v", canceled, err)
	}
	replayed, err := app.CancelExport(job.Task.ID, command)
	if err != nil || replayed.Task.State != exportjob.StateCanceled ||
		replayed.Task.StateRevision != canceled.Task.StateRevision {
		t.Fatalf("cancel replay=%+v err=%v", replayed, err)
	}
}

func TestAppImportPreflightGrantWriteAndEOFSucceed(t *testing.T) {
	var writeCalls atomic.Int32
	var writtenPayload string
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("X-Influxdb-Version", "1.12.4")
		switch request.URL.Path {
		case "/ping":
			writer.WriteHeader(http.StatusNoContent)
		case "/query":
			writer.Header().Set("Content-Type", "application/json")
			_, _ = writer.Write([]byte(`{"results":[{"statement_id":0}]}`))
		case "/write":
			writeCalls.Add(1)
			payload, _ := io.ReadAll(request.Body)
			writtenPayload = string(payload)
			if request.URL.Query().Get("db") != "metrics" || request.URL.Query().Get("rp") != "autogen" ||
				request.URL.Query().Get("precision") != "ns" {
				writer.WriteHeader(http.StatusBadRequest)
				return
			}
			writer.WriteHeader(http.StatusNoContent)
		default:
			writer.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	root := t.TempDir()
	app := NewApp()
	app.ctx = context.Background()
	if err := app.initialize(app.ctx, root); err != nil {
		t.Fatal(err)
	}
	defer app.shutdown(context.Background())
	profileView, err := app.SaveProfile(SaveProfileInput{
		Name: "import", BaseURL: server.URL, Environment: profile.EnvironmentDevelopment,
		AuthMode: transport.AuthNone, ProtectionMode: protection.ProtectedLocked,
	})
	if err != nil {
		t.Fatal(err)
	}
	opened, err := app.OpenConnection(profileView.ID)
	if err != nil {
		t.Fatal(err)
	}
	sourceBytes := []byte("cpu,host=app value=9007199254740993i 1700000000000000001\n")
	sourcePath := filepath.Join(t.TempDir(), "source.lp")
	if err := os.WriteFile(sourcePath, sourceBytes, 0o600); err != nil {
		t.Fatal(err)
	}
	sourceHash := sha256.Sum256(sourceBytes)
	preflight, err := app.PreflightImport(PreflightImportInput{
		ClientRequestID: uuid.NewString(), ProfileID: profileView.ID,
		SourcePath: sourcePath, Source: transfer.ImportSourceIdentity{
			SHA256: hex.EncodeToString(sourceHash[:]), SizeBytes: strconv.Itoa(len(sourceBytes)),
		},
		Format: transfer.ImportStageLP,
		Target: transfer.ImportTarget{Database: "metrics", RetentionPolicy: "autogen"},
	})
	if err != nil || !preflight.Ready || preflight.Job.Task.State != transfer.ImportReady {
		t.Fatalf("preflight=%+v err=%v", preflight, err)
	}
	if _, err := app.UnlockProtection(protection.UnlockRequest{
		ConnectionID: profileView.ID, CommandRequestID: uuid.NewString(),
		ExpectedConnectionGeneration: opened.ConnectionGeneration,
		ExpectedProtectionRevision:   opened.Protection.ProtectionRevision,
	}); err != nil {
		t.Fatal(err)
	}
	commandID := uuid.NewString()
	preview, err := app.PreviewImportRun(profileView.ID, transfer.PreviewRunRequest{
		JobID: preflight.Job.Task.ID, CommandRequestID: commandID,
		ExpectedStateRevision: preflight.Job.Task.StateRevision, Action: transfer.GrantStart,
	})
	if err != nil || preview.ImportRunGrant == nil {
		t.Fatalf("preview=%+v err=%v", preview, err)
	}
	started, err := app.StartImport(profileView.ID, transfer.AuthorizedStartRequest{
		JobID: preflight.Job.Task.ID, CommandRequestID: commandID,
		ExpectedStateRevision: preflight.Job.Task.StateRevision, Action: transfer.GrantStart,
		ImportRunGrant: preview.ImportRunGrant.Token,
	})
	if err != nil || started.Task.State != transfer.ImportRunning {
		t.Fatalf("started=%+v err=%v", started, err)
	}
	var finished transfer.ImportJob
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		finished, err = app.GetImportJob(started.Task.ID)
		if err == nil && finished.Task.Terminal {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if err != nil || finished.Task.State != transfer.ImportSucceeded || !finished.Task.Terminal {
		t.Fatalf("finished=%+v err=%v", finished, err)
	}
	replayed, err := app.RunImportNext(profileView.ID, started.Task.ID, "metrics", "autogen")
	if err != nil || replayed.Job.Task.State != transfer.ImportSucceeded ||
		replayed.Job.Task.StateRevision != finished.Task.StateRevision ||
		replayed.Job.Task.SnapshotRevision != finished.Task.SnapshotRevision {
		t.Fatalf("replayed=%+v err=%v", replayed, err)
	}
	if writeCalls.Load() != 1 || writtenPayload != strings.TrimSuffix(string(sourceBytes), "\n") {
		t.Fatalf("write calls=%d payload=%q", writeCalls.Load(), writtenPayload)
	}

	second, err := app.PreflightImport(PreflightImportInput{
		ClientRequestID: uuid.NewString(), ProfileID: profileView.ID, SourcePath: sourcePath,
		Source: transfer.ImportSourceIdentity{
			SHA256: hex.EncodeToString(sourceHash[:]), SizeBytes: strconv.Itoa(len(sourceBytes)),
		},
		Format: transfer.ImportStageLP,
		Target: transfer.ImportTarget{Database: "metrics", RetentionPolicy: "autogen"},
	})
	if err != nil || !second.Ready {
		t.Fatalf("second preflight=%+v err=%v", second, err)
	}
	canceled, err := app.CancelImport(second.Job.Task.ID, transfer.ImportCommandEnvelope{
		CommandRequestID: uuid.NewString(), ExpectedStateRevision: second.Job.Task.StateRevision,
	})
	if err != nil || canceled.Task.State != transfer.ImportCanceled || !canceled.Task.Terminal {
		t.Fatalf("canceled=%+v err=%v", canceled, err)
	}
	encodedCanceled, err := json.Marshal(canceled)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(encodedCanceled), `"stateRevision":"`+canceled.Task.StateRevision+`"`) {
		t.Fatalf("stateRevision was not encoded as a Wails string: %s", encodedCanceled)
	}
	cleaned, err := app.CleanupImport(canceled.Task.ID, transfer.ImportCommandEnvelope{
		CommandRequestID: uuid.NewString(), ExpectedStateRevision: canceled.Task.StateRevision,
	})
	if err != nil || cleaned.Task.State != transfer.ImportCanceled || !cleaned.Task.Terminal {
		t.Fatalf("cleaned=%+v err=%v", cleaned, err)
	}
	stagingPath := filepath.Join(root, "staging", "import-"+canceled.Task.ID+".canonical.lp")
	if _, err := os.Stat(stagingPath); !os.IsNotExist(err) {
		t.Fatalf("staging file remains after App.CleanupImport: %v", err)
	}
}

func TestQueryResultPageViewMatchesWailsWireContract(t *testing.T) {
	falseValue := false
	view, err := queryResultPageView(query.ResultPage{
		Rows: [][]query.TypedScalar{{
			{Kind: query.ScalarNull},
			{Kind: query.ScalarString, StringValue: ""},
			{Kind: query.ScalarBoolean, BooleanValue: &falseValue},
			{Kind: query.ScalarInt64, DecimalText: "9007199254740993"},
		}},
		NextCursor: "cursor",
		EOF:        false,
	})
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(view)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"rows":[[{"kind":"null"},{"kind":"string","value":""},{"kind":"boolean","value":false},{"kind":"int64","decimalText":"9007199254740993"}]],"nextCursor":"cursor","eof":false}`
	if string(encoded) != want {
		t.Fatalf("unexpected Wails result page: %s", encoded)
	}
}
