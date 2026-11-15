package httpapi_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/getkin/kin-openapi/openapi3filter"
	"github.com/getkin/kin-openapi/routers"
	"github.com/getkin/kin-openapi/routers/legacy"
	"github.com/google/uuid"

	"github.com/asrasari/notification-service/api"
	"github.com/asrasari/notification-service/internal/httpapi"
	"github.com/asrasari/notification-service/internal/logging"
	"github.com/asrasari/notification-service/internal/notification"
	"github.com/asrasari/notification-service/internal/postgres"
)

// The spec is loaded and validated once for the whole package.
var (
	specOnce   sync.Once
	specDoc    *openapi3.T
	specRouter routers.Router
	specErr    error
)

func loadSpec(t *testing.T) (*openapi3.T, routers.Router) {
	t.Helper()
	specOnce.Do(func() {
		// The spec answers /docs with HTML; validate it as a plain string.
		openapi3filter.RegisterBodyDecoder("text/html", openapi3filter.PlainBodyDecoder)

		doc, err := openapi3.NewLoader().LoadFromData(api.Spec)
		if err != nil {
			specErr = err
			return
		}
		if err := doc.Validate(context.Background()); err != nil {
			specErr = err
			return
		}
		specDoc = doc
		specRouter, specErr = legacy.NewRouter(doc)
	})
	if specErr != nil {
		t.Fatalf("api/openapi.yaml: %v", specErr)
	}
	return specDoc, specRouter
}

// D-016 §1: the spec itself must be a valid OpenAPI 3.0.3 document.
func TestSpecIsValid(t *testing.T) {
	doc, _ := loadSpec(t)
	if doc.OpenAPI != "3.0.3" {
		t.Errorf("openapi = %q, want 3.0.3", doc.OpenAPI)
	}
	if doc.Paths.Find("/metrics") != nil {
		t.Error("/metrics must stay out of the spec until M4")
	}
}

// checkContract validates one exchange against the spec (D-016 §1): the
// status code must be documented for the operation, and the content type and
// body must match. Requests no operation matches (the JSON 404/405
// fallbacks) are checked against the Problem schema.
func checkContract(t *testing.T, req *http.Request, r response) {
	t.Helper()
	doc, router := loadSpec(t)

	route, params, err := router.FindRoute(req)
	if err != nil {
		if r.status != http.StatusNotFound && r.status != http.StatusMethodNotAllowed {
			t.Errorf("contract: %s %s has no operation in the spec but answered %d", req.Method, req.URL.Path, r.status)
			return
		}
		checkProblemSchema(t, doc, r)
		return
	}

	in := &openapi3filter.ResponseValidationInput{
		RequestValidationInput: &openapi3filter.RequestValidationInput{Request: req, PathParams: params, Route: route},
		Status:                 r.status,
		Header:                 r.header,
		Options:                &openapi3filter.Options{IncludeResponseStatus: true, MultiError: true},
	}
	in.SetBodyBytes(r.body)
	if err := openapi3filter.ValidateResponse(context.Background(), in); err != nil {
		t.Errorf("contract: %s %s → %d does not match the spec:\n%v\nbody: %.500s", req.Method, req.URL.Path, r.status, err, r.body)
	}
}

func checkProblemSchema(t *testing.T, doc *openapi3.T, r response) {
	t.Helper()
	if ct := r.header.Get("Content-Type"); ct != "application/problem+json" {
		t.Errorf("contract: fallback Content-Type = %q, want application/problem+json", ct)
	}
	var body any
	if err := json.Unmarshal(r.body, &body); err != nil {
		t.Errorf("contract: fallback body is not JSON: %v", err)
		return
	}
	if err := doc.Components.Schemas["Problem"].Value.VisitJSON(body); err != nil {
		t.Errorf("contract: fallback body does not match Problem: %v", err)
	}
}

// D-016 §1: every path and method in the spec is routed. A routed request
// can still be a 404 (unknown id), so the check uses the access log's
// matched route instead of the status code.
func TestEveryOperationIsRouted(t *testing.T) {
	doc, _ := loadSpec(t)
	var logs bytes.Buffer
	logger, err := logging.New(&logs, "info", "json")
	if err != nil {
		t.Fatal(err)
	}
	// Only routing is under test, so a store that finds nothing is enough.
	h := httpapi.NewHandler(logger, fakePinger{}, notFoundStore{})

	for path, item := range doc.Paths.Map() {
		for method := range item.Operations() {
			t.Run(method+" "+path, func(t *testing.T) {
				logs.Reset()
				target := strings.NewReplacer("{id}", "0196e9a0-0000-7000-8000-000000000000").Replace(path)
				req := httptest.NewRequest(method, target, strings.NewReader("{}"))
				req.Header.Set("Content-Type", "application/json")
				rec := httptest.NewRecorder()
				h.ServeHTTP(rec, req)

				var line struct{ Route string }
				if err := json.Unmarshal(logs.Bytes(), &line); err != nil {
					t.Fatalf("access log: %v\n%s", err, logs.String())
				}
				if line.Route != method+" "+path {
					t.Errorf("routed to %q (status %d), want %q", line.Route, rec.Code, method+" "+path)
				}
			})
		}
	}
}

type fakePinger struct{}

func (fakePinger) Ping(context.Context) error { return nil }

// notFoundStore finds nothing and stores nothing.
type notFoundStore struct{}

func (notFoundStore) Create(context.Context, notification.Draft) (notification.Notification, bool, error) {
	return notification.Notification{}, false, errors.New("not implemented")
}
func (notFoundStore) CreateBatch(context.Context, []notification.Draft) (postgres.BatchResult, error) {
	return postgres.BatchResult{}, errors.New("not implemented")
}
func (notFoundStore) Get(context.Context, uuid.UUID) (notification.Notification, []notification.Attempt, error) {
	return notification.Notification{}, nil, postgres.ErrNotFound
}
func (notFoundStore) List(context.Context, postgres.ListFilter) ([]notification.Notification, *postgres.Cursor, error) {
	return nil, nil, nil
}
func (notFoundStore) BatchSummary(context.Context, uuid.UUID) (notification.BatchSummary, error) {
	return notification.BatchSummary{}, postgres.ErrNotFound
}
func (notFoundStore) Cancel(context.Context, uuid.UUID) (notification.Notification, error) {
	return notification.Notification{}, postgres.ErrNotFound
}
func (notFoundStore) CancelBatch(context.Context, uuid.UUID) (int, error) {
	return 0, postgres.ErrNotFound
}
