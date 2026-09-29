// Copyright 2025 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package serve

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/ray-project/ray/go/internal/dashboard/head"
	rayerrors "github.com/ray-project/ray/go/pkg/errors"
)

// fakeController simulates the ServeController actor for unit tests. The
// cross-language calls (GetPythonActorWithNamespace + ActorTask) require a
// real runtime, so tests inject this fake via the package-level
// getServeControllerFn variable and the ServeHead controller cache.
type fakeController struct {
	mu            sync.Mutex
	alive         bool
	details       map[string]interface{}
	statuses      map[string]int32
	remainingApps int
	applyCalls    []map[string]interface{}
	deleteCalls   []string
	scaleCalls    []struct {
		deploymentID map[string]interface{}
		replicas     int
	}
	shutdownCalled bool
	applyErr       error
	deleteErr      error
	scaleErr       error
	detailsErr     error
}

func newFakeController() *fakeController {
	return &fakeController{alive: true, statuses: map[string]int32{}, details: map[string]interface{}{}}
}

func (c *fakeController) checkAlive() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.alive {
		return errControllerDead
	}
	return nil
}

func (c *fakeController) getServeInstanceDetails() (map[string]interface{}, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.detailsErr != nil {
		return nil, c.detailsErr
	}
	return c.details, nil
}

func (c *fakeController) applyConfig(config map[string]interface{}) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.applyErr != nil {
		return c.applyErr
	}
	c.applyCalls = append(c.applyCalls, config)
	return nil
}

func (c *fakeController) shutdown() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.shutdownCalled = true
	return nil
}

func (c *fakeController) deleteApps(names []string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.deleteErr != nil {
		return c.deleteErr
	}
	c.deleteCalls = append(c.deleteCalls, names...)
	for _, name := range names {
		delete(c.statuses, name)
	}
	return nil
}

func (c *fakeController) getServeStatuses(names []string) ([][]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([][]byte, 0, len(names))
	for _, name := range names {
		if status, ok := c.statuses[name]; ok {
			out = append(out, serveStatusProto(status))
		} else {
			out = append(out, serveStatusProto(serveStatusNotStarted))
		}
	}
	return out, nil
}

func (c *fakeController) listServeStatuses() ([][]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([][]byte, 0, c.remainingApps)
	for i := 0; i < c.remainingApps; i++ {
		out = append(out, serveStatusProto(1))
	}
	return out, nil
}

func (c *fakeController) updateDeploymentReplicas(deploymentID map[string]interface{}, targetNumReplicas int) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.scaleErr != nil {
		return c.scaleErr
	}
	c.scaleCalls = append(c.scaleCalls, struct {
		deploymentID map[string]interface{}
		replicas     int
	}{deploymentID, targetNumReplicas})
	return nil
}

var errControllerDead = context.Canceled

// serveStatusProto builds a minimal StatusOverview protobuf (wire format)
// carrying the given application status enum value. It uses the field numbers
// from src/ray/protobuf/serve.proto: StatusOverview.app_status=1,
// ApplicationStatusInfo.status=1.
func serveStatusProto(status int32) []byte {
	// ApplicationStatusInfo { status(1, varint) }.
	appInfo := appendVarint(nil, 1, 8, uint64(status))
	// StatusOverview { app_status(1, bytes) }.
	return appendBytes(nil, 1, 10, appInfo)
}

func appendVarint(out []byte, field, tag, val uint64) []byte {
	out = appendVarintBytes(out, (field<<3)|tag)
	return appendVarintBytes(out, val)
}

func appendVarintBytes(out []byte, v uint64) []byte {
	for v >= 0x80 {
		out = append(out, byte(v)|0x80)
		v >>= 7
	}
	return append(out, byte(v))
}

func appendBytes(out []byte, field, tag uint64, payload []byte) []byte {
	out = appendVarintBytes(out, (field<<3)|tag)
	out = appendVarintBytes(out, uint64(len(payload)))
	return append(out, payload...)
}

// newServeHead builds a ServeHead with an empty config.
func newServeHead() *ServeHead {
	return New(&head.HeadConfig{SessionName: "session_test"}, nil)
}

// serve performs an HTTP request against the ServeHead mux.
func serve(t *testing.T, h *ServeHead, method, path string, body string) *httptest.ResponseRecorder {
	t.Helper()
	mux := http.NewServeMux()
	if err := h.RegisterHTTP(mux); err != nil {
		t.Fatal(err)
	}
	var reader io.Reader
	if body != "" {
		reader = bytes.NewReader([]byte(body))
	}
	req := httptest.NewRequest(method, path, reader)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)
	return rr
}

// setController installs a fake controller into the ServeHead cache.
func setController(h *ServeHead, c serveController) {
	h.controllerMu.Lock()
	defer h.controllerMu.Unlock()
	h.controller = c
}

func TestServeHeadNameAndHealth(t *testing.T) {
	h := New(&head.HeadConfig{}, nil)
	if h.Name() != "ServeHead" {
		t.Fatalf("name = %q", h.Name())
	}
	if !h.Healthy() {
		t.Fatal("should be healthy")
	}
	if err := h.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
}

func TestServeVersion(t *testing.T) {
	h := newServeHead()
	rr := serve(t, h, "GET", "/api/ray/version", "")
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rr.Code, rr.Body.String())
	}
	var resp VersionResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Version != "4" || resp.RayVersion != "3.0.0.dev0" || resp.RayCommit == "" || resp.SessionName != "session_test" {
		t.Fatalf("version response = %+v", resp)
	}
}

// TestGetServeControllerNotInitializedDegrade verifies that when the Go
// runtime is not initialized (getServeControllerFn surfaces
// ErrRuntimeNotInitialized) and the lazy EnsureInitialized also fails (as it
// does in a unit test without go_runtime.so), getServeController degrades to
// nil instead of panicking or erroring, so the handler returns the empty
// schema.
func TestGetServeControllerNotInitializedDegrade(t *testing.T) {
	orig := getServeControllerFn
	getServeControllerFn = func() (serveController, error) {
		return nil, rayerrors.ErrRuntimeNotInitialized
	}
	defer func() { getServeControllerFn = orig }()

	h := newServeHead()
	controller, err := h.getServeController()
	if err != nil {
		t.Fatalf("getServeController error = %v, want nil (degrade)", err)
	}
	if controller != nil {
		t.Fatalf("getServeController = %v, want nil (degrade on not-initialized)", controller)
	}
}

// TestGetServeControllerNotInitializedHandlerDegrade verifies the HTTP handler
// surfaces the empty serve schema (not a 500) when the runtime is not
// initialized.
func TestGetServeControllerNotInitializedHandlerDegrade(t *testing.T) {
	orig := getServeControllerFn
	getServeControllerFn = func() (serveController, error) {
		return nil, rayerrors.ErrRuntimeNotInitialized
	}
	defer func() { getServeControllerFn = orig }()

	h := newServeHead()
	rr := serve(t, h, "GET", "/api/serve/applications/", "")
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s, want 200 (empty schema degrade)", rr.Code, rr.Body.String())
	}
	var resp map[string]interface{}
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if _, ok := resp["applications"]; !ok {
		t.Fatalf("applications key missing in %+v", resp)
	}
}

func TestGetApplicationsNoController(t *testing.T) {
	h := newServeHead()
	// No controller installed -> empty schema dict.
	rr := serve(t, h, "GET", "/api/serve/applications/", "")
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rr.Code, rr.Body.String())
	}
	var resp map[string]interface{}
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp["deploy_mode"] != "MULTI_APP" {
		t.Fatalf("deploy_mode = %v", resp["deploy_mode"])
	}
	if resp["target_capacity"] != nil {
		t.Fatalf("target_capacity = %v, want null", resp["target_capacity"])
	}
}

func TestGetApplicationsInvalidAPIType(t *testing.T) {
	h := newServeHead()
	rr := serve(t, h, "GET", "/api/serve/applications/?api_type=invalid", "")
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rr.Code)
	}
	if ct := rr.Header().Get("Content-Type"); ct != "text/plain; charset=utf-8" {
		t.Fatalf("content-type = %q", ct)
	}
	if !strings.Contains(rr.Body.String(), "Invalid 'api_type' value") {
		t.Fatalf("body = %s", rr.Body.String())
	}
	// Valid api_type with no controller -> 200 empty schema.
	rr = serve(t, h, "GET", "/api/serve/applications/?api_type=declarative", "")
	if rr.Code != http.StatusOK {
		t.Fatalf("valid api_type status = %d, want 200", rr.Code)
	}
}

func TestGetApplicationsWithController(t *testing.T) {
	h := newServeHead()
	fc := newFakeController()
	fc.details = map[string]interface{}{
		"deploy_mode":     "MULTI_APP",
		"controller_info": map[string]interface{}{"node_id": "n1"},
		"applications": map[string]interface{}{
			"app1": map[string]interface{}{"status": "RUNNING"},
		},
	}
	setController(h, fc)

	rr := serve(t, h, "GET", "/api/serve/applications/", "")
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rr.Code, rr.Body.String())
	}
	var resp map[string]interface{}
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	apps, ok := resp["applications"].(map[string]interface{})
	if !ok || apps["app1"] == nil {
		t.Fatalf("applications = %+v", resp["applications"])
	}
}

func TestGetApplicationsControllerError(t *testing.T) {
	h := newServeHead()
	fc := newFakeController()
	fc.detailsErr = context.Canceled
	setController(h, fc)

	rr := serve(t, h, "GET", "/api/serve/applications/", "")
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rr.Code)
	}
}

func TestDeleteApplicationsEmptyBodyShutdown(t *testing.T) {
	h := newServeHead()
	fc := newFakeController()
	setController(h, fc)

	rr := serve(t, h, "DELETE", "/api/serve/applications/", "")
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rr.Code, rr.Body.String())
	}
	fc.mu.Lock()
	shutdownCalled := fc.shutdownCalled
	fc.mu.Unlock()
	if !shutdownCalled {
		t.Fatal("expected shutdown to be called for empty body")
	}

	// Empty body with no controller -> 200, no shutdown.
	h2 := newServeHead()
	rr = serve(t, h2, "DELETE", "/api/serve/applications/", "")
	if rr.Code != http.StatusOK {
		t.Fatalf("no controller status = %d", rr.Code)
	}
}

func TestDeleteApplicationsNoNames(t *testing.T) {
	h := newServeHead()
	setController(h, newFakeController())
	rr := serve(t, h, "DELETE", "/api/serve/applications/", `{"applications":[{"name":""}]}`)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d", rr.Code)
	}
	if !strings.Contains(rr.Body.String(), "No application names provided") {
		t.Fatalf("body = %s", rr.Body.String())
	}
}

func TestDeleteApplicationsDeletes(t *testing.T) {
	h := newServeHead()
	fc := newFakeController()
	fc.statuses = map[string]int32{"app1": 1, "app2": 1}
	fc.remainingApps = 0
	setController(h, fc)

	rr := serve(t, h, "DELETE", "/api/serve/applications/",
		`{"applications":[{"name":"app1"},{"name":"app2"}]}`)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rr.Code, rr.Body.String())
	}
	var resp map[string]string
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(resp["message"], "no apps left") {
		t.Fatalf("message = %q", resp["message"])
	}
	fc.mu.Lock()
	deleteCalls := fc.deleteCalls
	shutdownCalled := fc.shutdownCalled
	fc.mu.Unlock()
	if len(deleteCalls) != 2 || deleteCalls[0] != "app1" || deleteCalls[1] != "app2" {
		t.Fatalf("delete calls = %v", deleteCalls)
	}
	if !shutdownCalled {
		t.Fatal("expected shutdown when no apps remain")
	}
}

func TestDeleteApplicationsRemaining(t *testing.T) {
	h := newServeHead()
	fc := newFakeController()
	fc.statuses = map[string]int32{"app1": 1}
	fc.remainingApps = 1
	setController(h, fc)

	rr := serve(t, h, "DELETE", "/api/serve/applications/", `{"applications":[{"name":"app1"}]}`)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rr.Code, rr.Body.String())
	}
	fc.mu.Lock()
	shutdownCalled := fc.shutdownCalled
	fc.mu.Unlock()
	if shutdownCalled {
		t.Fatal("did not expect shutdown when apps remain")
	}
}

func TestDeleteApplicationsInvalidBody(t *testing.T) {
	h := newServeHead()
	setController(h, newFakeController())
	rr := serve(t, h, "DELETE", "/api/serve/applications/", `not-json`)
	if rr.Code != http.StatusOK {
		t.Fatalf("invalid json body should be treated as empty -> shutdown, status = %d", rr.Code)
	}
}

func TestPutApplications(t *testing.T) {
	h := newServeHead()
	fc := newFakeController()
	setController(h, fc)

	body := `{"applications":[{"name":"app1","deployments":[{"name":"d1","num_replicas":2}]}]}`
	rr := serve(t, h, "PUT", "/api/serve/applications/", body)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rr.Code, rr.Body.String())
	}
	fc.mu.Lock()
	applyCalls := fc.applyCalls
	fc.mu.Unlock()
	if len(applyCalls) != 1 {
		t.Fatalf("apply calls = %d", len(applyCalls))
	}
	apps := applyCalls[0]["applications"].([]interface{})
	if len(apps) != 1 {
		t.Fatalf("applications = %+v", apps)
	}
}

func TestPutApplicationsInvalidBody(t *testing.T) {
	h := newServeHead()
	setController(h, newFakeController())
	rr := serve(t, h, "PUT", "/api/serve/applications/", `not-json`)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rr.Code)
	}
}

// TestGetApplicationsDependenciesNotInstalled verifies the B-F3 behavior: when
// the ray[serve] dependencies are not installed (the Python validate_endpoint
// import fails), GET /api/serve/applications/ returns 501.
func TestGetApplicationsDependenciesNotInstalled(t *testing.T) {
	orig := checkServeDepsFn
	checkServeDepsFn = func() error { return errServeDependenciesNotInstalled }
	defer func() { checkServeDepsFn = orig }()

	h := newServeHead()
	rr := serve(t, h, "GET", "/api/serve/applications/", "")
	if rr.Code != http.StatusNotImplemented {
		t.Fatalf("status = %d, want 501", rr.Code)
	}
	if !strings.Contains(rr.Body.String(), "Serve dependencies are not installed") {
		t.Fatalf("body = %s", rr.Body.String())
	}
}

// TestPutApplicationsDependenciesNotInstalled verifies that PUT also returns
// 501 when the serve dependencies are missing (Python decorates
// put_all_applications with @validate_endpoint too).
func TestPutApplicationsDependenciesNotInstalled(t *testing.T) {
	orig := checkServeDepsFn
	checkServeDepsFn = func() error { return errServeDependenciesNotInstalled }
	defer func() { checkServeDepsFn = orig }()

	h := newServeHead()
	rr := serve(t, h, "PUT", "/api/serve/applications/", `{"applications":[{"name":"app1"}]}`)
	if rr.Code != http.StatusNotImplemented {
		t.Fatalf("status = %d, want 501", rr.Code)
	}
}

// TestPutApplicationsSchemaValidation verifies the B-D10 behavior: the PUT body
// is validated against the ServeDeploySchema required fields (matching the
// Python pydantic parse_obj which returns 400 on ValidationError).
func TestPutApplicationsSchemaValidation(t *testing.T) {
	h := newServeHead()
	setController(h, newFakeController())

	// Missing applications field -> 400.
	rr := serve(t, h, "PUT", "/api/serve/applications/", `{}`)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("empty body status = %d, want 400", rr.Code)
	}
	// Applications not a list -> 400.
	rr = serve(t, h, "PUT", "/api/serve/applications/", `{"applications":"nope"}`)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("non-list applications status = %d, want 400", rr.Code)
	}
	// Application without import_path or deployments -> 400.
	rr = serve(t, h, "PUT", "/api/serve/applications/", `{"applications":[{"name":"app1"}]}`)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("no import_path status = %d, want 400, body=%s", rr.Code, rr.Body.String())
	}
	// Empty name -> 400.
	rr = serve(t, h, "PUT", "/api/serve/applications/", `{"applications":[{"name":"","import_path":"pkg.dag"}]}`)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("empty name status = %d, want 400", rr.Code)
	}
	// Duplicate names -> 400.
	rr = serve(t, h, "PUT", "/api/serve/applications/",
		`{"applications":[{"name":"a","import_path":"pkg.dag"},{"name":"a","import_path":"pkg.dag2"}]}`)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("duplicate name status = %d, want 400", rr.Code)
	}
	// Valid config with import_path -> 200.
	rr = serve(t, h, "PUT", "/api/serve/applications/", `{"applications":[{"name":"app1","import_path":"pkg.dag"}]}`)
	if rr.Code != http.StatusOK {
		t.Fatalf("valid import_path status = %d, want 200, body=%s", rr.Code, rr.Body.String())
	}
}

// TestPutApplicationsStartsController verifies that when no controller is
// running the PUT handler starts one (via startControllerFn) before deploying,
// mirroring serve_start_async, and returns 200.
func TestPutApplicationsStartsController(t *testing.T) {
	origStart := startControllerFn
	startControllerFn = func(h *ServeHead, ctx context.Context, config map[string]interface{}) (serveController, error) {
		return newFakeController(), nil
	}
	defer func() { startControllerFn = origStart }()

	h := newServeHead()
	body := `{"applications":[{"name":"app1","deployments":[{"name":"d1"}]}]}`
	rr := serve(t, h, "PUT", "/api/serve/applications/", body)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200, body=%s", rr.Code, rr.Body.String())
	}
}

// TestPutApplicationsStartControllerError verifies that a controller startup
// failure surfaces as a 500 (aligned with the Python decorator which returns
// 500 for any exception raised by serve_start_async).
func TestPutApplicationsStartControllerError(t *testing.T) {
	origStart := startControllerFn
	startControllerFn = func(h *ServeHead, ctx context.Context, config map[string]interface{}) (serveController, error) {
		return nil, fmt.Errorf("start failed")
	}
	defer func() { startControllerFn = origStart }()

	h := newServeHead()
	rr := serve(t, h, "PUT", "/api/serve/applications/", `{"applications":[{"name":"app1","deployments":[{"name":"d1"}]}]}`)
	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500, body=%s", rr.Code, rr.Body.String())
	}
}

// TestBuildHTTPOptions verifies the ProxyLocation -> DeploymentMode mapping and
// the merge of config http_options over the computed location, aligned with
// ServeHead.put_all_applications.
func TestBuildHTTPOptions(t *testing.T) {
	// Default proxy_location is EveryNode.
	opts := buildHTTPOptions(map[string]interface{}{})
	if opts["location"] != "EveryNode" {
		t.Fatalf("default location = %v, want EveryNode", opts["location"])
	}

	// Disabled -> NoServer; explicit http_options are merged underneath.
	config := map[string]interface{}{
		"proxy_location": "Disabled",
		"http_options":   map[string]interface{}{"port": 8001, "host": "0.0.0.0"},
	}
	opts = buildHTTPOptions(config)
	if opts["location"] != "NoServer" {
		t.Fatalf("disabled location = %v, want NoServer", opts["location"])
	}
	if opts["port"] != 8001 || opts["host"] != "0.0.0.0" {
		t.Fatalf("http options not merged: %+v", opts)
	}

	// HeadOnly keeps its value.
	opts = buildHTTPOptions(map[string]interface{}{"proxy_location": "HeadOnly"})
	if opts["location"] != "HeadOnly" {
		t.Fatalf("headonly location = %v, want HeadOnly", opts["location"])
	}
}

func TestScaleDeployment(t *testing.T) {
	h := newServeHead()
	fc := newFakeController()
	setController(h, fc)

	rr := serve(t, h, "POST",
		"/api/v1/applications/myapp/deployments/mydep/scale",
		`{"target_num_replicas": 3}`)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rr.Code, rr.Body.String())
	}
	fc.mu.Lock()
	scaleCalls := fc.scaleCalls
	fc.mu.Unlock()
	if len(scaleCalls) != 1 {
		t.Fatalf("scale calls = %d", len(scaleCalls))
	}
	if scaleCalls[0].replicas != 3 {
		t.Fatalf("replicas = %d", scaleCalls[0].replicas)
	}
	if scaleCalls[0].deploymentID["name"] != "mydep" || scaleCalls[0].deploymentID["app_name"] != "myapp" {
		t.Fatalf("deployment id = %+v", scaleCalls[0].deploymentID)
	}
}

func TestScaleDeploymentMissingPath(t *testing.T) {
	h := newServeHead()
	setController(h, newFakeController())
	// Go's ServeMux always populates both path values, so the guard is
	// exercised by calling the handler directly with a bare request.
	req := httptest.NewRequest("POST", "/api/v1/applications/x/deployments/y/scale",
		bytes.NewReader([]byte(`{"target_num_replicas": 1}`)))
	rr := httptest.NewRecorder()
	h.handleScaleDeployment(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rr.Code)
	}
}

func TestScaleDeploymentInvalidBody(t *testing.T) {
	h := newServeHead()
	setController(h, newFakeController())
	rr := serve(t, h, "POST",
		"/api/v1/applications/myapp/deployments/mydep/scale", `not-json`)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rr.Code)
	}
	// Missing target_num_replicas.
	rr = serve(t, h, "POST",
		"/api/v1/applications/myapp/deployments/mydep/scale", `{}`)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("missing field status = %d, want 400", rr.Code)
	}
}

func TestScaleDeploymentNoController(t *testing.T) {
	h := newServeHead()
	rr := serve(t, h, "POST",
		"/api/v1/applications/myapp/deployments/mydep/scale", `{"target_num_replicas": 1}`)
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rr.Code)
	}
}

func TestScaleDeploymentErrorMapping(t *testing.T) {
	cases := []struct {
		name     string
		err      error
		wantCode int
	}{
		{"deleted", errScaleDeleted, http.StatusPreconditionFailed},
		{"external scaler", errScaleExternal, http.StatusPreconditionFailed},
		{"not found", errScaleNotFound, http.StatusBadRequest},
		{"internal", errScaleInternal, http.StatusServiceUnavailable},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newServeHead()
			fc := newFakeController()
			fc.scaleErr = tc.err
			setController(h, fc)
			rr := serve(t, h, "POST",
				"/api/v1/applications/myapp/deployments/mydep/scale", `{"target_num_replicas": 1}`)
			if rr.Code != tc.wantCode {
				t.Fatalf("status = %d, want %d, body=%s", rr.Code, tc.wantCode, rr.Body.String())
			}
		})
	}
}

var errScaleDeleted = fmt.Errorf("Deployment is deleted")
var errScaleExternal = fmt.Errorf("Current value: external_scaler_enabled=false")
var errScaleNotFound = fmt.Errorf("Application 'myapp' not found")
var errScaleInternal = fmt.Errorf("boom")
