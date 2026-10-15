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
// Package serve implements the ServeHead dashboard module: cross-language
// calls into the Python ServeController actor for the Serve applications
// REST API. Aligned with the Python ServeHead in
// python/ray/dashboard/modules/serve/serve_head.py.
package serve

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/ray-project/ray/go/internal/dashboard/head"
	rayerrors "github.com/ray-project/ray/go/pkg/errors"
	"github.com/ray-project/ray/go/pkg/log"
	"github.com/ray-project/ray/go/pkg/runtime/api"
	"github.com/ray-project/ray/go/pkg/version"
	"google.golang.org/protobuf/encoding/protowire"
)

// Cross-language actor identity constants for the original ServeController,
// which the Go dashboard head connects to directly (no wrapper actor). The
// name/namespace/module/className quadruple is centralized here. Aligned with
// the detached controller created by get_controller_impl in
// python/ray/serve/_private/default_impl.py (name SERVE_CONTROLLER_NAME,
// namespace SERVE_NAMESPACE).
const (
	// serveControllerName is SERVE_CONTROLLER_NAME
	// ("SERVE_CONTROLLER_ACTOR" from ray.serve._private.constants).
	serveControllerName = "SERVE_CONTROLLER_ACTOR"
	// serveNamespace is SERVE_NAMESPACE.
	serveNamespace = "serve"
	// serveControllerModule is the Python module of the controller class.
	serveControllerModule = "ray.serve._private.controller"
	// serveControllerClass is the Python actor class name.
	serveControllerClass = "ServeController"
)

// serveControllerResources mirrors the ServeController's resources
// {HEAD_NODE_RESOURCE_NAME: 0.001} plus num_cpus=0: the 0.001 head-node
// resource pins it to the head node and zeroes the CPU quota. It is used only
// by the dependency probe (checkServeDepsFn) to schedule the probe worker on
// the head node, mirroring how the controller itself is scheduled. A var (not
// const) because Go maps are not constant expressions.
var serveControllerResources = map[string]float64{
	"node:__internal_head__": 0.001,
	api.ResourceCPU:          0,
}

// Version constants aligned with python/ray/dashboard/modules/version.py and
// python/ray/_version.py (the same values used by the job module).
const (
	// currentVersion is CURRENT_VERSION.
	currentVersion = "4"
)

// Serve deploy/timeout constants aligned with the Python serve client.
const (
	// clientPollingInterval is CLIENT_POLLING_INTERVAL_S (1s).
	clientPollingInterval = time.Second
	// deleteAppsTimeout is the blocking delete_apps timeout (60s).
	deleteAppsTimeout = 60 * time.Second
	// startControllerTimeout is how long the PUT handler waits for the
	// detached ServeController to appear after submitting _start_controller,
	// aligned with HTTP_PROXY_TIMEOUT (60s) which _start_controller uses
	// internally to wait for the HTTP proxies.
	startControllerTimeout = 60 * time.Second
	// startControllerPollInterval is the polling interval while waiting for
	// the controller actor to appear in GCS.
	startControllerPollInterval = 500 * time.Millisecond
)

// Valid user-supplied api_type values, aligned with
// APIType.get_valid_user_values() (excludes "unknown").
var validAPITypes = []string{"imperative", "declarative"}

// Usage tag KV constants aligned with usage_lib.record_extra_usage_tag: the
// tag is stored under "extra_usage_tag_<lowercased TagKey>" in the
// "usage_stats" namespace.
const (
	extraUsageTagPrefix = "extra_usage_tag_"
	usageStatsNamespace = "usage_stats"
	// serveRestAPIVersionTag is TagKey.SERVE_REST_API_VERSION recorded with
	// value "v2" on every dashboard PUT (aligned with
	// ServeHead.put_all_applications).
	serveRestAPIVersionTag = "serve_rest_api_version"
)

// errServeDependenciesNotInstalled is the 501 error returned when the ray[serve]
// dependencies are not installed, matching the text returned by
// validate_endpoint in python/ray/dashboard/modules/serve/serve_head.py.
var errServeDependenciesNotInstalled = errors.New("Serve dependencies are not installed. Please run `pip install \"ray[serve]\"`.")

// errServeDepsProbeTimeout is returned when the dependency probe exceeds its
// timeout. Unlike errServeDependenciesNotInstalled it is deliberately NOT
// cached: a timed-out probe is not evidence that ray[serve] is missing (a
// cold-start python worker can be slow to answer), so the next serve request
// re-probes instead of serving a cached 501 for the process lifetime.
var errServeDepsProbeTimeout = errors.New("Serve dependency probe timed out. The python worker did not answer in time; the probe is retried on the next request.")

// serveDependencyProbeModule is a ray.serve module whose import fails when the
// ray[serve] extra is missing. Importing ray.serve._private.utils pulls in
// ray.serve.schema / ray.serve._private.constants which depend on the optional
// fastapi/starlette/uvicorn packages (the ray[serve] extra), mirroring the
// `from ray import serve` import that validate_endpoint performs. The probed
// function is a pure helper with no side effects.
const serveDependencyProbeModule = "ray.serve._private.utils"
const serveDependencyProbeFunction = "get_random_string"

// serveDepsProbeTimeout bounds the cross-language dependency probe. The
// Python validate_endpoint imports ray.serve locally (never blocking); this Go
// probe schedules a Python worker, which can stall forever when the
// cross-language runtime cannot schedule one. A finite timeout keeps a stuck
// probe from holding depsMu and hanging every serve request.
const serveDepsProbeTimeout = 10 * time.Second

// checkServeDepsFn probes whether ray.serve is importable on a Python worker.
// It is a package-level variable so tests can inject a fake without loading
// go_runtime.so. It returns ErrRuntimeNotInitialized when the Go runtime is
// not yet initialized, and errServeDependenciesNotInstalled when the import
// fails.
var checkServeDepsFn = func() error {
	if !api.IsInitialized() {
		// The Go runtime is not initialized (e.g. the GCS was not ready during
		// `ray start`). Surface ErrRuntimeNotInitialized so the caller lazily
		// initializes and retries; without the runtime no Python worker can be
		// scheduled to probe the import.
		return rayerrors.ErrRuntimeNotInitialized
	}
	// Use RemotePython[string] (num_returns=1) rather than RemotePythonVoid:
	// get_random_string returns a value, and a void call (num_returns=0) yields
	// a nil object ref that api.Get always fails on ("object reference is nil"),
	// making every probe misreport dependencies as not installed.
	//
	// Pin the probe to the head node with the same resources as the
	// ServeController (node:__internal_head__: 0.001, CPU: 0). A bare
	// RemotePython defaults to {CPU: 1}, which can never be satisfied on a
	// CPU-less head node (KubeRay head groups run without a CPU resource):
	// the raylet logs "The lease ... requires {CPU: 1} ... cannot provide the
	// requested resources", so every probe times out after serveDepsProbeTimeout
	// and every serve request pays a 10s stall before failing — KubeRay can
	// never read serve application status in time (RayService stuck in
	// FailedToGetServeDeploymentStatus). Pinning to the
	// head node mirrors how the controller itself is scheduled.
	ref, err := api.RemotePython[string](serveDependencyProbeModule, serveDependencyProbeFunction, "").
		WithResources(serveControllerResources).Call()
	if err != nil {
		return err
	}
	// Bound the probe with a goroutine + select timeout rather than
	// GetWithTimeout: the C-level ObjectStoreOperations::Get treats a timeout as
	// a fatal CHECK failure (crashes the head), so a finite timeout cannot be
	// passed down. The Python validate_endpoint imports ray.serve locally and
	// never blocks, but this probe schedules a Python worker; when the
	// cross-language runtime cannot schedule one the Get would otherwise block
	// forever, hanging every serve request behind depsMu. A goroutine-level
	// timeout degrades to the same 501 the Python path returns on ImportError.
	type probeResult struct {
		err error
	}
	done := make(chan probeResult, 1)
	go func() {
		_, err := api.Get(ref)
		done <- probeResult{err: err}
	}()
	select {
	case res := <-done:
		if res.err != nil {
			// The Python worker failed to import ray.serve._private.utils, so
			// the ray[serve] dependencies are not installed.
			return errServeDependenciesNotInstalled
		}
		return nil
	case <-time.After(serveDepsProbeTimeout):
		// The probe never completed; degrade this request with a dedicated
		// timeout error (500) that is not cached, so the next serve request
		// re-probes. The abandoned Get goroutine may outlive the request, but
		// it no longer holds depsMu, so subsequent serve requests are not
		// blocked.
		return errServeDepsProbeTimeout
	}
}

// ServeHead implements the Serve applications API.
type ServeHead struct {
	cfg    *head.HeadConfig
	client *head.GCSClient

	controllerMu sync.Mutex
	controller   serveController

	depsMu      sync.Mutex
	depsChecked bool
	depsOK      bool
}

// New creates a ServeHead backed by the given GCS client.
func New(cfg *head.HeadConfig, client *head.GCSClient) *ServeHead {
	return &ServeHead{cfg: cfg, client: client}
}

// Name returns the module name.
func (h *ServeHead) Name() string { return "ServeHead" }

// Start has no background tasks.
func (h *ServeHead) Start(ctx context.Context) error { return nil }

// Healthy reports the module health.
func (h *ServeHead) Healthy() bool { return true }

// RegisterHTTP registers the serve routes.
func (h *ServeHead) RegisterHTTP(mux *http.ServeMux) error {
	mux.HandleFunc("GET /api/ray/version", h.handleVersion)
	mux.HandleFunc("GET /api/serve/applications/", h.handleGetApplications)
	mux.HandleFunc("DELETE /api/serve/applications/", h.handleDeleteApplications)
	mux.HandleFunc("PUT /api/serve/applications/", h.handlePutApplications)
	mux.HandleFunc("POST /api/v1/applications/{application_name}/deployments/{deployment_name}/scale", h.handleScaleDeployment)
	return nil
}

// VersionResponse mirrors python/ray/dashboard/modules/version.py.
type VersionResponse struct {
	Version     string `json:"version"`
	RayVersion  string `json:"ray_version"`
	RayCommit   string `json:"ray_commit"`
	SessionName string `json:"session_name"`
	// Runtime is a Go-extension additive field exposing the dashboard head's
	// own runtime init state for oncall. Python has no such field; consumers
	// reading the original four keys are unaffected.
	Runtime *head.RuntimeStatus `json:"runtime,omitempty"`
}

// handleVersion serves GET /api/ray/version, aligned with ServeHead.get_version.
func (h *ServeHead) handleVersion(w http.ResponseWriter, r *http.Request) {
	status := head.RuntimeStatusInfo()
	head.WriteRawJSON(w, http.StatusOK, &VersionResponse{
		Version:     currentVersion,
		RayVersion:  version.RayVersion,
		RayCommit:   version.RayCommit,
		SessionName: h.cfg.SessionName,
		Runtime:     &status,
	})
}

// serveController is the subset of the ServeController actor surface used by
// the serve routes. getServeController returns *api.PythonActorHandle wrapped
// in pythonServeController which satisfies it; the interface lets tests
// inject a fake.
type serveController interface {
	checkAlive() error
	getServeInstanceDetails() (map[string]interface{}, error)
	applyConfig(config map[string]interface{}) error
	shutdown() error
	deleteApps(names []string) error
	getServeStatuses(names []string) ([][]byte, error)
	listServeStatuses() ([][]byte, error)
	updateDeploymentReplicas(deploymentID map[string]interface{}, targetNumReplicas int) error
}

// pythonServeController adapts *api.PythonActorHandle to serveController via
// the cross-language ActorTask/Get API.
type pythonServeController struct {
	handle *api.PythonActorHandle
}

func (c *pythonServeController) checkAlive() error {
	ref, err := api.ActorTask[any](c.handle, "check_alive").Remote()
	if err != nil {
		return err
	}
	if _, err := api.Get(ref); err != nil {
		return err
	}
	return nil
}

func (c *pythonServeController) getServeInstanceDetails() (map[string]interface{}, error) {
	// get_serve_instance_details_native returns the instance details dict with
	// every Enum already reduced to its msgpack-native value (see the thin
	// method in python/ray/serve/_private/controller.py).
	ref, err := api.ActorTask[map[string]interface{}](c.handle, "get_serve_instance_details_native").Remote()
	if err != nil {
		return nil, err
	}
	return api.Get(ref)
}

func (c *pythonServeController) applyConfig(config map[string]interface{}) error {
	// apply_config_native(config, deployment_time) constructs the
	// ServeDeploySchema on the Python side (the controller cannot accept a raw
	// dict cross-language). is_full_update stays at its default True, aligned
	// with the Python dashboard default RAY_SERVE_DEPLOY_IS_FULL_UPDATE=1.
	ref, err := api.ActorTask[any](c.handle, "apply_config_native", config, 0.0).Remote()
	if err != nil {
		return err
	}
	if _, err := api.Get(ref); err != nil {
		return err
	}
	return nil
}

func (c *pythonServeController) shutdown() error {
	ref, err := api.ActorTask[any](c.handle, "graceful_shutdown").Remote()
	if err != nil {
		return err
	}
	// graceful_shutdown kills the controller, so the awaited Get surfaces a
	// RayActorError; that is the expected outcome of a shutdown and is
	// swallowed (any other error is also treated as the controller being gone).
	if _, err := api.Get(ref); err != nil {
		log.Log.V(1).Info("serve controller shutdown returned error", "err", err)
	}
	return nil
}

func (c *pythonServeController) deleteApps(names []string) error {
	ref, err := api.ActorTask[any](c.handle, "delete_apps", names).Remote()
	if err != nil {
		return err
	}
	if _, err := api.Get(ref); err != nil {
		return err
	}
	return nil
}

func (c *pythonServeController) getServeStatuses(names []string) ([][]byte, error) {
	ref, err := api.ActorTask[[][]byte](c.handle, "get_serve_statuses", names).Remote()
	if err != nil {
		return nil, err
	}
	return api.Get(ref)
}

func (c *pythonServeController) listServeStatuses() ([][]byte, error) {
	ref, err := api.ActorTask[[][]byte](c.handle, "list_serve_statuses").Remote()
	if err != nil {
		return nil, err
	}
	return api.Get(ref)
}

func (c *pythonServeController) updateDeploymentReplicas(deploymentID map[string]interface{}, targetNumReplicas int) error {
	// update_deployment_replicas_native constructs the DeploymentID on the
	// Python side from the raw dict (see the thin method in
	// python/ray/serve/_private/controller.py).
	ref, err := api.ActorTask[any](c.handle, "update_deployment_replicas_native", deploymentID, targetNumReplicas).Remote()
	if err != nil {
		return err
	}
	if _, err := api.Get(ref); err != nil {
		return err
	}
	return nil
}

// getServeControllerFn returns the original ServeController actor handle
// (discovered by name/namespace), or nil when no serve instance is running. It
// is a package-level variable so tests can inject a fake controller.
//
// The controller is detached and created by the Python serve machinery
// (get_controller_impl), so Go only discovers it: no wrapper actor is created
// and no resources need to be inherited. A not-initialized runtime is surfaced
// so the caller can lazily initialize and retry; absence of the controller
// means "no serve instance running" -> caller degrades (aligned with
// ServeHead.get_serve_controller).
var getServeControllerFn = func() (serveController, error) {
	handle, err := api.GetPythonActorWithNamespace(
		serveControllerName, serveNamespace, serveControllerModule, serveControllerClass)
	if err != nil {
		if errors.Is(err, rayerrors.ErrRuntimeNotInitialized) {
			return nil, err
		}
		// Debug level (suppressed at the default INFO level), aligned with the
		// Python ServeHead which logs this at logger.debug so it does not spam
		// the dashboard log on every request when no serve instance is running.
		log.Log.V(1).Info("serve: no real controller, degrade", "err", err)
		return nil, nil
	}
	return &pythonServeController{handle: handle}, nil
}

// getServeController returns the cached ServeController, validating it with
// check_alive first and re-fetching on failure, aligned with
// ServeHead.get_serve_controller. It returns nil when no serve instance is
// running.
func (h *ServeHead) getServeController() (serveController, error) {
	h.controllerMu.Lock()
	defer h.controllerMu.Unlock()
	if h.controller != nil {
		if err := h.controller.checkAlive(); err != nil {
			log.Log.Info("serve controller is dead, re-fetching")
			h.controller = nil
		} else {
			return h.controller, nil
		}
	}
	controller, err := getServeControllerFn()
	if err != nil {
		log.Log.V(1).Info("no serve controller available", "err", err)
		return nil, nil
	}
	if controller == nil {
		// No serve instance running (getServeControllerFn could not find the
		// real controller). Aligned with ServeHead.get_serve_controller
		// returning None.
		return nil, nil
	}
	// Validate the discovered controller with check_alive before caching: a
	// stale handle pointing at a dead controller degrades to nil (aligned with
	// ServeHead.get_serve_controller returning None when no serve instance is
	// running) instead of poisoning the cache with a dead actor that later
	// surfaces as a 503.
	if err := controller.checkAlive(); err != nil {
		log.Log.Info("serve controller is dead, no serve instance running", "err", err)
		return nil, nil
	}
	h.controller = controller
	return controller, nil
}

// checkServeDependencies verifies that the ray[serve] dependencies are
// installed, aligned with validate_endpoint in
// python/ray/dashboard/modules/serve/serve_head.py which imports ray.serve
// before handling any serve request and returns 501 "Serve dependencies are
// not installed" on ImportError. A positive result is cached per process like
// the flow module's dependency check; a failed probe is cached too, but a
// timed-out probe is not (a slow python worker must not degrade serve for the
// process lifetime). When the Go runtime cannot be initialized
// (so no Python worker can be scheduled to probe the import), the check
// degrades to proceeding: the caller still degrades to the empty schema when
// no serve instance is running.
func (h *ServeHead) checkServeDependencies() error {
	h.depsMu.Lock()
	defer h.depsMu.Unlock()
	if h.depsChecked {
		if h.depsOK {
			return nil
		}
		return errServeDependenciesNotInstalled
	}
	if err := checkServeDepsFn(); err != nil {
		if errors.Is(err, rayerrors.ErrRuntimeNotInitialized) {
			// The Go runtime is not available (the withRuntimeInitialization
			// middleware would normally have initialized it; when a handler is
			// reached with the runtime still down, e.g. a direct handler test,
			// the probe cannot run). Skip the dependency check and let the
			// caller degrade to "no serve instance" (empty schema) instead of
			// reporting a misleading 501.
			return nil
		}
		if errors.Is(err, errServeDepsProbeTimeout) {
			// A timed-out probe is not evidence that the dependencies are
			// missing. Leave depsChecked/depsOK untouched so the next serve
			// request re-probes instead of serving a cached 501 forever.
			return err
		}
		h.depsChecked = true
		h.depsOK = false
		return errServeDependenciesNotInstalled
	}
	h.depsChecked = true
	h.depsOK = true
	return nil
}

// handleGetApplications serves GET /api/serve/applications/, aligned with
// ServeHead.get_serve_instance_details.
func (h *ServeHead) handleGetApplications(w http.ResponseWriter, r *http.Request) {
	// Dependency pre-check before any other handling, aligned with Python's
	// @validate_endpoint() decorator which runs before get_serve_instance_details
	// parses the request: a missing ray[serve] install returns 501.
	if err := h.checkServeDependencies(); err != nil {
		if errors.Is(err, errServeDependenciesNotInstalled) {
			http.Error(w, err.Error(), http.StatusNotImplemented)
			return
		}
		// A timed-out probe (or any other probe failure) is a transient
		// server-side problem, not "dependencies missing": degrade with 500
		// and let the next request re-probe.
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	apiTypeStr := r.URL.Query().Get("api_type")
	if apiTypeStr != "" {
		apiTypeLower := strings.ToLower(apiTypeStr)
		if !containsString(validAPITypes, apiTypeLower) {
			http.Error(w, fmt.Sprintf(
				"Invalid 'api_type' value: '%s'. Must be one of: %s",
				apiTypeStr, strings.Join(validAPITypes, ", ")),
				http.StatusBadRequest)
			return
		}
	}

	controller, err := h.getServeController()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if controller == nil {
		// No serve instance running: return the empty schema dict.
		head.WriteRawJSON(w, http.StatusOK, serveEmptyInstanceDetails())
		return
	}
	details, err := controller.getServeInstanceDetails()
	if err != nil {
		http.Error(w, "Failed to get a response from the controller. The GCS may be down, please retry later: "+err.Error(),
			http.StatusServiceUnavailable)
		return
	}
	head.WriteRawJSON(w, http.StatusOK, details)
}

// serveEmptyInstanceDetails returns ServeInstanceDetails.get_empty_schema_dict()
// (python/ray/serve/schema.py), representing no Serve instance running. This
// tree's ServeInstanceDetails has no deploy_mode field (single-app mode was
// removed upstream), so the mirror carries the four keys of the current schema.
func serveEmptyInstanceDetails() map[string]interface{} {
	return map[string]interface{}{
		"controller_info": map[string]interface{}{},
		"proxies":         map[string]interface{}{},
		"applications":    map[string]interface{}{},
		"target_capacity": nil,
	}
}

// serveDeploySchema is a minimal Go mirror of the Python ServeDeploySchema
// used to extract the application names for the delete path.
type serveDeploySchema struct {
	Applications []struct {
		Name string `json:"name"`
	} `json:"applications"`
}

// validateServeDeploySchema validates the parsed PUT body against the required
// fields of the Python ServeDeploySchema (python/ray/serve/schema.py): the
// "applications" list must be present (it is a required field marked with
// Field(...)), every application must have a non-empty name, and each
// application must either provide an import_path or inline deployments
// (ServeApplicationSchema.import_path is REQUIRED unless the application is
// defined inline through deployments). Duplicate application names and
// duplicate route prefixes are rejected, mirroring the pydantic validators.
// It returns a human-readable error message when the config is invalid.
func validateServeDeploySchema(config map[string]interface{}) error {
	appsRaw, ok := config["applications"].([]interface{})
	if !ok || len(appsRaw) == 0 {
		return errors.New("applications field is required and must be a non-empty list")
	}
	seenNames := map[string]bool{}
	seenRoutes := map[string]bool{}
	for i, raw := range appsRaw {
		app, ok := raw.(map[string]interface{})
		if !ok {
			return fmt.Errorf("application at index %d must be a dictionary", i)
		}
		name, _ := app["name"].(string)
		if name == "" {
			return errors.New("Application names must be nonempty.")
		}
		if seenNames[name] {
			return fmt.Errorf("Found multiple configs for application \"%s\". Please remove all duplicates.", name)
		}
		seenNames[name] = true
		if route, _ := app["route_prefix"].(string); route != "" {
			if seenRoutes[route] {
				return fmt.Errorf("Found duplicate applications for route prefix \"%s\". Please ensure each application's route_prefix is unique.", route)
			}
			seenRoutes[route] = true
		}
		importPath, _ := app["import_path"].(string)
		deployments, _ := app["deployments"].([]interface{})
		if importPath == "" && len(deployments) == 0 {
			return fmt.Errorf("application \"%s\" must provide an import_path or deployments", name)
		}
	}
	return nil
}

// handleDeleteApplications serves DELETE /api/serve/applications/, aligned
// with the company-customized ServeHead.delete_serve_applications: an empty
// body shuts down the whole Serve instance; a non-empty body deletes the named
// applications (blocking until NOT_STARTED or 60s timeout) and shuts down
// Serve when no applications remain.
func (h *ServeHead) handleDeleteApplications(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	var reqJSON map[string]interface{}
	jsonValid := len(body) > 0 && json.Unmarshal(body, &reqJSON) == nil

	// Case 1: empty body -> shutdown the whole Serve instance.
	if !jsonValid || len(reqJSON) == 0 {
		controller, err := h.getServeController()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if controller != nil {
			if err := controller.shutdown(); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
		}
		w.WriteHeader(http.StatusOK)
		return
	}

	// Case 2: non-empty body -> parse as ServeDeploySchema and delete apps.
	var config serveDeploySchema
	if err := json.Unmarshal(body, &config); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	appNames := make([]string, 0, len(config.Applications))
	for _, app := range config.Applications {
		if app.Name != "" {
			appNames = append(appNames, app.Name)
		}
	}
	if len(appNames) == 0 {
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "No application names provided in request.")
		return
	}

	controller, err := h.getServeController()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if controller == nil {
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "No Serve controller running, exit.")
		return
	}

	if err := controller.deleteApps(appNames); err != nil {
		http.Error(w, fmt.Sprintf("Unexpected error while deleting applications %v: %v", appNames, err),
			http.StatusBadRequest)
		return
	}
	// Block until every app reaches NOT_STARTED, aligned with
	// ServeClient.delete_apps(blocking=True).
	deadline := time.Now().Add(deleteAppsTimeout)
	allDeleted := false
	for time.Now().Before(deadline) {
		statusesBytes, err := controller.getServeStatuses(appNames)
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to delete applications %v: %v", appNames, err), http.StatusBadRequest)
			return
		}
		allDeleted = true
		for _, statusBytes := range statusesBytes {
			if serveAppStatus(statusBytes) != serveStatusNotStarted {
				allDeleted = false
				break
			}
		}
		if allDeleted {
			break
		}
		select {
		case <-time.After(clientPollingInterval):
		case <-r.Context().Done():
			return
		}
	}
	if !allDeleted {
		http.Error(w, fmt.Sprintf("Failed to delete applications %v: Some of these applications weren't deleted after 60s", appNames),
			http.StatusBadRequest)
		return
	}

	// Check whether any applications remain; if not, shut down Serve.
	remaining, err := controller.listServeStatuses()
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if len(remaining) == 0 {
		if err := controller.shutdown(); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		head.WriteRawJSON(w, http.StatusOK, map[string]string{
			"message": fmt.Sprintf("Applications %v deleted, no apps left, Serve shutdown", appNames),
		})
		return
	}
	head.WriteRawJSON(w, http.StatusOK, map[string]string{
		"message": fmt.Sprintf("Applications %v deleted", appNames),
	})
}

// Serve application status enum values, aligned with ApplicationStatus in
// src/ray/protobuf/serve.proto.
const (
	serveStatusNotStarted = 5
)

// serveAppStatus decodes the StatusOverview protobuf bytes returned by the
// ServeController and returns the application status enum value.
func serveAppStatus(statusBytes []byte) int32 {
	// StatusOverview { app_status(1)=ApplicationStatusInfo, deployment_statuses(2), name(3) }.
	// ApplicationStatusInfo { status(1)=enum, message(2), deployment_timestamp(3) }.
	var status int32
	for len(statusBytes) > 0 {
		num, typ, n := protowire.ConsumeTag(statusBytes)
		if n < 0 {
			return 0
		}
		statusBytes = statusBytes[n:]
		if typ == protowire.BytesType {
			fieldBytes, n := protowire.ConsumeBytes(statusBytes)
			if n < 0 {
				return 0
			}
			statusBytes = statusBytes[n:]
			if num == 1 {
				// app_status: parse the nested ApplicationStatusInfo.
				status = decodeAppStatusInfo(fieldBytes)
			}
			continue
		}
		n = protowire.ConsumeFieldValue(num, typ, statusBytes)
		if n < 0 {
			return 0
		}
		statusBytes = statusBytes[n:]
	}
	return status
}

// decodeAppStatusInfo decodes the ApplicationStatusInfo protobuf and returns
// its status enum value.
func decodeAppStatusInfo(data []byte) int32 {
	for len(data) > 0 {
		num, typ, n := protowire.ConsumeTag(data)
		if n < 0 {
			return 0
		}
		data = data[n:]
		if num == 1 && typ == protowire.VarintType {
			v, n := protowire.ConsumeVarint(data)
			if n < 0 {
				return 0
			}
			return int32(v)
		}
		n = protowire.ConsumeFieldValue(num, typ, data)
		if n < 0 {
			return 0
		}
		data = data[n:]
	}
	return 0
}

// handlePutApplications serves PUT /api/serve/applications/, aligned with
// ServeHead.put_all_applications. When no Serve controller is running it
// starts one cross-language (mirroring serve_start_async) before deploying the
// applications, so the endpoint works on a fresh cluster instead of returning
// 503. An existing controller is reused (its http_options are ignored, matching
// the Python behavior when Serve is already running).
func (h *ServeHead) handlePutApplications(w http.ResponseWriter, r *http.Request) {
	// Dependency pre-check, aligned with the Python @validate_endpoint()
	// decorator on put_all_applications.
	if err := h.checkServeDependencies(); err != nil {
		if errors.Is(err, errServeDependenciesNotInstalled) {
			http.Error(w, err.Error(), http.StatusNotImplemented)
			return
		}
		// A timed-out probe (or any other probe failure) is a transient
		// server-side problem, not "dependencies missing": degrade with 500
		// and let the next request re-probe.
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	var config map[string]interface{}
	if err := json.Unmarshal(body, &config); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if config == nil {
		http.Error(w, "applications field is required", http.StatusBadRequest)
		return
	}
	// Validate against the ServeDeploySchema required fields, aligned with the
	// Python pydantic validation (ServeDeploySchema.parse_obj) which returns 400
	// on ValidationError (serve_head.py:202-207).
	if err := validateServeDeploySchema(config); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	controller, err := h.getServeController()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if controller == nil {
		// No Serve instance running: start one, aligned with
		// serve_start_async. A startup failure is a server-side error (the
		// Python decorator surfaces any exception from serve_start_async as a
		// 500); only deploy failures map to 400 below.
		controller, err = startControllerFn(h, r.Context(), config)
		if err != nil {
			log.Log.Error(err, "serve: failed to start controller on PUT")
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
	if err := controller.applyConfig(config); err != nil {
		// Aligned with the Python deploy_apps RayTaskError -> 400 branch.
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	// Record the SERVE_REST_API_VERSION usage tag, aligned with
	// ServeHead.put_all_applications. The config's http_options/logging_config
	// are carried inside the deploy config passed to applyConfig (the controller
	// parses ServeDeploySchema which includes them); http_options are only
	// honored on startup, which matches the Python behavior of ignoring them
	// when Serve is already running.
	h.recordServeAPIVersionTag(r.Context())
	log.Log.Info("serve: applications deployed", "path", r.URL.Path)
	w.WriteHeader(http.StatusOK)
}

// startControllerFn starts a Serve instance when none is running. It is a
// package-level variable so tests can inject a fake; the production
// implementation is startServeController.
var startControllerFn = func(h *ServeHead, ctx context.Context, config map[string]interface{}) (serveController, error) {
	return h.startServeController(ctx, config)
}

// startServeController starts a Serve instance cross-language and returns the
// controller handle, mirroring serve_start_async in
// python/ray/serve/_private/api.py: it builds the http/grpc/logging options
// from the deploy config and submits _start_controller on a Python worker. The
// controller is a detached named actor (SERVE_CONTROLLER_ACTOR in the "serve"
// namespace, created by the controller impl with lifetime="detached"), so Go
// does not need the returned handle: it only waits for the task to run and the
// controller actor to appear in GCS, then re-discovers it via
// getServeControllerFn. The _start_controller task itself blocks until the HTTP
// proxies are ready, so a successful submission + actor appearance means the
// instance is fully started.
func (h *ServeHead) startServeController(ctx context.Context, config map[string]interface{}) (serveController, error) {
	httpOptions := buildHTTPOptions(config)
	grpcOptions := config["grpc_options"]
	if grpcOptions == nil {
		grpcOptions = map[string]interface{}{}
	}
	// Python's _start_controller accepts None and substitutes defaults
	// (LoggingConfig() / gRPCOptions(**{})); pass an empty dict instead of Go
	// nil because the cross-language arg serializer panics on a nil interface
	// (reflect.TypeOf(nil).Kind()).
	loggingConfig := config["logging_config"]
	if loggingConfig == nil {
		loggingConfig = map[string]interface{}{}
	}

	log.Log.Info("serve: starting controller cross-language",
		"http_options", httpOptions, "grpc_options", grpcOptions, "logging_config", loggingConfig)

	// Submit _start_controller on a Python worker with num_cpus=0 (aligned with
	// serve_start_async's .options(num_cpus=0)). The return value is an
	// ActorHandle that Go cannot deserialize, so use RemotePythonVoid and rely
	// on the detached named controller appearing in GCS instead.
	ref, err := api.RemotePythonVoid("ray.serve._private.api", "_start_controller", "").
		WithResources(map[string]float64{api.ResourceCPU: 0}).
		Call(httpOptions, grpcOptions, loggingConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to submit serve controller start: %w", err)
	}
	if ref != nil {
		// Wait for the controller task result; a failure here (e.g. the Python
		// worker raised inside _start_controller) surfaces the error from the
		// object store instead of a misleading timeout below.
		if _, err := api.Get(ref); err != nil {
			return nil, fmt.Errorf("serve controller start failed: %w", err)
		}
	}

	// The controller is detached, so it survives even though the task that
	// created it has finished. Wait for it to appear under its well-known name.
	deadline := time.Now().Add(startControllerTimeout)
	for time.Now().Before(deadline) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if controller, err := getServeControllerFn(); err != nil {
			return nil, err
		} else if controller != nil {
			log.Log.Info("serve: controller started cross-language")
			return controller, nil
		}
		select {
		case <-time.After(startControllerPollInterval):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return nil, fmt.Errorf("serve controller did not start within %s", startControllerTimeout)
}

// buildHTTPOptions assembles the full_http_options dict passed to
// _start_controller, aligned with ServeHead.put_all_applications:
// location = ProxyLocation._to_deployment_mode(config.proxy_location) merged
// over the config's http_options fields. ProxyLocation.Disabled maps to
// DeploymentMode.NoServer; HeadOnly/EveryNode keep their values.
func buildHTTPOptions(config map[string]interface{}) map[string]interface{} {
	httpOptions := map[string]interface{}{}
	if opts, ok := config["http_options"].(map[string]interface{}); ok {
		for k, v := range opts {
			httpOptions[k] = v
		}
	}
	location := "EveryNode"
	if proxyLocation, ok := config["proxy_location"].(string); ok && proxyLocation != "" {
		if proxyLocation == "Disabled" {
			location = "NoServer"
		} else {
			location = proxyLocation
		}
	}
	httpOptions["location"] = location
	return httpOptions
}

// recordServeAPIVersionTag writes TagKey.SERVE_REST_API_VERSION="v2" to the
// GCS internal KV usage_stats namespace, aligned with usage_lib's
// record_extra_usage_tag. The Python side de-duplicates identical values;
// Go mirrors that by only writing when the tag is absent or different. A
// failure is logged, never fatal.
func (h *ServeHead) recordServeAPIVersionTag(ctx context.Context) {
	if h.client == nil {
		return
	}
	key := extraUsageTagPrefix + serveRestAPIVersionTag
	reply, err := h.client.InternalKVGet(ctx, usageStatsNamespace, key)
	if err == nil && len(reply.Value) > 0 && string(reply.Value) == "v2" {
		return
	}
	if _, err := h.client.InternalKVPut(ctx, usageStatsNamespace, key, []byte("v2"), true); err != nil {
		log.Log.V(1).Info("failed to record serve api version usage tag", "err", err)
	}
}

// handleScaleDeployment serves POST
// /api/v1/applications/{application_name}/deployments/{deployment_name}/scale,
// aligned with ServeHead.scale_deployment.
func (h *ServeHead) handleScaleDeployment(w http.ResponseWriter, r *http.Request) {
	applicationName := r.PathValue("application_name")
	deploymentName := r.PathValue("deployment_name")
	if applicationName == "" || deploymentName == "" {
		head.WriteRawJSON(w, http.StatusBadRequest, map[string]string{
			"error": "Missing application_name or deployment_name in path",
		})
		return
	}

	var requestData map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&requestData); err != nil {
		head.WriteRawJSON(w, http.StatusBadRequest, map[string]string{
			"error": fmt.Sprintf("Invalid request body: %v", err),
		})
		return
	}
	targetNumReplicas, ok := requestData["target_num_replicas"].(float64)
	if !ok {
		head.WriteRawJSON(w, http.StatusBadRequest, map[string]string{
			"error": "Invalid request body: target_num_replicas is required and must be an integer",
		})
		return
	}

	controller, err := h.getServeController()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if controller == nil {
		head.WriteRawJSON(w, http.StatusServiceUnavailable, map[string]string{
			"error": "Serve controller is not available",
		})
		return
	}

	deploymentID := map[string]interface{}{
		"name":     deploymentName,
		"app_name": applicationName,
	}
	log.Log.Info("scaling deployment",
		"deployment", deploymentName, "application", applicationName,
		"target_num_replicas", int(targetNumReplicas))
	if err := controller.updateDeploymentReplicas(deploymentID, int(targetNumReplicas)); err != nil {
		errStr := err.Error()
		switch {
		case strings.Contains(errStr, "Deployment is deleted"):
			head.WriteRawJSON(w, http.StatusPreconditionFailed, map[string]string{"error": "Deployment is deleted"})
		case strings.Contains(errStr, "external_scaler_enabled"):
			head.WriteRawJSON(w, http.StatusPreconditionFailed, map[string]string{"error": errStr})
		case strings.Contains(errStr, "not found"):
			head.WriteRawJSON(w, http.StatusBadRequest, map[string]string{"error": "Application or Deployment not found"})
		default:
			log.Log.Error(err, "internal server error while scaling deployment")
			head.WriteRawJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "Internal Server Error"})
		}
		return
	}
	head.WriteRawJSON(w, http.StatusOK, map[string]string{
		"message": "Scaling request received. Deployment will get scaled asynchronously.",
	})
}

// containsString reports whether the slice contains the target string.
func containsString(slice []string, target string) bool {
	for _, s := range slice {
		if s == target {
			return true
		}
	}
	return false
}
