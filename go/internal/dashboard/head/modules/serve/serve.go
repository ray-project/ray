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

// Cross-language actor identity constants for the ServeControllerWrapper, the
// dashboard's proxy into the real ServeController, centralized here so the
// name/namespace/module/className quadruple lives in one place. Aligned with
// python/ray/go/dashboard/cross_language/serve_wrapper.py.
const (
	// serveControllerName is the ServeControllerWrapper actor name.
	serveControllerName = "go_dashboard_serve_wrapper"
	// serveNamespace is SERVE_NAMESPACE.
	serveNamespace = "serve"
	// serveControllerModule is the Python module of the wrapper actor.
	serveControllerModule = "ray.go.dashboard.cross_language.serve_wrapper"
	// serveControllerClass is the Python actor class name.
	serveControllerClass = "ServeControllerWrapper"
)

// Resource/lifecycle constants for the ServeControllerWrapper, aligned with
// the original ServeController created by get_controller_impl in
// python/ray/serve/_private/default_impl.py: num_cpus=0, resources={
// HEAD_NODE_RESOURCE_NAME: 0.001} (where HEAD_NODE_RESOURCE_NAME is
// "node:__internal_head__"), max_restarts=-1, max_task_retries=-1 and
// max_concurrency=CONTROLLER_MAX_CONCURRENCY (15000). The wrapper is a thin
// proxy over the real controller, so it inherits the controller's resource
// footprint (zero CPU quota, pinned to the head node) rather than the Go
// default of CPU=1.
const (
	// serveControllerMaxRestarts is max_restarts=-1 (unlimited restarts).
	serveControllerMaxRestarts = -1
	// serveControllerMaxTaskRetries is max_task_retries=-1 (unlimited retries).
	serveControllerMaxTaskRetries = -1
	// serveControllerMaxConcurrency is CONTROLLER_MAX_CONCURRENCY (15000).
	serveControllerMaxConcurrency = 15000
)

// serveControllerResources mirrors the controller's resources
// {HEAD_NODE_RESOURCE_NAME: 0.001} plus num_cpus=0: the 0.001 head-node
// resource pins it to the head node and zeroes the CPU quota. A var (not
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

// serveDependencyProbeModule is a ray.serve module whose import fails when the
// ray[serve] extra is missing. Importing ray.serve._private.utils pulls in
// ray.serve.schema / ray.serve._private.constants which depend on the optional
// fastapi/starlette/uvicorn packages (the ray[serve] extra), mirroring the
// `from ray import serve` import that validate_endpoint performs. The probed
// function is a pure helper with no side effects.
const serveDependencyProbeModule = "ray.serve._private.utils"
const serveDependencyProbeFunction = "get_random_string"

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
	ref, err := api.RemotePython[string](serveDependencyProbeModule, serveDependencyProbeFunction, "").Call()
	if err != nil {
		return err
	}
	if _, err := api.Get(ref); err != nil {
		// The Python worker failed to import ray.serve._private.utils, so the
		// ray[serve] dependencies are not installed.
		return errServeDependenciesNotInstalled
	}
	return nil
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
}

// handleVersion serves GET /api/ray/version, aligned with ServeHead.get_version.
func (h *ServeHead) handleVersion(w http.ResponseWriter, r *http.Request) {
	head.WriteRawJSON(w, http.StatusOK, &VersionResponse{
		Version:     currentVersion,
		RayVersion:  version.RayVersion,
		RayCommit:   version.RayCommit,
		SessionName: h.cfg.SessionName,
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
	ref, err := api.ActorTask[map[string]interface{}](c.handle, "get_serve_instance_details").Remote()
	if err != nil {
		return nil, err
	}
	return api.Get(ref)
}

func (c *pythonServeController) applyConfig(config map[string]interface{}) error {
	// apply_config(config, deployment_time=0.0, *, is_full_update=True). The
	// keyword-only is_full_update defaults to True (aligned with the Python
	// dashboard default RAY_SERVE_DEPLOY_IS_FULL_UPDATE=1), so only the two
	// positional args are passed cross-language.
	ref, err := api.ActorTask[any](c.handle, "apply_config", config, 0.0).Remote()
	if err != nil {
		return err
	}
	if _, err := api.Get(ref); err != nil {
		return err
	}
	return nil
}

func (c *pythonServeController) shutdown() error {
	ref, err := api.ActorTask[any](c.handle, "shutdown").Remote()
	if err != nil {
		return err
	}
	// The wrapper's shutdown calls the real controller's graceful_shutdown and
	// swallows the RayActorError raised when the controller is killed; any
	// error here is treated as the controller being gone, which is the expected
	// outcome of a shutdown.
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
	ref, err := api.ActorTask[any](c.handle, "update_deployment_replicas", deploymentID, targetNumReplicas).Remote()
	if err != nil {
		return err
	}
	if _, err := api.Get(ref); err != nil {
		return err
	}
	return nil
}

// getServeControllerFn returns the ServeControllerWrapper actor handle, or an
// error when no serve instance is running. It is a package-level variable so
// tests can inject a fake controller.
//
// The wrapper is a non-detached dashboard proxy: it is created on first use
// (Get-first, Create-fallback) and recycled with the dashboard driver, so a
// fresh Create never collides with a leftover actor. Its constructor binds the
// real ServeController via ray.get_actor, so creation fails (and the caller
// degrades) when no serve instance is running.
var getServeControllerFn = func() (serveController, error) {
	handle, err := api.GetPythonActorWithNamespace(
		serveControllerName, serveNamespace, serveControllerModule, serveControllerClass)
	if err == nil {
		log.Log.Info("serve: got existing wrapper")
		return &pythonServeController{handle: handle}, nil
	}
	// A not-initialized runtime (e.g. the GCS was not ready when the head
	// started) is surfaced to the caller so it can lazily initialize and retry;
	// any other error means no serve instance is running and we degrade.
	if errors.Is(err, rayerrors.ErrRuntimeNotInitialized) {
		return nil, err
	}
	// No wrapper yet. Only create it when a real ServeController exists,
	// otherwise the wrapper's __init__ (which binds the controller via
	// ray.get_actor) dies asynchronously and the cached dead handle surfaces
	// as a misleading 503. Mirroring ServeHead.get_serve_controller, absence
	// of the controller means "no serve instance running" -> caller degrades.
	// SERVE_CONTROLLER_ACTOR is the detached controller name from
	// ray.serve._private.constants.
	if _, err := api.GetPythonActorWithNamespace(
		"SERVE_CONTROLLER_ACTOR", serveNamespace, "ray.serve._private.controller", "ServeController"); err != nil {
		if errors.Is(err, rayerrors.ErrRuntimeNotInitialized) {
			return nil, err
		}
		// Debug level (suppressed at the default INFO level), aligned with the
		// Python ServeHead which logs this at logger.debug so it does not spam
		// the dashboard log on every request when no serve instance is running.
		log.Log.V(1).Info("serve: no real controller, degrade", "err", err)
		return nil, nil
	}
	log.Log.Info("serve: real controller exists, creating wrapper")
	handle, err = api.RemotePythonActor(serveControllerModule, serveControllerClass).
		WithName(serveControllerName).WithNamespace(serveNamespace).
		WithResources(serveControllerResources).
		WithMaxRestarts(serveControllerMaxRestarts).
		WithMaxTaskRetries(serveControllerMaxTaskRetries).
		WithMaxConcurrency(serveControllerMaxConcurrency).
		Create()
	if err != nil {
		return nil, err
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
	if errors.Is(err, rayerrors.ErrRuntimeNotInitialized) {
		// The Go runtime failed to initialize at startup (e.g. the GCS was not
		// ready during `ray start`). Lazily initialize it now, mirroring the
		// Python dashboard's require_initialized which ray.init()s on the first
		// request, so the head recovers the cross-language runtime without a
		// restart. EnsureInitialized is a no-op once initialized and serialized
		// against concurrent calls. Retry once after a successful init; on
		// failure degrade to "no serve instance" so the caller returns the
		// empty schema instead of a hard error.
		if initErr := head.EnsureInitialized(h.cfg); initErr != nil {
			log.Log.Info("serve: go runtime not initialized, degrade", "err", initErr)
			return nil, nil
		}
		controller, err = getServeControllerFn()
	}
	if err != nil {
		log.Log.V(1).Info("no serve controller available", "err", err)
		return nil, nil
	}
	if controller == nil {
		// No serve instance running (getServeControllerFn could not find a
		// real controller, so it did not create a wrapper). Aligned with
		// ServeHead.get_serve_controller returning None.
		return nil, nil
	}
	// The wrapper is created asynchronously (RemotePythonActor.Create returns
	// before the wrapper __init__ binds the real controller), so a freshly
	// created wrapper whose __init__ failed to find a real controller is
	// already dead. Validate it before caching so a dead wrapper degrades to
	// nil (aligned with ServeHead.get_serve_controller returning None when no
	// serve instance is running) instead of poisoning the cache with a dead
	// actor that later surfaces as a 503.
	if err := controller.checkAlive(); err != nil {
		log.Log.Info("serve controller wrapper is dead, no serve instance running", "err", err)
		return nil, nil
	}
	h.controller = controller
	return controller, nil
}

// checkServeDependencies verifies that the ray[serve] dependencies are
// installed, aligned with validate_endpoint in
// python/ray/dashboard/modules/serve/serve_head.py which imports ray.serve
// before handling any serve request and returns 501 "Serve dependencies are
// not installed" on ImportError. The result is cached per process like the
// flow module's dependency check. When the Go runtime cannot be initialized
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
			if initErr := head.EnsureInitialized(h.cfg); initErr != nil {
				log.Log.Info("serve: go runtime not initialized, skip dependency check", "err", initErr)
				return nil
			}
			err = checkServeDepsFn()
		}
		if err != nil {
			h.depsChecked = true
			h.depsOK = false
			return errServeDependenciesNotInstalled
		}
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
		http.Error(w, err.Error(), http.StatusNotImplemented)
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
// (python/ray/serve/schema.py), representing no Serve instance running.
func serveEmptyInstanceDetails() map[string]interface{} {
	return map[string]interface{}{
		"deploy_mode":     "MULTI_APP",
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
		http.Error(w, err.Error(), http.StatusNotImplemented)
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
	// are carried inside the deploy config passed to applyConfig (the wrapper
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
			if errors.Is(err, rayerrors.ErrRuntimeNotInitialized) {
				return nil, err
			}
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
