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

package agent

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"go.uber.org/zap/zapcore"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"

	"github.com/go-logr/logr"
	"github.com/ray-project/ray/go/internal/common"
	"github.com/ray-project/ray/go/internal/runtime_env"
	"github.com/ray-project/ray/go/pkg/gcs"
	"github.com/ray-project/ray/go/pkg/log"
	"github.com/ray-project/ray/go/pkg/log/zap"
	runtime_env_agent_pb "github.com/ray-project/ray/go/proto"
)

// ExcludedSourceClientServer is the source process excluded from reference tracking.
const ExcludedSourceClientServer = "client_server"

// URIWithType is a URI paired with its type.
type URIWithType struct {
	URI     string
	URIType string
}

// CreatedEnvResult is the outcome of a runtime env creation.
type CreatedEnvResult struct {
	Success        bool
	Result         string
	CreationTimeMs int64
}

type ReferenceTable struct {
	// refsMu serializes the read-modify-write sequences on the two maps
	// below. sync.Map only makes single operations atomic; the Load-then-
	// Store counting pattern loses updates under concurrency, and a lost
	// decrement can drop a runtime env that a second holder still uses.
	refsMu                   sync.Mutex
	runtimeEnvRefs           sync.Map
	uriRef                   sync.Map
	urisParser               func(*runtime_env.RuntimeEnv) []URIWithType
	unusedURIsCallback       func([]URIWithType)
	unusedRuntimeEnvCallback func(string)
	excludeSources           map[string]struct{}
}

func NewReferenceTable(
	urisParser func(*runtime_env.RuntimeEnv) []URIWithType,
	unusedURIsCallback func([]URIWithType),
	unusedRuntimeEnvCallback func(string),
	excludeSources map[string]struct{},
) *ReferenceTable {
	return &ReferenceTable{
		urisParser:               urisParser,
		unusedURIsCallback:       unusedURIsCallback,
		unusedRuntimeEnvCallback: unusedRuntimeEnvCallback,
		excludeSources:           excludeSources,
	}
}

func (rt *ReferenceTable) IncreaseReference(
	env *runtime_env.RuntimeEnv,
	serializedEnv string,
	sourceProcess string,
) {
	if _, excluded := rt.excludeSources[sourceProcess]; excluded {
		return
	}

	rt.refsMu.Lock()
	defer rt.refsMu.Unlock()

	if count, _ := rt.runtimeEnvRefs.Load(serializedEnv); count != nil {
		rt.runtimeEnvRefs.Store(serializedEnv, count.(int)+1)
	} else {
		rt.runtimeEnvRefs.Store(serializedEnv, 1)
	}

	uris := rt.urisParser(env)
	for _, uri := range uris {
		if count, _ := rt.uriRef.Load(uri.URI); count != nil {
			rt.uriRef.Store(uri.URI, count.(int)+1)
		} else {
			rt.uriRef.Store(uri.URI, 1)
		}
	}
}

func (rt *ReferenceTable) DecreaseReference(
	env *runtime_env.RuntimeEnv,
	serializedEnv string,
	sourceProcess string,
) error {
	if _, excluded := rt.excludeSources[sourceProcess]; excluded {
		return nil
	}

	rt.refsMu.Lock()
	defer rt.refsMu.Unlock()

	var refCount int
	if count, exists := rt.runtimeEnvRefs.Load(serializedEnv); exists && count.(int) > 0 {
		refCount = count.(int) - 1
		if refCount == 0 {
			rt.runtimeEnvRefs.Delete(serializedEnv)
		} else {
			rt.runtimeEnvRefs.Store(serializedEnv, refCount)
		}
	} else {
		log.Log.V(0).Info("Runtime env does not exist", "serializedEnv", serializedEnv)
		return fmt.Errorf("runtime env %s has no reference", serializedEnv)
	}

	uris := rt.urisParser(env)

	var unusedURIs []URIWithType
	for _, uri := range uris {
		if count, exists := rt.uriRef.Load(uri.URI); exists && count.(int) > 0 {
			newCount := count.(int) - 1
			if newCount == 0 {
				rt.uriRef.Delete(uri.URI)
				unusedURIs = append(unusedURIs, uri)
			} else {
				rt.uriRef.Store(uri.URI, newCount)
			}
		} else {
			log.Log.V(0).Info("URI does not exist", "uri", uri.URI)
		}
	}

	if len(unusedURIs) > 0 {
		go rt.unusedURIsCallback(unusedURIs)
	}

	if refCount == 0 {
		go rt.unusedRuntimeEnvCallback(serializedEnv)
	}

	return nil
}

func (rt *ReferenceTable) GetRefCount(serializedEnv string) int {
	if count, exists := rt.runtimeEnvRefs.Load(serializedEnv); exists {
		return count.(int)
	}
	return 0
}

func (rt *ReferenceTable) HasReference(serializedEnv string) bool {
	count, exists := rt.runtimeEnvRefs.Load(serializedEnv)
	return exists && count.(int) > 0
}

type RuntimeEnvAgentService struct {
	envCache            sync.Map
	envLocks            sync.Map
	referenceTable      *ReferenceTable
	pluginManager       *runtime_env.RuntimeEnvPluginManager
	gcsClient           gcs.Client
	runtimeEnvDir       string
	tempDir             string
	loggingParams       *LoggingConfig
	address             string
	runtimeEnvAgentPort int
	nodeIP              string
	perJobLoggerCache   map[string]*PerJobLogger // jobID -> PerJobLogger
	maxJobLoggerCount   int
	loggerMutex         sync.RWMutex
}

type PerJobLogger struct {
	Logger logr.Logger
	Ts     time.Time
}

// LoggingConfig holds the logging configuration parameters.
type LoggingConfig struct {
	Level               zapcore.Level
	Format              string
	Filename            string
	RotationBytes       int
	RotationBackupCount int
	LogsDir             string
	StdoutFilepath      string
	StderrFilepath      string
}

// RuntimeEnvAgentConfig is the agent configuration.
type RuntimeEnvAgentConfig struct {
	RuntimeEnvDir       string
	TempDir             string
	Address             string
	RuntimeEnvAgentPort int
	GcsClient           gcs.Client
	LoggingParams       *LoggingConfig
}

// NewRuntimeEnvAgentService creates a new agent service.
func NewRuntimeEnvAgentService(
	config *RuntimeEnvAgentConfig,
) (*RuntimeEnvAgentService, error) {
	// Create the plugin manager.
	pluginManager := runtime_env.NewRuntimeEnvPluginManager()

	// Use the configured resources directory, falling back to the default.
	runtimeEnvDir := runtime_env.DefaultResourcesDir
	if config != nil && config.RuntimeEnvDir != "" {
		runtimeEnvDir = config.RuntimeEnvDir
	}

	tempDir := runtime_env.DefaultResourcesDir
	if config != nil && config.TempDir != "" {
		tempDir = config.TempDir
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	g, ctx := errgroup.WithContext(ctx)

	type pluginInfo struct {
		name   string
		create func() (runtime_env.RuntimeEnvPlugin, error)
	}

	plugins := []pluginInfo{
		{name: runtime_env.FieldWorkingDir, create: func() (runtime_env.RuntimeEnvPlugin, error) {
			return runtime_env.NewWorkingDirPlugin(runtimeEnvDir, config.GcsClient)
		}},
		{name: runtime_env.FieldJavaJars, create: func() (runtime_env.RuntimeEnvPlugin, error) {
			return runtime_env.NewJavaJarsPlugin(runtimeEnvDir, config.GcsClient)
		}},
		{name: runtime_env.FieldContainer, create: func() (runtime_env.RuntimeEnvPlugin, error) {
			return runtime_env.NewContainerPlugin(tempDir)
		}},
		{name: runtime_env.FieldImageURI, create: func() (runtime_env.RuntimeEnvPlugin, error) {
			return runtime_env.NewImageURIPlugin(tempDir)
		}},
		{name: runtime_env.FieldNsight, create: func() (runtime_env.RuntimeEnvPlugin, error) {
			return runtime_env.NewNsightPlugin(runtimeEnvDir), nil
		}},
		{name: runtime_env.FieldRocprofSys, create: func() (runtime_env.RuntimeEnvPlugin, error) {
			return runtime_env.NewRocProfSysPlugin(runtimeEnvDir), nil
		}},
		{name: runtime_env.FieldPip, create: func() (runtime_env.RuntimeEnvPlugin, error) {
			return runtime_env.NewPipPlugin(runtimeEnvDir)
		}},
		{name: runtime_env.FieldConda, create: func() (runtime_env.RuntimeEnvPlugin, error) {
			return runtime_env.NewCondaPlugin(runtimeEnvDir)
		}},
		{name: runtime_env.FieldUv, create: func() (runtime_env.RuntimeEnvPlugin, error) {
			return runtime_env.NewUvPlugin(runtimeEnvDir)
		}},
		{name: runtime_env.FieldPyModules, create: func() (runtime_env.RuntimeEnvPlugin, error) {
			return runtime_env.NewPyModulesPlugin(runtimeEnvDir, config.GcsClient)
		}},
		{name: runtime_env.FieldPyExecutable, create: func() (runtime_env.RuntimeEnvPlugin, error) {
			return runtime_env.NewPyExecutablePlugin(), nil
		}},
	}

	for _, plugin := range plugins {
		plugin := plugin
		g.Go(func() error {
			p, err := plugin.create()
			if err != nil {
				return fmt.Errorf("failed to create %s plugin: %w", plugin.name, err)
			}
			if err := pluginManager.AddPlugin(p); err != nil {
				return fmt.Errorf("failed to add %s plugin to manager: %w", plugin.name, err)
			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, fmt.Errorf("plugin initialization failed: %w", err)
	}

	excludeSources := map[string]struct{}{
		ExcludedSourceClientServer: {},
	}

	// Resolve the node IP address.
	// Java counterpart: RayNativeRuntime resolves the node IP.
	// Note: Direct-link mode (GetRayRuntime) has been removed. Use Plugin mode worker.go instead.
	// For runtime_env_agent, we use the common fallback method to get node IP.
	var nodeIP string
	nodeIP = common.GetNodeIpAddress(&config.Address)

	service := &RuntimeEnvAgentService{
		pluginManager:       pluginManager,
		gcsClient:           config.GcsClient,
		runtimeEnvDir:       runtimeEnvDir,
		tempDir:             tempDir,
		loggingParams:       config.LoggingParams,
		address:             config.Address,
		runtimeEnvAgentPort: config.RuntimeEnvAgentPort,
		nodeIP:              nodeIP,
		perJobLoggerCache:   make(map[string]*PerJobLogger),
		maxJobLoggerCount:   MaxJobLoggerCount,
	}

	service.referenceTable = NewReferenceTable(
		service.urisParser,
		service.unusedURIsProcessor,
		service.unusedRuntimeEnvProcessor,
		excludeSources,
	)

	if err := runtime_env.InitializeInternalKV(config.GcsClient); err != nil {
		return nil, fmt.Errorf("InitializeInternalKV failed (runtime_env KV unavailable): %w", err)
	}

	log.Log.Info("Starting runtime env agent",
		"pid", os.Getpid(),
		"raylet_pid", os.Getenv("RAY_RAYLET_PID"),
		"address", config.Address,
		"port", config.RuntimeEnvAgentPort,
	)

	return service, nil
}

// GetOrCreateRuntimeEnv creates or gets the runtime env.
func (s *RuntimeEnvAgentService) GetOrCreateRuntimeEnv(
	ctx context.Context,
	request *runtime_env_agent_pb.GetOrCreateRuntimeEnvRequest,
) (*runtime_env_agent_pb.GetOrCreateRuntimeEnvReply, error) {
	serializedEnv := request.SerializedRuntimeEnv

	runtimeEnv, err := runtime_env.Deserialize(serializedEnv)
	if err != nil {
		return &runtime_env_agent_pb.GetOrCreateRuntimeEnvReply{
			Status:       runtime_env_agent_pb.AgentRpcStatus_AGENT_RPC_STATUS_FAILED,
			ErrorMessage: fmt.Sprintf("[Node %s] %s", s.nodeIP, err.Error()),
		}, nil
	}

	s.referenceTable.IncreaseReference(runtimeEnv, serializedEnv, request.SourceProcess)

	lockIface, _ := s.envLocks.LoadOrStore(serializedEnv, &sync.Mutex{})
	lock := lockIface.(*sync.Mutex)

	lock.Lock()
	defer lock.Unlock()

	if cachedResult, exists := s.envCache.Load(serializedEnv); exists {
		createdEnvResult, ok := cachedResult.(*CreatedEnvResult)
		if ok && createdEnvResult != nil {
			if createdEnvResult.Success {
				log.Log.Info("Runtime env already created successfully",
					"serialized_env", serializedEnv,
					"context", createdEnvResult.Result,
				)
				return &runtime_env_agent_pb.GetOrCreateRuntimeEnvReply{
					Status:                      runtime_env_agent_pb.AgentRpcStatus_AGENT_RPC_STATUS_OK,
					SerializedRuntimeEnvContext: createdEnvResult.Result,
				}, nil
			}
			log.Log.Info("Runtime env already failed",
				"serialized_env", serializedEnv,
				"err", createdEnvResult.Result,
			)

			if err := s.referenceTable.DecreaseReference(runtimeEnv, serializedEnv, request.SourceProcess); err != nil {
				log.Log.Error(err, "Failed to decrease reference on cached failure",
					"serialized_env", serializedEnv, "source_process", request.SourceProcess)
			}
			return &runtime_env_agent_pb.GetOrCreateRuntimeEnvReply{
				Status:       runtime_env_agent_pb.AgentRpcStatus_AGENT_RPC_STATUS_FAILED,
				ErrorMessage: fmt.Sprintf("[Node %s] %s", s.nodeIP, createdEnvResult.Result),
			}, nil
		}
		// Unexpected cache entry type or nil: treat as a cache miss and recreate.
		log.Log.V(0).Info("Invalid cached result type or nil, will recreate", "serialized_env", serializedEnv)
	}

	start := time.Now()
	runtimeEnvConfig, err := runtime_env.FromProtoRuntimeEnvConfig(request.RuntimeEnvConfig)
	if err != nil {
		return &runtime_env_agent_pb.GetOrCreateRuntimeEnvReply{
			Status:       runtime_env_agent_pb.AgentRpcStatus_AGENT_RPC_STATUS_FAILED,
			ErrorMessage: fmt.Sprintf("[Node %s] failed to parse runtime env config: %v", s.nodeIP, err),
		}, nil
	}

	setupTimeoutSeconds := common.DefaultRuntimeEnvTimeoutSeconds
	if v, exists := runtimeEnvConfig[runtime_env.ConfigFieldSetupTimeoutSeconds]; exists && v != nil {
		if timeout, ok := v.(int); ok {
			setupTimeoutSeconds = timeout
		} else if timeout, ok := v.(float64); ok {
			setupTimeoutSeconds = int(timeout)
		}
	}

	success, serializedContext, errorMsg := s.createRuntimeEnvWithRetry(ctx, request, runtimeEnv, setupTimeoutSeconds, &runtimeEnvConfig)
	creationTimeMs := time.Since(start).Milliseconds()

	createdEnvResult := &CreatedEnvResult{
		Success:        success,
		CreationTimeMs: creationTimeMs,
	}
	if success {
		createdEnvResult.Result = serializedContext
	} else {
		createdEnvResult.Result = errorMsg
		if err := s.referenceTable.DecreaseReference(runtimeEnv, serializedEnv, request.SourceProcess); err != nil {
			log.Log.Error(err, "Critical: Failed to decrease reference when creation failed - may cause reference leak",
				"serialized_env", serializedEnv, "source_process", request.SourceProcess)
		}
	}
	s.envCache.Store(serializedEnv, createdEnvResult)

	if success {
		return &runtime_env_agent_pb.GetOrCreateRuntimeEnvReply{
			Status:                      runtime_env_agent_pb.AgentRpcStatus_AGENT_RPC_STATUS_OK,
			SerializedRuntimeEnvContext: serializedContext,
		}, nil
	}
	return &runtime_env_agent_pb.GetOrCreateRuntimeEnvReply{
		Status:       runtime_env_agent_pb.AgentRpcStatus_AGENT_RPC_STATUS_FAILED,
		ErrorMessage: fmt.Sprintf("[Node %s] %s", s.nodeIP, errorMsg),
	}, nil
}

func (s *RuntimeEnvAgentService) createRuntimeEnvWithRetry(
	ctx context.Context,
	request *runtime_env_agent_pb.GetOrCreateRuntimeEnvRequest,
	runtimeEnv *runtime_env.RuntimeEnv,
	setupTimeoutSeconds int,
	runtimeEnvConfig *runtime_env.RuntimeEnvConfig,
) (bool, string, string) {
	numRetries := RuntimeEnvRetryTimes
	retryIntervalMs := RuntimeEnvRetryIntervalMs

	errorMessage := ""
	serializedContext := ""
	for i := 0; i < numRetries; i++ {
		if i != 0 {
			time.Sleep(time.Duration(retryIntervalMs) * time.Millisecond)
		}

		func() {
			createCtx, cancel := context.WithTimeout(ctx, time.Duration(setupTimeoutSeconds)*time.Second)
			defer cancel()

			runtimeEnvContext, err := s.setupRuntimeEnv(createCtx, request, runtimeEnv, runtimeEnvConfig)
			if err != nil {
				errorMessage = err.Error()
				log.Log.V(0).Info("Runtime env setup failed",
					"attempt", i+1,
					"max_retries", numRetries,
					"serialized_env", request.SerializedRuntimeEnv,
					"error", errorMessage,
				)
				return
			}

			serializedContext, err = runtimeEnvContext.SerializeContext()
			if err != nil {
				errorMessage = err.Error()
				log.Log.V(0).Info("Failed to serialize runtime env context",
					"attempt", i+1,
					"max_retries", numRetries,
					"serialized_env", request.SerializedRuntimeEnv,
					"error", errorMessage,
				)
				return
			}

			errorMessage = ""
		}()

		if errorMessage == "" {
			break
		}
	}

	if errorMessage != "" {
		log.Log.Info("Runtime env creation failed after retries",
			"serialized_env", request.SerializedRuntimeEnv,
			"num_retries", numRetries,
			"error", errorMessage,
		)
		return false, "", errorMessage
	}

	log.Log.Info("Successfully created runtime env",
		"serialized_env", request.SerializedRuntimeEnv,
		"runtime_env_context", serializedContext,
	)
	return true, serializedContext, ""
}

func (s *RuntimeEnvAgentService) setupRuntimeEnv(
	ctx context.Context,
	request *runtime_env_agent_pb.GetOrCreateRuntimeEnvRequest,
	runtimeEnv *runtime_env.RuntimeEnv,
	runtimeEnvConfig *runtime_env.RuntimeEnvConfig,
) (*runtime_env.RuntimeEnvContext, error) {
	logFiles := []string{}
	if v, exists := (*runtimeEnvConfig)[runtime_env.ConfigFieldLogFiles]; exists && v != nil {
		if files, ok := v.([]string); ok {
			logFiles = files
		}
	}

	perJobLogger := s.getOrCreateLogger(request.JobId, logFiles)

	// Extract env vars; use an empty map when runtimeEnv is nil.
	envVars := make(map[string]string)
	if runtimeEnv != nil {
		envVars = runtimeEnv.EnvVars()
	}

	context := runtime_env.NewRuntimeEnvContext(envVars)

	// If runtimeEnv is nil, re-create and initialize an empty RuntimeEnv.
	if runtimeEnv == nil {
		runtimeEnv = &runtime_env.RuntimeEnv{}
		*runtimeEnv = make(runtime_env.RuntimeEnv)
	}

	// Warn about unrecognized plugin fields.
	for _, pluginEntry := range runtimeEnv.Plugins() {
		name := pluginEntry.Key
		if _, exists := s.pluginManager.GetPlugin(name); !exists {
			perJobLogger.Logger.V(0).Info(
				fmt.Sprintf("runtime_env field %s is not recognized by Ray and will be ignored. In the future, unrecognized fields in the runtime_env will raise an exception.", name),
			)
		}
	}

	workingDirPluginCtx, exists := s.pluginManager.GetPlugin(runtime_env.FieldWorkingDir)
	if !exists {
		return nil, fmt.Errorf("working_dir plugin not found")
	}

	err := runtime_env.CreateForPluginIfNeeded(&runtime_env.PluginCreateContext{
		Ctx:        ctx,
		RuntimeEnv: runtimeEnv,
		Plugin:     workingDirPluginCtx.ClassInstance,
		URICache:   workingDirPluginCtx.URICache,
		Context:    context,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create working_dir: %w", err)
	}

	workingDirURI := runtimeEnv.WorkingDirURI()

	scope, err := workingDirPluginCtx.ClassInstance.(*runtime_env.WorkingDirPlugin).SetupWorkingDirEnv(workingDirURI)
	if err != nil {
		return nil, fmt.Errorf("failed to setup working dir env: %w", err)
	}
	defer scope.Restore()

	sortedPlugins := s.pluginManager.SortedPluginSetupContexts()
	for _, pluginSetupContext := range sortedPlugins {
		plugin := pluginSetupContext.ClassInstance
		if plugin.Name() != runtime_env.FieldWorkingDir {
			err := runtime_env.CreateForPluginIfNeeded(&runtime_env.PluginCreateContext{
				Ctx:        ctx,
				RuntimeEnv: runtimeEnv,
				Plugin:     pluginSetupContext.ClassInstance,
				URICache:   pluginSetupContext.URICache,
				Context:    context,
			})
			if err != nil {
				return nil, fmt.Errorf("failed to create plugin %s: %w", plugin.Name(), err)
			}
		}
	}

	return context, nil
}

// DeleteRuntimeEnvIfPossible deletes the runtime env if possible.
func (s *RuntimeEnvAgentService) DeleteRuntimeEnvIfPossible(
	ctx context.Context,
	request *runtime_env_agent_pb.DeleteRuntimeEnvIfPossibleRequest,
) (*runtime_env_agent_pb.DeleteRuntimeEnvIfPossibleReply, error) {

	serializedEnv := request.GetSerializedRuntimeEnv()
	sourceProcess := request.GetSourceProcess()

	log.Log.Info("Got request from source process to decrease reference for runtime env",
		"source_process", sourceProcess,
		"serialized_runtime_env", serializedEnv,
	)

	runtime_env, err := runtime_env.Deserialize(serializedEnv)
	if err != nil {
		return &runtime_env_agent_pb.DeleteRuntimeEnvIfPossibleReply{
			Status:       runtime_env_agent_pb.AgentRpcStatus_AGENT_RPC_STATUS_FAILED,
			ErrorMessage: fmt.Sprintf("[Node %s] %s", s.nodeIP, err.Error()),
		}, nil
	}

	err = s.referenceTable.DecreaseReference(runtime_env, serializedEnv, sourceProcess)
	if err != nil {
		return &runtime_env_agent_pb.DeleteRuntimeEnvIfPossibleReply{
			Status:       runtime_env_agent_pb.AgentRpcStatus_AGENT_RPC_STATUS_FAILED,
			ErrorMessage: fmt.Sprintf("[Node %s] Failed to decrement reference for runtime env: %v", s.nodeIP, err),
		}, nil
	}

	return &runtime_env_agent_pb.DeleteRuntimeEnvIfPossibleReply{
		Status: runtime_env_agent_pb.AgentRpcStatus_AGENT_RPC_STATUS_OK,
	}, nil
}

// GetRuntimeEnvsInfo returns info for all runtime envs.
func (s *RuntimeEnvAgentService) GetRuntimeEnvsInfo(
	ctx context.Context,
	request *runtime_env_agent_pb.GetRuntimeEnvsInfoRequest,
) (*runtime_env_agent_pb.GetRuntimeEnvsInfoReply, error) {
	limit := request.GetLimit()

	runtimeEnvStates := make(map[string]*runtime_env_agent_pb.RuntimeEnvState)

	s.referenceTable.runtimeEnvRefs.Range(func(key, value interface{}) bool {
		serializedEnv := key.(string)
		refCnt := value.(int)

		state := &runtime_env_agent_pb.RuntimeEnvState{
			RuntimeEnv: serializedEnv,
			RefCnt:     int64(refCnt),
		}
		runtimeEnvStates[serializedEnv] = state
		return true
	})

	s.envCache.Range(func(key, value interface{}) bool {
		serializedEnv := key.(string)
		createEnvResult, ok := value.(*CreatedEnvResult)
		if !ok || createEnvResult == nil {
			log.Log.V(0).Info("Invalid cached result type or nil in GetRuntimeEnvsInfo", "serialized_env", serializedEnv)
			return true
		}

		state, exists := runtimeEnvStates[serializedEnv]
		if !exists {
			state = &runtime_env_agent_pb.RuntimeEnvState{
				RuntimeEnv: serializedEnv,
			}
			runtimeEnvStates[serializedEnv] = state
		}

		state.Success = proto.Bool(createEnvResult.Success)
		if !createEnvResult.Success {
			state.Error = proto.String(createEnvResult.Result)
		}
		state.CreationTimeMs = proto.Int64(createEnvResult.CreationTimeMs)
		return true
	})

	reply := &runtime_env_agent_pb.GetRuntimeEnvsInfoReply{
		RuntimeEnvStates: make([]*runtime_env_agent_pb.RuntimeEnvState, 0, len(runtimeEnvStates)),
		Total:            int64(len(runtimeEnvStates)),
	}

	count := int64(0)
	for _, state := range runtimeEnvStates {
		if limit != -1 && count >= limit {
			break
		}
		count++
		reply.RuntimeEnvStates = append(reply.RuntimeEnvStates, state)
	}

	return reply, nil
}

// urisParser parses all URIs from the runtime env,
// matching Python's _uris_parser implementation.
func (s *RuntimeEnvAgentService) urisParser(env *runtime_env.RuntimeEnv) []URIWithType {
	var urisWithType []URIWithType
	if env == nil {
		return urisWithType
	}

	// Collect URIs from every plugin.
	for name, pluginSetupContext := range s.pluginManager.GetPlugins() {
		plugin := pluginSetupContext.ClassInstance
		uris := plugin.GetURIs(env)
		for _, uri := range uris {
			urisWithType = append(urisWithType, URIWithType{
				URI:     uri,
				URIType: name,
			})
		}
	}
	return urisWithType
}

func (s *RuntimeEnvAgentService) unusedURIsProcessor(uris []URIWithType) {
	for _, uriWithType := range uris {
		pluginSetupContext, exists := s.pluginManager.GetPlugin(uriWithType.URIType)
		if exists && pluginSetupContext != nil {
			pluginSetupContext.URICache.MarkUnused(uriWithType.URI)
		}
	}
}

func (s *RuntimeEnvAgentService) checkJobLoggerCountLimit() {
	for len(s.perJobLoggerCache) >= s.maxJobLoggerCount {
		var oldestJobID string
		var oldestLogger *PerJobLogger
		oldestTime := time.Now()

		for jobID, logger := range s.perJobLoggerCache {
			if logger.Ts.Before(oldestTime) {
				oldestTime = logger.Ts
				oldestJobID = jobID
				oldestLogger = logger
			}
		}

		if oldestJobID != "" && oldestLogger != nil {
			delete(s.perJobLoggerCache, oldestJobID)
			log.Log.Info("Removed oldest job logger due to cache limit",
				"job_id", oldestJobID,
				"cache_size", len(s.perJobLoggerCache),
			)
		} else {
			break
		}
	}
}

func (s *RuntimeEnvAgentService) getOrCreateLogger(jobID []byte, logFiles []string) *PerJobLogger {
	jobIDStr := string(jobID)

	s.loggerMutex.Lock()
	defer s.loggerMutex.Unlock()

	if logger, exists := s.perJobLoggerCache[jobIDStr]; exists {
		logger.Ts = time.Now()
		return logger
	}

	// Check logger count limit before creating new one
	s.checkJobLoggerCountLimit()

	// Convert logging parameters to options
	opts := s.loggingParamsToOptions(jobIDStr, logFiles)

	// Create per-job logger (matches Python's setup_component_logger)
	perJobLogger := zap.SetupComponentLogger(opts...).WithName(fmt.Sprintf("runtime_env_%s", jobIDStr))

	// Store in cache
	cachedLogger := &PerJobLogger{
		Logger: perJobLogger,
		Ts:     time.Now(),
	}
	s.perJobLoggerCache[jobIDStr] = cachedLogger

	return cachedLogger
}

// loggingParamsToOptions converts LoggingConfig to zap options for per-job loggers.
// This method is used by RuntimeEnvAgentService to create loggers for specific jobs.
// The jobIDStr parameter is used to generate the main log filename (runtime_env_setup-{job_id}.log).
// Additional logFiles can be provided for extra output destinations.
func (s *RuntimeEnvAgentService) loggingParamsToOptions(jobIDStr string, logFiles []string) []zap.Option {
	if s.loggingParams == nil {
		return nil
	}

	// Build output paths (matches Python's params["filename"])
	logsDir := s.loggingParams.LogsDir
	outputPaths := make([]string, 0, 1+len(logFiles))

	// Main log file: runtime_env_setup-{job_id}.log
	mainLog := fmt.Sprintf("runtime_env_setup-%s.log", jobIDStr)
	if logsDir != "" {
		mainLog = filepath.Join(logsDir, mainLog)
	}
	outputPaths = append(outputPaths, mainLog)

	// Additional log files
	for _, f := range logFiles {
		if f != "" {
			if logsDir != "" {
				f = filepath.Join(logsDir, f)
			}
			outputPaths = append(outputPaths, f)
		}
	}

	// Use unified logging options builder from runtime_env package
	return runtime_env.BuildLoggingOptions(
		s.loggingParams.Level,
		s.loggingParams.Format,
		outputPaths,
		s.loggingParams.RotationBytes,
		s.loggingParams.RotationBackupCount,
	)
}

func (s *RuntimeEnvAgentService) unusedRuntimeEnvProcessor(serializedEnv string) {
	if cacheEnvs, exists := s.envCache.Load(serializedEnv); exists {
		envResult, ok := cacheEnvs.(*CreatedEnvResult)
		if !ok || envResult == nil {
			log.Log.V(0).Info("Invalid cached result type or nil in unusedRuntimeEnvProcessor", "serialized_env", serializedEnv)
			return
		}
		if !envResult.Success {
			envToDelete := serializedEnv
			go func() {
				time.Sleep(time.Duration(BadRuntimeEnvCacheTTLSeconds) * time.Second)
				s.envCache.Delete(envToDelete)
				log.Log.Info("Removed failed runtime env from cache",
					"serialized_env", envToDelete,
				)
			}()
		} else {
			s.envCache.Delete(serializedEnv)
			log.Log.Info("Removed successful runtime env from cache",
				"serialized_env", serializedEnv,
			)
		}
	}
}
