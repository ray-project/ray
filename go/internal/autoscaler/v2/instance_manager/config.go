// Copyright 2026 The Ray Authors.
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

package instance_manager

import (
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"strconv"

	"github.com/mohae/deepcopy"
	"github.com/ray-project/ray/go/internal/autoscaler"
	"github.com/ray-project/ray/go/internal/autoscaler/v2/constant"
	service "github.com/ray-project/ray/go/internal/autoscaler/v2/service"
	"github.com/ray-project/ray/go/internal/common"
	"github.com/ray-project/ray/go/internal/gcs/native"
	"github.com/ray-project/ray/go/pkg/gcs"
	"github.com/ray-project/ray/go/pkg/ids"
	"github.com/ray-project/ray/go/pkg/log"
	"gopkg.in/yaml.v3"
)

// IConfigReader is the interface reading the autoscaling config.
type IConfigReader interface {
	GetCachedAutoscalingConfig() (*AutoscalingConfig, error)
	RefreshCachedAutoscalingConfig() error
}

// AutoscalingConfig is the autoscaling config struct.
type AutoscalingConfig struct {
	SyncContinuously       bool
	Configs                map[string]interface{}
	runtimeHash            string // SHA1 hash of the runtime config, used to detect config changes
	fileMountsContentsHash string // SHA1 hash of the file mounts contents, used to detect file syncs in monitor mode
}

func NewAutoscalingConfig(configs map[string]interface{}, skipContentHashIn ...bool) (*AutoscalingConfig, error) {
	skipContentHash := false
	if len(skipContentHashIn) > 0 {
		skipContentHash = skipContentHashIn[0]
	}

	asConfig := &AutoscalingConfig{
		SyncContinuously: false,
	}
	if err := asConfig.updateConfigs(configs, skipContentHash); err != nil {
		return nil, err
	}
	return asConfig, nil
}

// Config initialization and validation.
func (asConfig *AutoscalingConfig) updateConfigs(configs map[string]interface{}, skipContentHash bool) error {
	var err error
	asConfig.Configs, err = autoscaler.PrepareConfig(configs)
	if err != nil {
		return err
	}
	if err = autoscaler.ValidateConfig(asConfig.Configs); err != nil {
		return err
	}
	if skipContentHash {
		return nil
	}
	if err = asConfig.calculateHashes(); err != nil {
		return err
	}
	asConfig.SyncContinuously = func(config *AutoscalingConfig) bool {
		if val, ok := config.Configs["generate_file_mounts_contents_hash"]; ok {
			if boolVal, ok := val.(bool); ok {
				return boolVal
			} else {
				return true
			}
		} else {
			return true
		}
	}(asConfig)

	return nil
}

// calculateHashes computes the hashes of the runtime config and the file mount
// contents.
func (asConfig *AutoscalingConfig) calculateHashes() error {
	log.Log.V(1).Info("Calculating hashes for file mounts and ray commands")

	// Extract the needed parameters from the config.
	fileMounts := autoscaler.GetValueWithDefault[map[string]string](asConfig.Configs, make(map[string]string), "file_mounts")
	clusterSyncedFiles := autoscaler.GetValueWithDefault[[]string](asConfig.Configs, make([]string, 0), "cluster_synced_files")
	extraObjs := [][]string{
		autoscaler.GetValueWithDefault[[]string](asConfig.Configs, make([]string, 0), "worker_setup_commands"),
		autoscaler.GetValueWithDefault[[]string](asConfig.Configs, make([]string, 0), "worker_start_ray_commands"),
	}
	generateFileMountsContentsHash := autoscaler.GetValueWithDefault[bool](asConfig.Configs, true, "generate_file_mounts_contents_hash")

	// Call HashRuntimeConf from the autoscaler package to do the actual hash
	// computation.
	runtimeHash, fileMountsContentsHash, err := autoscaler.HashRuntimeConf(
		fileMounts,
		clusterSyncedFiles,
		extraObjs,
		generateFileMountsContentsHash,
	)
	if err != nil {
		return fmt.Errorf("failed to calculate hashes: %w", err)
	}

	// Store the computed hashes on the AutoscalingConfig instance.
	// These hashes are read by the property methods below.
	asConfig.runtimeHash = runtimeHash
	asConfig.fileMountsContentsHash = fileMountsContentsHash

	return nil
}

// FileConfigReader is a file based config reader.
// It reads the Ray cluster autoscaling config from a YAML file.
type FileConfigReader struct {
	// Absolute path of the config file.
	configFilePath string
	// Whether to skip the hash computation of file mounts and Ray commands.
	skipContentHash bool
	// Cached autoscaling config.
	cachedConfig *AutoscalingConfig
}

// NewFileConfigReader creates a new file config reader.
func NewFileConfigReader(configFilePath string, skipContentHashIn ...bool) (IConfigReader, error) {
	var err error
	skipContentHash := true
	if len(skipContentHashIn) > 0 {
		skipContentHash = skipContentHashIn[0]
	}

	// TODO: filepath.Abs cannot produce the correct absolute config file path
	// here, so the caller must provide the correct absolute config file path.
	//configFilePath, err = filepath.Abs(configFilePath)
	configFilePath, err = common.ExpandUser(configFilePath)
	if err != nil {
		return nil, err
	}
	if _, err = os.Stat(configFilePath); err != nil {
		return nil, err
	}

	fileConfigReader := &FileConfigReader{
		configFilePath:  configFilePath,
		skipContentHash: skipContentHash,
	}
	fileConfigReader.cachedConfig, err = fileConfigReader.read()
	if err != nil {
		return nil, err
	}

	return fileConfigReader, nil
}

// read reads and parses the autoscaling config from the config file.
func (fcr *FileConfigReader) read() (*AutoscalingConfig, error) {
	// Open the config file.
	file, err := os.Open(fcr.configFilePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open config file %s: %w", fcr.configFilePath, err)
	}
	defer file.Close()
	// Read the file contents.
	content, err := io.ReadAll(file)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file %s: %w", fcr.configFilePath, err)
	}

	// Parse the YAML contents into a map.
	var configMap map[string]interface{}
	if err := yaml.Unmarshal(content, &configMap); err != nil {
		return nil, fmt.Errorf("failed to parse YAML config: %w", err)
	}

	// Create the AutoscalingConfig object.
	asConfig, err := NewAutoscalingConfig(configMap, fcr.skipContentHash)
	if err != nil {
		return nil, fmt.Errorf("failed to create autoscaling config: %w", err)
	}
	return asConfig, nil
}

// GetCachedAutoscalingConfig returns the most recently read autoscaling
// config.
func (fcr *FileConfigReader) GetCachedAutoscalingConfig() (*AutoscalingConfig, error) {
	if fcr.cachedConfig == nil {
		return nil, fmt.Errorf("config not initialized, call RefreshCachedAutoscalingConfig first")
	}
	return fcr.cachedConfig, nil
}

// RefreshCachedAutoscalingConfig refreshes the cached autoscaling config from
// the file.
func (fcr *FileConfigReader) RefreshCachedAutoscalingConfig() error {
	newConfig, err := fcr.read()
	if err != nil {
		return err
	}

	fcr.cachedConfig = newConfig
	return nil
}

// ReadOnlyProviderConfigReader is the read-only provider config reader.
// It serves the notebook mode / manually set up cluster mode, reporting state
// to the user in the same way.
type ReadOnlyProviderConfigReader struct {
	configs    map[string]interface{}
	gcsClient  gcs.Client
	gcsAddress string
}

// NewReadOnlyProviderConfigReader creates a new read-only provider config
// reader.
func NewReadOnlyProviderConfigReader(gcsAddress string) (IConfigReader, error) {
	configs := deepcopy.Copy(constant.BASE_READONLY_CONFIG).(map[string]interface{})

	gcsClient, err := gcs.GetClient()
	if err != nil || gcsClient == nil {
		gcsClient, err = native.ConnectClient(gcs.ClientOptions{
			Address:   gcsAddress,
			ClusterID: ids.NilClusterID(),
			TimeoutMs: 10000,
		})
		if err != nil || gcsClient == nil {
			return nil, fmt.Errorf("failed to connect to GCS at '%s': %w", gcsAddress, err)
		}
	}

	reader := &ReadOnlyProviderConfigReader{
		configs:    configs,
		gcsAddress: gcsAddress,
		gcsClient:  gcsClient,
	}
	return reader, nil
}

// RefreshCachedAutoscalingConfig refreshes the cached autoscaling config from
// GCS.
func (rop *ReadOnlyProviderConfigReader) RefreshCachedAutoscalingConfig() error {
	// If the GCS client is not initialized.
	if rop.gcsClient == nil {
		return fmt.Errorf("gcs client can not be nil")
	}

	// Get the cluster resource state from GCS.
	statusReply, err := rop.gcsClient.GetAutoscalerStatus(constant.Ctx)
	if err != nil {
		return fmt.Errorf("failed to get autoscaler status from GCS: %w", err)
	}

	if statusReply == nil || statusReply.ClusterResourceState == nil {
		log.Log.V(1).Info("No cluster resource state available from GCS")
		return fmt.Errorf("no cluster resource state available from GCS")
	}

	rayClusterResourceState := statusReply.ClusterResourceState

	// Format the config of each node type (using map[string]interface{}).
	availableNodeTypes := make(map[string]interface{})
	var headNodeType string

	for _, nodeState := range rayClusterResourceState.NodeStates {
		nodeType := nodeState.RayNodeTypeName

		// If there is no node type name, generate one from the node ID.
		if nodeType == "" && len(nodeState.NodeId) > 0 {
			nodeType = autoscaler.FormatReadonlyNodeType(hex.EncodeToString(nodeState.NodeId))
		}

		// Check whether this is the head node.
		isHead := autoscaler.IsHeadNode(nodeState)
		if isHead {
			headNodeType = nodeType
		}

		// Create the node type if it does not exist yet.
		if _, exists := availableNodeTypes[nodeType]; !exists {
			availableNodeTypes[nodeType] = map[string]interface{}{
				"resources":   deepcopy.Copy(nodeState.TotalResources),
				"min_workers": 0,
				"max_workers": func() int {
					if isHead {
						return 0
					}
					return 1
				}(),
				"node_config": make(map[string]interface{}),
			}
		} else if !isHead {
			// Update max_workers (non-head node).
			nodeConfig := availableNodeTypes[nodeType].(map[string]interface{})
			currentMax := nodeConfig["max_workers"].(int)
			nodeConfig["max_workers"] = currentMax + 1
		}
	}

	// Update the config dict when there are available node types.
	if len(availableNodeTypes) > 0 {
		// Update the available_node_types field.
		rop.configs["available_node_types"] = autoscaler.MergeMap(rop.configs["available_node_types"].(map[string]interface{}), availableNodeTypes)

		// Update the max_workers field.
		rop.configs["max_workers"] = len(availableNodeTypes)

		// Set the head_node_type field.
		if headNodeType == "" {
			return fmt.Errorf("head node type should be found")
		}

		rop.configs["head_node_type"] = headNodeType
	}

	// Remove the idle_timeout_minutes field in read-only mode.
	delete(rop.configs, "idle_timeout_minutes")

	log.Log.V(1).Info("Refreshed readonly provider config",
		"node_types_count", len(availableNodeTypes),
		"head_node_type", rop.configs["head_node_type"])

	return nil
}

// GetCachedAutoscalingConfig returns the most recently read autoscaling
// config.
func (rop *ReadOnlyProviderConfigReader) GetCachedAutoscalingConfig() (*AutoscalingConfig, error) {
	if rop.configs == nil {
		return nil, fmt.Errorf("config not initialized")
	}
	return NewAutoscalingConfig(rop.configs, true)
}

type NodeTypeConfig struct {
	Name             service.NodeType
	MinWorkerNodes   int32
	MaxWorkerNodes   int32
	IdleWorkerNodes  int32
	IdleTimeoutS     float64
	Resources        map[string]float64
	Labels           map[string]string
	LaunchConfigHash string
}

func NewNodeTypeConfig() (*NodeTypeConfig, error) {
	nodeTypeConfig := &NodeTypeConfig{
		IdleWorkerNodes: 0,
	}

	return nodeTypeConfig, nil
}

// Provider is the cloud provider type.
// Corresponds to Python instance_manager.config.Provider enum.
type Provider int

const (
	ProviderUnknown Provider = iota
	ProviderAliyun
	ProviderAWS
	ProviderAzure
	ProviderGCP
	ProviderKubeRay
	ProviderLocal
	ProviderReadOnly
)

// InstanceReconcileConfig is the timeout and retry config of instance state
// reconciliation.
// Corresponds to Python instance_manager.config.InstanceReconcileConfig; the
// defaults of each field are read from environment variables.
type InstanceReconcileConfig struct {
	// RequestStatusTimeoutS is the timeout (seconds) waiting for a REQUESTED
	// instance to be allocated a cloud instance.
	RequestStatusTimeoutS int
	// AllocateStatusTimeoutS is the timeout (seconds) waiting for an ALLOCATED
	// instance to reach RAY_RUNNING.
	AllocateStatusTimeoutS int
	// RayInstallStatusTimeoutS is the timeout (seconds) waiting for a
	// RAY_INSTALLING instance to reach RAY_RUNNING.
	RayInstallStatusTimeoutS int
	// TerminatingStatusTimeoutS is the timeout (seconds) waiting for a
	// TERMINATING instance to reach TERMINATED.
	TerminatingStatusTimeoutS int
	// RayStopRequestedStatusTimeoutS is the timeout (seconds) waiting for a
	// RAY_STOP_REQUESTED instance to stop.
	RayStopRequestedStatusTimeoutS int
	// TransientStatusWarnIntervalS is the interval (seconds) at which to warn
	// about a transient status instance not being updated for a long time.
	TransientStatusWarnIntervalS int
	// MaxNumRetryRequestToAllocate is the max number of retries re-queueing a
	// REQUESTED instance after the allocation times out.
	MaxNumRetryRequestToAllocate int
}

// envInteger reads an integer env var, returning the default when unset or
// unparsable.
// Corresponds to Python ray._private.ray_constants.env_integer.
func envInteger(name string, defaultValue int) int {
	v := os.Getenv(name)
	if v == "" {
		return defaultValue
	}
	i, err := strconv.Atoi(v)
	if err != nil {
		log.Log.Error(fmt.Errorf("invalid integer value %s for env %s, using default %d", v, name, defaultValue), "")
		return defaultValue
	}
	return i
}

// NewInstanceReconcileConfig builds the instance reconcile config, reading
// each field from environment variables.
func NewInstanceReconcileConfig() *InstanceReconcileConfig {
	return &InstanceReconcileConfig{
		RequestStatusTimeoutS:          envInteger("RAY_AUTOSCALER_RECONCILE_REQUEST_STATUS_TIMEOUT_S", 10*60),
		AllocateStatusTimeoutS:         envInteger("RAY_AUTOSCALER_RECONCILE_ALLOCATE_STATUS_TIMEOUT_S", 300),
		RayInstallStatusTimeoutS:       envInteger("RAY_AUTOSCALER_RECONCILE_RAY_INSTALL_STATUS_TIMEOUT_S", 30*60),
		TerminatingStatusTimeoutS:      envInteger("RAY_AUTOSCALER_RECONCILE_TERMINATING_STATUS_TIMEOUT_S", 300),
		RayStopRequestedStatusTimeoutS: envInteger("RAY_AUTOSCALER_RECONCILE_RAY_STOP_REQUESTED_STATUS_TIMEOUT_S", 300),
		TransientStatusWarnIntervalS:   envInteger("RAY_AUTOSCALER_RECONCILE_TRANSIENT_STATUS_WARN_INTERVAL_S", 90),
		MaxNumRetryRequestToAllocate:   envInteger("RAY_AUTOSCALER_RECONCILE_MAX_NUM_RETRY_REQUEST_TO_ALLOCATE", 3),
	}
}

// toInt64 converts a numeric value in a config map to int64, returning the
// default on a type mismatch.
// Values parsed from YAML may be int/int64/float64 and need uniform handling.
func toInt64(v interface{}, defaultValue int64) int64 {
	switch n := v.(type) {
	case int:
		return int64(n)
	case int32:
		return int64(n)
	case int64:
		return n
	case uint:
		return int64(n)
	case uint64:
		return int64(n)
	case float32:
		return int64(n)
	case float64:
		return int64(n)
	default:
		return defaultValue
	}
}

// toFloat64 converts a numeric value in a config map to float64, returning the
// default on a type mismatch.
func toFloat64(v interface{}, defaultValue float64) float64 {
	switch n := v.(type) {
	case int:
		return float64(n)
	case int32:
		return float64(n)
	case int64:
		return float64(n)
	case uint:
		return float64(n)
	case uint64:
		return float64(n)
	case float32:
		return float64(n)
	case float64:
		return n
	default:
		return defaultValue
	}
}

// toFloat64Map converts a map[string]interface{} in a config map to
// map[string]float64.
func toFloat64Map(v interface{}) map[string]float64 {
	m, ok := v.(map[string]interface{})
	if !ok {
		return nil
	}
	out := make(map[string]float64, len(m))
	for k, val := range m {
		out[k] = toFloat64(val, 0)
	}
	return out
}

// toStringMap converts a map[string]interface{} in a config map to
// map[string]string.
func toStringMap(v interface{}) map[string]string {
	m, ok := v.(map[string]interface{})
	if !ok {
		return nil
	}
	out := make(map[string]string, len(m))
	for k, val := range m {
		if s, ok := val.(string); ok {
			out[k] = s
		}
	}
	return out
}

// GetConfig gets the config entry with the given name, returning the default
// when it does not exist.
// Corresponds to Python AutoscalingConfig.get_config.
func (asConfig *AutoscalingConfig) GetConfig(configName string, defaultValue interface{}) interface{} {
	if v, ok := asConfig.Configs[configName]; ok {
		return v
	}
	return defaultValue
}

// GetNodeTypeSpecificConfig gets the given config entry of the given node
// type.
// Corresponds to Python AutoscalingConfig.get_node_type_specific_config;
// returns nil when the node type or the config entry does not exist.
func (asConfig *AutoscalingConfig) GetNodeTypeSpecificConfig(rayNodeType, configName string) interface{} {
	availableNodeTypes := autoscaler.GetValueWithDefault[map[string]interface{}](asConfig.Configs, nil, "available_node_types")
	if availableNodeTypes == nil {
		return nil
	}
	nodeConfig, ok := availableNodeTypes[rayNodeType].(map[string]interface{})
	if !ok {
		return nil
	}
	return nodeConfig[configName]
}

// GetHeadNodeType returns the head node type.
// Corresponds to Python AutoscalingConfig.get_head_node_type:
// with a single node type it returns that type directly, otherwise it returns
// the head_node_type config entry.
func (asConfig *AutoscalingConfig) GetHeadNodeType() string {
	availableNodeTypes := autoscaler.GetValueWithDefault[map[string]interface{}](asConfig.Configs, nil, "available_node_types")
	if len(availableNodeTypes) == 1 {
		for nodeType := range availableNodeTypes {
			return nodeType
		}
	}
	return autoscaler.GetValueWithDefault[string](asConfig.Configs, "", "head_node_type")
}

// GetNodeTypeConfigs returns the config of each node type in
// available_node_types.
// Corresponds to Python AutoscalingConfig.get_node_type_configs:
// the head node type's max_worker_nodes is incremented by 1 (counting the head
// node itself); returns nil when there are no available node types.
func (asConfig *AutoscalingConfig) GetNodeTypeConfigs() map[string]*NodeTypeConfig {
	availableNodeTypes := autoscaler.GetValueWithDefault[map[string]interface{}](asConfig.Configs, nil, "available_node_types")
	if len(availableNodeTypes) == 0 {
		return nil
	}

	headNodeType := asConfig.GetHeadNodeType()
	if headNodeType == "" {
		log.Log.Info("Head node type not found while getting node type configs")
		return nil
	}

	nodeTypeConfigs := make(map[string]*NodeTypeConfig, len(availableNodeTypes))
	for nodeType, raw := range availableNodeTypes {
		nodeConfig, ok := raw.(map[string]interface{})
		if !ok {
			log.Log.Info("Invalid node type config", "node_type", nodeType)
			continue
		}
		maxWorkerNodes := toInt64(nodeConfig["max_workers"], 0)
		if nodeType == headNodeType {
			maxWorkerNodes++
		}
		var idleTimeoutS float64
		if v, ok := nodeConfig["idle_timeout_s"]; ok && v != nil {
			idleTimeoutS = toFloat64(v, 0)
		}
		nodeTypeConfigs[nodeType] = &NodeTypeConfig{
			Name:            nodeType,
			MinWorkerNodes:  int32(toInt64(nodeConfig["min_workers"], 0)),
			MaxWorkerNodes:  int32(maxWorkerNodes),
			IdleWorkerNodes: int32(toInt64(nodeConfig["idle_workers"], 0)),
			IdleTimeoutS:    idleTimeoutS,
			Resources:       toFloat64Map(nodeConfig["resources"]),
			Labels:          toStringMap(nodeConfig["labels"]),
			// LaunchConfigHash corresponds to Python
			// hash_launch_conf(node_config, auth); the Go port does not have
			// that hash computation yet, so it stays empty for now (the
			// scheduler skips the launch config check on an empty hash).
			// TODO: fill this field after porting hash_launch_conf.
			LaunchConfigHash: "",
		}
	}
	return nodeTypeConfigs
}

// GetMaxNumWorkerNodes returns the configured max number of worker nodes, nil
// when unconfigured.
// Corresponds to Python AutoscalingConfig.get_max_num_worker_nodes.
func (asConfig *AutoscalingConfig) GetMaxNumWorkerNodes() *int64 {
	v, ok := asConfig.Configs["max_workers"]
	if !ok || v == nil {
		return nil
	}
	maxNumWorkers := toInt64(v, 0)
	return &maxNumWorkers
}

// GetMaxNumNodes returns the cluster-wide max node count (including the head
// node), nil when unconfigured.
// Corresponds to Python AutoscalingConfig.get_max_num_nodes.
func (asConfig *AutoscalingConfig) GetMaxNumNodes() *int64 {
	maxNumWorkers := asConfig.GetMaxNumWorkerNodes()
	if maxNumWorkers == nil {
		return nil
	}
	maxNumNodes := *maxNumWorkers + 1
	return &maxNumNodes
}

// GetUpscalingSpeed returns the upscaling speed factor, defaulting to 0.0 when
// unconfigured.
// Corresponds to Python
// AutoscalingConfig.get_upscaling_speed(DEFAULT_UPSCALING_SPEED).
func (asConfig *AutoscalingConfig) GetUpscalingSpeed() float64 {
	return toFloat64(asConfig.GetConfig("upscaling_speed", 0.0), 0.0)
}

// GetMaxConcurrentLaunches returns the max number of instances launched
// concurrently per reconcile.
// Corresponds to Python
// AutoscalingConfig.get_max_concurrent_launches(AUTOSCALER_MAX_CONCURRENT_LAUNCHES).
func (asConfig *AutoscalingConfig) GetMaxConcurrentLaunches() int {
	return envInteger("AUTOSCALER_MAX_CONCURRENT_LAUNCHES", 10)
}

// DisableNodeUpdaters reports whether the node updaters (Ray install/stop
// etc.) are disabled.
// Corresponds to Python AutoscalingConfig.disable_node_updaters
// (DISABLE_NODE_UPDATERS_KEY, default false).
func (asConfig *AutoscalingConfig) DisableNodeUpdaters() bool {
	return autoscaler.GetValueWithDefault[bool](asConfig.Configs, false, "provider", "disable_node_updaters")
}

// GetIdleTimeoutS returns the global idle timeout in seconds
// (idle_timeout_minutes * 60), nil when unconfigured.
// Corresponds to Python AutoscalingConfig.get_idle_timeout_s.
func (asConfig *AutoscalingConfig) GetIdleTimeoutS() *float64 {
	v, ok := asConfig.Configs["idle_timeout_minutes"]
	if !ok || v == nil {
		return nil
	}
	idleTimeoutS := toFloat64(v, 0) * 60
	return &idleTimeoutS
}

// DisableLaunchConfigCheck reports whether the launch config change check is
// disabled.
// Corresponds to Python AutoscalingConfig.disable_launch_config_check
// (default true).
func (asConfig *AutoscalingConfig) DisableLaunchConfigCheck() bool {
	return autoscaler.GetValueWithDefault[bool](asConfig.Configs, true, "provider", "disable_launch_config_check")
}

// GetInstanceReconcileConfig returns the instance state reconcile config.
// Corresponds to Python AutoscalingConfig.get_instance_reconcile_config.
func (asConfig *AutoscalingConfig) GetInstanceReconcileConfig() *InstanceReconcileConfig {
	return NewInstanceReconcileConfig()
}

// GetProvider returns the cloud provider type.
// Corresponds to the Python AutoscalingConfig.provider property.
func (asConfig *AutoscalingConfig) GetProvider() Provider {
	providerStr := autoscaler.GetValueWithDefault[string](asConfig.Configs, "", "provider", "type")
	switch providerStr {
	case "local":
		return ProviderLocal
	case "aws":
		return ProviderAWS
	case "azure":
		return ProviderAzure
	case "gcp":
		return ProviderGCP
	case "aliyun":
		return ProviderAliyun
	case "kuberay":
		return ProviderKubeRay
	case "readonly":
		return ProviderReadOnly
	default:
		return ProviderUnknown
	}
}
