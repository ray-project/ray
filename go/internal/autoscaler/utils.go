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

package autoscaler

import (
	"bytes"
	"crypto/sha1"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"

	"github.com/ray-project/ray/go/internal/autoscaler/local"
	"github.com/ray-project/ray/go/internal/common"
	"github.com/ray-project/ray/go/pkg/log"
	"github.com/xeipuuv/gojsonschema"

	"github.com/mohae/deepcopy"

	"github.com/ray-project/ray/go/proto"
)

// FormatReadonlyNodeType formats a readonly node type name.
// It returns the formatted node type name of the form "node_<hex_id>".
func FormatReadonlyNodeType(nodeID string) string {
	return fmt.Sprintf("node_%s", nodeID)
}

// IsHeadNode reports whether the node state belongs to a head node.
func IsHeadNode(nodeState *proto.NodeState) bool {
	_, ok := nodeState.TotalResources["node:__internal_head__"]
	return ok
}

/*
PrepareConfig preprocesses and normalizes the autoscaling config.
The function applies a series of transformations and validations to the raw
config map to make it satisfy the Ray cluster autoscaling requirements.
*/
func PrepareConfig(config map[string]interface{}) (map[string]interface{}, error) {
	provider, ok := config["provider"].(map[string]interface{})
	if !ok {
		provider = make(map[string]interface{})
		config["provider"] = provider
	}

	providerType, _ := provider["type"].(string)
	isLocal := providerType == "local"
	isKuberay := providerType == "kuberay"

	// Apply provider specific handling.
	if isLocal {
		// Local mode: adapt the config to the local environment.
		var err error
		config, err = local.PrepareLocal(config)
		if err != nil {
			return nil, fmt.Errorf("failed to prepare local config: %w", err)
		}
	} else if isKuberay {
		// KubeRay mode: return the config unchanged.
		return config, nil
	}
	withDefaults, err := filloutDefaults(config)
	if err != nil {
		return nil, err
	}
	mergeSetupCommands(withDefaults)
	if err := ValidateDockerConfig(withDefaults); err != nil {
		return nil, fmt.Errorf("invalid docker config: %w", err)
	}
	if err = fillNodeTypeMinMaxWorkers(withDefaults); err != nil {
		return nil, err
	}
	return withDefaults, nil
}

// ValidateConfig validates the validity and completeness of the Ray
// autoscaling config.
func ValidateConfig(config map[string]interface{}) error {
	if config == nil {
		return fmt.Errorf("config is nil")
	}
	schemaBytes, err := LoadRaySchemaConfig()
	if err != nil {
		return err
	}
	schemaLoader := gojsonschema.NewBytesLoader(schemaBytes)
	configBytes, err := json.Marshal(config)
	if err != nil {
		return err
	}
	documentLoader := gojsonschema.NewBytesLoader(configBytes)
	_, err = gojsonschema.Validate(schemaLoader, documentLoader)
	if err != nil {
		return err
	}

	if _, exists := config["cluster_synced_files"]; !exists {
		return fmt.Errorf("missing 'cluster_synced_files' field in the cluster configuration. " +
			"This is likely due to the Ray version running in the cluster being greater than " +
			"the Ray version running on your laptop. Please try updating Ray on your local " +
			"machine and make sure the versions match")
	}
	if availableNodeTypes, exists := config["available_node_types"].(map[string]interface{}); exists {
		// head_node_type must be present.
		headNodeType, hasHeadNodeType := config["head_node_type"].(string)
		if !hasHeadNodeType {
			return fmt.Errorf("you must specify `head_node_type` if `available_node_types` is set")
		}

		// head_node_type must be valid.
		if _, exists := availableNodeTypes[headNodeType]; !exists {
			return fmt.Errorf("`head_node_type` must be one of `available_node_types`")
		}

		// The sum of min_workers must not exceed max_workers.
		sumMinWorkers := 0
		for _, nodeTypeData := range availableNodeTypes {
			nodeTypeMap, ok := nodeTypeData.(map[string]interface{})
			if !ok {
				continue
			}
			minWorkers := GetValueWithDefault[int](nodeTypeMap, 0, "min_workers")
			sumMinWorkers += minWorkers
		}
		// Check whether the sum exceeds the global max_workers limit so the
		// cluster never grows beyond the configured maximum size.
		maxWorkers, ok := config["max_workers"].(int)
		if ok && sumMinWorkers > maxWorkers {
			return fmt.Errorf("the specified global `max_workers` is smaller than the " +
				"sum of `min_workers` of all the available node types")
		}
	}
	// Windows specific check.
	if runtime.GOOS == "windows" {
		if GetValueWithDefault[bool](config, false, "file_mounts_sync_continuously") {
			return fmt.Errorf("`file_mounts_sync_continuously` is not supported on Windows. " +
				"Please set this to False when running on Windows")
		}
	}
	return nil
}

// filloutDefaults fills in the default configuration.
func filloutDefaults(config map[string]interface{}) (map[string]interface{}, error) {
	// Get the default config, which contains all standard fields and sensible
	// defaults required by the provider.
	defaults, err := GetDefaultConfig(config["provider"].(map[string]interface{}))
	if err != nil {
		return nil, fmt.Errorf("failed to get default config: %w", err)
	}

	MergeMap(defaults, config)

	mergedConfig := deepcopy.Copy(defaults).(map[string]interface{})

	mergedConfig["auth"] = GetValueWithDefault[map[string]interface{}](mergedConfig, make(map[string]interface{}), "auth")

	hasAvailableNodeTypes := ContainKey(mergedConfig, "available_node_types")
	hasHeadNode := ContainKey(mergedConfig, "head_node")
	hasWorkerNodes := ContainKey(mergedConfig, "worker_nodes")

	isLegacyConfig := hasAvailableNodeTypes && (hasHeadNode || hasWorkerNodes)

	if isLegacyConfig {
		mergedConfig, err = mergeLegacyYamlWithDefaults(mergedConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to merge legacy yaml with defaults: %w", err)
		}
	}

	delete(mergedConfig, "min_workers")

	_, err = translateTrivialLegacyConfig(mergedConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to translate trivial legacy config: %w", err)
	}

	return mergedConfig, nil
}

// mergeSetupCommands merges the setup commands.
func mergeSetupCommands(config map[string]interface{}) {
	setupCommands := GetValueWithDefault[[]interface{}](config, []interface{}{}, "setup_commands")

	headSetupCommands := GetValueWithDefault[[]interface{}](config, []interface{}{}, "head_setup_commands")
	config["head_setup_commands"] = append(setupCommands, headSetupCommands...)

	workerSetupCommands := GetValueWithDefault[[]interface{}](config, []interface{}{}, "worker_setup_commands")
	config["worker_setup_commands"] = append(setupCommands, workerSetupCommands...)
}

// fillNodeTypeMinMaxWorkers fills min_workers and max_workers of node types.
func fillNodeTypeMinMaxWorkers(config map[string]interface{}) error {
	var err error
	err = nil
	globalMaxWorkers, err := GetValue[int](config, "max_workers")
	if err != nil {
		log.Log.V(1).Error(err, "")
		return err
	}

	nodeTypes, err := GetValue[map[string]interface{}](config, "available_node_types")
	if err != nil {
		log.Log.V(1).Error(err, "")
		return err
	}
	headNodeType, _ := config["head_node_type"].(string)

	for nodeTypeName, nodeTypeData := range nodeTypes {
		nodeTypeMap, ok := nodeTypeData.(map[string]interface{})
		if !ok {
			continue
		}

		PutIfAbsent[int](nodeTypeMap, 0, "min_workers")

		if !ContainKey(nodeTypeMap, "max_workers") {
			if nodeTypeName == headNodeType {
				nodeTypeMap["max_workers"] = 0
			} else {
				nodeTypeMap["max_workers"] = globalMaxWorkers
			}
		}
	}
	return nil
}

// mergeLegacyYamlWithDefaults merges a legacy YAML config with the defaults
// and converts it to the new multi node type format.
func mergeLegacyYamlWithDefaults(mergedConfig map[string]interface{}) (map[string]interface{}, error) {
	log.Log.V(1).Info("Converting legacy cluster config to a multi node type cluster config. " +
		"Multi-node-type cluster configs are the recommended format for configuring Ray clusters. " +
		"See the docs for more information:\n" +
		"https://docs.ray.io/en/master/cluster/config.html#full-configuration")
	var err error
	err = nil
	defaultHeadType, err := GetValue[string](mergedConfig, "head_node_type")
	if err != nil {
		log.Log.V(1).Error(err, "")
		return nil, err
	}

	availableNodeTypes := GetValueWithDefault[map[string]interface{}](mergedConfig, make(map[string]interface{}), "available_node_types")
	if len(availableNodeTypes) != 2 {
		err = fmt.Errorf("default config should have exactly two node types, got %d", len(availableNodeTypes))
		log.Log.V(1).Error(err, "")
		return nil, err
	}

	var defaultWorkerType string
	for nodeTypeName := range availableNodeTypes {
		if nodeTypeName != defaultHeadType {
			defaultWorkerType = nodeTypeName
			break
		}
	}

	var headNodeInfo map[string]interface{}
	if ContainKey(mergedConfig, "head_node") {
		headNode := GetValueWithDefault[map[string]interface{}](mergedConfig, make(map[string]interface{}), "head_node")
		headNodeInfo = map[string]interface{}{
			"node_config": headNode,
			"resources":   GetValueWithDefault[map[string]interface{}](headNode, make(map[string]interface{}), "resources"),
			"min_workers": 0,
			"max_workers": 0,
		}
	} else {
		headNodeInfo = GetValueWithDefault[map[string]interface{}](mergedConfig, make(map[string]interface{}), "available_node_types", defaultHeadType)
	}

	var workerNodeInfo map[string]interface{}
	if ContainKey(mergedConfig, "worker_nodes") {
		workerNodes := GetValueWithDefault[map[string]interface{}](mergedConfig, make(map[string]interface{}), "worker_nodes")
		maxWorkers, ok := mergedConfig["max_workers"].(int)
		if !ok {
			err = fmt.Errorf("max_workers is not specified or is not an int")
			log.Log.V(1).Error(err, "")
			return nil, err
		}
		workerNodeInfo = map[string]interface{}{
			"node_config": workerNodes,
			"resources":   GetValueWithDefault[map[string]interface{}](workerNodes, make(map[string]interface{}), "resources"),
			"min_workers": GetValueWithDefault[int](mergedConfig, 0, "min_workers"),
			"max_workers": maxWorkers,
		}
	} else {
		workerNodeInfo = GetValueWithDefault[map[string]interface{}](mergedConfig, make(map[string]interface{}), "available_node_types", defaultWorkerType)
	}

	mergedConfig["available_node_types"] = map[string]interface{}{
		NODE_TYPE_LEGACY_HEAD:   headNodeInfo,
		NODE_TYPE_LEGACY_WORKER: workerNodeInfo,
	}

	mergedConfig["head_node_type"] = NODE_TYPE_LEGACY_HEAD

	headNode := GetValueWithDefault[map[string]interface{}](mergedConfig, make(map[string]interface{}), "head_node")
	workerNodes := GetValueWithDefault[map[string]interface{}](mergedConfig, make(map[string]interface{}), "worker_nodes")
	DeleteMapEle(headNode, "resources")
	DeleteMapEle(workerNodes, "resources")

	return mergedConfig, nil
}

// translateTrivialLegacyConfig drops the empty deprecated fields
// ("head_node" and "worker_nodes").
func translateTrivialLegacyConfig(config map[string]interface{}) (map[string]interface{}, error) {
	removableFields := []string{"head_node", "worker_nodes"}

	for _, field := range removableFields {
		value, exists := config[field]
		if !exists {
			continue
		}

		isEmpty := false
		if value == nil {
			isEmpty = true
		} else if m, ok := value.(map[string]interface{}); ok && len(m) == 0 {
			isEmpty = true
		}

		if isEmpty {
			log.Log.V(1).Info(fmt.Sprintf("Dropping the empty legacy field %s. %s is not supported for ray>=2.0.0. "+
				"It is recommended to remove %s from the cluster config.", field, field, field))
			delete(config, field)
		}
	}

	return config, nil
}

// raySchemaJson is the autoscaler config JSON schema, embedded into the
// binary so the deployed raygo executable does not depend on the source
// tree, runfiles, or wheel file layout.
//
//go:embed ray-schema.json
var raySchemaJson []byte

// LoadRaySchemaConfig returns the autoscaler config JSON schema content.
func LoadRaySchemaConfig() ([]byte, error) {
	return raySchemaJson, nil
}

// hashCache caches the already computed config hashes to avoid recompute.
var hashCache = make(map[string]string)

// HashRuntimeConf computes the hashes of the runtime config and file mount
// contents.
func HashRuntimeConf(
	fileMounts map[string]string,
	clusterSyncedFiles []string,
	extraObjs [][]string,
	generateFileMountsContentsHash bool,
) (string, string, error) {
	var err error
	err = nil
	runtimeHasher := sha1.New()
	contentsHasher := sha1.New()

	addContentHashes := func(path string, allowNonExistingPathsIn ...bool) error {
		var allowNonExistingPaths = false
		if len(allowNonExistingPathsIn) > 0 {
			allowNonExistingPaths = allowNonExistingPathsIn[0]
		}

		addHashOfFile := func(fpath string) error {
			file, err := os.Open(fpath)
			if err != nil {
				return fmt.Errorf("failed to open file %s: %w", fpath, err)
			}
			defer file.Close()

			// Read the file content in 1MB chunks and feed the hasher.
			buf := make([]byte, 1024*1024) // 1MB chunks
			for {
				n, err := file.Read(buf)
				if n > 0 {
					contentsHasher.Write(buf[:n])
				}
				if err == io.EOF {
					break
				}
				if err != nil {
					return fmt.Errorf("failed to read file %s: %w", fpath, err)
				}
			}
			return nil
		}

		path, err := common.ExpandUser(path)
		if err != nil {
			return err
		}

		if allowNonExistingPaths && !common.PathExists(path) {
			return nil
		}

		// Handle directories.
		if common.PathIsDir(path) {
			type DirInfo struct {
				DirPath   string
				FileNames []string
			}

			var dirs []DirInfo
			err = filepath.WalkDir(path, func(dirPath string, dirEntry os.DirEntry, err error) error {
				if err != nil {
					return err
				}

				if !dirEntry.IsDir() {
					return nil
				}

				entries, err := os.ReadDir(dirPath)
				if err != nil {
					return err
				}

				var fileNames []string
				for _, entry := range entries {
					if !entry.IsDir() {
						fileNames = append(fileNames, entry.Name())
					}
				}
				sort.Strings(fileNames)
				dirs = append(dirs, DirInfo{
					DirPath:   dirPath,
					FileNames: fileNames,
				})
				return nil
			})
			if err != nil {
				return fmt.Errorf("failed to walk directory %s: %w", path, err)
			}

			sort.Slice(dirs, func(i, j int) bool {
				return dirs[i].DirPath < dirs[j].DirPath
			})

			for _, d := range dirs {
				contentsHasher.Write([]byte(d.DirPath))

				for _, name := range d.FileNames {
					contentsHasher.Write([]byte(name))

					fpath := filepath.Join(d.DirPath, name)
					if err := addHashOfFile(fpath); err != nil {
						return err
					}
				}
			}
		} else {
			// Regular files are hashed directly.
			return addHashOfFile(path)
		}

		return nil
	}

	fileMountsStr, err := marshalWithSortedKeys(fileMounts)
	if err != nil {
		return "", "", err
	}

	extraObjsStr, err := json.Marshal(extraObjs)
	if err != nil {
		return "", "", err
	}

	var buf bytes.Buffer
	buf.Write(fileMountsStr)
	buf.Write(extraObjsStr)

	confStr := string(buf.Bytes())

	var fileMountsContentsHash string
	if _, isCached := hashCache[confStr]; !isCached || generateFileMountsContentsHash {
		localPaths := make([]string, 0, len(fileMounts))
		for _, localPath := range fileMounts {
			localPaths = append(localPaths, localPath)
		}
		sort.Strings(localPaths)
		for _, localPath := range localPaths {
			err = addContentHashes(localPath)
			if err != nil {
				return "", "", err
			}
		}

		headNodeContentsHash := hex.EncodeToString(contentsHasher.Sum(nil))

		if _, isCached := hashCache[confStr]; !isCached {
			runtimeHasher.Write([]byte(confStr))
			runtimeHasher.Write([]byte(headNodeContentsHash))
			hashCache[confStr] = hex.EncodeToString(runtimeHasher.Sum(nil))
		}

		if clusterSyncedFiles != nil {
			localPaths := make([]string, 0, len(clusterSyncedFiles))
			for _, localPath := range clusterSyncedFiles {
				localPaths = append(localPaths, localPath)
			}
			sort.Strings(localPaths)
			for _, localPath := range localPaths {
				err = addContentHashes(localPath, true)
				if err != nil {
					return "", "", err
				}
			}
		}

		fileMountsContentsHash = hex.EncodeToString(contentsHasher.Sum(nil))
	} else {
		fileMountsContentsHash = ""
	}

	runtimeHash := hashCache[confStr]
	return runtimeHash, fileMountsContentsHash, nil
}

// marshalWithSortedKeys serializes a map with keys in sorted order.
func marshalWithSortedKeys(data map[string]string) ([]byte, error) {
	keys := make([]string, 0, len(data))
	for k := range data {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var builder strings.Builder
	builder.WriteString("{")
	for i, k := range keys {
		// Serialize the key.
		keyBytes, err := json.Marshal(k)
		if err != nil {
			return nil, err
		}
		builder.Write(keyBytes)
		builder.WriteString(":")

		// Serialize the value.
		valBytes, err := json.Marshal(data[k])
		if err != nil {
			return nil, err
		}
		builder.Write(valBytes)

		// Add the separator (skipped after the last element).
		if i < len(keys)-1 {
			builder.WriteString(",")
		}
	}
	builder.WriteString("}")

	return []byte(builder.String()), nil
}
