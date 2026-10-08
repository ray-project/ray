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

package local

import (
	_ "embed"
	"errors"
	"fmt"

	"github.com/mohae/deepcopy"
	"github.com/ray-project/ray/go/pkg/log"
)

const LOCAL_CLUSTER_NODE_TYPE = "local.cluster.node"

const unsupportedFieldMessage = "The field %s is not supported for on-premise clusters."

// PrepareLocal prepares the local cluster config for the cluster launcher and
// the autoscaler.
func PrepareLocal(configIn map[string]interface{}) (map[string]interface{}, error) {
	config := deepcopy.Copy(configIn).(map[string]interface{})

	// Reject the unsupported legacy fields.
	unsupportedFields := []string{"head_node", "worker_nodes", "available_node_types"}
	for _, field := range unsupportedFields {
		if val, exists := config[field]; exists && val != nil {
			// Providing these fields means the config uses an outdated format.
			err := errors.New(fmt.Sprintf(unsupportedFieldMessage, field))
			log.Log.V(1).Error(err, "")
			return nil, err
		}
	}

	// Create the standardized single node type config.
	config["available_node_types"] = map[string]interface{}{
		LOCAL_CLUSTER_NODE_TYPE: map[string]interface{}{
			"node_config": make(map[string]interface{}),
			"resources":   make(map[string]interface{}),
		},
	}
	// The head node type is the only node type of a local cluster.
	config["head_node_type"] = LOCAL_CLUSTER_NODE_TYPE

	// Read the provider section to determine the management mode.
	provider, ok := config["provider"].(map[string]interface{})
	if !ok {
		provider = make(map[string]interface{})
		config["provider"] = provider
	}

	// Pick the config handling logic based on the management mode.
	if _, hasCoordinatorAddress := provider["coordinator_address"]; hasCoordinatorAddress {
		// Coordinator mode: for automated cluster management with a central
		// coordination service. The coordinator keeps the state of all nodes
		// and answers HTTP requests.
		var err error
		config, err = prepareCoordinator(config)
		if err != nil {
			return nil, fmt.Errorf("failed to prepare coordinator config: %w", err)
		}
	} else {
		// Manual mode: for simple static IP list configs where the address of
		// every node is spelled out in the config file.
		var err error
		config, err = prepareManual(config)
		if err != nil {
			return nil, fmt.Errorf("failed to prepare manual config: %w", err)
		}
	}

	return config, nil
}

// prepareCoordinator prepares the coordinator mode local cluster config.
// Coordinator mode relies on a central coordination service to manage node
// state automatically and fits production clusters that need automated
// management.
func prepareCoordinator(configIn map[string]interface{}) (map[string]interface{}, error) {
	config := deepcopy.Copy(configIn).(map[string]interface{})

	// The maximum number of workers the coordinator may allocate is required.
	maxWorkers, hasMaxWorkers := config["max_workers"]
	if !hasMaxWorkers {
		err := errors.New("the field `max_workers` is required when using an automatically managed on-premise cluster")
		log.Log.V(1).Error(err, "")
		return nil, err
	}

	// Read the node type config.
	nodeTypes, ok := config["available_node_types"].(map[string]interface{})
	if !ok {
		err := errors.New("available_node_types should be a map")
		log.Log.V(1).Error(err, "")
		return nil, err
	}

	nodeType, ok := nodeTypes[LOCAL_CLUSTER_NODE_TYPE].(map[string]interface{})
	if !ok {
		err := errors.New("local cluster node type config not found")
		log.Log.V(1).Error(err, "")
		return nil, err
	}

	// Move the global min_workers into the node type config; the autoscaler
	// no longer uses the global min_workers field.
	minWorkers, _ := config["min_workers"].(int)
	if minWorkers == 0 {
		minWorkers = 0
	}
	nodeType["min_workers"] = minWorkers

	delete(config, "min_workers")

	nodeType["max_workers"] = maxWorkers.(int)

	return config, nil
}

// prepareManual prepares the manual mode local cluster config.
// Manual mode requires the user to specify the address of every node and fits
// simple static cluster deployments.
func prepareManual(configIn map[string]interface{}) (map[string]interface{}, error) {
	var err error
	err = nil
	config := deepcopy.Copy(configIn).(map[string]interface{})

	provider, ok := config["provider"].(map[string]interface{})
	if !ok {
		err = errors.New("provider should be a map")
		log.Log.V(1).Error(err, "")
		return nil, err
	}

	// The required fields are worker_ips and head_ip.
	workerIPs, hasWorkerIPs := provider["worker_ips"].([]interface{})
	_, hasHeadIP := provider["head_ip"]

	if !hasWorkerIPs || !hasHeadIP {
		err = errors.New("please supply a `head_ip` and list of `worker_ips`. Alternatively, supply a `coordinator_address`")
		log.Log.V(1).Error(err, "")
		return nil, err
	}

	numIPs := len(workerIPs)

	nodeTypes, ok := config["available_node_types"].(map[string]interface{})
	if !ok {
		err = errors.New("available_node_types should be a map")
		log.Log.V(1).Error(err, "")
		return nil, err
	}

	nodeType, ok := nodeTypes[LOCAL_CLUSTER_NODE_TYPE].(map[string]interface{})
	if !ok {
		err = errors.New("local cluster node type config not found")
		log.Log.V(1).Error(err, "")
		return nil, err
	}

	_, hasMaxWorkers := config["max_workers"]
	if !hasMaxWorkers {
		config["max_workers"] = numIPs
	}

	maxWorkers, _ := config["max_workers"].(int)

	minWorkers, hasMinWorkers := config["min_workers"].(int)
	if !hasMinWorkers {
		minWorkers = numIPs
	}
	delete(config, "min_workers")

	if minWorkers > numIPs {
		log.Log.V(1).Info(fmt.Sprintf("The value of `min_workers` supplied (%d) is greater than the "+
			"number of available worker ips (%d). Setting `min_workers=%d`", minWorkers, numIPs, numIPs))
		nodeType["min_workers"] = numIPs
	} else {
		nodeType["min_workers"] = minWorkers
	}

	if maxWorkers > numIPs {
		log.Log.V(1).Info(fmt.Sprintf("The value of `max_workers` supplied (%d) is greater than the "+
			"number of available worker ips (%d). Setting `max_workers=%d`.", maxWorkers, numIPs, numIPs))
		nodeType["max_workers"] = numIPs
		config["max_workers"] = numIPs
	} else {
		nodeType["max_workers"] = maxWorkers
	}

	if maxWorkers < numIPs {
		log.Log.V(1).Info(fmt.Sprintf("The value of `max_workers` supplied (%d) is less than the "+
			"number of available worker ips (%d). At most %d Ray worker nodes will connect to the cluster.", maxWorkers, numIPs, maxWorkers))
	}

	return config, nil
}

// defaultsYaml is the local provider's default config, embedded into the
// binary so the deployed raygo executable does not depend on the source
// tree, runfiles, or wheel file layout.
//
//go:embed defaults.yaml
var defaultsYaml []byte

// LoadLocalDefaultsConfig returns the local provider's default config content.
func LoadLocalDefaultsConfig() ([]byte, error) {
	return defaultsYaml, nil
}
