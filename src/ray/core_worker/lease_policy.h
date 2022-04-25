// Copyright 2017 The Ray Authors.
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

#pragma once

#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "ray/common/id.h"
#include "ray/common/task/task_spec.h"
#include "src/ray/protobuf/common.pb.h"

namespace ray {
namespace core {

struct LocalityData {
  uint64_t object_size;
  absl::flat_hash_set<NodeID> nodes_containing_object;
};

/// Interface for providers of locality data to the lease policy.
class LocalityDataProviderInterface {
 public:
  virtual absl::optional<LocalityData> GetLocalityData(const ObjectID &object_id) = 0;

  virtual ~LocalityDataProviderInterface() {}
};

/// Interface for mocking the lease policy.
class LeasePolicyInterface {
 public:
  /// Get the address of the best worker node for a lease request for the provided task.
  virtual std::pair<rpc::Address, bool> GetBestNodeForTask(
      const TaskSpecification &spec) = 0;

  virtual ~LeasePolicyInterface() {}
};

using NodeAddrFactory =
    std::function<absl::optional<rpc::Address>(const NodeID &node_id)>;

/// Class used by the core worker to implement a locality-aware lease policy for
/// picking a worker node for a lease request. This class is not thread-safe.
class LocalityAwareLeasePolicy : public LeasePolicyInterface {
 public:
  LocalityAwareLeasePolicy(
      std::shared_ptr<LocalityDataProviderInterface> locality_data_provider,
      NodeAddrFactory node_addr_factory,
      const rpc::Address fallback_rpc_address)
      : locality_data_provider_(locality_data_provider),
        node_addr_factory_(node_addr_factory),
        fallback_rpc_address_(fallback_rpc_address) {}

  ~LocalityAwareLeasePolicy() {}

  /// Get the address of the best worker node for a lease request for the provided task.
  std::pair<rpc::Address, bool> GetBestNodeForTask(
      const TaskSpecification &spec) override;

 private:
  /// Get the best worker node for a lease request for the provided task.
  absl::optional<NodeID> GetBestNodeIdForTask(const TaskSpecification &spec);

  /// Provider of locality data that will be used in choosing the best lessor.
  std::shared_ptr<LocalityDataProviderInterface> locality_data_provider_;

  /// Factory for building node RPC addresses given a NodeID.
  NodeAddrFactory node_addr_factory_;

  /// RPC address of fallback node (usually the local node).
  const rpc::Address fallback_rpc_address_;
};

/// Class used by the core worker to implement a local-only lease policy for picking
/// a worker node for a lease request. This class is not thread-safe.
class LocalLeasePolicy : public LeasePolicyInterface {
 public:
  LocalLeasePolicy(const rpc::Address local_node_rpc_address)
      : local_node_rpc_address_(local_node_rpc_address) {}

  ~LocalLeasePolicy() {}

  /// Get the address of the local node for a lease request for the provided task.
  std::pair<rpc::Address, bool> GetBestNodeForTask(
      const TaskSpecification &spec) override;

 private:
  /// RPC address of the local node.
  const rpc::Address local_node_rpc_address_;
};

}  // namespace core
}  // namespace ray
