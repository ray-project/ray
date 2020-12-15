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
  /// Get the address of the best lessor node for the provided task.
  virtual absl::optional<rpc::Address> GetBestNodeForTask(
      const TaskSpecification &spec) = 0;

  virtual ~LeasePolicyInterface() {}
};

typedef std::function<absl::optional<rpc::Address>(const NodeID &node_id)>
    NodeAddrFactory;

/// Class used by the core worker to implement a lease policy for picking the best
/// lessor node. This class is not thread-safe.
class LeasePolicy : public LeasePolicyInterface {
 public:
  LeasePolicy(std::shared_ptr<LocalityDataProviderInterface> locality_data_provider,
              NodeAddrFactory node_addr_factory)
      : locality_data_provider_(locality_data_provider),
        node_addr_factory_(node_addr_factory) {}

  ~LeasePolicy() {}

  /// Get the address of the best lessor node for the provided task.
  absl::optional<rpc::Address> GetBestNodeForTask(const TaskSpecification &spec);

 private:
  /// Get the best lessor node for the provided task.
  absl::optional<NodeID> GetBestNodeIdForTask(const TaskSpecification &spec);

  /// Get the best lessor node for the provided objects.
  absl::optional<NodeID> GetBestNodeIdForObjects(const std::vector<ObjectID> &object_ids);

  /// Provider of locality data that will be used in choosing the best lessor.
  std::shared_ptr<LocalityDataProviderInterface> locality_data_provider_;

  /// Factory for building node RPC addresses given a NodeID.
  NodeAddrFactory node_addr_factory_;
};

}  // namespace ray
