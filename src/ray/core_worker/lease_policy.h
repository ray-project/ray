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

#include <utility>

#include "absl/container/flat_hash_set.h"
#include "ray/common/id.h"
#include "ray/common/lease/lease_spec.h"
#include "src/ray/protobuf/common.pb.h"

namespace ray {
namespace core {

struct LocalityData {
  uint64_t object_size{};
  absl::flat_hash_set<NodeID> nodes_containing_object;
};

/// Interface for providers of locality data to the lease policy.
class LocalityDataProviderInterface {
 public:
  virtual std::optional<LocalityData> GetLocalityData(
      const ObjectID &object_id) const = 0;

  virtual ~LocalityDataProviderInterface() = default;
};

/// Interface for mocking the lease policy.
class LeasePolicyInterface {
 public:
  /// Get the address of the best worker node for a lease request.
  virtual std::pair<rpc::Address, bool> GetBestNodeForLease(
      const LeaseSpecification &spec) = 0;

  virtual ~LeasePolicyInterface() = default;
};

using NodeAddrFactory = std::function<std::optional<rpc::Address>(const NodeID &node_id)>;

/// Class used by the core worker to implement a locality-aware lease policy for
/// picking a worker node for a lease request. This class is not thread-safe.
class LocalityAwareLeasePolicy : public LeasePolicyInterface {
 public:
  LocalityAwareLeasePolicy(LocalityDataProviderInterface &locality_data_provider,
                           NodeAddrFactory node_addr_factory,
                           rpc::Address fallback_rpc_address)
      : locality_data_provider_(locality_data_provider),
        node_addr_factory_(std::move(node_addr_factory)),
        fallback_rpc_address_(std::move(fallback_rpc_address)) {}

  ~LocalityAwareLeasePolicy() override = default;

  /// Get the address of the best worker node for a lease request.
  std::pair<rpc::Address, bool> GetBestNodeForLease(
      const LeaseSpecification &spec) override;

 private:
  /// Get the best worker node for a lease request.
  std::optional<NodeID> GetBestNodeIdForLease(const LeaseSpecification &spec);

  /// Provider of locality data that will be used in choosing the best lessor.
  LocalityDataProviderInterface &locality_data_provider_;

  /// Factory for building node RPC addresses given a NodeID.
  NodeAddrFactory node_addr_factory_;

  /// RPC address of fallback node (usually the local node).
  rpc::Address fallback_rpc_address_;
};

/// Class used by the core worker to implement a local-only lease policy for picking
/// a worker node for a lease request. This class is not thread-safe.
class LocalLeasePolicy : public LeasePolicyInterface {
 public:
  explicit LocalLeasePolicy(rpc::Address local_node_rpc_address)
      : local_node_rpc_address_(std::move(local_node_rpc_address)) {}

  ~LocalLeasePolicy() override = default;

  /// Get the address of the local node for a lease request.
  std::pair<rpc::Address, bool> GetBestNodeForLease(
      const LeaseSpecification &spec) override;

 private:
  /// RPC address of the local node.
  rpc::Address local_node_rpc_address_;
};

}  // namespace core
}  // namespace ray
