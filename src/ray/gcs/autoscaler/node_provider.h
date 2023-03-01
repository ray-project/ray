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

#include "src/ray/protobuf/node_provider.pb.h"

namespace ray {
namespace autoscaler {

/**
 * Interface for node provider that autoscaler interacts with.
 */
class INodeProvider {
 public:
  virtual ~INodeProvider() = default;

  // Query the available node types.
  virtual rpc::AvailableNodeTypesResponse GetAvailableNodeTypes() const = 0;

  // Create new nodes.
  virtual std::vector<rpc::CreationStatus> CreateNodes(const std::string &node_type_id,
                                                       int32_t count) = 0;

  virtual std::vector<rpc::TerminationStatus> TerminateNodes(
      const std::vector<std::string> &node_ids) = 0;

  virtual std::vector<rpc::Node> GetNodesStatus(
      const std::vector<std::string> &node_ids) const = 0;

  virtual std::vector<rpc::Node> GetNodesStatus(rpc::RayNodeKind node_kind) const = 0;

  virtual std::vector<rpc::Node> GetNodesStatus(rpc::NodeStatus node_status) const = 0;
};
}  // namespace autoscaler
}  // namespace ray
