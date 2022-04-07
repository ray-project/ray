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
#include <optional>

#include "ray/raylet/scheduling/cluster_resource_data.h"

namespace ray {
namespace raylet_scheduling_policy {

/// NodeScorer is a scorer to make a grade to the node, which is used for scheduling
/// decision.
class NodeScorer {
 public:
  virtual ~NodeScorer() = default;

  /// \brief Score according to node resources.
  ///
  /// \param required_resources The required resources.
  /// \param node_resources The node resources which contains available and total
  /// resources.
  /// \return Score of the node.
  virtual double Score(const ResourceRequest &required_resources,
                       const NodeResources &node_resources) = 0;
};

/// LeastResourceScorer is a score plugin that favors nodes with fewer allocation
/// requested resources based on requested resources.
class LeastResourceScorer : public NodeScorer {
 public:
  double Score(const ResourceRequest &required_resources,
               const NodeResources &node_resources) override;

 private:
  /// \brief Calculate one of the resource scores.
  ///
  /// \param requested Quantity of one of the required resources.
  /// \param available Quantity of one of the available resources.
  /// \return Score of the node.
  double Calculate(const FixedPoint &requested, const FixedPoint &available);
};

}  // namespace raylet_scheduling_policy
}  // namespace ray
