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

#include "ray/common/placement_group.h"

namespace ray {
void PlacementGroupSpecification::ConstructBundles() {
  for (int i = 0; i < message_->bundles_size(); i++) {
    bundles_.push_back(BundleSpecification(message_->bundles(i)));
  }
}

PlacementGroupID PlacementGroupSpecification::PlacementGroupId() const {
  if (message_->placement_group_id().empty()) {
    return PlacementGroupID::Nil();
  } else {
    return PlacementGroupID::FromBinary(message_->placement_group_id());
  }
}

std::vector<BundleSpecification> PlacementGroupSpecification::GetBundles() const {
  return bundles_;
}

rpc::PlacementStrategy PlacementGroupSpecification::GetStrategy() const {
  return message_->strategy();
}

BundleSpecification PlacementGroupSpecification::GetBundle(int position) const {
  return bundles_[position];
}

std::string PlacementGroupSpecification::GetName() const {
  return std::string(message_->name());
}

double PlacementGroupSpecification::GetMaxCpuFractionPerNode() const {
  return message_->max_cpu_fraction_per_node();
}
}  // namespace ray
