// Copyright 2019-2020 The Ray Authors.
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

#include "ray/common/lease/lease.h"

#include <string>
#include <utility>
#include <vector>

#include "absl/strings/str_format.h"

namespace ray {

RayLease::RayLease(rpc::LeaseSpec lease_spec)
    : lease_spec_(LeaseSpecification(std::move(lease_spec))) {
  ComputeDependencies();
}

RayLease::RayLease(LeaseSpecification lease_spec) : lease_spec_(std::move(lease_spec)) {
  ComputeDependencies();
}

RayLease::RayLease(LeaseSpecification lease_spec, std::string preferred_node_id)
    : lease_spec_(std::move(lease_spec)),
      preferred_node_id_(std::move(preferred_node_id)) {
  ComputeDependencies();
}

const LeaseSpecification &RayLease::GetLeaseSpecification() const { return lease_spec_; }

const std::vector<rpc::ObjectReference> &RayLease::GetDependencies() const {
  return dependencies_;
}

const std::string &RayLease::GetPreferredNodeID() const { return preferred_node_id_; }

void RayLease::ComputeDependencies() { dependencies_ = lease_spec_.GetDependencies(); }

std::string RayLease::DebugString() const {
  return absl::StrFormat("lease_spec={%s}", lease_spec_.DebugString());
}

}  // namespace ray
