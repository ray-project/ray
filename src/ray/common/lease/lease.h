// Copyright 2025 The Ray Authors.
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

#include <inttypes.h>

#include <string>
#include <vector>

#include "ray/common/lease/lease_spec.h"

namespace ray {

/// \class RayLease
///
/// A RayLease represents a Ray lease and a specification of its execution (e.g.,
/// resource demands). The lease's specification contains both immutable fields,
/// determined at submission time, and mutable fields, determined at execution
/// time.
class RayLease {
 public:
  /// Construct an empty lease. This should only be used to pass a lease
  /// as an out parameter to a function or method.
  // TODO(#55923): Remove this constructor and refactor worker.h to use unique_ptr
  RayLease() = default;

  /// Construct a `RayLease` object from a protobuf message.
  explicit RayLease(rpc::LeaseSpec lease_spec)
      : lease_spec_(LeaseSpecification(std::move(lease_spec))) {}

  /// Construct a `RayLease` object from a `LeaseSpecification`.
  explicit RayLease(LeaseSpecification lease_spec) : lease_spec_(std::move(lease_spec)) {}

  RayLease(LeaseSpecification lease_spec, std::string preferred_node_id)
      : lease_spec_(std::move(lease_spec)),
        preferred_node_id_(std::move(preferred_node_id)) {}

  /// Get the immutable specification for the lease.
  ///
  /// \return The immutable specification for the lease.
  const LeaseSpecification &GetLeaseSpecification() const { return lease_spec_; }

  /// Get the lease's object dependencies. This comprises the immutable lease
  /// arguments and the mutable execution dependencies.
  ///
  /// \return The object dependencies.
  const std::vector<rpc::ObjectReference> &GetDependencies() const {
    return lease_spec_.GetDependencies();
  }

  /// Get the lease's preferred node id for scheduling. If the returned value
  /// is empty, then it means the lease has no preferred node.
  ///
  /// \return The preferred node id.
  const std::string &GetPreferredNodeID() const { return preferred_node_id_; }

  std::string DebugString() const {
    return absl::StrFormat("lease_spec={%s}", lease_spec_.DebugString());
  }

 private:
  /// RayLease specification object, consisting of immutable information about this
  /// lease determined at submission time. Includes resource demand, object
  /// dependencies, etc.
  LeaseSpecification lease_spec_;

  std::string preferred_node_id_;
};

}  // namespace ray
