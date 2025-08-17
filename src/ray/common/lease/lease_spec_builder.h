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

#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "ray/common/id.h"
#include "ray/common/lease/lease_spec.h"
#include "src/ray/protobuf/common.pb.h"

namespace ray {

/// Helper class for building a `LeaseSpecification` object.
/// This is similar to TaskSpecBuilder but focused only on lease-relevant fields.
class LeaseSpecBuilder {
 public:
  LeaseSpecBuilder() : message_(std::make_shared<rpc::LeaseSpec>()) {}

  /// Consume the `message_` data member and construct `LeaseSpecification`.
  /// NOTICE: Builder is invalidated after this function.
  LeaseSpecification ConsumeAndBuild() && {
    return LeaseSpecification(std::move(message_));
  }

  /// Get a reference to the internal protobuf message object.
  const rpc::LeaseSpec &GetMessage() const { return *message_; }

  /// Set the common lease attributes.
  /// These are fields that are common to all lease types.
  ///
  /// \return Reference to the builder object itself.
  LeaseSpecBuilder &BuildCommonLeaseSpec(
      const JobID &job_id,
      const rpc::Address &caller_address,
      const google::protobuf::Map<std::string, double> &required_resources,
      const google::protobuf::Map<std::string, double> &required_placement_resources,
      const rpc::SchedulingStrategy &scheduling_strategy,
      const google::protobuf::Map<std::string, std::string> &label_selector,
      int64_t depth,
      const std::vector<rpc::ObjectReference> &dependencies,
      const Language &language,
      const rpc::RuntimeEnvInfo &runtime_env_info,
      const TaskID &parent_task_id,
      const ray::rpc::FunctionDescriptor &function_descriptor,
      const std::string &task_name,
      uint64_t attempt_number);

  /// Set the actor creation lease spec attributes.
  /// These are fields specific to actor creation leases.
  ///
  /// \return Reference to the builder object itself.
  LeaseSpecBuilder &SetActorCreationLeaseSpec(
      const ActorID &actor_id,
      const ActorID &root_detached_actor_id,
      int64_t max_restarts,
      bool is_detached_actor,
      const google::protobuf::RepeatedPtrField<std::string> &dynamic_worker_options);

  /// Set the normal task lease spec attributes.
  /// These are fields specific to normal task leases.
  ///
  /// \return Reference to the builder object itself.
  LeaseSpecBuilder &SetNormalLeaseSpec(int max_retries);

 private:
  std::shared_ptr<rpc::LeaseSpec> message_;
};

}  // namespace ray
