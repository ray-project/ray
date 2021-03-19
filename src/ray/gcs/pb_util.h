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

#include <memory>

#include "ray/common/id.h"
#include "ray/common/task/task_spec.h"
#include "src/ray/protobuf/gcs.pb.h"

namespace ray {

namespace gcs {

/// Helper function to produce error table data.
inline std::shared_ptr<ray::rpc::ErrorTableData> CreateErrorTableData(
    const std::string &error_type, const std::string &error_msg, double timestamp,
    const JobID &job_id = JobID::Nil()) {
  auto error_info_ptr = std::make_shared<ray::rpc::ErrorTableData>();
  error_info_ptr->set_type(error_type);
  error_info_ptr->set_error_message(error_msg);
  error_info_ptr->set_timestamp(timestamp);
  error_info_ptr->set_job_id(job_id.Binary());
  return error_info_ptr;
}

/// Helper function to produce actor table data.
inline std::shared_ptr<ray::rpc::ActorTableData> CreateActorTableData(
    const TaskSpecification &task_spec, const ray::rpc::Address &address,
    ray::rpc::ActorTableData::ActorState state, uint64_t num_restarts) {
  RAY_CHECK(task_spec.IsActorCreationTask());
  auto actor_id = task_spec.ActorCreationId();
  auto actor_info_ptr = std::make_shared<ray::rpc::ActorTableData>();
  // Set all of the static fields for the actor. These fields will not change
  // even if the actor fails or is reconstructed.
  actor_info_ptr->set_actor_id(actor_id.Binary());
  actor_info_ptr->set_parent_id(task_spec.CallerId().Binary());
  actor_info_ptr->set_actor_creation_dummy_object_id(
      task_spec.ActorDummyObject().Binary());
  actor_info_ptr->set_job_id(task_spec.JobId().Binary());
  actor_info_ptr->set_max_restarts(task_spec.MaxActorRestarts());
  actor_info_ptr->set_is_detached(task_spec.IsDetachedActor());
  // Set the fields that change when the actor is restarted.
  actor_info_ptr->set_num_restarts(num_restarts);
  actor_info_ptr->mutable_address()->CopyFrom(address);
  actor_info_ptr->mutable_owner_address()->CopyFrom(
      task_spec.GetMessage().caller_address());
  actor_info_ptr->set_state(state);
  return actor_info_ptr;
}

/// Helper function to produce worker failure data.
inline std::shared_ptr<ray::rpc::WorkerTableData> CreateWorkerFailureData(
    const NodeID &raylet_id, const WorkerID &worker_id, const std::string &address,
    int32_t port, int64_t timestamp, rpc::WorkerExitType disconnect_type,
    const std::shared_ptr<rpc::RayException> &creation_task_exception = nullptr) {
  auto worker_failure_info_ptr = std::make_shared<ray::rpc::WorkerTableData>();
  worker_failure_info_ptr->mutable_worker_address()->set_raylet_id(raylet_id.Binary());
  worker_failure_info_ptr->mutable_worker_address()->set_worker_id(worker_id.Binary());
  worker_failure_info_ptr->mutable_worker_address()->set_ip_address(address);
  worker_failure_info_ptr->mutable_worker_address()->set_port(port);
  worker_failure_info_ptr->set_timestamp(timestamp);
  worker_failure_info_ptr->set_exit_type(disconnect_type);
  if (creation_task_exception != nullptr) {
    // this pointer will be freed by protobuf internal codes
    auto copied_data = new rpc::RayException(*creation_task_exception);
    worker_failure_info_ptr->set_allocated_creation_task_exception(copied_data);
  }
  return worker_failure_info_ptr;
}

/// Helper function to produce object location change.
///
/// \param node_id The node ID that this object appeared on or was evicted by.
/// \param is_add Whether the object is appeared on the node.
/// \return The object location change created by this method.
inline std::shared_ptr<ray::rpc::ObjectLocationChange> CreateObjectLocationChange(
    const NodeID &node_id, bool is_add) {
  auto object_location_change = std::make_shared<ray::rpc::ObjectLocationChange>();
  object_location_change->set_is_add(is_add);
  object_location_change->set_node_id(node_id.Binary());
  return object_location_change;
}

}  // namespace gcs

}  // namespace ray
