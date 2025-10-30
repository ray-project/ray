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
#include <string>

#include "ray/common/id.h"
#include "ray/common/lease/lease_spec.h"
#include "ray/common/scheduling/cluster_resource_data.h"
#include "ray/common/task/task_spec.h"
#include "ray/observability/ray_event_recorder_interface.h"
#include "ray/util/counter_map.h"
#include "ray/util/event.h"
#include "src/ray/protobuf/core_worker.pb.h"
#include "src/ray/protobuf/export_actor_data.pb.h"
#include "src/ray/protobuf/gcs_service.pb.h"

namespace ray {
namespace gcs {

/// GcsActor just wraps `ActorTableData` and provides some convenient interfaces to access
/// the fields inside `ActorTableData`.
/// This class is not thread-safe.
class GcsActor {
 public:
  /// Create a GcsActor by actor_table_data.
  ///
  /// \param actor_table_data Data of the actor (see gcs.proto).
  /// \param counter The counter to report metrics to.
  explicit GcsActor(
      rpc::ActorTableData actor_table_data,
      std::shared_ptr<CounterMap<std::pair<rpc::ActorTableData::ActorState, std::string>>>
          counter,
      observability::RayEventRecorderInterface &recorder,
      const std::string &session_name)
      : actor_table_data_(std::move(actor_table_data)),
        counter_(std::move(counter)),
        export_event_write_enabled_(IsExportAPIEnabledActor()),
        ray_event_recorder_(recorder),
        session_name_(session_name) {
    RefreshMetrics();
  }

  /// Create a GcsActor by actor_table_data and task_spec.
  /// This is only for ALIVE actors.
  ///
  /// \param actor_table_data Data of the actor (see gcs.proto).
  /// \param task_spec Task spec of the actor.
  /// \param counter The counter to report metrics to.
  explicit GcsActor(
      rpc::ActorTableData actor_table_data,
      rpc::TaskSpec task_spec,
      std::shared_ptr<CounterMap<std::pair<rpc::ActorTableData::ActorState, std::string>>>
          counter,
      observability::RayEventRecorderInterface &recorder,
      const std::string &session_name)
      : actor_table_data_(std::move(actor_table_data)),
        task_spec_(std::make_unique<rpc::TaskSpec>(std::move(task_spec))),
        counter_(std::move(counter)),
        export_event_write_enabled_(IsExportAPIEnabledActor()),
        ray_event_recorder_(recorder),
        session_name_(session_name) {
    lease_spec_ = std::make_unique<LeaseSpecification>(*task_spec_);
    RAY_CHECK(actor_table_data_.state() != rpc::ActorTableData::DEAD);
    RefreshMetrics();
  }

  /// Create a GcsActor by TaskSpec.
  ///
  /// \param task_spec Contains the actor creation task specification.
  /// \param ray_namespace Namespace of the actor.
  /// \param counter The counter to report metrics to.
  explicit GcsActor(
      rpc::TaskSpec task_spec,
      std::string ray_namespace,
      std::shared_ptr<CounterMap<std::pair<rpc::ActorTableData::ActorState, std::string>>>
          counter,
      observability::RayEventRecorderInterface &recorder,
      const std::string &session_name)
      : task_spec_(std::make_unique<rpc::TaskSpec>(std::move(task_spec))),
        counter_(std::move(counter)),
        export_event_write_enabled_(IsExportAPIEnabledActor()),
        ray_event_recorder_(recorder),
        session_name_(session_name) {
    RAY_CHECK(task_spec_->type() == TaskType::ACTOR_CREATION_TASK);
    const auto &actor_creation_task_spec = task_spec_->actor_creation_task_spec();
    actor_table_data_.set_actor_id(actor_creation_task_spec.actor_id());
    actor_table_data_.set_job_id(task_spec_->job_id());
    actor_table_data_.set_max_restarts(actor_creation_task_spec.max_actor_restarts());
    actor_table_data_.set_num_restarts(0);
    actor_table_data_.set_num_restarts_due_to_lineage_reconstruction(0);

    actor_table_data_.mutable_function_descriptor()->CopyFrom(
        task_spec_->function_descriptor());

    actor_table_data_.set_is_detached(actor_creation_task_spec.is_detached());
    actor_table_data_.set_name(actor_creation_task_spec.name());
    actor_table_data_.mutable_owner_address()->CopyFrom(task_spec_->caller_address());

    actor_table_data_.set_state(rpc::ActorTableData::DEPENDENCIES_UNREADY);

    actor_table_data_.mutable_address()->set_node_id(NodeID::Nil().Binary());
    actor_table_data_.mutable_address()->set_worker_id(WorkerID::Nil().Binary());

    actor_table_data_.set_ray_namespace(ray_namespace);
    if (task_spec_->scheduling_strategy().scheduling_strategy_case() ==
        rpc::SchedulingStrategy::SchedulingStrategyCase::
            kPlacementGroupSchedulingStrategy) {
      actor_table_data_.set_placement_group_id(task_spec_->scheduling_strategy()
                                                   .placement_group_scheduling_strategy()
                                                   .placement_group_id());
    }

    // Set required resources.
    auto resource_map =
        GetCreationTaskSpecification().GetRequiredResources().GetResourceMap();
    actor_table_data_.mutable_required_resources()->insert(resource_map.begin(),
                                                           resource_map.end());

    const auto &function_descriptor = task_spec_->function_descriptor();
    switch (function_descriptor.function_descriptor_case()) {
    case rpc::FunctionDescriptor::FunctionDescriptorCase::kJavaFunctionDescriptor:
      actor_table_data_.set_class_name(
          function_descriptor.java_function_descriptor().class_name());
      break;
    case rpc::FunctionDescriptor::FunctionDescriptorCase::kPythonFunctionDescriptor:
      actor_table_data_.set_class_name(
          function_descriptor.python_function_descriptor().class_name());
      break;
    default:
      // TODO(Alex): Handle the C++ case, which we currently don't have an
      // easy equivalent to class_name for.
      break;
    }

    actor_table_data_.set_serialized_runtime_env(
        task_spec_->runtime_env_info().serialized_runtime_env());
    if (task_spec_->call_site().size() > 0) {
      actor_table_data_.set_call_site(task_spec_->call_site());
    }
    if (task_spec_->label_selector().label_constraints_size() > 0) {
      *actor_table_data_.mutable_label_selector() =
          ray::LabelSelector(task_spec_->label_selector()).ToStringMap();
    }
    lease_spec_ = std::make_unique<LeaseSpecification>(*task_spec_);
    RefreshMetrics();
  }

  ~GcsActor() {
    // We don't decrement the value when it becomes DEAD because we don't want to
    // lose the # of dead actors count when this class is GC'ed.
    if (last_metric_state_ && last_metric_state_.value() != rpc::ActorTableData::DEAD) {
      RAY_LOG(DEBUG) << "Decrementing state at "
                     << rpc::ActorTableData::ActorState_Name(last_metric_state_.value())
                     << " " << GetActorTableData().class_name();
      counter_->Decrement(
          std::make_pair(last_metric_state_.value(), GetActorTableData().class_name()));
    }
  }

  /// Get the node id on which this actor is created.
  NodeID GetNodeID() const;
  /// Get the id of the worker on which this actor is created.
  WorkerID GetWorkerID() const;
  /// Get the actor's owner ID.
  WorkerID GetOwnerID() const;
  /// Get the node ID of the actor's owner.
  NodeID GetOwnerNodeID() const;
  /// Get the address of the actor's owner.
  const rpc::Address &GetOwnerAddress() const;

  /// Update the `Address` of this actor (see gcs.proto).
  void UpdateAddress(const rpc::Address &address);
  /// Get the `Address` of this actor.
  const rpc::Address &GetAddress() const;

  /// Update the state of this actor and refreshes metrics. Do not update the
  /// state of the underlying proto directly via set_state(), otherwise metrics
  /// will get out of sync.
  void UpdateState(rpc::ActorTableData::ActorState state);
  /// Get the state of this gcs actor.
  rpc::ActorTableData::ActorState GetState() const;

  /// Get the id of this actor.
  ActorID GetActorID() const;
  /// Returns whether or not this is a detached actor.
  bool IsDetached() const;
  /// Get the name of this actor.
  std::string GetName() const;
  /// Get the namespace of this actor.
  std::string GetRayNamespace() const;
  /// Get the task specification of this actor.
  TaskSpecification GetCreationTaskSpecification() const;
  const LeaseSpecification &GetLeaseSpecification() const;

  /// Get the immutable ActorTableData of this actor.
  const rpc::ActorTableData &GetActorTableData() const;
  /// Get the mutable ActorTableData of this actor.
  rpc::ActorTableData *GetMutableActorTableData();
  rpc::TaskSpec *GetMutableTaskSpec();
  rpc::LeaseSpec *GetMutableLeaseSpec();
  /// Write an event containing this actor's ActorTableData
  /// to file for the Export API.
  void WriteActorExportEvent(bool is_actor_registration) const;
  // Verify if export events should be written for EXPORT_ACTOR source types
  bool IsExportAPIEnabledActor() const {
    return IsExportAPIEnabledSourceType(
        "EXPORT_ACTOR",
        RayConfig::instance().enable_export_api_write(),
        RayConfig::instance().enable_export_api_write_config());
  }

  const ResourceRequest &GetAcquiredResources() const;
  void SetAcquiredResources(ResourceRequest &&resource_request);
  bool GetGrantOrReject() const;
  void SetGrantOrReject(bool grant_or_reject);

 private:
  void RefreshMetrics() {
    auto cur_state = GetState();
    if (last_metric_state_) {
      RAY_LOG(DEBUG) << "Swapping state from "
                     << rpc::ActorTableData::ActorState_Name(last_metric_state_.value())
                     << " to " << rpc::ActorTableData::ActorState_Name(cur_state)
                     << " for : " << GetActorID();
      counter_->Swap(
          std::make_pair(last_metric_state_.value(), GetActorTableData().class_name()),
          std::make_pair(cur_state, GetActorTableData().class_name()));
    } else {
      RAY_LOG(DEBUG) << "Incrementing state at "
                     << rpc::ActorTableData::ActorState_Name(cur_state) << " "
                     << GetActorTableData().class_name();
      counter_->Increment(std::make_pair(cur_state, GetActorTableData().class_name()));
    }
    last_metric_state_ = cur_state;
  }

  rpc::ExportActorData::ActorState ConvertActorStateToExport(
      rpc::ActorTableData::ActorState actor_state) const {
    switch (actor_state) {
    case rpc::ActorTableData::DEPENDENCIES_UNREADY:
      return rpc::ExportActorData::DEPENDENCIES_UNREADY;
    case rpc::ActorTableData::PENDING_CREATION:
      return rpc::ExportActorData::PENDING_CREATION;
    case rpc::ActorTableData::ALIVE:
      return rpc::ExportActorData::ALIVE;
    case rpc::ActorTableData::RESTARTING:
      return rpc::ExportActorData::RESTARTING;
    case rpc::ActorTableData::DEAD:
      return rpc::ExportActorData::DEAD;
    default:
      // Unknown rpc::ActorTableData::ActorState value
      RAY_LOG(FATAL) << "Invalid value for rpc::ActorTableData::ActorState"
                     << rpc::ActorTableData::ActorState_Name(actor_state);
      return rpc::ExportActorData::DEAD;
    }
  }

  rpc::events::ActorLifecycleEvent::State ConvertActorStateToLifecycleEvent(
      rpc::ActorTableData::ActorState actor_state) const {
    switch (actor_state) {
    case rpc::ActorTableData::DEPENDENCIES_UNREADY:
      return rpc::events::ActorLifecycleEvent::DEPENDENCIES_UNREADY;
    case rpc::ActorTableData::PENDING_CREATION:
      return rpc::events::ActorLifecycleEvent::PENDING_CREATION;
    case rpc::ActorTableData::ALIVE:
      return rpc::events::ActorLifecycleEvent::ALIVE;
    case rpc::ActorTableData::RESTARTING:
      return rpc::events::ActorLifecycleEvent::RESTARTING;
    case rpc::ActorTableData::DEAD:
      return rpc::events::ActorLifecycleEvent::DEAD;
    default:
      RAY_LOG(FATAL) << "Invalid value for rpc::ActorTableData::ActorState"
                     << rpc::ActorTableData::ActorState_Name(actor_state);
      return rpc::events::ActorLifecycleEvent::DEAD;
    }
  }

  /// The actor meta data which contains the task specification as well as the state of
  /// the gcs actor and so on (see gcs.proto).
  rpc::ActorTableData actor_table_data_;
  const std::unique_ptr<rpc::TaskSpec> task_spec_;
  /// Resources acquired by this actor.
  ResourceRequest acquired_resources_;
  /// Reference to the counter to use for actor state metrics tracking.
  std::shared_ptr<CounterMap<std::pair<rpc::ActorTableData::ActorState, std::string>>>
      counter_;
  /// Whether the actor's target node only grants or rejects the lease request.
  bool grant_or_reject_ = false;
  /// The last recorded metric state.
  std::optional<rpc::ActorTableData::ActorState> last_metric_state_;
  /// If true, actor events are exported for Export API
  bool export_event_write_enabled_ = false;
  std::unique_ptr<LeaseSpecification> lease_spec_;
  /// Event recorder and session name for Ray events
  observability::RayEventRecorderInterface &ray_event_recorder_;
  std::string session_name_;
};

using RestartActorForLineageReconstructionCallback =
    std::function<void(std::shared_ptr<GcsActor>)>;
using CreateActorCallback = std::function<void(
    std::shared_ptr<GcsActor>, const rpc::PushTaskReply &reply, const Status &status)>;

}  // namespace gcs
}  // namespace ray
