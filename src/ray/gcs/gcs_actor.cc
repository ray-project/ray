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

#include "ray/gcs/gcs_actor.h"

#include <memory>
#include <string>

#include "ray/observability/ray_actor_definition_event.h"
#include "ray/observability/ray_actor_lifecycle_event.h"
#include "ray/util/logging.h"
#include "src/ray/protobuf/public/events_actor_lifecycle_event.pb.h"
#include "src/ray/protobuf/public/events_base_event.pb.h"

namespace ray {
namespace gcs {

NodeID GcsActor::GetNodeID() const {
  const auto &node_id_binary = actor_table_data_.address().node_id();
  if (node_id_binary.empty()) {
    return NodeID::Nil();
  }
  return NodeID::FromBinary(node_id_binary);
}

void GcsActor::UpdateAddress(const rpc::Address &address) {
  actor_table_data_.mutable_address()->CopyFrom(address);
}

const rpc::Address &GcsActor::GetAddress() const { return actor_table_data_.address(); }

WorkerID GcsActor::GetWorkerID() const {
  const auto &address = actor_table_data_.address();
  if (address.worker_id().empty()) {
    return WorkerID::Nil();
  }
  return WorkerID::FromBinary(address.worker_id());
}

WorkerID GcsActor::GetOwnerID() const {
  return WorkerID::FromBinary(GetOwnerAddress().worker_id());
}

NodeID GcsActor::GetOwnerNodeID() const {
  return NodeID::FromBinary(GetOwnerAddress().node_id());
}

const rpc::Address &GcsActor::GetOwnerAddress() const {
  return actor_table_data_.owner_address();
}

void GcsActor::UpdateState(rpc::ActorTableData::ActorState state) {
  actor_table_data_.set_state(state);
  RefreshMetrics();
}

rpc::ActorTableData::ActorState GcsActor::GetState() const {
  return actor_table_data_.state();
}

ActorID GcsActor::GetActorID() const {
  return ActorID::FromBinary(actor_table_data_.actor_id());
}

bool GcsActor::IsDetached() const { return actor_table_data_.is_detached(); }

std::string GcsActor::GetName() const { return actor_table_data_.name(); }

std::string GcsActor::GetRayNamespace() const {
  return actor_table_data_.ray_namespace();
}

TaskSpecification GcsActor::GetCreationTaskSpecification() const {
  // The task spec is not available when the actor is dead.
  RAY_CHECK(actor_table_data_.state() != rpc::ActorTableData::DEAD);
  return TaskSpecification(*task_spec_);
}

const rpc::ActorTableData &GcsActor::GetActorTableData() const {
  return actor_table_data_;
}

rpc::ActorTableData *GcsActor::GetMutableActorTableData() { return &actor_table_data_; }

void GcsActor::WriteActorExportEvent(bool is_actor_registration) const {
  // If ray event is enabled and recorder present, emit actor events to the aggregator.
  if (RayConfig::instance().enable_ray_event()) {
    std::vector<std::unique_ptr<observability::RayEventInterface>> events;
    if (is_actor_registration) {
      events.push_back(std::make_unique<observability::RayActorDefinitionEvent>(
          actor_table_data_, session_name_));
    }
    events.push_back(std::make_unique<observability::RayActorLifecycleEvent>(
        actor_table_data_,
        ConvertActorStateToLifecycleEvent(actor_table_data_.state()),
        session_name_));

    ray_event_recorder_.AddEvents(std::move(events));
    return;
  }

  /// Verify actor export events should be written to file
  /// and then write actor_table_data_ as an export event.
  if (!export_event_write_enabled_) {
    return;
  }
  std::shared_ptr<rpc::ExportActorData> export_actor_data_ptr =
      std::make_shared<rpc::ExportActorData>();

  export_actor_data_ptr->set_actor_id(actor_table_data_.actor_id());
  export_actor_data_ptr->set_job_id(actor_table_data_.job_id());
  export_actor_data_ptr->set_state(ConvertActorStateToExport(actor_table_data_.state()));
  export_actor_data_ptr->set_is_detached(actor_table_data_.is_detached());
  export_actor_data_ptr->set_name(actor_table_data_.name());
  export_actor_data_ptr->set_pid(actor_table_data_.pid());
  export_actor_data_ptr->set_ray_namespace(actor_table_data_.ray_namespace());
  export_actor_data_ptr->set_serialized_runtime_env(
      actor_table_data_.serialized_runtime_env());
  export_actor_data_ptr->set_class_name(actor_table_data_.class_name());
  export_actor_data_ptr->mutable_death_cause()->CopyFrom(actor_table_data_.death_cause());
  export_actor_data_ptr->mutable_required_resources()->insert(
      actor_table_data_.required_resources().begin(),
      actor_table_data_.required_resources().end());
  export_actor_data_ptr->set_node_id(actor_table_data_.node_id());
  export_actor_data_ptr->set_placement_group_id(actor_table_data_.placement_group_id());
  export_actor_data_ptr->set_repr_name(actor_table_data_.repr_name());
  export_actor_data_ptr->mutable_labels()->insert(task_spec_.get()->labels().begin(),
                                                  task_spec_.get()->labels().end());
  *export_actor_data_ptr->mutable_label_selector() = actor_table_data_.label_selector();

  RayExportEvent(export_actor_data_ptr).SendEvent();
}

rpc::TaskSpec *GcsActor::GetMutableTaskSpec() { return task_spec_.get(); }

rpc::LeaseSpec *GcsActor::GetMutableLeaseSpec() {
  return &lease_spec_->GetMutableMessage();
}

const LeaseSpecification &GcsActor::GetLeaseSpecification() const { return *lease_spec_; }

const ResourceRequest &GcsActor::GetAcquiredResources() const {
  return acquired_resources_;
}
void GcsActor::SetAcquiredResources(ResourceRequest &&resource_request) {
  acquired_resources_ = std::move(resource_request);
}

bool GcsActor::GetGrantOrReject() const { return grant_or_reject_; }

void GcsActor::SetGrantOrReject(bool grant_or_reject) {
  grant_or_reject_ = grant_or_reject;
}

}  // namespace gcs
}  // namespace ray
