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

#include "ray/gcs/gcs_server/gcs_placement_group_manager.h"

#include "ray/common/ray_config.h"
#include "ray/gcs/pb_util.h"
#include "ray/stats/stats.h"
#include "ray/util/asio_util.h"
#include "src/ray/protobuf/gcs.pb.h"

namespace ray {
namespace gcs {

void GcsPlacementGroup::UpdateState(
    rpc::PlacementGroupTableData::PlacementGroupState state) {
  placement_group_table_data_.set_state(state);
}

rpc::PlacementGroupTableData::PlacementGroupState GcsPlacementGroup::GetState() const {
  return placement_group_table_data_.state();
}

PlacementGroupID GcsPlacementGroup::GetPlacementGroupID() const {
  return PlacementGroupID::FromBinary(placement_group_table_data_.placement_group_id());
}

std::string GcsPlacementGroup::GetName() const {
  return placement_group_table_data_.name();
}

std::vector<std::shared_ptr<BundleSpecification>> GcsPlacementGroup::GetBundles() const {
  const auto &bundles = placement_group_table_data_.bundles();
  std::vector<std::shared_ptr<BundleSpecification>> ret_bundles;
  for (auto &bundle : bundles) {
    ret_bundles.push_back(std::make_shared<BundleSpecification>(bundle));
  }
  return ret_bundles;
}

std::vector<std::shared_ptr<BundleSpecification>> GcsPlacementGroup::GetUnplacedBundles()
    const {
  const auto &bundles = placement_group_table_data_.bundles();
  std::vector<std::shared_ptr<BundleSpecification>> unplaced_bundles;
  for (auto &bundle : bundles) {
    if (NodeID::FromBinary(bundle.node_id()).IsNil()) {
      unplaced_bundles.push_back(std::make_shared<BundleSpecification>(bundle));
    }
  }
  return unplaced_bundles;
}

rpc::PlacementStrategy GcsPlacementGroup::GetStrategy() const {
  return placement_group_table_data_.strategy();
}

const rpc::PlacementGroupTableData &GcsPlacementGroup::GetPlacementGroupTableData() {
  return placement_group_table_data_;
}

std::string GcsPlacementGroup::DebugString() const {
  std::stringstream stream;
  stream << "placement group id = " << GetPlacementGroupID() << ", name = " << GetName()
         << ", strategy = " << GetStrategy();
  return stream.str();
}

rpc::Bundle *GcsPlacementGroup::GetMutableBundle(int bundle_index) {
  return placement_group_table_data_.mutable_bundles(bundle_index);
}

const ActorID GcsPlacementGroup::GetCreatorActorId() const {
  return ActorID::FromBinary(placement_group_table_data_.creator_actor_id());
}

const JobID GcsPlacementGroup::GetCreatorJobId() const {
  return JobID::FromBinary(placement_group_table_data_.creator_job_id());
}

void GcsPlacementGroup::MarkCreatorJobDead() {
  placement_group_table_data_.set_creator_job_dead(true);
}

void GcsPlacementGroup::MarkCreatorActorDead() {
  placement_group_table_data_.set_creator_actor_dead(true);
}

bool GcsPlacementGroup::IsPlacementGroupRemovable() const {
  return placement_group_table_data_.creator_job_dead() &&
         placement_group_table_data_.creator_actor_dead();
}

/////////////////////////////////////////////////////////////////////////////////////////

GcsPlacementGroupManager::GcsPlacementGroupManager(
    boost::asio::io_context &io_context,
    std::shared_ptr<GcsPlacementGroupSchedulerInterface> scheduler,
    std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage,
    GcsNodeManager &gcs_node_manager)
    : io_context_(io_context),
      gcs_placement_group_scheduler_(std::move(scheduler)),
      gcs_table_storage_(std::move(gcs_table_storage)),
      gcs_node_manager_(gcs_node_manager) {
  Tick();
}

void GcsPlacementGroupManager::RegisterPlacementGroup(
    const std::shared_ptr<GcsPlacementGroup> &placement_group, StatusCallback callback) {
  RAY_CHECK(callback);

  // TODO(ffbin): If GCS is restarted, GCS client will repeatedly send
  // `CreatePlacementGroup` requests,
  //  which will lead to resource leakage, we will solve it in next pr.
  // Mark the callback as pending and invoke it after the placement_group has been
  // successfully created.
  placement_group_to_register_callback_[placement_group->GetPlacementGroupID()] =
      std::move(callback);
  registered_placement_groups_.emplace(placement_group->GetPlacementGroupID(),
                                       placement_group);
  pending_placement_groups_.emplace_back(std::move(placement_group));
  SchedulePendingPlacementGroups();
}

PlacementGroupID GcsPlacementGroupManager::GetPlacementGroupIDByName(
    const std::string &name) {
  PlacementGroupID placement_group_id = PlacementGroupID::Nil();
  for (const auto &iter : registered_placement_groups_) {
    if (iter.second->GetName() == name) {
      placement_group_id = iter.first;
      break;
    }
  }
  return placement_group_id;
}

void GcsPlacementGroupManager::OnPlacementGroupCreationFailed(
    std::shared_ptr<GcsPlacementGroup> placement_group) {
  RAY_LOG(INFO) << "Failed to create placement group " << placement_group->GetName()
                << ", id: " << placement_group->GetPlacementGroupID() << ", try again.";
  // We will attempt to schedule this placement_group once an eligible node is
  // registered.
  auto state = placement_group->GetState();
  RAY_CHECK(state == rpc::PlacementGroupTableData::RESCHEDULING ||
            state == rpc::PlacementGroupTableData::PENDING ||
            state == rpc::PlacementGroupTableData::REMOVED)
      << "State: " << state;
  if (state == rpc::PlacementGroupTableData::RESCHEDULING) {
    // NOTE: If a node is dead, the placement group scheduler should try to recover the
    // group by rescheduling the bundles of the dead node. This should have higher
    // priority than trying to place other placement groups.
    pending_placement_groups_.emplace_front(std::move(placement_group));
  } else {
    pending_placement_groups_.emplace_back(std::move(placement_group));
  }

  MarkSchedulingDone();
  RetryCreatingPlacementGroup();
}

void GcsPlacementGroupManager::OnPlacementGroupCreationSuccess(
    const std::shared_ptr<GcsPlacementGroup> &placement_group) {
  RAY_LOG(INFO) << "Successfully created placement group " << placement_group->GetName()
                << ", id: " << placement_group->GetPlacementGroupID();
  placement_group->UpdateState(rpc::PlacementGroupTableData::CREATED);
  auto placement_group_id = placement_group->GetPlacementGroupID();
  RAY_CHECK_OK(gcs_table_storage_->PlacementGroupTable().Put(
      placement_group_id, placement_group->GetPlacementGroupTableData(),
      [this, placement_group_id](Status status) {
        RAY_CHECK_OK(status);

        // Invoke callback for registration request of this placement_group
        // and remove it from placement_group_to_register_callback_.
        auto iter = placement_group_to_register_callback_.find(placement_group_id);
        if (iter != placement_group_to_register_callback_.end()) {
          iter->second(Status::OK());
          placement_group_to_register_callback_.erase(iter);
        }
        MarkSchedulingDone();
        SchedulePendingPlacementGroups();
      }));
}

void GcsPlacementGroupManager::SchedulePendingPlacementGroups() {
  // Update the placement group load to report load information to the autoscaler.
  if (pending_placement_groups_.empty() || IsSchedulingInProgress()) {
    return;
  }
  const auto placement_group = pending_placement_groups_.front();
  const auto &placement_group_id = placement_group->GetPlacementGroupID();
  // Do not reschedule if the placement group has removed already.
  if (registered_placement_groups_.contains(placement_group_id)) {
    MarkSchedulingStarted(placement_group_id);
    gcs_placement_group_scheduler_->ScheduleUnplacedBundles(
        placement_group,
        [this](std::shared_ptr<GcsPlacementGroup> placement_group) {
          OnPlacementGroupCreationFailed(std::move(placement_group));
        },
        [this](std::shared_ptr<GcsPlacementGroup> placement_group) {
          OnPlacementGroupCreationSuccess(std::move(placement_group));
        });
  }
  pending_placement_groups_.pop_front();
}

void GcsPlacementGroupManager::HandleCreatePlacementGroup(
    const ray::rpc::CreatePlacementGroupRequest &request,
    ray::rpc::CreatePlacementGroupReply *reply,
    ray::rpc::SendReplyCallback send_reply_callback) {
  auto placement_group_id =
      PlacementGroupID::FromBinary(request.placement_group_spec().placement_group_id());
  auto placement_group = std::make_shared<GcsPlacementGroup>(request);

  RAY_LOG(INFO) << "Registering placement group, " << placement_group->DebugString();
  // We need this call here because otherwise, if placement group is removed right after
  // here, it can cause inconsistent states.
  registered_placement_groups_.emplace(placement_group_id, placement_group);

  RAY_CHECK_OK(gcs_table_storage_->PlacementGroupTable().Put(
      placement_group_id, placement_group->GetPlacementGroupTableData(),
      [this, request, reply, send_reply_callback, placement_group_id,
       placement_group](Status status) {
        RAY_CHECK_OK(status);
        if (!registered_placement_groups_.contains(placement_group_id)) {
          std::stringstream stream;
          stream << "Placement group of id " << placement_group_id
                 << " has been removed before registration.";
          GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::NotFound(stream.str()));
          return;
        }

        RegisterPlacementGroup(placement_group, [reply, send_reply_callback,
                                                 placement_group](Status status) {
          if (status.ok()) {
            RAY_LOG(INFO) << "Finished registering placement group, "
                          << placement_group->DebugString();
          } else {
            RAY_LOG(INFO) << "Failed to register placement group, "
                          << placement_group->DebugString()
                          << ", cause: " << status.message();
          }
          GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
        });
      }));
}

void GcsPlacementGroupManager::HandleRemovePlacementGroup(
    const rpc::RemovePlacementGroupRequest &request,
    rpc::RemovePlacementGroupReply *reply, rpc::SendReplyCallback send_reply_callback) {
  const auto placement_group_id =
      PlacementGroupID::FromBinary(request.placement_group_id());

  RemovePlacementGroup(placement_group_id, [send_reply_callback, reply,
                                            placement_group_id](Status status) {
    if (status.ok()) {
      RAY_LOG(INFO) << "Placement group of an id, " << placement_group_id
                    << " is removed successfully.";
    }
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
  });
}

void GcsPlacementGroupManager::RemovePlacementGroup(
    const PlacementGroupID &placement_group_id,
    StatusCallback on_placement_group_removed) {
  RAY_CHECK(on_placement_group_removed);
  // If the placement group has been already removed, don't do anything.
  auto placement_group_it = registered_placement_groups_.find(placement_group_id);
  if (placement_group_it == registered_placement_groups_.end()) {
    on_placement_group_removed(Status::OK());
    return;
  }
  auto placement_group = placement_group_it->second;
  registered_placement_groups_.erase(placement_group_it);

  // Destroy all bundles.
  gcs_placement_group_scheduler_->DestroyPlacementGroupBundleResourcesIfExists(
      placement_group_id);
  // Cancel the scheduling request if necessary.
  if (IsSchedulingInProgress(placement_group_id)) {
    // If the placement group is scheduling.
    gcs_placement_group_scheduler_->MarkScheduleCancelled(placement_group_id);
  }

  // Remove a placement group from a pending list if exists.
  auto pending_it = std::find_if(
      pending_placement_groups_.begin(), pending_placement_groups_.end(),
      [placement_group_id](const std::shared_ptr<GcsPlacementGroup> &placement_group) {
        return placement_group->GetPlacementGroupID() == placement_group_id;
      });
  if (pending_it != pending_placement_groups_.end()) {
    // The placement group was pending scheduling, remove it from the queue.
    pending_placement_groups_.erase(pending_it);
  }

  // Flush the status and respond to workers.
  placement_group->UpdateState(rpc::PlacementGroupTableData::REMOVED);
  RAY_CHECK_OK(gcs_table_storage_->PlacementGroupTable().Put(
      placement_group->GetPlacementGroupID(),
      placement_group->GetPlacementGroupTableData(),
      [this, on_placement_group_removed, placement_group_id](Status status) {
        RAY_CHECK_OK(status);
        // If placement group hasn't been created yet, send a response to a core worker
        // that the creation of placement group has failed.
        auto it = placement_group_to_register_callback_.find(placement_group_id);
        if (it != placement_group_to_register_callback_.end()) {
          it->second(
              Status::NotFound("Placement group is removed before it is created."));
          placement_group_to_register_callback_.erase(it);
        }
        on_placement_group_removed(status);
      }));
}

void GcsPlacementGroupManager::HandleGetPlacementGroup(
    const rpc::GetPlacementGroupRequest &request, rpc::GetPlacementGroupReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  PlacementGroupID placement_group_id =
      PlacementGroupID::FromBinary(request.placement_group_id());
  RAY_LOG(DEBUG) << "Getting placement group info, placement group id = "
                 << placement_group_id;

  auto on_done = [placement_group_id, reply, send_reply_callback](
                     const Status &status,
                     const boost::optional<PlacementGroupTableData> &result) {
    if (result) {
      reply->mutable_placement_group_table_data()->CopyFrom(*result);
    }
    RAY_LOG(DEBUG) << "Finished getting placement group info, placement group id = "
                   << placement_group_id;
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
  };

  Status status =
      gcs_table_storage_->PlacementGroupTable().Get(placement_group_id, on_done);
  if (!status.ok()) {
    on_done(status, boost::none);
  }
}

void GcsPlacementGroupManager::RetryCreatingPlacementGroup() {
  execute_after(io_context_, [this] { SchedulePendingPlacementGroups(); },
                RayConfig::instance().gcs_create_placement_group_retry_interval_ms());
}

void GcsPlacementGroupManager::OnNodeDead(const NodeID &node_id) {
  RAY_LOG(INFO) << "Node " << node_id
                << " failed, rescheduling the placement groups on the dead node.";
  auto bundles = gcs_placement_group_scheduler_->GetBundlesOnNode(node_id);
  for (const auto &bundle : bundles) {
    auto iter = registered_placement_groups_.find(bundle.first);
    if (iter != registered_placement_groups_.end()) {
      for (const auto &bundle_index : bundle.second) {
        iter->second->GetMutableBundle(bundle_index)->clear_node_id();
      }
      // TODO(ffbin): If we have a placement group bundle that requires a unique resource
      // (for example gpu resource when there’s only one gpu node), this can postpone
      // creating until a node with the resources is added. we will solve it in next pr.
      if (iter->second->GetState() != rpc::PlacementGroupTableData::RESCHEDULING) {
        iter->second->UpdateState(rpc::PlacementGroupTableData::RESCHEDULING);
        pending_placement_groups_.emplace_front(iter->second);
      }
    }
  }

  SchedulePendingPlacementGroups();
}

void GcsPlacementGroupManager::CleanPlacementGroupIfNeededWhenJobDead(
    const JobID &job_id) {
  for (const auto &it : registered_placement_groups_) {
    auto &placement_group = it.second;
    if (placement_group->GetCreatorJobId() != job_id) {
      continue;
    }
    placement_group->MarkCreatorJobDead();
    if (placement_group->IsPlacementGroupRemovable()) {
      RemovePlacementGroup(placement_group->GetPlacementGroupID(), [](Status status) {});
    }
  }
}

void GcsPlacementGroupManager::CleanPlacementGroupIfNeededWhenActorDead(
    const ActorID &actor_id) {
  for (const auto &it : registered_placement_groups_) {
    auto &placement_group = it.second;
    if (placement_group->GetCreatorActorId() != actor_id) {
      continue;
    }
    placement_group->MarkCreatorActorDead();
    if (placement_group->IsPlacementGroupRemovable()) {
      RemovePlacementGroup(placement_group->GetPlacementGroupID(), [](Status status) {});
    }
  }
}

void GcsPlacementGroupManager::CollectStats() const {
  stats::PendingPlacementGroups.Record(pending_placement_groups_.size());
}

void GcsPlacementGroupManager::Tick() {
  UpdatePlacementGroupLoad();
  execute_after(io_context_, [this] { Tick(); }, 1000 /* milliseconds */);
}

void GcsPlacementGroupManager::UpdatePlacementGroupLoad() {
  std::shared_ptr<rpc::PlacementGroupLoad> placement_group_load =
      std::make_shared<rpc::PlacementGroupLoad>();
  int total_cnt = 0;
  for (const auto &pending_pg_spec : pending_placement_groups_) {
    auto placement_group_data = placement_group_load->add_placement_group_data();
    auto placement_group_table_data = pending_pg_spec->GetPlacementGroupTableData();
    placement_group_data->Swap(&placement_group_table_data);
    total_cnt += 1;
    if (total_cnt >= RayConfig::instance().max_placement_group_load_report_size()) {
      break;
    }
  }
  gcs_node_manager_.UpdatePlacementGroupLoad(move(placement_group_load));
}

}  // namespace gcs
}  // namespace ray
