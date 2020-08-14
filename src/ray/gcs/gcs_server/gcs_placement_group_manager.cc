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
  auto bundles = placement_group_table_data_.bundles();
  std::vector<std::shared_ptr<BundleSpecification>> ret_bundles;
  for (auto &bundle : bundles) {
    ret_bundles.push_back(std::make_shared<BundleSpecification>(bundle));
  }
  return ret_bundles;
}

rpc::PlacementStrategy GcsPlacementGroup::GetStrategy() const {
  return placement_group_table_data_.strategy();
}

const rpc::PlacementGroupTableData &GcsPlacementGroup::GetPlacementGroupTableData() {
  return placement_group_table_data_;
}

const std::string GcsPlacementGroup::DebugString() const {
  std::stringstream stream;
  stream << "placement group id = " << GetPlacementGroupID() << ", name = " << GetName()
         << ", strategy = " << GetStrategy();
  return stream.str();
}

/////////////////////////////////////////////////////////////////////////////////////////

GcsPlacementGroupManager::GcsPlacementGroupManager(
    boost::asio::io_context &io_context,
    std::shared_ptr<GcsPlacementGroupSchedulerInterface> scheduler,
    std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage)
    : io_context_(io_context),
      gcs_placement_group_scheduler_(std::move(scheduler)),
      gcs_table_storage_(std::move(gcs_table_storage)) {}

void GcsPlacementGroupManager::RegisterPlacementGroup(
    const ray::rpc::CreatePlacementGroupRequest &request, StatusCallback callback) {
  RAY_CHECK(callback);
  const auto &placement_group_spec = request.placement_group_spec();
  auto placement_group_id =
      PlacementGroupID::FromBinary(placement_group_spec.placement_group_id());
  auto placement_group = std::make_shared<GcsPlacementGroup>(request);

  // TODO(ffbin): If GCS is restarted, GCS client will repeatedly send
  // `CreatePlacementGroup` requests,
  //  which will lead to resource leakage, we will solve it in next pr.
  // Mark the callback as pending and invoke it after the placement_group has been
  // successfully created.
  placement_group_to_register_callback_[placement_group_id] = std::move(callback);
  registered_placement_groups_.emplace(placement_group_id, placement_group);
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
  RAY_LOG(WARNING) << "Failed to create placement group " << placement_group->GetName()
                   << ", try again.";
  // We will attempt to schedule this placement_group once an eligible node is
  // registered.
  pending_placement_groups_.emplace_back(std::move(placement_group));
  MarkSchedulingDone();
  RetryCreatingPlacementGroup();
}

void GcsPlacementGroupManager::OnPlacementGroupCreationSuccess(
    const std::shared_ptr<GcsPlacementGroup> &placement_group) {
  RAY_LOG(INFO) << "Successfully created placement group " << placement_group->GetName();
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
        auto placement_group_it = registered_placement_groups_.find(placement_group_id);
        // Add an entry to the created map if placement group is not removed at this
        // point.
        if (placement_group_it != registered_placement_groups_.end()) {
          created_placement_group_.emplace(placement_group_id,
                                           placement_group_it->second);
        }
        SchedulePendingPlacementGroups();
      }));
}

void GcsPlacementGroupManager::SchedulePendingPlacementGroups() {
  if (pending_placement_groups_.empty() || IsSchedulingInProgress()) {
    return;
  }
  const auto placement_group = pending_placement_groups_.front();
  // Pending placement group should have been always registered.
  RAY_CHECK(registered_placement_groups_.find(placement_group->GetPlacementGroupID()) !=
            registered_placement_groups_.end());
  MarkSchedulingStarted(placement_group->GetPlacementGroupID());
  gcs_placement_group_scheduler_->Schedule(
      placement_group,
      [this](std::shared_ptr<GcsPlacementGroup> placement_group) {
        OnPlacementGroupCreationFailed(std::move(placement_group));
      },
      [this](std::shared_ptr<GcsPlacementGroup> placement_group) {
        OnPlacementGroupCreationSuccess(std::move(placement_group));
      });
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
        if (registered_placement_groups_.find(placement_group_id) ==
            registered_placement_groups_.end()) {
          std::stringstream stream;
          stream << "Placement group of id " << placement_group_id
                 << " has been removed before registration.";
          GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::NotFound(stream.str()));
          return;
        }

        RegisterPlacementGroup(
            request, [reply, send_reply_callback, placement_group](Status status) {
              if (status.ok()) {
                RAY_LOG(INFO) << "Finished registering placement group, "
                              << placement_group->DebugString();
              } else {
                RAY_LOG(WARNING) << "Failed to register placement group, "
                                 << placement_group->DebugString();
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
    } else {
      RAY_LOG(WARNING) << "Removing a placement group of an id, " << placement_group_id
                       << " failed.";
    }
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
  });
}

void GcsPlacementGroupManager::RemovePlacementGroup(
    const PlacementGroupID &placement_group_id,
    StatusCallback on_placement_group_removed) {
  RAY_CHECK(on_placement_group_removed);
  // If the placement group is not registered, it means it is already removed or wasn't
  // created before. Just fail the request in this case.
  if (registered_placement_groups_.find(placement_group_id) ==
      registered_placement_groups_.end()) {
    RAY_CHECK(registered_placement_groups_.find(placement_group_id) ==
              registered_placement_groups_.end());
    std::stringstream stream;
    stream << "Placement group of id " << placement_group_id
           << " is already removed or doesn't exist";
    on_placement_group_removed(Status::NotFound(stream.str()));
    return;
  }

  auto created_placement_group_it = created_placement_group_.find(placement_group_id);
  if (created_placement_group_it != created_placement_group_.end()) {
    // If the placement group is already created.
    gcs_placement_group_scheduler_->DestroyPlacementGroupBundleResources(
        placement_group_id);
    created_placement_group_.erase(created_placement_group_it);
  } else if (IsSchedulingInProgress(placement_group_id)) {
    // If the placement group is scheduling.
    gcs_placement_group_scheduler_->MarkScheduleCancelled(placement_group_id);
  } else {
    // If placement group is pending
    auto pending_it = std::find_if(
        pending_placement_groups_.begin(), pending_placement_groups_.end(),
        [placement_group_id](const std::shared_ptr<GcsPlacementGroup> &placement_group) {
          return placement_group->GetPlacementGroupID() == placement_group_id;
        });

    if (pending_it != pending_placement_groups_.end()) {
      // The placement group was pending scheduling, remove it from the queue.
      pending_placement_groups_.erase(pending_it);
    }
    // This can happen only when placement group is removed as soon as it is created.
    // TODO(sang): Make the initial registration synchronous, so that we don't need to
    // care about this edge case.
  }

  auto placement_grouop_it = registered_placement_groups_.find(placement_group_id);
  auto placement_group = placement_grouop_it->second;
  registered_placement_groups_.erase(placement_grouop_it);
  // If placement group hasn't been created yet, send a response to a core worker that
  // the creation of placement group has failed.
  auto it = placement_group_to_register_callback_.find(placement_group_id);
  if (it != placement_group_to_register_callback_.end()) {
    std::stringstream stream;
    stream << "Placement group of id " << placement_group_id
           << " is removed before it is created";
    it->second(Status::NotFound(stream.str()));
    placement_group_to_register_callback_.erase(it);
  }

  placement_group->UpdateState(rpc::PlacementGroupTableData::REMOVED);
  RAY_CHECK_OK(gcs_table_storage_->PlacementGroupTable().Put(
      placement_group->GetPlacementGroupID(),
      placement_group->GetPlacementGroupTableData(),
      [on_placement_group_removed](Status status) {
        RAY_CHECK_OK(status);
        on_placement_group_removed(status);
      }));
}

void GcsPlacementGroupManager::RetryCreatingPlacementGroup() {
  execute_after(io_context_, [this] { SchedulePendingPlacementGroups(); },
                RayConfig::instance().gcs_create_placement_group_retry_interval_ms());
}

void GcsPlacementGroupManager::MarkSchedulingStarted(
    const PlacementGroupID placement_group_id) {
  scheduling_progress_.id = placement_group_id;
  scheduling_progress_.is_creating = true;
}

void GcsPlacementGroupManager::MarkSchedulingDone() {
  scheduling_progress_.id = PlacementGroupID::Nil();
  scheduling_progress_.is_creating = false;
}

bool GcsPlacementGroupManager::IsSchedulingInProgress() {
  return scheduling_progress_.is_creating;
}

bool GcsPlacementGroupManager::IsSchedulingInProgress(
    const PlacementGroupID &placement_group_id) {
  return scheduling_progress_.is_creating &&
         scheduling_progress_.id == placement_group_id;
}

}  // namespace gcs
}  // namespace ray
