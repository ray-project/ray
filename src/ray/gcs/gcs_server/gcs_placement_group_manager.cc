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

/////////////////////////////////////////////////////////////////////////////////////////

GcsPlacementGroupManager::GcsPlacementGroupManager(
    boost::asio::io_context &io_context,
    std::shared_ptr<GcsPlacementGroupSchedulerInterface> scheduler,
    std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage)
    : gcs_placement_group_scheduler_(std::move(scheduler)),
      gcs_table_storage_(std::move(gcs_table_storage)),
      reschedule_timer_(io_context) {}

void GcsPlacementGroupManager::RegisterPlacementGroup(
    const ray::rpc::CreatePlacementGroupRequest &request, EmptyCallback callback) {
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
  RAY_LOG(WARNING) << "Failed to create placement group " << placement_group->GetName()
                   << ", try again.";
  // We will attempt to schedule this placement_group once an eligible node is
  // registered.
  pending_placement_groups_.emplace_back(std::move(placement_group));
  is_creating_ = false;
  ScheduleTick();
}

void GcsPlacementGroupManager::OnPlacementGroupCreationSuccess(
    std::shared_ptr<GcsPlacementGroup> placement_group) {
  RAY_LOG(INFO) << "Successfully created placement group " << placement_group->GetName();
  placement_group->UpdateState(rpc::PlacementGroupTableData::ALIVE);
  auto placement_group_id = placement_group->GetPlacementGroupID();
  RAY_CHECK_OK(gcs_table_storage_->PlacementGroupTable().Put(
      placement_group_id, placement_group->GetPlacementGroupTableData(),
      [this, placement_group_id](Status status) {
        RAY_CHECK_OK(status);

        // Invoke callback for registration request of this placement_group
        // and remove it from placement_group_to_register_callback_.
        auto iter = placement_group_to_register_callback_.find(placement_group_id);
        if (iter != placement_group_to_register_callback_.end()) {
          iter->second();
          placement_group_to_register_callback_.erase(iter);
        }
        is_creating_ = false;
        SchedulePendingPlacementGroups();
      }));
}

void GcsPlacementGroupManager::SchedulePendingPlacementGroups() {
  if (pending_placement_groups_.empty() || is_creating_) {
    return;
  }
  is_creating_ = true;
  gcs_placement_group_scheduler_->Schedule(
      pending_placement_groups_.front(),
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
  const auto &name = request.placement_group_spec().name();
  const auto &strategy = request.placement_group_spec().strategy();
  RAY_LOG(INFO) << "Registering placement group, placement group id = "
                << placement_group_id << ", name = " << name
                << ", strategy = " << PlacementStrategy_Name(strategy);
  auto placement_group = std::make_shared<GcsPlacementGroup>(request);
  RAY_CHECK_OK(gcs_table_storage_->PlacementGroupTable().Put(
      placement_group_id, placement_group->GetPlacementGroupTableData(),
      [this, request, reply, send_reply_callback, placement_group_id, name,
       strategy](Status status) {
        RAY_CHECK_OK(status);
        RegisterPlacementGroup(request, [reply, send_reply_callback, placement_group_id,
                                         name, strategy]() {
          RAY_LOG(INFO) << "Finished registering placement group, placement group id = "
                        << placement_group_id << ", name = " << name
                        << ", strategy = " << strategy;
          GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
        });
      }));
}

void GcsPlacementGroupManager::ScheduleTick() {
  reschedule_timer_.expires_from_now(boost::posix_time::milliseconds(500));
  reschedule_timer_.async_wait([this](const boost::system::error_code &error) {
    if (error == boost::asio::error::operation_aborted) {
      return;
    } else {
      SchedulePendingPlacementGroups();
    }
  });
}

}  // namespace gcs
}  // namespace ray
