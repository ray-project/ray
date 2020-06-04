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


#include"ray/gcs/gcs_server/gcs_placement_group_manager.h"
#include <ray/common/ray_config.h>
#include <ray/gcs/pb_util.h>
#include <ray/protobuf/gcs.pb.h>

namespace ray {
namespace gcs {

void GcsPlacementGroup::UpdateState(rpc::PlacementGroupTableData::PlacementGroupState state) {
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

std::vector<BundleSpecification>  GcsPlacementGroup::GetBundles() const {
  auto bundles = placement_group_table_data_.bundles();
  std::vector<BundleSpecification>ret_bundles;
  for(auto iter = bundles.begin(); iter != bundles.end(); iter++){
    ret_bundles.push_back(BundleSpecification(std::move(*iter)));
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

GcsPlacementGroupManager::GcsPlacementGroupManager(boost::asio::io_context &io_context, 
                                 std::shared_ptr<GcsPlacementGroupSchedulerInterface> scheduler,
                                 gcs::PlacementGroupInfoAccessor &placement_group_info_accessor,
                                 std::shared_ptr<gcs::GcsPubSub> gcs_pub_sub)
    : gcs_placement_group_scheduler_(std::move(scheduler)),
      placement_group_info_accessor_(placement_group_info_accessor),
      gcs_pub_sub_(std::move(gcs_pub_sub)),
      reschedule_timer_(io_context) {}

Status GcsPlacementGroupManager::RegisterPlacementGroup(
    const ray::rpc::CreatePlacementGroupRequest &request,
    std::function<void(std::shared_ptr<GcsPlacementGroup>)> callback) {
  RAY_CHECK(callback);
  const auto &placement_group_spec = request.placement_group_spec();
  auto placement_group_id = PlacementGroupID::FromBinary(placement_group_spec.placement_group_id());

  auto iter = registered_placement_groups_.find(placement_group_id);
  if (iter != registered_placement_groups_.end() &&
      iter->second->GetState() == rpc::PlacementGroupTableData::ALIVE) {
    // When the network fails, Driver/Worker is not sure whether GcsServer has received
    // the request of placement_group creation task, so Driver/Worker will try again and again until
    // receiving the reply from GcsServer. If the placement_group has been created successfully then
    // just reply to the caller.
    callback(iter->second);
    return Status::OK();
  }

  auto pending_register_iter = placement_group_to_register_callbacks_.find(placement_group_id);
  if (pending_register_iter != placement_group_to_register_callbacks_.end()) {
    // It is a duplicate message, just mark the callback as pending and invoke it after
    // the placement_group has been successfully created.
    pending_register_iter->second.emplace_back(std::move(callback));
    return Status::OK();
  }

  auto placement_group = std::make_shared<GcsPlacementGroup>(request);

  // Mark the callback as pending and invoke it after the placement_group has been successfully
  // created.
  placement_group_to_register_callbacks_[placement_group_id].emplace_back(std::move(callback));
  RAY_CHECK(registered_placement_groups_.emplace(placement_group->GetPlacementGroupID(), placement_group).second);
  pending_placement_groups_.emplace_back(std::move(placement_group));
  SchedulePendingPlacementGroups();
  return Status::OK();
}

PlacementGroupID GcsPlacementGroupManager::GetPlacementGroupIDByName(const std::string &name) {
  PlacementGroupID placement_group_id = PlacementGroupID::Nil();
  auto it = named_placement_groups_.find(name);
  if (it != named_placement_groups_.end()) {
    placement_group_id = it->second;
  }
  return placement_group_id;
}

void GcsPlacementGroupManager::OnPlacementGroupCreationFailed(std::shared_ptr<GcsPlacementGroup> placement_group) {
  // We will attempt to schedule this placement_group once an eligible node is
  // registered.
  pending_placement_groups_.emplace_back(std::move(placement_group));
  is_creating_ = false;
  ScheduleTick();
}

void GcsPlacementGroupManager::OnPlacementGroupCreationSuccess(std::shared_ptr<GcsPlacementGroup> placement_group) {
  auto placement_group_id = placement_group->GetPlacementGroupID();
  RAY_CHECK(registered_placement_groups_.count(placement_group_id) > 0);
  placement_group->UpdateState(rpc::PlacementGroupTableData::ALIVE);
  
  auto placement_group_table_data =
      std::make_shared<rpc::PlacementGroupTableData>(placement_group->GetPlacementGroupTableData());
  // The backend storage is reliable in the future, so the status must be ok.
  RAY_CHECK_OK(placement_group_info_accessor_.AsyncUpdate(
      placement_group_id, placement_group_table_data, [this, placement_group_id, placement_group_table_data](Status status) {
        RAY_CHECK_OK(gcs_pub_sub_->Publish(ACTOR_CHANNEL, placement_group_id.Hex(),
                                           placement_group_table_data->SerializeAsString(),
                                           nullptr));
      }));

  // Invoke all callbacks for all registration requests of this placement_group (duplicated
  // requests are included) and remove all of them from placement_group_to_register_callbacks_.
  auto iter = placement_group_to_register_callbacks_.find(placement_group_id);
  if (iter != placement_group_to_register_callbacks_.end()) {
    for (auto &callback : iter->second) {
      callback(placement_group);
    }
    placement_group_to_register_callbacks_.erase(iter);
  }
  is_creating_ = false;
  SchedulePendingPlacementGroups();
}

void GcsPlacementGroupManager::SchedulePendingPlacementGroups() {
  if (pending_placement_groups_.empty() || is_creating_) {
    return;
  }
  RAY_LOG(DEBUG) << "Scheduling placement_group, size = " << pending_placement_groups_.size();
  auto placement_groups = std::move(pending_placement_groups_);
  is_creating_ = true;
  gcs_placement_group_scheduler_->Schedule(*placement_groups.begin());
  
}

void GcsPlacementGroupManager::HandleCreatePlacementGroup(
    const ray::rpc::CreatePlacementGroupRequest &request, ray::rpc::CreatePlacementGroupReply *reply,
    ray::rpc::SendReplyCallback send_reply_callback) {
  auto placement_group_id =
      PlacementGroupID::FromBinary(request.placement_group_spec().placement_group_id());

  RAY_LOG(INFO) << "Registering placement group, placement group id = " << placement_group_id;
  Status status = RegisterPlacementGroup(
      request,
      [reply, send_reply_callback, placement_group_id](std::shared_ptr<gcs::GcsPlacementGroup> placement_group) {
        RAY_LOG(INFO) << "Registered placement group, placement group id = " << placement_group_id;
        GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
      });
  if (!status.ok()) {
    RAY_LOG(ERROR) << "Failed to create placement group: " << status.ToString();
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
  }
}

void GcsPlacementGroupManager::ScheduleTick() {
  reschedule_timer_.expires_from_now(boost::posix_time::milliseconds(5));
  reschedule_timer_.async_wait([this](const boost::system::error_code &error) {
    if (error == boost::system::errc::operation_canceled) {
      return;
    } else {
      SchedulePendingPlacementGroups();
    }
  });
}

} //namespace
} //namespace

