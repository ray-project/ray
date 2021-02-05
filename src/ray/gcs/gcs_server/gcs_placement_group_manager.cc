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
  for (const auto &bundle : bundles) {
    ret_bundles.push_back(std::make_shared<BundleSpecification>(bundle));
  }
  return ret_bundles;
}

std::vector<std::shared_ptr<BundleSpecification>> GcsPlacementGroup::GetUnplacedBundles()
    const {
  const auto &bundles = placement_group_table_data_.bundles();
  std::vector<std::shared_ptr<BundleSpecification>> unplaced_bundles;
  for (const auto &bundle : bundles) {
    if (NodeID::FromBinary(bundle.node_id()).IsNil()) {
      unplaced_bundles.push_back(std::make_shared<BundleSpecification>(bundle));
    }
  }
  return unplaced_bundles;
}

rpc::PlacementStrategy GcsPlacementGroup::GetStrategy() const {
  return placement_group_table_data_.strategy();
}

const rpc::PlacementGroupTableData &GcsPlacementGroup::GetPlacementGroupTableData()
    const {
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

bool GcsPlacementGroup::IsPlacementGroupLifetimeDone() const {
  return !IsDetached() && placement_group_table_data_.creator_job_dead() &&
         placement_group_table_data_.creator_actor_dead();
}

bool GcsPlacementGroup::IsDetached() const {
  return placement_group_table_data_.is_detached();
}

/////////////////////////////////////////////////////////////////////////////////////////

GcsPlacementGroupManager::GcsPlacementGroupManager(
    boost::asio::io_context &io_context,
    std::shared_ptr<GcsPlacementGroupSchedulerInterface> scheduler,
    std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage,
    GcsResourceManager &gcs_resource_manager)
    : io_context_(io_context),
      gcs_placement_group_scheduler_(std::move(scheduler)),
      gcs_table_storage_(std::move(gcs_table_storage)),
      gcs_resource_manager_(gcs_resource_manager) {
  Tick();
}

void GcsPlacementGroupManager::RegisterPlacementGroup(
    const std::shared_ptr<GcsPlacementGroup> &placement_group, StatusCallback callback) {
  // NOTE: After the abnormal recovery of the network between GCS client and GCS server or
  // the GCS server is restarted, it is required to continue to register placement group
  // successfully.
  RAY_CHECK(callback);
  const auto &placement_group_id = placement_group->GetPlacementGroupID();

  auto iter = registered_placement_groups_.find(placement_group_id);
  if (iter != registered_placement_groups_.end()) {
    auto pending_register_iter =
        placement_group_to_register_callback_.find(placement_group_id);
    if (pending_register_iter != placement_group_to_register_callback_.end()) {
      // 1. The GCS client sends the `RegisterPlacementGroup` request to the GCS server.
      // 2. The GCS client receives some network errors.
      // 3. The GCS client resends the `RegisterPlacementGroup` request to the GCS server.
      pending_register_iter->second = std::move(callback);
    } else {
      // 1. The GCS client sends the `RegisterPlacementGroup` request to the GCS server.
      // 2. The GCS server flushes the placement group to the storage and restarts before
      // replying to the GCS client.
      // 3. The GCS client resends the `RegisterPlacementGroup` request to the GCS server.
      RAY_LOG(INFO) << "Placement group " << placement_group_id
                    << " is already registered.";
      callback(Status::OK());
    }
    return;
  }
  if (!placement_group->GetName().empty()) {
    auto it = named_placement_groups_.find(placement_group->GetName());
    if (it == named_placement_groups_.end()) {
      named_placement_groups_.emplace(placement_group->GetName(),
                                      placement_group->GetPlacementGroupID());
    } else {
      std::stringstream stream;
      stream << "Failed to create placement group '"
             << placement_group->GetPlacementGroupID() << "' because name '"
             << placement_group->GetName() << "' already exists.";
      RAY_LOG(WARNING) << stream.str();
      callback(Status::Invalid(stream.str()));
      return;
    }
  }

  // Mark the callback as pending and invoke it after the placement_group has been
  // successfully created.
  placement_group_to_register_callback_[placement_group->GetPlacementGroupID()] =
      std::move(callback);
  registered_placement_groups_.emplace(placement_group->GetPlacementGroupID(),
                                       placement_group);
  pending_placement_groups_.emplace_back(placement_group);

  RAY_CHECK_OK(gcs_table_storage_->PlacementGroupTable().Put(
      placement_group_id, placement_group->GetPlacementGroupTableData(),
      [this, placement_group_id, placement_group](Status status) {
        RAY_CHECK_OK(status);
        if (!registered_placement_groups_.contains(placement_group_id)) {
          auto iter = placement_group_to_register_callback_.find(placement_group_id);
          if (iter != placement_group_to_register_callback_.end()) {
            std::stringstream stream;
            stream << "Placement group of id " << placement_group_id
                   << " has been removed before registration.";
            iter->second(Status::NotFound(stream.str()));
            placement_group_to_register_callback_.erase(iter);
          }
        } else {
          SchedulePendingPlacementGroups();
        }
      }));
}

PlacementGroupID GcsPlacementGroupManager::GetPlacementGroupIDByName(
    const std::string &name) {
  PlacementGroupID placement_group_id = PlacementGroupID::Nil();
  auto it = named_placement_groups_.find(name);
  if (it != named_placement_groups_.end()) {
    placement_group_id = it->second;
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

        // Invoke all callbacks for all `WaitPlacementGroupUntilReady` requests of this
        // placement group and remove all of them from
        // placement_group_to_create_callbacks_.
        auto pg_to_create_iter =
            placement_group_to_create_callbacks_.find(placement_group_id);
        if (pg_to_create_iter != placement_group_to_create_callbacks_.end()) {
          for (auto &callback : pg_to_create_iter->second) {
            callback(status);
          }
          placement_group_to_create_callbacks_.erase(pg_to_create_iter);
        }
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
  auto placement_group = std::make_shared<GcsPlacementGroup>(request);
  RAY_LOG(INFO) << "Registering placement group, " << placement_group->DebugString();
  RegisterPlacementGroup(placement_group, [reply, send_reply_callback,
                                           placement_group](Status status) {
    if (status.ok()) {
      RAY_LOG(INFO) << "Finished registering placement group, "
                    << placement_group->DebugString();
    } else {
      RAY_LOG(INFO) << "Failed to register placement group, "
                    << placement_group->DebugString() << ", cause: " << status.message();
    }
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
  });
  ++counts_[CountType::CREATE_PLACEMENT_GROUP_REQUEST];
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
  ++counts_[CountType::REMOVE_PLACEMENT_GROUP_REQUEST];
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
  auto placement_group = std::move(placement_group_it->second);
  registered_placement_groups_.erase(placement_group_it);
  placement_group_to_create_callbacks_.erase(placement_group_id);

  // Remove placement group from `named_placement_groups_` if its name is not empty.
  if (!placement_group->GetName().empty()) {
    auto it = named_placement_groups_.find(placement_group->GetName());
    if (it != named_placement_groups_.end() &&
        it->second == placement_group->GetPlacementGroupID()) {
      named_placement_groups_.erase(it);
    }
  }

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
  ++counts_[CountType::GET_PLACEMENT_GROUP_REQUEST];
}

void GcsPlacementGroupManager::HandleGetNamedPlacementGroup(
    const rpc::GetNamedPlacementGroupRequest &request,
    rpc::GetNamedPlacementGroupReply *reply, rpc::SendReplyCallback send_reply_callback) {
  const std::string &name = request.name();
  RAY_LOG(DEBUG) << "Getting named placement group info, name = " << name;

  // Try to look up the placement Group ID for the named placement group.
  auto placement_group_id = GetPlacementGroupIDByName(name);

  if (placement_group_id.IsNil()) {
    // The placement group was not found.
    RAY_LOG(DEBUG) << "Placement Group with name '" << name << "' was not found";
  } else {
    const auto &iter = registered_placement_groups_.find(placement_group_id);
    RAY_CHECK(iter != registered_placement_groups_.end());
    reply->mutable_placement_group_table_data()->CopyFrom(
        iter->second->GetPlacementGroupTableData());
    RAY_LOG(DEBUG) << "Finished get named placement group info, placement group id = "
                   << placement_group_id;
  }
  GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
  ++counts_[CountType::GET_NAMED_PLACEMENT_GROUP_REQUEST];
}

void GcsPlacementGroupManager::HandleGetAllPlacementGroup(
    const rpc::GetAllPlacementGroupRequest &request,
    rpc::GetAllPlacementGroupReply *reply, rpc::SendReplyCallback send_reply_callback) {
  RAY_LOG(DEBUG) << "Getting all placement group info.";
  auto on_done =
      [reply, send_reply_callback](
          const std::unordered_map<PlacementGroupID, PlacementGroupTableData> &result) {
        for (auto &data : result) {
          reply->add_placement_group_table_data()->CopyFrom(data.second);
        }
        RAY_LOG(DEBUG) << "Finished getting all placement group info.";
        GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
      };
  Status status = gcs_table_storage_->PlacementGroupTable().GetAll(on_done);
  if (!status.ok()) {
    on_done(std::unordered_map<PlacementGroupID, PlacementGroupTableData>());
  }
  ++counts_[CountType::GET_ALL_PLACEMENT_GROUP_REQUEST];
}

void GcsPlacementGroupManager::HandleWaitPlacementGroupUntilReady(
    const rpc::WaitPlacementGroupUntilReadyRequest &request,
    rpc::WaitPlacementGroupUntilReadyReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  PlacementGroupID placement_group_id =
      PlacementGroupID::FromBinary(request.placement_group_id());
  RAY_LOG(DEBUG) << "Waiting for placement group until ready, placement group id = "
                 << placement_group_id;

  auto callback = [placement_group_id, reply, send_reply_callback](const Status &status) {
    RAY_LOG(DEBUG)
        << "Finished waiting for placement group until ready, placement group id = "
        << placement_group_id;
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
  };

  // If the placement group does not exist or it has been successfully created, return
  // directly.
  const auto &iter = registered_placement_groups_.find(placement_group_id);
  if (iter == registered_placement_groups_.end()) {
    // Check whether the placement group does not exist or is removed.
    auto on_done = [this, placement_group_id, reply, callback, send_reply_callback](
                       const Status &status,
                       const boost::optional<PlacementGroupTableData> &result) {
      if (result) {
        RAY_LOG(DEBUG) << "Placement group is removed, placement group id = "
                       << placement_group_id;
        GCS_RPC_SEND_REPLY(send_reply_callback, reply,
                           Status::NotFound("Placement group is removed."));
      } else {
        // `wait` is a method of placement group object. Placement group object is
        // obtained by create placement group api, so it can guarantee the existence of
        // placement group.
        // GCS client does not guarantee the order of placement group creation and
        // wait, so GCS may call wait placement group first and then create placement
        // group.
        placement_group_to_create_callbacks_[placement_group_id].emplace_back(
            std::move(callback));
      }
    };

    Status status =
        gcs_table_storage_->PlacementGroupTable().Get(placement_group_id, on_done);
    if (!status.ok()) {
      on_done(status, boost::none);
    }
  } else if (iter->second->GetState() == rpc::PlacementGroupTableData::CREATED) {
    RAY_LOG(DEBUG) << "Placement group is created, placement group id = "
                   << placement_group_id;
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
  } else {
    placement_group_to_create_callbacks_[placement_group_id].emplace_back(
        std::move(callback));
  }

  ++counts_[CountType::WAIT_PLACEMENT_GROUP_UNTIL_READY_REQUEST];
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
    if (placement_group->IsPlacementGroupLifetimeDone()) {
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
    if (placement_group->IsPlacementGroupLifetimeDone()) {
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
  gcs_resource_manager_.UpdatePlacementGroupLoad(move(placement_group_load));
}

void GcsPlacementGroupManager::Initialize(const GcsInitData &gcs_init_data) {
  std::unordered_map<NodeID, std::vector<rpc::Bundle>> node_to_bundles;
  for (auto &item : gcs_init_data.PlacementGroups()) {
    auto placement_group = std::make_shared<GcsPlacementGroup>(item.second);
    if (item.second.state() != rpc::PlacementGroupTableData::REMOVED) {
      registered_placement_groups_.emplace(item.first, placement_group);
      if (!placement_group->GetName().empty()) {
        named_placement_groups_.emplace(placement_group->GetName(),
                                        placement_group->GetPlacementGroupID());
      }

      if (item.second.state() == rpc::PlacementGroupTableData::PENDING ||
          item.second.state() == rpc::PlacementGroupTableData::RESCHEDULING) {
        pending_placement_groups_.emplace_back(std::move(placement_group));
      }

      if (item.second.state() == rpc::PlacementGroupTableData::CREATED ||
          item.second.state() == rpc::PlacementGroupTableData::RESCHEDULING) {
        const auto &bundles = item.second.bundles();
        for (const auto &bundle : bundles) {
          if (!NodeID::FromBinary(bundle.node_id()).IsNil()) {
            node_to_bundles[NodeID::FromBinary(bundle.node_id())].emplace_back(bundle);
          }
        }
      }
    }
  }

  // Notify raylets to release unused bundles.
  gcs_placement_group_scheduler_->ReleaseUnusedBundles(node_to_bundles);

  SchedulePendingPlacementGroups();
}

std::string GcsPlacementGroupManager::DebugString() const {
  std::ostringstream stream;
  stream << "GcsPlacementGroupManager: {CreatePlacementGroup request count: "
         << counts_[CountType::CREATE_PLACEMENT_GROUP_REQUEST]
         << ", RemovePlacementGroup request count: "
         << counts_[CountType::REMOVE_PLACEMENT_GROUP_REQUEST]
         << ", GetPlacementGroup request count: "
         << counts_[CountType::GET_PLACEMENT_GROUP_REQUEST]
         << ", GetAllPlacementGroup request count: "
         << counts_[CountType::GET_ALL_PLACEMENT_GROUP_REQUEST]
         << ", WaitPlacementGroupUntilReady request count: "
         << counts_[CountType::WAIT_PLACEMENT_GROUP_UNTIL_READY_REQUEST]
         << ", Registered placement groups count: " << registered_placement_groups_.size()
         << ", Named placement group count: " << named_placement_groups_.size()
         << ", Pending placement groups count: " << pending_placement_groups_.size()
         << "}";
  return stream.str();
}

}  // namespace gcs
}  // namespace ray
