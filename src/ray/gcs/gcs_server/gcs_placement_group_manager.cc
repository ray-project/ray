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

#include "ray/common/asio/asio_util.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/ray_config.h"
#include "ray/gcs/pb_util.h"
#include "ray/stats/metric_defs.h"
#include "src/ray/protobuf/gcs.pb.h"

namespace ray {
namespace gcs {

void GcsPlacementGroup::UpdateState(
    rpc::PlacementGroupTableData::PlacementGroupState state) {
  placement_group_table_data_.set_state(state);
  RefreshMetrics();
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

std::string GcsPlacementGroup::GetRayNamespace() const {
  return placement_group_table_data_.ray_namespace();
}

std::vector<std::shared_ptr<const BundleSpecification>> &GcsPlacementGroup::GetBundles()
    const {
  // Fill the cache if it wasn't.
  if (cached_bundle_specs_.empty()) {
    const auto &bundles = placement_group_table_data_.bundles();
    for (const auto &bundle : bundles) {
      cached_bundle_specs_.push_back(std::make_shared<const BundleSpecification>(bundle));
    }
  }
  return cached_bundle_specs_;
}

std::vector<std::shared_ptr<const BundleSpecification>>
GcsPlacementGroup::GetUnplacedBundles() const {
  const auto &bundle_specs = GetBundles();

  std::vector<std::shared_ptr<const BundleSpecification>> unplaced_bundles;
  for (const auto &bundle : bundle_specs) {
    if (bundle->NodeId().IsNil()) {
      unplaced_bundles.push_back(bundle);
    }
  }
  return unplaced_bundles;
}

bool GcsPlacementGroup::HasUnplacedBundles() const {
  return !GetUnplacedBundles().empty();
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
  // Invalidate the cache.
  cached_bundle_specs_.clear();
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

double GcsPlacementGroup::GetMaxCpuFractionPerNode() const {
  return placement_group_table_data_.max_cpu_fraction_per_node();
}

const rpc::PlacementGroupStats &GcsPlacementGroup::GetStats() const {
  return placement_group_table_data_.stats();
}

rpc::PlacementGroupStats *GcsPlacementGroup::GetMutableStats() {
  return placement_group_table_data_.mutable_stats();
}

/////////////////////////////////////////////////////////////////////////////////////////

GcsPlacementGroupManager::GcsPlacementGroupManager(
    instrumented_io_context &io_context,
    std::shared_ptr<GcsPlacementGroupSchedulerInterface> scheduler,
    std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage,
    GcsResourceManager &gcs_resource_manager,
    std::function<std::string(const JobID &)> get_ray_namespace)
    : io_context_(io_context),
      gcs_placement_group_scheduler_(std::move(scheduler)),
      gcs_table_storage_(std::move(gcs_table_storage)),
      gcs_resource_manager_(gcs_resource_manager),
      get_ray_namespace_(get_ray_namespace) {
  placement_group_state_counter_.reset(
      new CounterMap<rpc::PlacementGroupTableData::PlacementGroupState>());
  placement_group_state_counter_->SetOnChangeCallback(
      [this](const rpc::PlacementGroupTableData::PlacementGroupState key) mutable {
        int64_t num_pg = placement_group_state_counter_->Get(key);
        ray::stats::STATS_placement_groups.Record(
            num_pg,
            {{"State", rpc::PlacementGroupTableData::PlacementGroupState_Name(key)},
             {"Source", "gcs"}});
      });
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
        placement_group_to_register_callbacks_.find(placement_group_id);
    if (pending_register_iter != placement_group_to_register_callbacks_.end()) {
      // 1. The GCS client sends the `RegisterPlacementGroup` request to the GCS server.
      // 2. The GCS client receives some network errors.
      // 3. The GCS client resends the `RegisterPlacementGroup` request to the GCS server.
      pending_register_iter->second.emplace_back(std::move(callback));
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
    auto &pgs_in_namespace = named_placement_groups_[placement_group->GetRayNamespace()];
    auto it = pgs_in_namespace.find(placement_group->GetName());
    if (it == pgs_in_namespace.end()) {
      pgs_in_namespace.emplace(placement_group->GetName(),
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

  placement_group_to_register_callbacks_[placement_group->GetPlacementGroupID()]
      .emplace_back(std::move(callback));
  registered_placement_groups_.emplace(placement_group->GetPlacementGroupID(),
                                       placement_group);
  AddToPendingQueue(placement_group);

  RAY_CHECK_OK(gcs_table_storage_->PlacementGroupTable().Put(
      placement_group_id,
      placement_group->GetPlacementGroupTableData(),
      [this, placement_group_id, placement_group](Status status) {
        // The backend storage is supposed to be reliable, so the status must be ok.
        RAY_CHECK_OK(status);
        if (registered_placement_groups_.contains(placement_group_id)) {
          auto iter = placement_group_to_register_callbacks_.find(placement_group_id);
          auto callbacks = std::move(iter->second);
          placement_group_to_register_callbacks_.erase(iter);
          for (const auto &callback : callbacks) {
            callback(status);
          }
          SchedulePendingPlacementGroups();
        } else {
          // The placement group registration is synchronous, so if we found the placement
          // group was deleted here, it must be triggered by the abnormal exit of job,
          // we will return directly in this case.
          RAY_CHECK(placement_group_to_register_callbacks_.count(placement_group_id) == 0)
              << "The placement group has been removed unexpectedly with an unknown "
                 "error. Please file a bug report on here: "
                 "https://github.com/ray-project/ray/issues";
          RAY_LOG(WARNING) << "Failed to create placement group '"
                           << placement_group->GetPlacementGroupID()
                           << "', because the placement group has been removed by GCS.";
          return;
        }
      }));
}

PlacementGroupID GcsPlacementGroupManager::GetPlacementGroupIDByName(
    const std::string &name, const std::string &ray_namespace) {
  PlacementGroupID placement_group_id = PlacementGroupID::Nil();
  auto namespace_it = named_placement_groups_.find(ray_namespace);
  if (namespace_it != named_placement_groups_.end()) {
    auto it = namespace_it->second.find(name);
    if (it != namespace_it->second.end()) {
      placement_group_id = it->second;
    }
  }
  return placement_group_id;
}

void GcsPlacementGroupManager::OnPlacementGroupCreationFailed(
    std::shared_ptr<GcsPlacementGroup> placement_group,
    ExponentialBackOff backoff,
    bool is_feasible) {
  RAY_LOG(DEBUG) << "Failed to create placement group " << placement_group->GetName()
                 << ", id: " << placement_group->GetPlacementGroupID() << ", try again.";

  auto stats = placement_group->GetMutableStats();
  if (!is_feasible) {
    // We will attempt to schedule this placement_group once an eligible node is
    // registered.
    stats->set_scheduling_state(rpc::PlacementGroupStats::INFEASIBLE);
    infeasible_placement_groups_.emplace_back(std::move(placement_group));
  } else {
    auto state = placement_group->GetState();
    RAY_CHECK(state == rpc::PlacementGroupTableData::RESCHEDULING ||
              state == rpc::PlacementGroupTableData::PENDING ||
              state == rpc::PlacementGroupTableData::REMOVED)
        << "State: " << state;

    if (state == rpc::PlacementGroupTableData::RESCHEDULING) {
      // NOTE: If a node is dead, the placement group scheduler should try to recover the
      // group by rescheduling the bundles of the dead node. This should have higher
      // priority than trying to place other placement groups.
      stats->set_scheduling_state(rpc::PlacementGroupStats::FAILED_TO_COMMIT_RESOURCES);
      AddToPendingQueue(std::move(placement_group), /* rank */ 0);
    } else if (state == rpc::PlacementGroupTableData::PENDING) {
      stats->set_scheduling_state(rpc::PlacementGroupStats::NO_RESOURCES);
      AddToPendingQueue(std::move(placement_group), std::nullopt, backoff);
    } else {
      stats->set_scheduling_state(rpc::PlacementGroupStats::REMOVED);
      AddToPendingQueue(std::move(placement_group), std::nullopt, backoff);
    }
  }

  io_context_.post([this] { SchedulePendingPlacementGroups(); },
                   "GcsPlacementGroupManager.SchedulePendingPlacementGroups");
  MarkSchedulingDone();
}

void GcsPlacementGroupManager::OnPlacementGroupCreationSuccess(
    const std::shared_ptr<GcsPlacementGroup> &placement_group) {
  RAY_LOG(INFO) << "Successfully created placement group " << placement_group->GetName()
                << ", id: " << placement_group->GetPlacementGroupID();

  // Setup stats.
  auto stats = placement_group->GetMutableStats();
  auto now = absl::GetCurrentTimeNanos();
  auto scheduling_latency_us =
      absl::Nanoseconds(now - stats->scheduling_started_time_ns()) /
      absl::Microseconds(1);
  auto creation_latency_us =
      absl::Nanoseconds(now - stats->creation_request_received_ns()) /
      absl::Microseconds(1);
  stats->set_scheduling_latency_us(scheduling_latency_us);
  stats->set_end_to_end_creation_latency_us(creation_latency_us);
  ray::stats::STATS_gcs_placement_group_scheduling_latency_ms.Record(
      scheduling_latency_us / 1e3);
  ray::stats::STATS_gcs_placement_group_creation_latency_ms.Record(creation_latency_us /
                                                                   1e3);
  stats->set_scheduling_state(rpc::PlacementGroupStats::FINISHED);

  // Update states and persists the information.
  placement_group->UpdateState(rpc::PlacementGroupTableData::CREATED);
  auto placement_group_id = placement_group->GetPlacementGroupID();
  RAY_CHECK_OK(gcs_table_storage_->PlacementGroupTable().Put(
      placement_group_id,
      placement_group->GetPlacementGroupTableData(),
      [this, placement_group_id](Status status) {
        RAY_CHECK_OK(status);

        if (RescheduleIfStillHasUnplacedBundles(placement_group_id)) {
          // If all the bundles are not created yet, don't complete
          // the creation and invoke a callback.
          // The call back will be called when all bundles are created.
          return;
        }
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
  io_context_.post([this] { SchedulePendingPlacementGroups(); },
                   "GcsPlacementGroupManager.SchedulePendingPlacementGroups");
  MarkSchedulingDone();
}

void GcsPlacementGroupManager::SchedulePendingPlacementGroups() {
  if (pending_placement_groups_.empty()) {
    RAY_LOG(DEBUG) << "No additional placement groups to schedule. Stop scheduling.";
    return;
  }

  if (IsSchedulingInProgress()) {
    RAY_LOG(DEBUG) << "Placement group scheduling is still in progress. New placement "
                      "groups will be scheduled after the current scheduling is done.";
    return;
  }

  bool is_new_placement_group_scheduled = false;
  while (!pending_placement_groups_.empty() && !is_new_placement_group_scheduled) {
    auto iter = pending_placement_groups_.begin();
    if (iter->first > absl::GetCurrentTimeNanos()) {
      // Here the rank equals the time to schedule, and it's an ordered tree,
      // it means all the other tasks should be scheduled after this one.
      // If the first one won't be scheduled, we just skip.
      // Tick will cover the next time retry.
      break;
    }
    auto backoff = iter->second.first;
    auto placement_group = std::move(iter->second.second);
    pending_placement_groups_.erase(iter);

    const auto &placement_group_id = placement_group->GetPlacementGroupID();
    // Do not reschedule if the placement group has removed already.
    if (registered_placement_groups_.contains(placement_group_id)) {
      auto stats = placement_group->GetMutableStats();
      stats->set_scheduling_attempt(stats->scheduling_attempt() + 1);
      stats->set_scheduling_started_time_ns(absl::GetCurrentTimeNanos());
      MarkSchedulingStarted(placement_group_id);
      gcs_placement_group_scheduler_->ScheduleUnplacedBundles(
          placement_group,
          [this, backoff](std::shared_ptr<GcsPlacementGroup> placement_group,
                          bool is_feasible) {
            OnPlacementGroupCreationFailed(
                std::move(placement_group), backoff, is_feasible);
          },
          [this](std::shared_ptr<GcsPlacementGroup> placement_group) {
            OnPlacementGroupCreationSuccess(std::move(placement_group));
          });
      is_new_placement_group_scheduled = true;
    }
    // If the placement group is not registered == removed.
  }
  ++counts_[CountType::SCHEDULING_PENDING_PLACEMENT_GROUP];
}

void GcsPlacementGroupManager::HandleCreatePlacementGroup(
    ray::rpc::CreatePlacementGroupRequest request,
    ray::rpc::CreatePlacementGroupReply *reply,
    ray::rpc::SendReplyCallback send_reply_callback) {
  const JobID &job_id =
      JobID::FromBinary(request.placement_group_spec().creator_job_id());
  auto placement_group = std::make_shared<GcsPlacementGroup>(
      request, get_ray_namespace_(job_id), placement_group_state_counter_);
  RAY_LOG(DEBUG) << "Registering placement group, " << placement_group->DebugString();
  RegisterPlacementGroup(placement_group,
                         [reply, send_reply_callback, placement_group](Status status) {
                           if (status.ok()) {
                             RAY_LOG(DEBUG) << "Finished registering placement group, "
                                            << placement_group->DebugString();
                           } else {
                             RAY_LOG(INFO) << "Failed to register placement group, "
                                           << placement_group->DebugString()
                                           << ", cause: " << status.message();
                           }
                           GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
                         });
  ++counts_[CountType::CREATE_PLACEMENT_GROUP_REQUEST];
}

void GcsPlacementGroupManager::HandleRemovePlacementGroup(
    rpc::RemovePlacementGroupRequest request,
    rpc::RemovePlacementGroupReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  const auto placement_group_id =
      PlacementGroupID::FromBinary(request.placement_group_id());

  RemovePlacementGroup(placement_group_id,
                       [send_reply_callback, reply, placement_group_id](Status status) {
                         if (status.ok()) {
                           RAY_LOG(INFO)
                               << "Placement group of an id, " << placement_group_id
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
  placement_group_to_register_callbacks_.erase(placement_group_id);

  // Remove placement group from `named_placement_groups_` if its name is not empty.
  if (!placement_group->GetName().empty()) {
    auto namespace_it = named_placement_groups_.find(placement_group->GetRayNamespace());
    if (namespace_it != named_placement_groups_.end()) {
      auto it = namespace_it->second.find(placement_group->GetName());
      if (it != namespace_it->second.end() &&
          it->second == placement_group->GetPlacementGroupID()) {
        namespace_it->second.erase(it);
      }
      if (namespace_it->second.empty()) {
        named_placement_groups_.erase(namespace_it);
      }
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
  RemoveFromPendingQueue(placement_group_id);

  // Remove a placement group from infeasible queue if exists.
  auto pending_it = std::find_if(
      infeasible_placement_groups_.begin(),
      infeasible_placement_groups_.end(),
      [placement_group_id](const std::shared_ptr<GcsPlacementGroup> &placement_group) {
        return placement_group->GetPlacementGroupID() == placement_group_id;
      });
  if (pending_it != infeasible_placement_groups_.end()) {
    // The placement group is infeasible now, remove it from the queue.
    infeasible_placement_groups_.erase(pending_it);
  }

  // Flush the status and respond to workers.
  placement_group->UpdateState(rpc::PlacementGroupTableData::REMOVED);
  placement_group->GetMutableStats()->set_scheduling_state(
      rpc::PlacementGroupStats::REMOVED);
  RAY_CHECK_OK(gcs_table_storage_->PlacementGroupTable().Put(
      placement_group->GetPlacementGroupID(),
      placement_group->GetPlacementGroupTableData(),
      [this, on_placement_group_removed, placement_group_id](Status status) {
        RAY_CHECK_OK(status);
        // If there is a driver waiting for the creation done, then send a message that
        // the placement group has been removed.
        auto it = placement_group_to_create_callbacks_.find(placement_group_id);
        if (it != placement_group_to_create_callbacks_.end()) {
          for (auto &callback : it->second) {
            callback(
                Status::NotFound("Placement group is removed before it is created."));
          }
          placement_group_to_create_callbacks_.erase(it);
        }
        on_placement_group_removed(status);
      }));
}

void GcsPlacementGroupManager::HandleGetPlacementGroup(
    rpc::GetPlacementGroupRequest request,
    rpc::GetPlacementGroupReply *reply,
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

  auto it = registered_placement_groups_.find(placement_group_id);
  if (it != registered_placement_groups_.end()) {
    on_done(Status::OK(), it->second->GetPlacementGroupTableData());
  } else {
    Status status =
        gcs_table_storage_->PlacementGroupTable().Get(placement_group_id, on_done);
    if (!status.ok()) {
      on_done(status, boost::none);
    }
  }
  ++counts_[CountType::GET_PLACEMENT_GROUP_REQUEST];
}

void GcsPlacementGroupManager::HandleGetNamedPlacementGroup(
    rpc::GetNamedPlacementGroupRequest request,
    rpc::GetNamedPlacementGroupReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  const std::string &name = request.name();
  RAY_LOG(DEBUG) << "Getting named placement group info, name = " << name;

  // Try to look up the placement Group ID for the named placement group.
  auto placement_group_id = GetPlacementGroupIDByName(name, request.ray_namespace());

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
    rpc::GetAllPlacementGroupRequest request,
    rpc::GetAllPlacementGroupReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  auto limit = request.has_limit() ? request.limit() : -1;

  RAY_LOG(DEBUG) << "Getting all placement group info.";
  auto on_done =
      [this, reply, send_reply_callback, limit](
          const absl::flat_hash_map<PlacementGroupID, PlacementGroupTableData> &result) {
        // Set the total number of pgs.
        auto total_pgs = result.size();
        reply->set_total(total_pgs);

        auto count = 0;
        for (const auto &[placement_group_id, data] : result) {
          if (limit != -1 && count >= limit) {
            break;
          }
          count += 1;

          auto it = registered_placement_groups_.find(placement_group_id);
          // If the pg entry exists in memory just copy from it since
          // it has less stale data. It is useful because we don't
          // persist placement group entry every time we update
          // stats.
          if (it != registered_placement_groups_.end()) {
            reply->add_placement_group_table_data()->CopyFrom(
                it->second->GetPlacementGroupTableData());
          } else {
            reply->add_placement_group_table_data()->CopyFrom(data);
          }
        }

        RAY_LOG(DEBUG) << "Finished getting all placement group info.";
        GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
      };
  Status status = gcs_table_storage_->PlacementGroupTable().GetAll(on_done);
  if (!status.ok()) {
    on_done(absl::flat_hash_map<PlacementGroupID, PlacementGroupTableData>());
  }
  ++counts_[CountType::GET_ALL_PLACEMENT_GROUP_REQUEST];
}

void GcsPlacementGroupManager::HandleWaitPlacementGroupUntilReady(
    rpc::WaitPlacementGroupUntilReadyRequest request,
    rpc::WaitPlacementGroupUntilReadyReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  PlacementGroupID placement_group_id =
      PlacementGroupID::FromBinary(request.placement_group_id());
  RAY_LOG(DEBUG) << "Waiting for placement group until ready, placement group id = "
                 << placement_group_id;

  WaitPlacementGroup(
      placement_group_id,
      [reply, send_reply_callback, placement_group_id](Status status) {
        if (status.ok()) {
          RAY_LOG(DEBUG)
              << "Finished waiting for placement group until ready, placement group id = "
              << placement_group_id;
        } else {
          RAY_LOG(WARNING) << "Failed to waiting for placement group until ready, "
                              "placement group id = "
                           << placement_group_id << ", cause: " << status.message();
        }
        GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
      });

  ++counts_[CountType::WAIT_PLACEMENT_GROUP_UNTIL_READY_REQUEST];
}

void GcsPlacementGroupManager::WaitPlacementGroup(
    const PlacementGroupID &placement_group_id, StatusCallback callback) {
  // If the placement group does not exist or it has been successfully created, return
  // directly.
  const auto &iter = registered_placement_groups_.find(placement_group_id);
  if (iter == registered_placement_groups_.end()) {
    // Check whether the placement group does not exist or is removed.
    auto on_done = [this, placement_group_id, callback](
                       const Status &status,
                       const boost::optional<PlacementGroupTableData> &result) {
      if (result) {
        RAY_LOG(DEBUG) << "Placement group is removed, placement group id = "
                       << placement_group_id;
        callback(Status::NotFound("Placement group is removed."));
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
    callback(Status::OK());
  } else {
    placement_group_to_create_callbacks_[placement_group_id].emplace_back(
        std::move(callback));
  }
}

void GcsPlacementGroupManager::AddToPendingQueue(
    std::shared_ptr<GcsPlacementGroup> pg,
    std::optional<int64_t> rank,
    std::optional<ExponentialBackOff> exp_backer) {
  if (!rank) {
    rank = absl::GetCurrentTimeNanos();
  }

  // Add the biggest delay that has seen so far.
  auto last_delay = 0;
  if (exp_backer) {
    last_delay = exp_backer->Current();
  }
  pg->GetMutableStats()->set_highest_retry_delay_ms(absl::Nanoseconds(last_delay) /
                                                    absl::Milliseconds(1));
  if (!exp_backer) {
    auto initial_delay_ns =
        1000000 *
        RayConfig::instance().gcs_create_placement_group_retry_min_interval_ms();
    exp_backer = ExponentialBackOff(
        initial_delay_ns,
        RayConfig::instance().gcs_create_placement_group_retry_multiplier(),
        1000000 *
            RayConfig::instance().gcs_create_placement_group_retry_max_interval_ms());
  } else {
    *rank += static_cast<int64_t>(exp_backer->Next());
  }
  auto val = std::make_pair(*exp_backer, std::move(pg));
  pending_placement_groups_.emplace(*rank, std::move(val));
}

void GcsPlacementGroupManager::RemoveFromPendingQueue(const PlacementGroupID &pg_id) {
  auto it = std::find_if(pending_placement_groups_.begin(),
                         pending_placement_groups_.end(),
                         [&pg_id](const auto &val) {
                           return val.second.second->GetPlacementGroupID() == pg_id;
                         });
  // The placement group was pending scheduling, remove it from the queue.
  if (it != pending_placement_groups_.end()) {
    pending_placement_groups_.erase(it);
  }
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
        RAY_LOG(INFO) << "Rescheduling a bundle when a node dies, placement group id:"
                      << iter->second->GetPlacementGroupID()
                      << " bundle index:" << bundle_index;
      }
      // TODO(ffbin): If we have a placement group bundle that requires a unique resource
      // (for example gpu resource when there’s only one gpu node), this can postpone
      // creating until a node with the resources is added. we will solve it in next pr.
      if (iter->second->GetState() != rpc::PlacementGroupTableData::RESCHEDULING) {
        iter->second->UpdateState(rpc::PlacementGroupTableData::RESCHEDULING);
        iter->second->GetMutableStats()->set_scheduling_state(
            rpc::PlacementGroupStats::QUEUED);
        AddToPendingQueue(iter->second, 0);
      }
    }
  }

  SchedulePendingPlacementGroups();
}

void GcsPlacementGroupManager::OnNodeAdd(const NodeID &node_id) {
  RAY_LOG(INFO)
      << "A new node: " << node_id
      << " registered, will try to reschedule all the infeasible placement groups.";

  // Move all the infeasible placement groups to the pending queue so that we can
  // reschedule them.
  if (infeasible_placement_groups_.size() > 0) {
    for (auto &pg : infeasible_placement_groups_) {
      AddToPendingQueue(std::move(pg));
    }
    infeasible_placement_groups_.clear();
  }
  SchedulePendingPlacementGroups();
}

void GcsPlacementGroupManager::CleanPlacementGroupIfNeededWhenJobDead(
    const JobID &job_id) {
  std::vector<PlacementGroupID> groups_to_remove;

  for (const auto &it : registered_placement_groups_) {
    auto &placement_group = it.second;
    if (placement_group->GetCreatorJobId() != job_id) {
      continue;
    }
    placement_group->MarkCreatorJobDead();
    if (placement_group->IsPlacementGroupLifetimeDone()) {
      groups_to_remove.push_back(placement_group->GetPlacementGroupID());
    }
  }

  for (const auto &group : groups_to_remove) {
    RemovePlacementGroup(group, [](Status status) {});
  }
}

void GcsPlacementGroupManager::CleanPlacementGroupIfNeededWhenActorDead(
    const ActorID &actor_id) {
  std::vector<PlacementGroupID> groups_to_remove;

  for (const auto &it : registered_placement_groups_) {
    auto &placement_group = it.second;
    if (placement_group->GetCreatorActorId() != actor_id) {
      continue;
    }
    placement_group->MarkCreatorActorDead();
    if (placement_group->IsPlacementGroupLifetimeDone()) {
      groups_to_remove.push_back(placement_group->GetPlacementGroupID());
    }
  }

  for (const auto &group : groups_to_remove) {
    RemovePlacementGroup(group, [](Status status) {});
  }
}

void GcsPlacementGroupManager::Tick() {
  UpdatePlacementGroupLoad();
  // To avoid scheduling exhaution in some race conditions.
  // Note that we don't currently have a known race condition that requires this, but we
  // added as a safety check. https://github.com/ray-project/ray/pull/18419
  SchedulePendingPlacementGroups();
  execute_after(
      io_context_, [this] { Tick(); }, 1000 /* milliseconds */);
}

void GcsPlacementGroupManager::UpdatePlacementGroupLoad() {
  std::shared_ptr<rpc::PlacementGroupLoad> placement_group_load =
      std::make_shared<rpc::PlacementGroupLoad>();
  int total_cnt = 0;
  for (const auto &elem : pending_placement_groups_) {
    const auto pending_pg_spec = elem.second.second;
    auto placement_group_data = placement_group_load->add_placement_group_data();
    auto placement_group_table_data = pending_pg_spec->GetPlacementGroupTableData();
    placement_group_data->Swap(&placement_group_table_data);
    total_cnt += 1;
    if (total_cnt >= RayConfig::instance().max_placement_group_load_report_size()) {
      break;
    }
  }
  // NOTE: Infeasible placement groups also belong to the pending queue when report
  // metrics.
  for (const auto &pending_pg_spec : infeasible_placement_groups_) {
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
  absl::flat_hash_map<NodeID, std::vector<rpc::Bundle>> node_to_bundles;
  absl::flat_hash_map<PlacementGroupID, std::vector<std::shared_ptr<BundleSpecification>>>
      group_to_bundles;
  for (auto &item : gcs_init_data.PlacementGroups()) {
    auto placement_group =
        std::make_shared<GcsPlacementGroup>(item.second, placement_group_state_counter_);
    if (item.second.state() != rpc::PlacementGroupTableData::REMOVED) {
      registered_placement_groups_.emplace(item.first, placement_group);
      if (!placement_group->GetName().empty()) {
        named_placement_groups_[placement_group->GetRayNamespace()].emplace(
            placement_group->GetName(), placement_group->GetPlacementGroupID());
      }

      if (item.second.state() == rpc::PlacementGroupTableData::PENDING ||
          item.second.state() == rpc::PlacementGroupTableData::RESCHEDULING) {
        AddToPendingQueue(std::move(placement_group));
      }

      if (item.second.state() == rpc::PlacementGroupTableData::CREATED ||
          item.second.state() == rpc::PlacementGroupTableData::RESCHEDULING) {
        const auto &bundles = item.second.bundles();
        for (const auto &bundle : bundles) {
          if (!NodeID::FromBinary(bundle.node_id()).IsNil()) {
            node_to_bundles[NodeID::FromBinary(bundle.node_id())].emplace_back(bundle);
            group_to_bundles[PlacementGroupID::FromBinary(
                                 bundle.bundle_id().placement_group_id())]
                .emplace_back(std::make_shared<BundleSpecification>(bundle));
          }
        }
      }
    }
  }

  // Notify raylets to release unused bundles.
  gcs_placement_group_scheduler_->ReleaseUnusedBundles(node_to_bundles);
  gcs_placement_group_scheduler_->Initialize(group_to_bundles);

  SchedulePendingPlacementGroups();
}

std::string GcsPlacementGroupManager::DebugString() const {
  uint64_t named_num_pgs = 0;
  for (auto it : named_placement_groups_) {
    named_num_pgs += it.second.size();
  }
  std::ostringstream stream;
  stream << "GcsPlacementGroupManager: "
         << "\n- CreatePlacementGroup request count: "
         << counts_[CountType::CREATE_PLACEMENT_GROUP_REQUEST]
         << "\n- RemovePlacementGroup request count: "
         << counts_[CountType::REMOVE_PLACEMENT_GROUP_REQUEST]
         << "\n- GetPlacementGroup request count: "
         << counts_[CountType::GET_PLACEMENT_GROUP_REQUEST]
         << "\n- GetAllPlacementGroup request count: "
         << counts_[CountType::GET_ALL_PLACEMENT_GROUP_REQUEST]
         << "\n- WaitPlacementGroupUntilReady request count: "
         << counts_[CountType::WAIT_PLACEMENT_GROUP_UNTIL_READY_REQUEST]
         << "\n- GetNamedPlacementGroup request count: "
         << counts_[CountType::GET_NAMED_PLACEMENT_GROUP_REQUEST]
         << "\n- Scheduling pending placement group count: "
         << counts_[CountType::SCHEDULING_PENDING_PLACEMENT_GROUP]
         << "\n- Registered placement groups count: "
         << registered_placement_groups_.size()
         << "\n- Named placement group count: " << named_num_pgs
         << "\n- Pending placement groups count: " << pending_placement_groups_.size()
         << "\n- Infeasible placement groups count: "
         << infeasible_placement_groups_.size();
  return stream.str();
}

void GcsPlacementGroupManager::RecordMetrics() const {
  ray::stats::STATS_gcs_placement_group_count.Record(pending_placement_groups_.size(),
                                                     "Pending");
  ray::stats::STATS_gcs_placement_group_count.Record(registered_placement_groups_.size(),
                                                     "Registered");
  ray::stats::STATS_gcs_placement_group_count.Record(infeasible_placement_groups_.size(),
                                                     "Infeasible");
  placement_group_state_counter_->FlushOnChangeCallbacks();
}

bool GcsPlacementGroupManager::IsInPendingQueue(
    const PlacementGroupID &placement_group_id) const {
  auto pending_it = std::find_if(pending_placement_groups_.begin(),
                                 pending_placement_groups_.end(),
                                 [&placement_group_id](const auto &val) {
                                   return val.second.second->GetPlacementGroupID() ==
                                          placement_group_id;
                                 });
  return pending_it != pending_placement_groups_.end();
}

bool GcsPlacementGroupManager::RescheduleIfStillHasUnplacedBundles(
    const PlacementGroupID &placement_group_id) {
  auto iter = registered_placement_groups_.find(placement_group_id);
  if (iter != registered_placement_groups_.end()) {
    auto &placement_group = iter->second;
    if (placement_group->HasUnplacedBundles()) {
      if ((!IsInPendingQueue(placement_group->GetPlacementGroupID())) &&
          placement_group->GetState() != rpc::PlacementGroupTableData::REMOVED) {
        RAY_LOG(INFO) << "The placement group still has unplaced bundles, so put "
                         "it to pending queue again, id:"
                      << placement_group->GetPlacementGroupID();
        placement_group->UpdateState(rpc::PlacementGroupTableData::RESCHEDULING);
        AddToPendingQueue(placement_group, 0);
        SchedulePendingPlacementGroups();
        return true;
      }
    }
  }
  return false;
}

}  // namespace gcs
}  // namespace ray
