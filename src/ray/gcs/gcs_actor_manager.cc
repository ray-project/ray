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

#include "ray/gcs/gcs_actor_manager.h"

#include <algorithm>
#include <boost/regex.hpp>
#include <limits>
#include <memory>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "ray/common/protobuf_utils.h"
#include "ray/common/ray_config.h"
#include "ray/common/task/task_spec.h"
#include "ray/stats/metric_defs.h"
#include "ray/util/logging.h"
#include "ray/util/time.h"

namespace {
/// The error message constructed from below methods is user-facing, so please avoid
/// including too much implementation detail or internal information.
void AddActorInfo(const ray::gcs::GcsActor *actor,
                  ray::rpc::ActorDiedErrorContext *mutable_actor_died_error_ctx) {
  if (actor == nullptr) {
    return;
  }
  RAY_CHECK(mutable_actor_died_error_ctx != nullptr);
  mutable_actor_died_error_ctx->set_owner_id(actor->GetOwnerID().Binary());
  mutable_actor_died_error_ctx->set_owner_ip_address(
      actor->GetOwnerAddress().ip_address());
  mutable_actor_died_error_ctx->set_node_ip_address(actor->GetAddress().ip_address());
  mutable_actor_died_error_ctx->set_pid(actor->GetActorTableData().pid());
  mutable_actor_died_error_ctx->set_name(actor->GetName());
  mutable_actor_died_error_ctx->set_ray_namespace(actor->GetRayNamespace());
  mutable_actor_died_error_ctx->set_class_name(actor->GetActorTableData().class_name());
  mutable_actor_died_error_ctx->set_actor_id(actor->GetActorID().Binary());
  const auto actor_state = actor->GetState();
  mutable_actor_died_error_ctx->set_never_started(
      actor_state == ray::rpc::ActorTableData::DEPENDENCIES_UNREADY ||
      actor_state == ray::rpc::ActorTableData::PENDING_CREATION);
}

const ray::rpc::ActorDeathCause GenWorkerDiedCause(
    const ray::gcs::GcsActor *actor,
    const std::string &ip_address,
    const ray::rpc::WorkerExitType &disconnect_type,
    const std::string &disconnect_detail) {
  ray::rpc::ActorDeathCause death_cause;
  if (disconnect_type == ray::rpc::WorkerExitType::NODE_OUT_OF_MEMORY) {
    auto oom_ctx = death_cause.mutable_oom_context();
    /// TODO(clarng): actors typically don't retry. Support actor (task) oom retry
    /// and set this value from raylet here.
    oom_ctx->set_fail_immediately(false);
    oom_ctx->set_error_message(disconnect_detail);
  } else {
    auto actor_died_error_ctx = death_cause.mutable_actor_died_error_context();
    actor_died_error_ctx->set_reason(ray::rpc::ActorDiedErrorContext::WORKER_DIED);
    AddActorInfo(actor, actor_died_error_ctx);
    actor_died_error_ctx->set_error_message(absl::StrCat(
        "The actor is dead because its worker process has died. Worker exit type: ",
        ray::rpc::WorkerExitType_Name(disconnect_type),
        " Worker exit detail: ",
        disconnect_detail));
  }
  return death_cause;
}

const ray::rpc::ActorDeathCause GenOwnerDiedCause(
    const ray::gcs::GcsActor *actor,
    const ray::WorkerID &owner_id,
    const ray::rpc::WorkerExitType disconnect_type,
    const std::string &disconnect_detail,
    const std::string &owner_ip_address) {
  ray::rpc::ActorDeathCause death_cause;
  auto actor_died_error_ctx = death_cause.mutable_actor_died_error_context();
  actor_died_error_ctx->set_reason(ray::rpc::ActorDiedErrorContext::OWNER_DIED);
  AddActorInfo(actor, actor_died_error_ctx);
  actor_died_error_ctx->set_error_message(
      absl::StrCat("The actor is dead because its owner has died. Owner Id: ",
                   owner_id.Hex(),
                   " Owner Ip address: ",
                   owner_ip_address,
                   " Owner worker exit type: ",
                   ray::rpc::WorkerExitType_Name(disconnect_type),
                   " Worker exit detail: ",
                   disconnect_detail));
  return death_cause;
}

const ray::rpc::ActorDeathCause GenKilledByApplicationCause(
    const ray::gcs::GcsActor *actor) {
  ray::rpc::ActorDeathCause death_cause;
  auto actor_died_error_ctx = death_cause.mutable_actor_died_error_context();
  actor_died_error_ctx->set_reason(ray::rpc::ActorDiedErrorContext::RAY_KILL);
  AddActorInfo(actor, actor_died_error_ctx);
  actor_died_error_ctx->set_error_message(
      "The actor is dead because it was killed by `ray.kill`.");
  return death_cause;
}

const ray::rpc::ActorDeathCause GenActorOutOfScopeCause(const ray::gcs::GcsActor *actor) {
  ray::rpc::ActorDeathCause death_cause;
  auto actor_died_error_ctx = death_cause.mutable_actor_died_error_context();
  actor_died_error_ctx->set_reason(ray::rpc::ActorDiedErrorContext::OUT_OF_SCOPE);
  AddActorInfo(actor, actor_died_error_ctx);
  actor_died_error_ctx->set_error_message(
      "The actor is dead because all references to the actor were removed.");
  return death_cause;
}

const ray::rpc::ActorDeathCause GenActorRefDeletedCause(const ray::gcs::GcsActor *actor) {
  ray::rpc::ActorDeathCause death_cause;
  auto actor_died_error_ctx = death_cause.mutable_actor_died_error_context();
  actor_died_error_ctx->set_reason(ray::rpc::ActorDiedErrorContext::REF_DELETED);
  AddActorInfo(actor, actor_died_error_ctx);
  actor_died_error_ctx->set_error_message(
      "The actor is dead because all references to the actor were removed including "
      "lineage ref count.");
  return death_cause;
}

// Returns true if an actor should be loaded to registered_actors_.
// `false` Cases:
// 0. state is DEAD, and is not restartable
// 1. root owner is job, and job is dead
// 2. root owner is another detached actor, and that actor is dead
bool OnInitializeActorShouldLoad(const ray::gcs::GcsInitData &gcs_init_data,
                                 ray::ActorID actor_id) {
  const auto &jobs = gcs_init_data.Jobs();
  const auto &actors = gcs_init_data.Actors();
  const auto &actor_task_specs = gcs_init_data.ActorTaskSpecs();

  const auto &actor_table_data = actors.find(actor_id);
  if (actor_table_data == actors.end()) {
    return false;
  }
  if (actor_table_data->second.state() == ray::rpc::ActorTableData::DEAD &&
      !ray::gcs::IsActorRestartable(actor_table_data->second)) {
    return false;
  }

  const auto &actor_task_spec = ray::map_find_or_die(actor_task_specs, actor_id);
  ray::ActorID root_detached_actor_id =
      ray::TaskSpecification(actor_task_spec).RootDetachedActorId();
  if (root_detached_actor_id.IsNil()) {
    // owner is job, NOT detached actor, should die with job
    auto job_iter = jobs.find(actor_id.JobId());
    return job_iter != jobs.end() && !job_iter->second.is_dead();
  } else if (actor_id == root_detached_actor_id) {
    // owner is itself, just live on
    return true;
  } else {
    // owner is another detached actor, should die with the owner actor
    // Root detached actor can be dead only if state() == DEAD.
    auto root_detached_actor_iter = actors.find(root_detached_actor_id);
    return root_detached_actor_iter != actors.end() &&
           root_detached_actor_iter->second.state() != ray::rpc::ActorTableData::DEAD;
  }
};

}  // namespace

namespace ray {
namespace gcs {

bool is_uuid(const std::string &str) {
  static const boost::regex e(
      "[a-f0-9]{8}-[a-f0-9]{4}-4[a-f0-9]{3}-[89aAbB][a-f0-9]{3}-[a-f0-9]{12}");
  return regex_match(str, e);  // note: case-sensitive now
}

const ray::rpc::ActorDeathCause GcsActorManager::GenNodeDiedCause(
    const ray::gcs::GcsActor *actor, std::shared_ptr<const rpc::GcsNodeInfo> node) {
  ray::rpc::ActorDeathCause death_cause;

  auto actor_died_error_ctx = death_cause.mutable_actor_died_error_context();
  actor_died_error_ctx->set_reason(ray::rpc::ActorDiedErrorContext::NODE_DIED);
  AddActorInfo(actor, actor_died_error_ctx);
  auto node_death_info = actor_died_error_ctx->mutable_node_death_info();
  node_death_info->CopyFrom(node->death_info());

  std::ostringstream oss;
  oss << "The actor died because its node has died. Node Id: "
      << NodeID::FromBinary(node->node_id()).Hex() << "\n";
  switch (node_death_info->reason()) {
  case rpc::NodeDeathInfo::EXPECTED_TERMINATION:
    oss << "\tthe actor's node was terminated expectedly: ";
    break;
  case rpc::NodeDeathInfo::UNEXPECTED_TERMINATION:
    oss << "\tthe actor's node was terminated unexpectedly: ";
    break;
  case rpc::NodeDeathInfo::AUTOSCALER_DRAIN_PREEMPTED:
    oss << "\tthe actor's node was preempted: ";
    break;
  default:
    // Control should not reach here, but in case it happens in unexpected scenarios,
    // log it and provide a generic message to the user.
    RAY_LOG(ERROR) << "Actor death is not expected to be caused by "
                   << rpc::NodeDeathInfo_Reason_Name(node_death_info->reason());
    oss << "\tthe actor's node was terminated: ";
  }
  oss << node_death_info->reason_message();
  actor_died_error_ctx->set_error_message(oss.str());

  return death_cause;
}

GcsActorManager::GcsActorManager(
    std::unique_ptr<GcsActorSchedulerInterface> scheduler,
    GcsTableStorage *gcs_table_storage,
    instrumented_io_context &io_context,
    pubsub::GcsPublisher *gcs_publisher,
    RuntimeEnvManager &runtime_env_manager,
    GCSFunctionManager &function_manager,
    std::function<void(const ActorID &)> destroy_owned_placement_group_if_needed,
    rpc::RayletClientPool &raylet_client_pool,
    rpc::CoreWorkerClientPool &worker_client_pool,
    observability::RayEventRecorderInterface &ray_event_recorder,
    const std::string &session_name,
    ray::observability::MetricInterface &actor_by_state_gauge,
    ray::observability::MetricInterface &gcs_actor_by_state_gauge)
    : gcs_actor_scheduler_(std::move(scheduler)),
      gcs_table_storage_(gcs_table_storage),
      io_context_(io_context),
      gcs_publisher_(gcs_publisher),
      raylet_client_pool_(raylet_client_pool),
      worker_client_pool_(worker_client_pool),
      ray_event_recorder_(ray_event_recorder),
      session_name_(session_name),
      destroy_owned_placement_group_if_needed_(
          std::move(destroy_owned_placement_group_if_needed)),
      runtime_env_manager_(runtime_env_manager),
      function_manager_(function_manager),
      usage_stats_client_(nullptr),
      actor_gc_delay_(RayConfig::instance().gcs_actor_table_min_duration_ms()),
      actor_by_state_gauge_(actor_by_state_gauge),
      gcs_actor_by_state_gauge_(gcs_actor_by_state_gauge) {
  RAY_CHECK(destroy_owned_placement_group_if_needed_);
  actor_state_counter_ = std::make_shared<
      CounterMap<std::pair<rpc::ActorTableData::ActorState, std::string>>>();
  actor_state_counter_->SetOnChangeCallback(
      [this](const std::pair<rpc::ActorTableData::ActorState, std::string> key) mutable {
        int64_t num_actors = actor_state_counter_->Get(key);
        actor_by_state_gauge_.Record(
            num_actors,
            {{"State", rpc::ActorTableData::ActorState_Name(key.first)},
             {"Name", key.second},
             {"Source", "gcs"},
             {"JobId", ""}});
      });
}

void GcsActorManager::HandleReportActorOutOfScope(
    rpc::ReportActorOutOfScopeRequest request,
    rpc::ReportActorOutOfScopeReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  auto actor_id = ActorID::FromBinary(request.actor_id());
  auto it = registered_actors_.find(actor_id);
  if (it != registered_actors_.end()) {
    auto actor = GetActor(actor_id);
    if (actor->GetActorTableData().num_restarts_due_to_lineage_reconstruction() >
        request.num_restarts_due_to_lineage_reconstruction()) {
      // Stale report
      RAY_LOG(INFO).WithField(actor_id)
          << "The out of scope report is stale, the actor has been restarted.";
      GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
      return;
    }

    DestroyActor(actor_id,
                 GenActorOutOfScopeCause(actor),
                 /*force_kill=*/true,
                 [reply, send_reply_callback]() {
                   GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
                 });
  } else {
    RAY_LOG(INFO).WithField(actor_id) << "The out of scope actor is already dead";
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
  }
}

void GcsActorManager::HandleRegisterActor(rpc::RegisterActorRequest request,
                                          rpc::RegisterActorReply *reply,
                                          rpc::SendReplyCallback send_reply_callback) {
  RAY_CHECK(request.task_spec().type() == TaskType::ACTOR_CREATION_TASK);
  auto actor_id =
      ActorID::FromBinary(request.task_spec().actor_creation_task_spec().actor_id());

  RAY_LOG(INFO).WithField(actor_id.JobId()).WithField(actor_id) << "Registering actor";
  Status status = RegisterActor(
      request, [reply, send_reply_callback, actor_id](const Status &register_status) {
        if (register_status.ok()) {
          RAY_LOG(INFO).WithField(actor_id.JobId()).WithField(actor_id)
              << "Registered actor";
        } else {
          RAY_LOG(WARNING).WithField(actor_id.JobId()).WithField(actor_id)
              << "Failed to register actor: " << register_status.ToString();
        }
        GCS_RPC_SEND_REPLY(send_reply_callback, reply, register_status);
      });
  if (!status.ok()) {
    RAY_LOG(WARNING).WithField(actor_id.JobId()).WithField(actor_id)
        << "Failed to register actor: " << status.ToString();
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
  }
  ++counts_[CountType::REGISTER_ACTOR_REQUEST];
}

void GcsActorManager::HandleRestartActorForLineageReconstruction(
    rpc::RestartActorForLineageReconstructionRequest request,
    rpc::RestartActorForLineageReconstructionReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  auto actor_id = ActorID::FromBinary(request.actor_id());
  RAY_LOG(INFO).WithField(actor_id.JobId()).WithField(actor_id)
      << "HandleRestartActorForLineageReconstruction";
  auto iter = registered_actors_.find(actor_id);
  if (iter == registered_actors_.end()) {
    GCS_RPC_SEND_REPLY(
        send_reply_callback, reply, Status::Invalid("Actor is permanently dead."));
    return;
  }

  auto success_callback = [reply, send_reply_callback, actor_id](
                              const std::shared_ptr<gcs::GcsActor> &actor) {
    RAY_LOG(INFO).WithField(actor_id.JobId()).WithField(actor_id) << "Restarted actor";
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
  };
  auto pending_restart_iter =
      actor_to_restart_for_lineage_reconstruction_callbacks_.find(actor_id);
  if (pending_restart_iter !=
      actor_to_restart_for_lineage_reconstruction_callbacks_.end()) {
    // This happens when the RestartActorForLineageReconstruction rpc is received and
    // being handled and then the connection is lost and the caller resends the same
    // request.
    pending_restart_iter->second.emplace_back(std::move(success_callback));
    return;
  }

  auto actor = iter->second;
  if (request.num_restarts_due_to_lineage_reconstruction() <=
      actor->GetActorTableData().num_restarts_due_to_lineage_reconstruction()) {
    // This is a stale message. This can happen when the
    // RestartActorForLineageReconstruction rpc is replied but the reply is lost, and the
    // caller resends the same request.
    RAY_LOG(INFO).WithField(actor_id)
        << "Ignore stale actor restart request. Current "
           "num_restarts_due_to_lineage_reconstruction: "
        << actor->GetActorTableData().num_restarts_due_to_lineage_reconstruction()
        << ", target num_restarts_due_to_lineage_reconstruction: "
        << request.num_restarts_due_to_lineage_reconstruction();
    success_callback(actor);
    return;
  }
  RAY_CHECK_EQ(
      request.num_restarts_due_to_lineage_reconstruction(),
      actor->GetActorTableData().num_restarts_due_to_lineage_reconstruction() + 1)
      << "Current num_restarts_due_to_lineage_reconstruction: "
      << actor->GetActorTableData().num_restarts_due_to_lineage_reconstruction()
      << ", target num_restarts_due_to_lineage_reconstruction: "
      << request.num_restarts_due_to_lineage_reconstruction();
  RAY_CHECK_EQ(actor->GetState(), rpc::ActorTableData::DEAD)
      << rpc::ActorTableData::ActorState_Name(actor->GetState());
  RAY_CHECK(IsActorRestartable(actor->GetActorTableData()));

  actor_to_restart_for_lineage_reconstruction_callbacks_[actor_id].emplace_back(
      std::move(success_callback));
  actor->GetMutableActorTableData()->set_num_restarts_due_to_lineage_reconstruction(
      actor->GetActorTableData().num_restarts_due_to_lineage_reconstruction() + 1);
  RestartActor(
      actor_id,
      /*need_reschedule=*/true,
      GenActorOutOfScopeCause(actor.get()),
      [this, actor]() {
        // If a creator dies before this callback is called, the actor could have
        // been already destroyed. It is okay not to invoke a callback because we
        // don't need to reply to the creator as it is already dead.
        auto registered_actor_it = registered_actors_.find(actor->GetActorID());
        if (registered_actor_it == registered_actors_.end()) {
          // NOTE(sang): This logic assumes that the ordering of backend call is
          // guaranteed. It is currently true because we use a single TCP socket
          // to call the default Redis backend. If ordering is not guaranteed, we
          // should overwrite the actor state to DEAD to avoid race condition.
          return;
        }
        auto restart_callback_iter =
            actor_to_restart_for_lineage_reconstruction_callbacks_.find(
                actor->GetActorID());
        RAY_CHECK(restart_callback_iter !=
                      actor_to_restart_for_lineage_reconstruction_callbacks_.end() &&
                  !restart_callback_iter->second.empty());
        auto callbacks = std::move(restart_callback_iter->second);
        actor_to_restart_for_lineage_reconstruction_callbacks_.erase(
            restart_callback_iter);
        for (auto &callback : callbacks) {
          callback(actor);
        }
      });
}

void GcsActorManager::HandleCreateActor(rpc::CreateActorRequest request,
                                        rpc::CreateActorReply *reply,
                                        rpc::SendReplyCallback send_reply_callback) {
  RAY_CHECK(request.task_spec().type() == TaskType::ACTOR_CREATION_TASK);
  auto actor_id =
      ActorID::FromBinary(request.task_spec().actor_creation_task_spec().actor_id());

  RAY_LOG(INFO).WithField(actor_id.JobId()).WithField(actor_id) << "Creating actor";
  Status status = CreateActor(
      request,
      [reply, send_reply_callback, actor_id](const std::shared_ptr<gcs::GcsActor> &actor,
                                             const rpc::PushTaskReply &task_reply,
                                             const Status &creation_task_status) {
        if (creation_task_status.IsSchedulingCancelled()) {
          // Actor creation is cancelled.
          reply->mutable_death_cause()->CopyFrom(
              actor->GetActorTableData().death_cause());
        } else {
          reply->mutable_actor_address()->CopyFrom(actor->GetAddress());
          reply->mutable_borrowed_refs()->CopyFrom(task_reply.borrowed_refs());
        }

        RAY_LOG(INFO).WithField(actor_id.JobId()).WithField(actor_id)
            << "Finished creating actor. Status: " << creation_task_status;
        GCS_RPC_SEND_REPLY(send_reply_callback, reply, creation_task_status);
      });
  if (!status.ok()) {
    RAY_LOG(WARNING).WithField(actor_id.JobId()).WithField(actor_id)
        << "Failed to create actor. Status: " << status.ToString();
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
  }
  ++counts_[CountType::CREATE_ACTOR_REQUEST];
}

void GcsActorManager::HandleGetActorInfo(rpc::GetActorInfoRequest request,
                                         rpc::GetActorInfoReply *reply,
                                         rpc::SendReplyCallback send_reply_callback) {
  ActorID actor_id = ActorID::FromBinary(request.actor_id());
  RAY_LOG(DEBUG).WithField(actor_id.JobId()).WithField(actor_id) << "Getting actor info";

  const auto &registered_actor_iter = registered_actors_.find(actor_id);
  GcsActor *ptr = nullptr;
  if (registered_actor_iter != registered_actors_.end()) {
    ptr = registered_actor_iter->second.get();
  } else {
    const auto &destroyed_actor_iter = destroyed_actors_.find(actor_id);
    if (destroyed_actor_iter != destroyed_actors_.end()) {
      ptr = destroyed_actor_iter->second.get();
    }
  }

  if (ptr != nullptr) {
    *reply->mutable_actor_table_data() = ptr->GetActorTableData();
  }

  RAY_LOG(DEBUG).WithField(actor_id.JobId()).WithField(actor_id)
      << "Finished getting actor info";
  GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
  ++counts_[CountType::GET_ACTOR_INFO_REQUEST];
}

void GcsActorManager::HandleGetAllActorInfo(rpc::GetAllActorInfoRequest request,
                                            rpc::GetAllActorInfoReply *reply,
                                            rpc::SendReplyCallback send_reply_callback) {
  size_t limit =
      (request.limit() > 0) ? request.limit() : std::numeric_limits<size_t>::max();
  RAY_LOG(DEBUG) << "Getting all actor info.";
  ++counts_[CountType::GET_ALL_ACTOR_INFO_REQUEST];

  const auto filter_fn = [](const rpc::GetAllActorInfoRequest::Filters &filters,
                            const rpc::ActorTableData &data) {
    if (filters.has_actor_id() &&
        ActorID::FromBinary(filters.actor_id()) != ActorID::FromBinary(data.actor_id())) {
      return false;
    }
    if (filters.has_job_id() &&
        JobID::FromBinary(filters.job_id()) != JobID::FromBinary(data.job_id())) {
      return false;
    }
    if (filters.has_state() && filters.state() != data.state()) {
      return false;
    }
    return true;
  };

  if (request.show_dead_jobs() == false) {
    size_t total_actors = registered_actors_.size() + destroyed_actors_.size();
    reply->set_total(total_actors);

    size_t count = 0;
    size_t num_filtered = 0;
    for (const auto &iter : registered_actors_) {
      if (count >= limit) {
        break;
      }

      // With filters, skip the actor if it doesn't match the filter.
      if (request.has_filters() &&
          !filter_fn(request.filters(), iter.second->GetActorTableData())) {
        ++num_filtered;
        continue;
      }

      count += 1;
      *reply->add_actor_table_data() = iter.second->GetActorTableData();
    }

    for (const auto &iter : destroyed_actors_) {
      if (count >= limit) {
        break;
      }
      // With filters, skip the actor if it doesn't match the filter.
      if (request.has_filters() &&
          !filter_fn(request.filters(), iter.second->GetActorTableData())) {
        ++num_filtered;
        continue;
      }

      count += 1;
      *reply->add_actor_table_data() = iter.second->GetActorTableData();
    }
    reply->set_num_filtered(num_filtered);
    RAY_LOG(DEBUG) << "Finished getting all actor info.";
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
    return;
  }

  RAY_CHECK(request.show_dead_jobs());
  // We don't maintain an in-memory cache of all actors which belong to dead
  // jobs, so fetch it from redis.
  gcs_table_storage_->ActorTable().GetAll(
      {[reply, send_reply_callback, limit, request = std::move(request), filter_fn](
           absl::flat_hash_map<ActorID, rpc::ActorTableData> &&result) {
         auto total_actors = result.size();

         reply->set_total(total_actors);
         auto arena = reply->GetArena();
         RAY_CHECK(arena != nullptr);
         auto ptr = google::protobuf::Arena::Create<
             absl::flat_hash_map<ActorID, rpc::ActorTableData>>(arena, std::move(result));
         size_t count = 0;
         size_t num_filtered = 0;
         for (auto &pair : *ptr) {
           if (count >= limit) {
             break;
           }
           // With filters, skip the actor if it doesn't match the filter.
           if (request.has_filters() && !filter_fn(request.filters(), pair.second)) {
             ++num_filtered;
             continue;
           }
           count += 1;

           reply->mutable_actor_table_data()->UnsafeArenaAddAllocated(&pair.second);
         }
         reply->set_num_filtered(num_filtered);
         GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
         RAY_LOG(DEBUG) << "Finished getting all actor info.";
       },
       io_context_});
}

void GcsActorManager::HandleGetNamedActorInfo(
    rpc::GetNamedActorInfoRequest request,
    rpc::GetNamedActorInfoReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  const std::string &name = request.name();
  const std::string &ray_namespace = request.ray_namespace();
  RAY_LOG(DEBUG) << "Getting actor info, name = " << name
                 << " , namespace = " << ray_namespace;

  // Try to look up the actor ID for the named actor.
  ActorID actor_id = GetActorIDByName(name, ray_namespace);

  Status status = Status::OK();
  auto iter = registered_actors_.find(actor_id);
  if (actor_id.IsNil() || iter == registered_actors_.end()) {
    // The named actor was not found or the actor is already removed.
    std::stringstream stream;
    stream << "Actor with name '" << name << "' was not found.";
    RAY_LOG(DEBUG) << stream.str();
    status = Status::NotFound(stream.str());
  } else {
    *reply->mutable_actor_table_data() = iter->second->GetActorTableData();
    *reply->mutable_task_spec() = *iter->second->GetMutableTaskSpec();
    RAY_LOG(DEBUG).WithField(actor_id.JobId()).WithField(actor_id)
        << "Finished getting actor info";
  }
  GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
  ++counts_[CountType::GET_NAMED_ACTOR_INFO_REQUEST];
}

void GcsActorManager::HandleListNamedActors(rpc::ListNamedActorsRequest request,
                                            rpc::ListNamedActorsReply *reply,
                                            rpc::SendReplyCallback send_reply_callback) {
  const std::string &ray_namespace = request.ray_namespace();
  RAY_LOG(DEBUG) << "Getting named actor names, namespace = " << ray_namespace;

  std::vector<std::pair<std::string, std::string>> actors =
      ListNamedActors(request.all_namespaces(), ray_namespace);
  for (const auto &actor : actors) {
    auto named_actor_indo = reply->add_named_actors_list();
    named_actor_indo->set_ray_namespace(actor.first);
    named_actor_indo->set_name(actor.second);
  }
  GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
  ++counts_[CountType::LIST_NAMED_ACTORS_REQUEST];
}

void GcsActorManager::HandleKillActorViaGcs(rpc::KillActorViaGcsRequest request,
                                            rpc::KillActorViaGcsReply *reply,
                                            rpc::SendReplyCallback send_reply_callback) {
  const auto &actor_id = ActorID::FromBinary(request.actor_id());
  auto it = registered_actors_.find(actor_id);
  if (it != registered_actors_.end()) {
    bool force_kill = request.force_kill();
    bool no_restart = request.no_restart();
    if (no_restart) {
      DestroyActor(actor_id, GenKilledByApplicationCause(GetActor(actor_id)));
    } else {
      KillActor(actor_id, force_kill);
    }

    GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
    RAY_LOG(DEBUG).WithField(actor_id.JobId()).WithField(actor_id)
        << "Finished killing actor, force_kill = " << force_kill
        << ", no_restart = " << no_restart;
  } else {
    GCS_RPC_SEND_REPLY(send_reply_callback,
                       reply,
                       Status::NotFound(absl::StrFormat(
                           "Could not find actor with ID %s.", actor_id.Hex())));
    RAY_LOG(DEBUG).WithField(actor_id.JobId()).WithField(actor_id)
        << "Could not find actor with ID " << actor_id.Hex();
  }
  ++counts_[CountType::KILL_ACTOR_REQUEST];
}

Status GcsActorManager::RegisterActor(const ray::rpc::RegisterActorRequest &request,
                                      std::function<void(Status)> register_callback) {
  RAY_CHECK(thread_checker_.IsOnSameThread());
  RAY_CHECK(register_callback);
  const auto &actor_creation_task_spec = request.task_spec().actor_creation_task_spec();
  auto actor_id = ActorID::FromBinary(actor_creation_task_spec.actor_id());

  auto iter = registered_actors_.find(actor_id);
  if (iter != registered_actors_.end()) {
    auto pending_register_iter = actor_to_register_callbacks_.find(actor_id);
    if (pending_register_iter != actor_to_register_callbacks_.end()) {
      // 1. The GCS client sends the `RegisterActor` request to the GCS server.
      // 2. The GCS client receives some network errors.
      // 3. The GCS client resends the `RegisterActor` request to the GCS server.
      pending_register_iter->second.emplace_back(std::move(register_callback));
    } else {
      // 1. The GCS client sends the `RegisterActor` request to the GCS server.
      // 2. The GCS server flushes the actor to the storage and restarts before replying
      // to the GCS client.
      // 3. The GCS client resends the `RegisterActor` request to the GCS server.
      register_callback(Status::OK());
    }
    return Status::OK();
  }

  const auto job_id = JobID::FromBinary(request.task_spec().job_id());

  // Use the namespace in task options by default. Otherwise use the
  // namespace from the job.
  std::string ray_namespace = actor_creation_task_spec.ray_namespace();
  RAY_CHECK(!ray_namespace.empty())
      << "`ray_namespace` should be set when creating actor in core worker.";
  auto actor = std::make_shared<GcsActor>(request.task_spec(),
                                          ray_namespace,
                                          actor_state_counter_,
                                          ray_event_recorder_,
                                          session_name_);
  if (!actor->GetName().empty()) {
    auto &actors_in_namespace = named_actors_[actor->GetRayNamespace()];
    auto it = actors_in_namespace.find(actor->GetName());
    if (it == actors_in_namespace.end()) {
      if (is_uuid(actor->GetRayNamespace()) && actor->IsDetached()) {
        std::ostringstream stream;
        stream
            << "It looks like you're creating a detached actor in an anonymous "
               "namespace. In order to access this actor in the future, you will need to "
               "explicitly connect to this namespace with ray.init(namespace=\""
            << actor->GetRayNamespace() << "\", ...)";

        auto error_data = CreateErrorTableData(
            "detached_actor_anonymous_namespace", stream.str(), absl::Now(), job_id);

        RAY_LOG(WARNING) << error_data.SerializeAsString();
        gcs_publisher_->PublishError(job_id.Hex(), std::move(error_data));
      }
      actors_in_namespace.emplace(actor->GetName(), actor->GetActorID());
    } else {
      std::stringstream stream;
      stream << "Actor with name '" << actor->GetName()
             << "' already exists in the namespace " << actor->GetRayNamespace();
      return Status::AlreadyExists(stream.str());
    }
  }

  actor_to_register_callbacks_[actor_id].push_back(std::move(register_callback));
  registered_actors_.emplace(actor->GetActorID(), actor);
  function_manager_.AddJobReference(actor_id.JobId());

  const auto &owner_address = actor->GetOwnerAddress();
  auto node_id = NodeID::FromBinary(owner_address.node_id());
  auto worker_id = WorkerID::FromBinary(owner_address.worker_id());
  RAY_CHECK(unresolved_actors_[node_id][worker_id].emplace(actor->GetActorID()).second);

  if (!actor->IsDetached()) {
    // This actor is owned. Send a long polling request to the actor's
    // owner to determine when the actor should be removed.
    PollOwnerForActorRefDeleted(actor);
  } else {
    // If it's a detached actor, we need to register the runtime env it used to GC.
    runtime_env_manager_.AddURIReference(actor->GetActorID().Hex(),
                                         request.task_spec().runtime_env_info());
  }

  // The backend storage is supposed to be reliable, so the status must be ok.
  gcs_table_storage_->ActorTaskSpecTable().Put(
      actor_id,
      request.task_spec(),
      {[this, actor](Status status) {
         gcs_table_storage_->ActorTable().Put(
             actor->GetActorID(),
             *actor->GetMutableActorTableData(),
             {[this, actor](Status put_status) {
                RAY_CHECK(thread_checker_.IsOnSameThread());
                // The backend storage is supposed to be reliable, so the status must be
                // ok.
                RAY_CHECK_OK(put_status);
                actor->WriteActorExportEvent(true);
                auto registered_actor_it = registered_actors_.find(actor->GetActorID());
                auto callback_iter =
                    actor_to_register_callbacks_.find(actor->GetActorID());
                RAY_CHECK(callback_iter != actor_to_register_callbacks_.end());
                if (registered_actor_it == registered_actors_.end()) {
                  // NOTE(sang): This logic assumes that the ordering of backend call is
                  // guaranteed. It is currently true because we use a single TCP socket
                  // to call the default Redis backend. If ordering is not guaranteed, we
                  // should overwrite the actor state to DEAD to avoid race condition.
                  RAY_LOG(INFO).WithField(actor->GetActorID())
                      << "Actor was killed before it was persisted in GCS Table Storage. "
                         "Owning worker should not try to create this actor";

                  for (auto &callback : callback_iter->second) {
                    callback(Status::SchedulingCancelled("Actor creation cancelled."));
                  }
                  return;
                }

                gcs_publisher_->PublishActor(actor->GetActorID(),
                                             actor->GetActorTableData());
                // Invoke all callbacks for all registration requests of this actor
                // (duplicated requests are included) and remove all of them from
                // actor_to_register_callbacks_.
                // Reply to the owner to indicate that the actor has been registered.
                for (auto &callback : callback_iter->second) {
                  callback(Status::OK());
                }
                actor_to_register_callbacks_.erase(callback_iter);
              },
              io_context_});
       },
       io_context_});

  return Status::OK();
}

Status GcsActorManager::CreateActor(const ray::rpc::CreateActorRequest &request,
                                    CreateActorCallback callback) {
  // NOTE: After the abnormal recovery of the network between GCS client and GCS server or
  // the GCS server is restarted, it is required to continue to create actor
  // successfully.
  RAY_CHECK(callback);
  const auto &actor_creation_task_spec = request.task_spec().actor_creation_task_spec();
  auto actor_id = ActorID::FromBinary(actor_creation_task_spec.actor_id());

  auto iter = registered_actors_.find(actor_id);
  if (iter == registered_actors_.end()) {
    RAY_LOG(DEBUG).WithField(actor_id.JobId()).WithField(actor_id)
        << "Actor may be already destroyed";
    return Status::Invalid("Actor may be already destroyed.");
  }

  const auto &actor_name = iter->second->GetName();
  // If the actor has the name, the name metadata should be stored already.
  if (!actor_name.empty()) {
    RAY_CHECK(!GetActorIDByName(actor_name, iter->second->GetRayNamespace()).IsNil());
  }

  if (iter->second->GetState() == rpc::ActorTableData::ALIVE) {
    // In case of temporary network failures, workers will re-send multiple duplicate
    // requests to GCS server.
    // In this case, we can just reply.
    // TODO(swang): Need to pass ref count info.
    callback(iter->second, rpc::PushTaskReply(), Status::OK());
    return Status::OK();
  }

  auto actor_creation_iter = actor_to_create_callbacks_.find(actor_id);
  if (actor_creation_iter != actor_to_create_callbacks_.end()) {
    // It is a duplicate message, just mark the callback as pending and invoke it after
    // the actor has been successfully created.
    actor_creation_iter->second.emplace_back(std::move(callback));
    return Status::OK();
  }
  // Mark the callback as pending and invoke it after the actor has been successfully
  // created or if the creation has been cancelled (e.g. via ray.kill() or the actor
  // handle going out-of-scope).
  actor_to_create_callbacks_[actor_id].emplace_back(std::move(callback));

  // If GCS restarts while processing `CreateActor` request, GCS client will resend the
  // `CreateActor` request.
  // After GCS restarts, the state of the actor may not be `DEPENDENCIES_UNREADY`.
  if (iter->second->GetState() != rpc::ActorTableData::DEPENDENCIES_UNREADY) {
    RAY_LOG(INFO).WithField(actor_id.JobId()).WithField(actor_id)
        << "Actor is already in the process of creation. Skip it directly";
    return Status::OK();
  }

  // Remove the actor from the unresolved actor map.
  const auto &actor_namespace = iter->second->GetRayNamespace();
  RAY_CHECK(!actor_namespace.empty())
      << "`ray_namespace` should be set when creating actor in core worker.";
  auto actor = std::make_shared<GcsActor>(request.task_spec(),
                                          actor_namespace,
                                          actor_state_counter_,
                                          ray_event_recorder_,
                                          session_name_);
  actor->UpdateState(rpc::ActorTableData::PENDING_CREATION);
  const auto &actor_table_data = actor->GetActorTableData();
  actor->GetMutableTaskSpec()->set_dependency_resolution_timestamp_ms(
      current_sys_time_ms());

  // Pub this state for dashboard showing.
  gcs_publisher_->PublishActor(actor_id, actor_table_data);
  actor->WriteActorExportEvent(false);
  RemoveUnresolvedActor(actor);

  // Update the registered actor as its creation task specification may have changed due
  // to resolved dependencies.
  registered_actors_[actor_id] = actor;

  // Schedule the actor.
  gcs_actor_scheduler_->Schedule(actor);
  return Status::OK();
}

ActorID GcsActorManager::GetActorIDByName(const std::string &name,
                                          const std::string &ray_namespace) const {
  ActorID actor_id = ActorID::Nil();
  auto namespace_it = named_actors_.find(ray_namespace);
  if (namespace_it != named_actors_.end()) {
    auto it = namespace_it->second.find(name);
    if (it != namespace_it->second.end()) {
      actor_id = it->second;
    }
  }
  return actor_id;
}

void GcsActorManager::RemoveActorNameFromRegistry(
    const std::shared_ptr<GcsActor> &actor) {
  // Remove actor from `named_actors_` if its name is not empty.
  if (!actor->GetName().empty()) {
    auto namespace_it = named_actors_.find(actor->GetRayNamespace());
    if (namespace_it != named_actors_.end()) {
      auto it = namespace_it->second.find(actor->GetName());
      if (it != namespace_it->second.end()) {
        RAY_LOG(INFO) << "Actor name " << actor->GetName() << " is cleaned up.";
        namespace_it->second.erase(it);
      }
      // If we just removed the last actor in the namespace, remove the map.
      if (namespace_it->second.empty()) {
        named_actors_.erase(namespace_it);
      }
    }
  }
}

std::vector<std::pair<std::string, std::string>> GcsActorManager::ListNamedActors(
    bool all_namespaces, const std::string &ray_namespace) const {
  std::vector<std::pair<std::string, std::string>> actors;
  if (all_namespaces) {
    for (const auto &namespace_it : named_actors_) {
      for (const auto &actor_it : namespace_it.second) {
        auto iter = registered_actors_.find(actor_it.second);
        if (iter != registered_actors_.end() &&
            iter->second->GetState() != rpc::ActorTableData::DEAD) {
          actors.push_back(std::make_pair(namespace_it.first, actor_it.first));
        }
      }
    }
  } else {
    auto namespace_it = named_actors_.find(ray_namespace);
    if (namespace_it != named_actors_.end()) {
      for (const auto &actor_it : namespace_it->second) {
        auto iter = registered_actors_.find(actor_it.second);
        if (iter != registered_actors_.end() &&
            iter->second->GetState() != rpc::ActorTableData::DEAD) {
          actors.push_back(std::make_pair(namespace_it->first, actor_it.first));
        }
      }
    }
  }
  return actors;
}

void GcsActorManager::PollOwnerForActorRefDeleted(
    const std::shared_ptr<GcsActor> &actor) {
  const auto &actor_id = actor->GetActorID();
  const auto &owner_node_id = actor->GetOwnerNodeID();
  const auto &owner_id = actor->GetOwnerID();
  auto &workers = owners_[owner_node_id];
  auto it = workers.find(owner_id);
  if (it == workers.end()) {
    RAY_LOG(DEBUG) << "Adding owner " << owner_id << " of actor " << actor_id
                   << ", job id = " << actor_id.JobId();
    it = workers.emplace(owner_id, Owner(actor->GetOwnerAddress())).first;
  }
  it->second.children_actor_ids_.insert(actor_id);

  rpc::WaitForActorRefDeletedRequest wait_request;
  wait_request.set_intended_worker_id(owner_id.Binary());
  wait_request.set_actor_id(actor_id.Binary());
  auto client = worker_client_pool_.GetOrConnect(it->second.address_);
  client->WaitForActorRefDeleted(
      std::move(wait_request),
      [this, owner_node_id, owner_id, actor_id](
          Status status, const rpc::WaitForActorRefDeletedReply &reply) {
        if (!status.ok()) {
          RAY_LOG(INFO) << "Worker " << owner_id
                        << " failed, destroying actor child, job id = "
                        << actor_id.JobId();
        } else {
          RAY_LOG(INFO) << "Actor " << actor_id
                        << " has no references, destroying actor, job id = "
                        << actor_id.JobId();
        }

        auto node_it = owners_.find(owner_node_id);
        if (node_it != owners_.end() && node_it->second.count(owner_id)) {
          // Only destroy the actor if its owner is still alive. The actor may
          // have already been destroyed if the owner died.
          DestroyActor(actor_id,
                       GenActorRefDeletedCause(GetActor(actor_id)),
                       /*force_kill=*/true);
        }
      });
}

void GcsActorManager::DestroyActor(const ActorID &actor_id,
                                   const rpc::ActorDeathCause &death_cause,
                                   bool force_kill,
                                   std::function<void()> done_callback) {
  RAY_CHECK(thread_checker_.IsOnSameThread());
  RAY_LOG(INFO).WithField(actor_id.JobId()).WithField(actor_id) << "Destroying actor";
  actor_to_restart_for_lineage_reconstruction_callbacks_.erase(actor_id);
  auto it = registered_actors_.find(actor_id);
  if (it == registered_actors_.end()) {
    RAY_LOG(INFO).WithField(actor_id) << "Tried to destroy actor that does not exist";
    if (done_callback) {
      done_callback();
    }
    return;
  }

  gcs_actor_scheduler_->OnActorDestruction(it->second);

  it->second->GetMutableActorTableData()->set_timestamp(current_sys_time_ms());
  const auto actor = it->second;

  RAY_LOG(DEBUG) << "Try to kill actor " << actor->GetActorID() << ", with status "
                 << rpc::ActorTableData::ActorState_Name(actor->GetState()) << ", name "
                 << actor->GetName();
  if (actor->GetState() == rpc::ActorTableData::DEPENDENCIES_UNREADY) {
    // The actor creation task still has unresolved dependencies. Remove from the
    // unresolved actors map.
    RemoveUnresolvedActor(actor);
  } else if (actor->GetState() != rpc::ActorTableData::DEAD) {
    // The actor is still alive or pending creation. Clean up all remaining
    // state.
    const auto &node_id = actor->GetNodeID();
    const auto &worker_id = actor->GetWorkerID();
    auto node_it = created_actors_.find(node_id);
    if (node_it != created_actors_.end() && node_it->second.count(worker_id)) {
      // The actor has already been created. Destroy the process by force-killing
      // it.
      NotifyRayletToKillActor(actor, death_cause, force_kill);
      RAY_CHECK(node_it->second.erase(actor->GetWorkerID()));
      if (node_it->second.empty()) {
        created_actors_.erase(node_it);
      }
    } else {
      if (!worker_id.IsNil()) {
        // The actor is in phase of creating, so we need to notify the core
        // worker exit to avoid process and resource leak.
        NotifyRayletToKillActor(actor, death_cause, force_kill);
      }
      CancelActorInScheduling(actor);
    }
  }

  // Update the actor to DEAD in case any callers are still alive. This can
  // happen if the owner of the actor dies while there are still callers.
  // TODO(swang): We can skip this step and delete the actor table entry
  // entirely if the callers check directly whether the owner is still alive.
  auto mutable_actor_table_data = actor->GetMutableActorTableData();
  auto time = current_sys_time_ms();
  if (actor->GetState() != rpc::ActorTableData::DEAD) {
    actor->UpdateState(rpc::ActorTableData::DEAD);
    mutable_actor_table_data->set_end_time(time);
    mutable_actor_table_data->mutable_death_cause()->CopyFrom(death_cause);
  } else if (mutable_actor_table_data->death_cause().context_case() ==
                 ContextCase::kActorDiedErrorContext &&
             death_cause.context_case() == ContextCase::kActorDiedErrorContext &&
             mutable_actor_table_data->death_cause()
                     .actor_died_error_context()
                     .reason() == rpc::ActorDiedErrorContext::OUT_OF_SCOPE &&
             death_cause.actor_died_error_context().reason() ==
                 rpc::ActorDiedErrorContext::REF_DELETED) {
    // Update death cause from restartable OUT_OF_SCOPE to non-restartable REF_DELETED
    mutable_actor_table_data->mutable_death_cause()->CopyFrom(death_cause);
  }
  mutable_actor_table_data->set_timestamp(time);

  const bool is_restartable = IsActorRestartable(*mutable_actor_table_data);
  if (!is_restartable) {
    AddDestroyedActorToCache(it->second);
    registered_actors_.erase(it);
    function_manager_.RemoveJobReference(actor_id.JobId());
    RemoveActorNameFromRegistry(actor);
    // Clean up the client to the actor's owner, if necessary.
    if (!actor->IsDetached()) {
      RemoveActorFromOwner(actor);
    } else {
      runtime_env_manager_.RemoveURIReference(actor_id.Hex());
    }
  }

  auto actor_table_data =
      std::make_shared<rpc::ActorTableData>(*mutable_actor_table_data);
  // The backend storage is reliable in the future, so the status must be ok.
  gcs_table_storage_->ActorTable().Put(
      actor->GetActorID(),
      *actor_table_data,
      {[this,
        actor,
        actor_id,
        actor_table_data,
        is_restartable,
        done_callback = std::move(done_callback)](Status status) {
         if (done_callback) {
           done_callback();
         }
         gcs_publisher_->PublishActor(actor_id,
                                      GenActorDataOnlyWithStates(*actor_table_data));
         if (!is_restartable) {
           gcs_table_storage_->ActorTaskSpecTable().Delete(actor_id,
                                                           {[](auto) {}, io_context_});
         }
         actor->WriteActorExportEvent(false);
         // Destroy placement group owned by this actor.
         destroy_owned_placement_group_if_needed_(actor_id);
       },
       io_context_});

  // Inform all creation callbacks that the actor was cancelled, not created.
  RunAndClearActorCreationCallbacks(
      actor,
      rpc::PushTaskReply(),
      Status::SchedulingCancelled("Actor creation cancelled."));
}

absl::flat_hash_map<WorkerID, absl::flat_hash_set<ActorID>>
GcsActorManager::GetUnresolvedActorsByOwnerNode(const NodeID &node_id) const {
  absl::flat_hash_map<WorkerID, absl::flat_hash_set<ActorID>> actor_ids_map;
  auto iter = unresolved_actors_.find(node_id);
  if (iter != unresolved_actors_.end()) {
    for (const auto &entry : iter->second) {
      const auto &owner_id = entry.first;
      auto &actor_ids = actor_ids_map[owner_id];
      actor_ids.insert(entry.second.begin(), entry.second.end());
    }
  }
  return actor_ids_map;
}

absl::flat_hash_set<ActorID> GcsActorManager::GetUnresolvedActorsByOwnerWorker(
    const NodeID &node_id, const WorkerID &worker_id) const {
  absl::flat_hash_set<ActorID> actor_ids;
  auto iter = unresolved_actors_.find(node_id);
  if (iter != unresolved_actors_.end()) {
    auto it = iter->second.find(worker_id);
    if (it != iter->second.end()) {
      actor_ids.insert(it->second.begin(), it->second.end());
    }
  }
  return actor_ids;
}

void GcsActorManager::OnWorkerDead(const ray::NodeID &node_id,
                                   const ray::WorkerID &worker_id) {
  OnWorkerDead(node_id,
               worker_id,
               "",
               rpc::WorkerExitType::SYSTEM_ERROR,
               "Worker exits unexpectedly.");
}

void GcsActorManager::OnWorkerDead(const ray::NodeID &node_id,
                                   const ray::WorkerID &worker_id,
                                   const std::string &worker_ip,
                                   const rpc::WorkerExitType disconnect_type,
                                   const std::string &disconnect_detail,
                                   const rpc::RayException *creation_task_exception) {
  std::string message = absl::StrCat("Worker ",
                                     worker_id.Hex(),
                                     " on node ",
                                     node_id.Hex(),
                                     " exits, type=",
                                     rpc::WorkerExitType_Name(disconnect_type),
                                     ", has creation_task_exception = ",
                                     (creation_task_exception != nullptr));
  RAY_LOG(DEBUG).WithField(worker_id)
      << "on worker dead, disconnect detail " << disconnect_detail;
  if (disconnect_type == rpc::WorkerExitType::INTENDED_USER_EXIT ||
      disconnect_type == rpc::WorkerExitType::INTENDED_SYSTEM_EXIT) {
    RAY_LOG(DEBUG).WithField(worker_id) << message;
  } else {
    RAY_LOG(WARNING).WithField(worker_id) << message;
  }

  bool need_reconstruct = disconnect_type != rpc::WorkerExitType::INTENDED_USER_EXIT &&
                          disconnect_type != rpc::WorkerExitType::USER_ERROR;
  // Destroy all actors that are owned by this worker.
  const auto it = owners_.find(node_id);
  if (it != owners_.end() && it->second.count(worker_id)) {
    auto owner = it->second.find(worker_id);
    // Make a copy of the children actor IDs since we will delete from the
    // list.
    const auto children_ids = owner->second.children_actor_ids_;
    for (const auto &child_id : children_ids) {
      DestroyActor(child_id,
                   GenOwnerDiedCause(GetActor(child_id),
                                     worker_id,
                                     disconnect_type,
                                     "Owner's worker process has crashed.",
                                     worker_ip));
    }
  }

  // The creator worker of these actors died before resolving their dependencies. In this
  // case, these actors will never be created successfully. So we need to destroy them,
  // to prevent actor tasks hang forever.
  auto unresolved_actors = GetUnresolvedActorsByOwnerWorker(node_id, worker_id);
  for (auto &actor_id : unresolved_actors) {
    if (registered_actors_.count(actor_id)) {
      DestroyActor(actor_id,
                   GenOwnerDiedCause(GetActor(actor_id),
                                     worker_id,
                                     disconnect_type,
                                     "Owner's worker process has crashed.",
                                     worker_ip));
    }
  }

  // Find if actor is already created or in the creation process (lease request is
  // granted)
  ActorID actor_id;
  auto iter = created_actors_.find(node_id);
  if (iter != created_actors_.end() && iter->second.count(worker_id)) {
    actor_id = iter->second[worker_id];
    iter->second.erase(worker_id);
    if (iter->second.empty()) {
      created_actors_.erase(iter);
    }
  } else {
    actor_id = gcs_actor_scheduler_->CancelOnWorker(node_id, worker_id);
    if (actor_id.IsNil()) {
      return;
    }
  }

  auto actor_iter = registered_actors_.find(actor_id);
  if (actor_iter != registered_actors_.end()) {
    gcs_actor_scheduler_->OnActorDestruction(actor_iter->second);
  }

  rpc::ActorDeathCause death_cause;
  if (creation_task_exception != nullptr) {
    absl::StrAppend(
        &message, ": ", creation_task_exception->formatted_exception_string());

    death_cause.mutable_creation_task_failure_context()->CopyFrom(
        *creation_task_exception);
  } else {
    death_cause = GenWorkerDiedCause(
        GetActor(actor_id), worker_ip, disconnect_type, disconnect_detail);
  }
  // Otherwise, try to reconstruct the actor that was already created or in the creation
  // process.
  RestartActor(actor_id, /*need_reschedule=*/need_reconstruct, death_cause);
}

void GcsActorManager::OnNodeDead(std::shared_ptr<const rpc::GcsNodeInfo> node,
                                 const std::string &node_ip_address) {
  const auto node_id = NodeID::FromBinary(node->node_id());
  RAY_LOG(DEBUG).WithField(node_id) << "Node is dead, reconstructing actors.";
  // Kill all children of owner actors on a dead node.
  const auto it = owners_.find(node_id);
  if (it != owners_.end()) {
    absl::flat_hash_map<WorkerID, ActorID> children_ids;
    // Make a copy of all the actor IDs owned by workers on the dead node.
    for (const auto &owner : it->second) {
      for (const auto &child_id : owner.second.children_actor_ids_) {
        children_ids.emplace(owner.first, child_id);
      }
    }

    if (!children_ids.empty()) {
      std::ostringstream oss;
      oss << "Node died; killing actors that were owned by workers on it: ";
      for (auto child_it = children_ids.begin(); child_it != children_ids.end();
           child_it++) {
        if (child_it != children_ids.begin()) {
          oss << ", ";
        }
        oss << child_it->second.Hex();
      }
      RAY_LOG(INFO).WithField(node_id) << oss.str();
    }

    for (const auto &[owner_id, child_id] : children_ids) {
      DestroyActor(child_id,
                   GenOwnerDiedCause(GetActor(child_id),
                                     owner_id,
                                     rpc::WorkerExitType::SYSTEM_ERROR,
                                     "Owner's node has crashed.",
                                     node_ip_address));
    }
  }

  // Cancel scheduling actors that haven't been created on the node.
  auto scheduling_actor_ids = gcs_actor_scheduler_->CancelOnNode(node_id);

  if (!scheduling_actor_ids.empty()) {
    std::ostringstream oss;
    oss << "Node died; rescheduling actors that were being scheduled on it: ";
    for (auto reschedule_it = scheduling_actor_ids.begin();
         reschedule_it != scheduling_actor_ids.end();
         reschedule_it++) {
      if (reschedule_it != scheduling_actor_ids.begin()) {
        oss << ", ";
      }
      oss << reschedule_it->Hex();
    }
    RAY_LOG(INFO).WithField(node_id) << oss.str();
  }

  for (auto &actor_id : scheduling_actor_ids) {
    RestartActor(actor_id,
                 /*need_reschedule=*/true,
                 GenNodeDiedCause(GetActor(actor_id), node));
  }

  // Try reconstructing all workers created on the node.
  auto iter = created_actors_.find(node_id);
  if (iter != created_actors_.end()) {
    auto created_actors = std::move(iter->second);
    // Remove all created actors from node_to_created_actors_.
    created_actors_.erase(iter);

    if (!created_actors.empty()) {
      std::ostringstream oss;
      oss << "Node died; reconstructing actors that were running on it: ";
      for (auto created_it = created_actors.begin(); created_it != created_actors.end();
           created_it++) {
        if (created_it != created_actors.begin()) {
          oss << ", ";
        }
        oss << created_it->second.Hex();
      }
      RAY_LOG(INFO).WithField(node_id) << oss.str();
    }

    for (auto &entry : created_actors) {
      // Reconstruct the removed actor.
      RestartActor(entry.second,
                   /*need_reschedule=*/true,
                   GenNodeDiedCause(GetActor(entry.second), node));
    }
  }

  // The creator node of these actors died before resolving their dependencies. In this
  // case, these actors will never be created successfully. So we need to destroy them,
  // to prevent actor tasks hang forever.
  auto unresolved_actors = GetUnresolvedActorsByOwnerNode(node_id);

  if (!unresolved_actors.empty()) {
    bool first = false;
    std::ostringstream oss;
    oss << "Node died; rescheduling actors that were resolving dependencies on it: ";
    for (auto unresolved_it = unresolved_actors.begin();
         unresolved_it != unresolved_actors.end();
         unresolved_it++) {
      for (auto actor_it = unresolved_it->second.begin();
           actor_it != unresolved_it->second.end();
           actor_it++) {
        if (!first) {
          oss << ", ";
        }
        first = false;
        oss << actor_it->Hex();
      }
    }
    RAY_LOG(INFO).WithField(node_id) << oss.str();
  }

  for (const auto &[owner_id, actor_ids] : unresolved_actors) {
    for (const auto &actor_id : actor_ids) {
      if (registered_actors_.count(actor_id)) {
        DestroyActor(actor_id,
                     GenOwnerDiedCause(GetActor(actor_id),
                                       owner_id,
                                       rpc::WorkerExitType::SYSTEM_ERROR,
                                       "Owner's node has crashed.",
                                       node_ip_address));
      }
    }
  }
}

void GcsActorManager::SetPreemptedAndPublish(const NodeID &node_id) {
  // The node has received a drain request, so we mark all of its actors
  // preempted. This state will be published to the raylets so that the
  // preemption may be retrieved upon actor death.
  if (created_actors_.find(node_id) == created_actors_.end()) {
    return;
  }

  for (const auto &id_iter : created_actors_.find(node_id)->second) {
    auto actor_iter = registered_actors_.find(id_iter.second);
    RAY_CHECK(actor_iter != registered_actors_.end())
        << "Could not find actor " << id_iter.second.Hex() << " in registered actors.";

    actor_iter->second->GetMutableActorTableData()->set_preempted(true);

    const auto &actor_id = id_iter.second;
    const auto &actor_table_data = actor_iter->second->GetActorTableData();

    gcs_table_storage_->ActorTable().Put(
        actor_id,
        actor_table_data,
        {[this, actor_id, actor_table_data](Status status) {
           gcs_publisher_->PublishActor(actor_id,
                                        GenActorDataOnlyWithStates(actor_table_data));
         },
         io_context_});
  }
}

void GcsActorManager::RestartActor(const ActorID &actor_id,
                                   bool need_reschedule,
                                   const rpc::ActorDeathCause &death_cause,
                                   std::function<void()> done_callback) {
  // If the owner and this actor is dead at the same time, the actor
  // could've been destroyed and dereigstered before restart.
  auto iter = registered_actors_.find(actor_id);
  if (iter == registered_actors_.end()) {
    RAY_LOG(DEBUG).WithField(actor_id.JobId()).WithField(actor_id)
        << "Actor is destroyed before restart";
    if (done_callback) {
      done_callback();
    }
    return;
  }

  auto &actor = iter->second;
  auto node_id = actor->GetNodeID();
  auto worker_id = actor->GetWorkerID();
  auto mutable_actor_table_data = actor->GetMutableActorTableData();
  // If the need_reschedule is set to false, then set the `remaining_restarts` to 0
  // so that the actor will never be rescheduled.
  int64_t max_restarts = mutable_actor_table_data->max_restarts();
  uint64_t num_restarts = mutable_actor_table_data->num_restarts();
  uint64_t num_restarts_due_to_node_preemption =
      mutable_actor_table_data->num_restarts_due_to_node_preemption();

  int64_t remaining_restarts;
  // Destroy placement group owned by this actor.
  destroy_owned_placement_group_if_needed_(actor_id);
  if (!need_reschedule) {
    remaining_restarts = 0;
  } else if (max_restarts == -1) {
    remaining_restarts = -1;
  } else {
    // Restarts due to node preemption do not count towards max_restarts.
    const auto effective_restarts = num_restarts - num_restarts_due_to_node_preemption;
    int64_t remaining = max_restarts - static_cast<int64_t>(effective_restarts);
    remaining_restarts = std::max(remaining, static_cast<int64_t>(0));
  }

  RAY_LOG(INFO).WithField(actor_id.JobId()).WithField(actor_id)
      << "Actor is failed on worker " << worker_id << " at node " << node_id
      << ", need_reschedule = " << need_reschedule
      << ", death context type = " << GetActorDeathCauseString(death_cause)
      << ", remaining_restarts = " << remaining_restarts
      << ", num_restarts = " << num_restarts
      << ", num_restarts_due_to_node_preemption = " << num_restarts_due_to_node_preemption
      << ", preempted = " << mutable_actor_table_data->preempted();

  if (remaining_restarts != 0 ||
      (need_reschedule && max_restarts > 0 && mutable_actor_table_data->preempted())) {
    // num_restarts must be set before updating GCS, or num_restarts will be inconsistent
    // between memory cache and storage.
    if (mutable_actor_table_data->preempted()) {
      mutable_actor_table_data->set_num_restarts_due_to_node_preemption(
          num_restarts_due_to_node_preemption + 1);
    }
    mutable_actor_table_data->set_num_restarts(num_restarts + 1);
    actor->UpdateState(rpc::ActorTableData::RESTARTING);
    // Make sure to reset the address before flushing to GCS. Otherwise,
    // GCS will mistakenly consider this lease request succeeds when restarting.
    actor->UpdateAddress(rpc::Address());
    mutable_actor_table_data->clear_resource_mapping();
    // The backend storage is reliable in the future, so the status must be ok.
    gcs_table_storage_->ActorTable().Put(
        actor_id,
        *mutable_actor_table_data,
        {[this, actor, actor_id, mutable_actor_table_data, done_callback](Status status) {
           if (done_callback) {
             done_callback();
           }
           gcs_publisher_->PublishActor(
               actor_id, GenActorDataOnlyWithStates(*mutable_actor_table_data));
           actor->WriteActorExportEvent(false);
         },
         io_context_});
    gcs_actor_scheduler_->Schedule(actor);
  } else {
    actor->UpdateState(rpc::ActorTableData::DEAD);
    mutable_actor_table_data->mutable_death_cause()->CopyFrom(death_cause);
    auto time = current_sys_time_ms();
    mutable_actor_table_data->set_end_time(time);
    mutable_actor_table_data->set_timestamp(time);

    // The backend storage is reliable in the future, so the status must be ok.
    gcs_table_storage_->ActorTable().Put(
        actor_id,
        *mutable_actor_table_data,
        {[this, actor, actor_id, mutable_actor_table_data, death_cause, done_callback](
             Status status) {
           // If actor was a detached actor, make sure to destroy it.
           // We need to do this because detached actors are not destroyed
           // when its owners are dead because it doesn't have owners.
           if (actor->IsDetached()) {
             DestroyActor(actor_id, death_cause);
           }
           if (done_callback) {
             done_callback();
           }
           gcs_publisher_->PublishActor(
               actor_id, GenActorDataOnlyWithStates(*mutable_actor_table_data));
           gcs_table_storage_->ActorTaskSpecTable().Delete(actor_id,
                                                           {[](auto) {}, io_context_});
           actor->WriteActorExportEvent(false);
         },
         io_context_});
    // The actor is dead, but we should not remove the entry from the
    // registered actors yet. If the actor is owned, we will destroy the actor
    // once the owner fails or notifies us that the actor has no references.
  }
}

void GcsActorManager::OnActorSchedulingFailed(
    std::shared_ptr<GcsActor> actor,
    rpc::RequestWorkerLeaseReply::SchedulingFailureType failure_type,
    const std::string &scheduling_failure_message) {
  if (failure_type == rpc::RequestWorkerLeaseReply::SCHEDULING_FAILED) {
    // We will attempt to schedule this actor once an eligible node is
    // registered.
    pending_actors_.emplace_back(std::move(actor));
    return;
  }
  if (failure_type == rpc::RequestWorkerLeaseReply::SCHEDULING_CANCELLED_INTENDED) {
    // Return directly if the actor was canceled actively as we've already done the
    // recreate and destroy operation when we killed the actor.
    return;
  }

  std::string error_msg;
  ray::rpc::ActorDeathCause death_cause;
  switch (failure_type) {
  case rpc::RequestWorkerLeaseReply::SCHEDULING_CANCELLED_PLACEMENT_GROUP_REMOVED:
    error_msg = absl::StrCat(
        "Could not create the actor because its associated placement group was "
        "removed.\n",
        scheduling_failure_message);
    death_cause.mutable_actor_unschedulable_context()->set_error_message(error_msg);
    break;
  case rpc::RequestWorkerLeaseReply::SCHEDULING_CANCELLED_RUNTIME_ENV_SETUP_FAILED:
    error_msg = absl::StrCat(
        "Could not create the actor because its associated runtime env failed to be "
        "created.\n",
        scheduling_failure_message);
    death_cause.mutable_runtime_env_failed_context()->set_error_message(error_msg);
    break;
  case rpc::RequestWorkerLeaseReply::SCHEDULING_CANCELLED_UNSCHEDULABLE:
    death_cause.mutable_actor_unschedulable_context()->set_error_message(
        scheduling_failure_message);
    break;
  default:
    RAY_LOG(FATAL) << "Unknown error, failure type "
                   << rpc::RequestWorkerLeaseReply::SchedulingFailureType_Name(
                          failure_type);
    break;
  }

  DestroyActor(actor->GetActorID(), death_cause);
}

void GcsActorManager::OnActorCreationSuccess(const std::shared_ptr<GcsActor> &actor,
                                             const rpc::PushTaskReply &reply) {
  auto actor_id = actor->GetActorID();
  liftime_num_created_actors_++;
  // NOTE: If an actor is deleted immediately after the user creates the actor, reference
  // counter may ReportActorOutOfScope to GCS server,
  // and GCS server will destroy the actor. The actor creation is asynchronous, it may be
  // destroyed before the actor creation is completed.
  auto registered_actor_it = registered_actors_.find(actor_id);
  if (registered_actor_it == registered_actors_.end() ||
      registered_actor_it->second->GetState() == rpc::ActorTableData::DEAD) {
    return;
  }

  if (reply.is_application_error()) {
    RAY_LOG(INFO).WithField(actor_id.JobId()).WithField(actor_id)
        << "Failed to create an actor due to the application failure";
    // NOTE: Alternatively we could also destroy the actor here right away. The actor will
    // be eventually destroyed as the CoreWorker runs the creation task will exit
    // eventually due to the creation task failure.
    RunAndClearActorCreationCallbacks(
        actor, reply, Status::CreationTaskError(reply.task_execution_error()));
  } else {
    RAY_LOG(INFO).WithField(actor_id.JobId()).WithField(actor_id)
        << "Actor created successfully";
  }

  auto mutable_actor_table_data = actor->GetMutableActorTableData();
  auto time = current_sys_time_ms();
  mutable_actor_table_data->set_timestamp(time);
  if (actor->GetState() != rpc::ActorTableData::RESTARTING) {
    mutable_actor_table_data->set_start_time(time);
  }
  actor->UpdateState(rpc::ActorTableData::ALIVE);

  // We should register the entry to the in-memory index before flushing them to
  // GCS because otherwise, there could be timing problems due to asynchronous Put.
  auto worker_id = actor->GetWorkerID();
  auto node_id = actor->GetNodeID();
  mutable_actor_table_data->set_node_id(node_id.Binary());
  mutable_actor_table_data->set_repr_name(reply.actor_repr_name());
  mutable_actor_table_data->set_preempted(false);
  RAY_CHECK(!worker_id.IsNil());
  RAY_CHECK(!node_id.IsNil());
  RAY_CHECK(created_actors_[node_id].emplace(worker_id, actor_id).second);

  auto actor_data_only_with_states =
      GenActorDataOnlyWithStates(*mutable_actor_table_data);
  // The backend storage is reliable in the future, so the status must be ok.
  gcs_table_storage_->ActorTable().Put(
      actor_id,
      *mutable_actor_table_data,
      {[this,
        actor_id,
        actor_data_only_with_states = std::move(actor_data_only_with_states),
        actor,
        reply](Status status) mutable {
         gcs_publisher_->PublishActor(actor_id, std::move(actor_data_only_with_states));
         actor->WriteActorExportEvent(false);
         // Invoke all callbacks for all registration requests of this actor (duplicated
         // requests are included) and remove all of them from
         // actor_to_create_callbacks_.
         RunAndClearActorCreationCallbacks(actor, reply, Status::OK());
       },
       io_context_});
}

void GcsActorManager::SchedulePendingActors() {
  if (pending_actors_.empty()) {
    return;
  }

  RAY_LOG(DEBUG) << "Scheduling actor creation tasks, size = " << pending_actors_.size();
  auto actors = std::move(pending_actors_);
  for (auto &actor : actors) {
    gcs_actor_scheduler_->Schedule(std::move(actor));
  }
}

void GcsActorManager::Initialize(const GcsInitData &gcs_init_data) {
  const auto &actor_task_specs = gcs_init_data.ActorTaskSpecs();
  absl::flat_hash_map<NodeID, std::vector<WorkerID>> node_to_workers;
  std::vector<ActorID> dead_actors;
  for (const auto &[actor_id, actor_table_data] : gcs_init_data.Actors()) {
    // We only load actors which are supposed to be alive:
    //   - Actors whose state != DEAD.
    //   - Non-deatched actors whose owner (job or root_detached_actor) is alive.
    //   - Detached actors which lives even when their original owner is dead.
    if (OnInitializeActorShouldLoad(gcs_init_data, actor_id)) {
      const auto &actor_task_spec = map_find_or_die(actor_task_specs, actor_id);
      auto actor = std::make_shared<GcsActor>(actor_table_data,
                                              actor_task_spec,
                                              actor_state_counter_,
                                              ray_event_recorder_,
                                              session_name_);
      registered_actors_.emplace(actor_id, actor);
      function_manager_.AddJobReference(actor->GetActorID().JobId());
      if (!actor->GetName().empty()) {
        named_actors_[actor->GetRayNamespace()][actor->GetName()] = actor->GetActorID();
      }

      if (actor_table_data.state() == ray::rpc::ActorTableData::DEPENDENCIES_UNREADY) {
        const auto &owner = actor->GetOwnerAddress();
        const auto &owner_node = NodeID::FromBinary(owner.node_id());
        const auto &owner_worker = WorkerID::FromBinary(owner.worker_id());
        RAY_CHECK(unresolved_actors_[owner_node][owner_worker]
                      .emplace(actor->GetActorID())
                      .second);
      } else if (actor_table_data.state() == ray::rpc::ActorTableData::ALIVE) {
        created_actors_[actor->GetNodeID()].emplace(actor->GetWorkerID(),
                                                    actor->GetActorID());
      }

      if (!actor->IsDetached()) {
        // This actor is owned. Send a long polling request to the actor's
        // owner to determine when the actor should be removed.
        PollOwnerForActorRefDeleted(actor);
      }

      if (!actor->GetWorkerID().IsNil()) {
        RAY_CHECK(!actor->GetNodeID().IsNil());
        node_to_workers[actor->GetNodeID()].emplace_back(actor->GetWorkerID());
      }
    } else {
      dead_actors.push_back(actor_id);
      auto actor = std::make_shared<GcsActor>(
          actor_table_data, actor_state_counter_, ray_event_recorder_, session_name_);
      destroyed_actors_.emplace(actor_id, actor);
      sorted_destroyed_actor_list_.emplace_back(
          actor_id, static_cast<int64_t>(actor_table_data.timestamp()));
    }
  }
  if (!dead_actors.empty()) {
    gcs_table_storage_->ActorTaskSpecTable().BatchDelete(dead_actors,
                                                         {[](auto) {}, io_context_});
  }
  sorted_destroyed_actor_list_.sort([](const std::pair<ActorID, int64_t> &left,
                                       const std::pair<ActorID, int64_t> &right) {
    return left.second < right.second;
  });

  // Notify raylets to release unused workers.
  gcs_actor_scheduler_->ReleaseUnusedActorWorkers(node_to_workers);

  RAY_LOG(DEBUG) << "The number of registered actors is " << registered_actors_.size()
                 << ", and the number of created actors is " << created_actors_.size();
  for (auto &item : registered_actors_) {
    auto &actor = item.second;
    if (actor->GetState() == ray::rpc::ActorTableData::PENDING_CREATION ||
        actor->GetState() == ray::rpc::ActorTableData::RESTARTING) {
      // We should not reschedule actors in state of `ALIVE`.
      // We could not reschedule actors in state of `DEPENDENCIES_UNREADY` because the
      // dependencies of them may not have been resolved yet.
      RAY_LOG(INFO).WithField(actor->GetActorID().JobId()).WithField(actor->GetActorID())
          << "Rescheduling a non-alive actor, state = "
          << rpc::ActorTableData::ActorState_Name(actor->GetState());
      gcs_actor_scheduler_->Reschedule(actor);
    }
  }
}

const absl::flat_hash_map<NodeID, absl::flat_hash_map<WorkerID, ActorID>>
    &GcsActorManager::GetCreatedActors() const {
  return created_actors_;
}

const absl::flat_hash_map<ActorID, std::shared_ptr<GcsActor>>
    &GcsActorManager::GetRegisteredActors() const {
  return registered_actors_;
}

void GcsActorManager::RemoveUnresolvedActor(const std::shared_ptr<GcsActor> &actor) {
  const auto &owner_address = actor->GetOwnerAddress();
  auto node_id = NodeID::FromBinary(owner_address.node_id());
  auto worker_id = WorkerID::FromBinary(owner_address.worker_id());
  auto iter = unresolved_actors_.find(node_id);
  if (iter != unresolved_actors_.end()) {
    auto it = iter->second.find(worker_id);
    RAY_CHECK(it != iter->second.end());
    RAY_CHECK(it->second.erase(actor->GetActorID()) != 0);
    if (it->second.empty()) {
      iter->second.erase(it);
      if (iter->second.empty()) {
        unresolved_actors_.erase(iter);
      }
    }
  }
}

void GcsActorManager::RemoveActorFromOwner(const std::shared_ptr<GcsActor> &actor) {
  const auto &actor_id = actor->GetActorID();
  const auto &owner_id = actor->GetOwnerID();
  RAY_LOG(DEBUG).WithField(actor_id).WithField(owner_id).WithField(actor_id.JobId())
      << "Erasing actor owned by worker";

  const auto &owner_node_id = actor->GetOwnerNodeID();
  auto &node = owners_[owner_node_id];
  auto worker_it = node.find(owner_id);
  RAY_CHECK(worker_it != node.end());
  auto &owner = worker_it->second;
  RAY_CHECK(owner.children_actor_ids_.erase(actor_id));
  if (owner.children_actor_ids_.empty()) {
    node.erase(worker_it);
    if (node.empty()) {
      owners_.erase(owner_node_id);
    }
  }
}

void GcsActorManager::NotifyRayletToKillActor(const std::shared_ptr<GcsActor> &actor,
                                              const rpc::ActorDeathCause &death_cause,
                                              bool force_kill) {
  rpc::KillLocalActorRequest request;
  request.set_intended_actor_id(actor->GetActorID().Binary());
  request.set_worker_id(actor->GetWorkerID().Binary());
  request.mutable_death_cause()->CopyFrom(death_cause);
  request.set_force_kill(force_kill);
  if (!actor->LocalRayletAddress()) {
    RAY_LOG(DEBUG) << "Actor " << actor->GetActorID() << " has not been assigned a lease";
    return;
  }
  auto actor_raylet_client =
      raylet_client_pool_.GetOrConnectByAddress(actor->LocalRayletAddress().value());
  RAY_LOG(DEBUG)
          .WithField(actor->GetActorID())
          .WithField(actor->GetWorkerID())
          .WithField(actor->GetNodeID())
      << "Send request to kill actor to worker at node";
  actor_raylet_client->KillLocalActor(
      request,
      [actor_id = actor->GetActorID()](const ray::Status &status,
                                       rpc::KillLocalActorReply &&reply) {
        if (!status.ok()) {
          RAY_LOG(ERROR) << "Failed to kill actor " << actor_id
                         << ", return status: " << status.ToString();
        } else {
          RAY_LOG(INFO) << "Killed actor " << actor_id << " successfully.";
        }
      });
}

void GcsActorManager::KillActor(const ActorID &actor_id, bool force_kill) {
  RAY_LOG(DEBUG).WithField(actor_id.JobId()).WithField(actor_id)
      << "Killing actor, force_kill = " << force_kill;
  auto it = registered_actors_.find(actor_id);
  if (it == registered_actors_.end()) {
    RAY_LOG(INFO) << "Tried to kill actor that does not exist " << actor_id;
    return;
  }

  const auto &actor = it->second;
  if (actor->GetState() == rpc::ActorTableData::DEAD ||
      actor->GetState() == rpc::ActorTableData::DEPENDENCIES_UNREADY) {
    return;
  }

  // The actor is still alive or pending creation.
  const auto &node_id = actor->GetNodeID();
  const auto &worker_id = actor->GetWorkerID();
  auto node_it = created_actors_.find(node_id);
  if (node_it != created_actors_.end() && node_it->second.count(worker_id)) {
    // The actor has already been created. Destroy the process by force-killing
    // it.
    NotifyRayletToKillActor(
        actor, GenKilledByApplicationCause(GetActor(actor_id)), force_kill);
  } else {
    const auto &lease_id = actor->GetLeaseSpecification().LeaseId();
    RAY_LOG(DEBUG).WithField(actor->GetActorID()).WithField(lease_id)
        << "The actor hasn't been created yet, cancel scheduling lease";
    if (!worker_id.IsNil()) {
      // The actor is in phase of creating, so we need to notify the core
      // worker exit to avoid process and resource leak.
      NotifyRayletToKillActor(
          actor, GenKilledByApplicationCause(GetActor(actor_id)), force_kill);
    }
    CancelActorInScheduling(actor);
    RestartActor(actor_id,
                 /*need_reschedule=*/true,
                 GenKilledByApplicationCause(GetActor(actor_id)));
  }
}

void GcsActorManager::AddDestroyedActorToCache(const std::shared_ptr<GcsActor> &actor) {
  if (destroyed_actors_.size() >=
      RayConfig::instance().maximum_gcs_destroyed_actor_cached_count()) {
    const auto &actor_id = sorted_destroyed_actor_list_.front().first;
    gcs_table_storage_->ActorTable().Delete(actor_id, {[](auto) {}, io_context_});
    destroyed_actors_.erase(actor_id);
    sorted_destroyed_actor_list_.pop_front();
  }

  if (destroyed_actors_.emplace(actor->GetActorID(), actor).second) {
    sorted_destroyed_actor_list_.emplace_back(
        actor->GetActorID(),
        static_cast<int64_t>(actor->GetActorTableData().timestamp()));
  }
}

void GcsActorManager::CancelActorInScheduling(const std::shared_ptr<GcsActor> &actor) {
  auto lease_id = actor->GetLeaseSpecification().LeaseId();
  RAY_LOG(DEBUG).WithField(actor->GetActorID()).WithField(lease_id)
      << "Cancel actor in scheduling, this may be due to resource re-eviction";
  const auto &actor_id = actor->GetActorID();
  const auto &node_id = actor->GetNodeID();
  // The actor has not been created yet. It is either being scheduled or is
  // pending scheduling.
  auto canceled_actor_id =
      gcs_actor_scheduler_->CancelOnWorker(actor->GetNodeID(), actor->GetWorkerID());
  if (!canceled_actor_id.IsNil()) {
    // The actor was being scheduled and has now been canceled.
    RAY_CHECK(canceled_actor_id == actor_id);
    // Return the actor's acquired resources (if any).
    gcs_actor_scheduler_->OnActorDestruction(actor);
  } else if (!RemovePendingActor(actor)) {
    // When actor creation request of this actor id is pending in raylet,
    // it doesn't responds, and the actor should be still in leasing state.
    // NOTE: We will cancel outstanding lease request by calling
    // `raylet_client->CancelWorkerLease`.
    gcs_actor_scheduler_->CancelOnLeasing(node_id, actor_id, lease_id);
    // Return the actor's acquired resources (if any).
    gcs_actor_scheduler_->OnActorDestruction(actor);
  }
}

const GcsActor *GcsActorManager::GetActor(const ActorID &actor_id) const {
  auto it = registered_actors_.find(actor_id);
  if (it != registered_actors_.end()) {
    return it->second.get();
  }

  it = destroyed_actors_.find(actor_id);
  if (it != destroyed_actors_.end()) {
    return it->second.get();
  }

  return nullptr;
}

bool GcsActorManager::RemovePendingActor(std::shared_ptr<GcsActor> actor) {
  const auto &actor_id = actor->GetActorID();
  auto pending_it = std::find_if(pending_actors_.begin(),
                                 pending_actors_.end(),
                                 [actor_id](const std::shared_ptr<GcsActor> &this_actor) {
                                   return this_actor->GetActorID() == actor_id;
                                 });

  // The actor was pending scheduling. Remove it from the queue.
  if (pending_it != pending_actors_.end()) {
    pending_actors_.erase(pending_it);
    return true;
  }
  return gcs_actor_scheduler_->CancelInFlightActorScheduling(actor);
}

void GcsActorManager::RunAndClearActorCreationCallbacks(
    const std::shared_ptr<GcsActor> &actor,
    const rpc::PushTaskReply &creation_task_reply,
    const Status &creation_task_status) {
  auto iter = actor_to_create_callbacks_.find(actor->GetActorID());
  if (iter != actor_to_create_callbacks_.end()) {
    for (auto &callback : iter->second) {
      callback(actor, creation_task_reply, creation_task_status);
    }
    actor_to_create_callbacks_.erase(iter);
  }
}

size_t GcsActorManager::GetPendingActorsCount() const {
  return gcs_actor_scheduler_->GetPendingActorsCount() + pending_actors_.size();
}

std::string GcsActorManager::DebugString() const {
  uint64_t num_named_actors = 0;
  for (const auto &pair : named_actors_) {
    num_named_actors += pair.second.size();
  }

  std::ostringstream stream;
  stream << "GcsActorManager: "
         << "\n- RegisterActor request count: "
         << counts_[CountType::REGISTER_ACTOR_REQUEST]
         << "\n- CreateActor request count: " << counts_[CountType::CREATE_ACTOR_REQUEST]
         << "\n- GetActorInfo request count: "
         << counts_[CountType::GET_ACTOR_INFO_REQUEST]
         << "\n- GetNamedActorInfo request count: "
         << counts_[CountType::GET_NAMED_ACTOR_INFO_REQUEST]
         << "\n- GetAllActorInfo request count: "
         << counts_[CountType::GET_ALL_ACTOR_INFO_REQUEST]
         << "\n- KillActor request count: " << counts_[CountType::KILL_ACTOR_REQUEST]
         << "\n- ListNamedActors request count: "
         << counts_[CountType::LIST_NAMED_ACTORS_REQUEST]
         << "\n- Registered actors count: " << registered_actors_.size()
         << "\n- Destroyed actors count: " << destroyed_actors_.size()
         << "\n- Named actors count: " << num_named_actors
         << "\n- Unresolved actors count: " << unresolved_actors_.size()
         << "\n- Pending actors count: " << GetPendingActorsCount()
         << "\n- Created actors count: " << created_actors_.size()
         << "\n- owners_: " << owners_.size()
         << "\n- actor_to_register_callbacks_: " << actor_to_register_callbacks_.size()
         << "\n- actor_to_restart_for_lineage_reconstruction_callbacks_: "
         << actor_to_restart_for_lineage_reconstruction_callbacks_.size()
         << "\n- actor_to_create_callbacks_: " << actor_to_create_callbacks_.size()
         << "\n- sorted_destroyed_actor_list_: " << sorted_destroyed_actor_list_.size();
  return stream.str();
}

void GcsActorManager::RecordMetrics() const {
  gcs_actor_by_state_gauge_.Record(registered_actors_.size(), {{"State", "Registered"}});
  gcs_actor_by_state_gauge_.Record(created_actors_.size(), {{"State", "Created"}});
  gcs_actor_by_state_gauge_.Record(destroyed_actors_.size(), {{"State", "Destroyed"}});
  gcs_actor_by_state_gauge_.Record(unresolved_actors_.size(), {{"State", "Unresolved"}});
  gcs_actor_by_state_gauge_.Record(GetPendingActorsCount(), {{"State", "Pending"}});
  if (usage_stats_client_ != nullptr) {
    usage_stats_client_->RecordExtraUsageCounter(usage::TagKey::ACTOR_NUM_CREATED,
                                                 liftime_num_created_actors_);
  }
  actor_state_counter_->FlushOnChangeCallbacks();
}

}  // namespace gcs
}  // namespace ray
