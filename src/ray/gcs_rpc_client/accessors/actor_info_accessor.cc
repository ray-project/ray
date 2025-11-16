// Copyright 2025 The Ray Authors.
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

#include "ray/gcs_rpc_client/accessors/actor_info_accessor.h"

#include <future>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "ray/gcs_rpc_client/rpc_client.h"
#include "ray/pubsub/gcs_subscriber.h"
#include "ray/util/container_util.h"

namespace ray {
namespace gcs {

ActorInfoAccessor::ActorInfoAccessor(GcsClientContext *context) : context_(context) {}

void ActorInfoAccessor::AsyncGet(
    const ActorID &actor_id, const OptionalItemCallback<rpc::ActorTableData> &callback) {
  RAY_LOG(DEBUG).WithField(actor_id).WithField(actor_id.JobId()) << "Getting actor info";
  rpc::GetActorInfoRequest request;
  request.set_actor_id(actor_id.Binary());
  context_->GetGcsRpcClient().GetActorInfo(
      std::move(request),
      [actor_id, callback](const Status &status, rpc::GetActorInfoReply &&reply) {
        if (reply.has_actor_table_data()) {
          callback(status, reply.actor_table_data());
        } else {
          callback(status, std::nullopt);
        }
        RAY_LOG(DEBUG).WithField(actor_id).WithField(actor_id.JobId())
            << "Finished getting actor info, status = " << status;
      });
}

void ActorInfoAccessor::AsyncGetAllByFilter(
    const std::optional<ActorID> &actor_id,
    const std::optional<JobID> &job_id,
    const std::optional<std::string> &actor_state_name,
    const MultiItemCallback<rpc::ActorTableData> &callback,
    int64_t timeout_ms) {
  RAY_LOG(DEBUG) << "Getting all actor info.";
  rpc::GetAllActorInfoRequest request;
  if (actor_id) {
    request.mutable_filters()->set_actor_id(actor_id.value().Binary());
  }
  if (job_id) {
    request.mutable_filters()->set_job_id(job_id.value().Binary());
  }
  if (actor_state_name) {
    static absl::flat_hash_map<std::string, rpc::ActorTableData::ActorState>
        actor_state_map = {
            {"DEPENDENCIES_UNREADY", rpc::ActorTableData::DEPENDENCIES_UNREADY},
            {"PENDING_CREATION", rpc::ActorTableData::PENDING_CREATION},
            {"ALIVE", rpc::ActorTableData::ALIVE},
            {"RESTARTING", rpc::ActorTableData::RESTARTING},
            {"DEAD", rpc::ActorTableData::DEAD}};
    request.mutable_filters()->set_state(actor_state_map[*actor_state_name]);
  }

  context_->GetGcsRpcClient().GetAllActorInfo(
      std::move(request),
      [callback](const Status &status, rpc::GetAllActorInfoReply &&reply) {
        callback(status,
                 VectorFromProtobuf(std::move(*reply.mutable_actor_table_data())));
        RAY_LOG(DEBUG) << "Finished getting all actor info, status = " << status;
      },
      timeout_ms);
}

void ActorInfoAccessor::AsyncGetByName(
    const std::string &name,
    const std::string &ray_namespace,
    const OptionalItemCallback<rpc::ActorTableData> &callback,
    int64_t timeout_ms) {
  RAY_LOG(DEBUG) << "Getting actor info, name = " << name;
  rpc::GetNamedActorInfoRequest request;
  request.set_name(name);
  request.set_ray_namespace(ray_namespace);
  context_->GetGcsRpcClient().GetNamedActorInfo(
      std::move(request),
      [name, callback](const Status &status, rpc::GetNamedActorInfoReply &&reply) {
        if (reply.has_actor_table_data()) {
          callback(status, reply.actor_table_data());
        } else {
          callback(status, std::nullopt);
        }
        RAY_LOG(DEBUG) << "Finished getting actor info, status = " << status
                       << ", name = " << name;
      },
      timeout_ms);
}

Status ActorInfoAccessor::SyncGetByName(const std::string &name,
                                        const std::string &ray_namespace,
                                        rpc::ActorTableData &actor_table_data,
                                        rpc::TaskSpec &task_spec) {
  rpc::GetNamedActorInfoRequest request;
  rpc::GetNamedActorInfoReply reply;
  request.set_name(name);
  request.set_ray_namespace(ray_namespace);
  auto status = context_->GetGcsRpcClient().SyncGetNamedActorInfo(
      std::move(request), &reply, rpc::GetGcsTimeoutMs());
  if (status.ok()) {
    actor_table_data = std::move(*reply.mutable_actor_table_data());
    task_spec = std::move(*reply.mutable_task_spec());
  }
  return status;
}

Status ActorInfoAccessor::SyncListNamedActors(
    bool all_namespaces,
    const std::string &ray_namespace,
    std::vector<std::pair<std::string, std::string>> &actors) {
  rpc::ListNamedActorsRequest request;
  request.set_all_namespaces(all_namespaces);
  request.set_ray_namespace(ray_namespace);
  rpc::ListNamedActorsReply reply;
  auto status = context_->GetGcsRpcClient().SyncListNamedActors(
      std::move(request), &reply, rpc::GetGcsTimeoutMs());
  if (!status.ok()) {
    return status;
  }
  actors.reserve(reply.named_actors_list_size());
  for (auto &actor_info :
       VectorFromProtobuf(std::move(*reply.mutable_named_actors_list()))) {
    actors.emplace_back(std::move(*actor_info.mutable_ray_namespace()),
                        std::move(*actor_info.mutable_name()));
  }
  return status;
}

void ActorInfoAccessor::AsyncRestartActorForLineageReconstruction(
    const ray::ActorID &actor_id,
    uint64_t num_restarts_due_to_lineage_reconstruction,
    const ray::gcs::StatusCallback &callback,
    int64_t timeout_ms) {
  rpc::RestartActorForLineageReconstructionRequest request;
  request.set_actor_id(actor_id.Binary());
  request.set_num_restarts_due_to_lineage_reconstruction(
      num_restarts_due_to_lineage_reconstruction);
  context_->GetGcsRpcClient().RestartActorForLineageReconstruction(
      std::move(request),
      [callback](const Status &status,
                 rpc::RestartActorForLineageReconstructionReply &&reply) {
        callback(status);
      },
      timeout_ms);
}

namespace {

// TODO(dayshah): Yes this is temporary. https://github.com/ray-project/ray/issues/54327
Status ComputeGcsStatus(const Status &grpc_status, const rpc::GcsStatus &gcs_status) {
  // If gRPC status is ok return the GCS status, otherwise return the gRPC status.
  if (grpc_status.ok()) {
    return gcs_status.code() == static_cast<int>(StatusCode::OK)
               ? Status::OK()
               : Status(StatusCode(gcs_status.code()), gcs_status.message());
  } else {
    return grpc_status;
  }
}

}  // namespace

void ActorInfoAccessor::AsyncRegisterActor(const ray::TaskSpecification &task_spec,
                                           const ray::gcs::StatusCallback &callback,
                                           int64_t timeout_ms) {
  RAY_CHECK(task_spec.IsActorCreationTask() && callback);
  rpc::RegisterActorRequest request;
  request.mutable_task_spec()->CopyFrom(task_spec.GetMessage());
  context_->GetGcsRpcClient().RegisterActor(
      std::move(request),
      [callback](const Status &status, rpc::RegisterActorReply &&reply) {
        callback(ComputeGcsStatus(status, reply.status()));
      },
      timeout_ms);
}

Status ActorInfoAccessor::SyncRegisterActor(const ray::TaskSpecification &task_spec) {
  RAY_CHECK(task_spec.IsActorCreationTask());
  rpc::RegisterActorRequest request;
  rpc::RegisterActorReply reply;
  request.mutable_task_spec()->CopyFrom(task_spec.GetMessage());
  auto status = context_->GetGcsRpcClient().SyncRegisterActor(
      std::move(request), &reply, rpc::GetGcsTimeoutMs());
  return ComputeGcsStatus(status, reply.status());
}

void ActorInfoAccessor::AsyncKillActor(const ActorID &actor_id,
                                       bool force_kill,
                                       bool no_restart,
                                       const ray::gcs::StatusCallback &callback,
                                       int64_t timeout_ms) {
  rpc::KillActorViaGcsRequest request;
  request.set_actor_id(actor_id.Binary());
  request.set_force_kill(force_kill);
  request.set_no_restart(no_restart);
  context_->GetGcsRpcClient().KillActorViaGcs(
      std::move(request),
      [callback](const Status &status, rpc::KillActorViaGcsReply &&reply) {
        if (callback) {
          callback(status);
        }
      },
      timeout_ms);
}

void ActorInfoAccessor::AsyncCreateActor(
    const ray::TaskSpecification &task_spec,
    const rpc::ClientCallback<rpc::CreateActorReply> &callback) {
  RAY_CHECK(task_spec.IsActorCreationTask() && callback);
  rpc::CreateActorRequest request;
  request.mutable_task_spec()->CopyFrom(task_spec.GetMessage());
  context_->GetGcsRpcClient().CreateActor(
      std::move(request),
      [callback](const Status &status, rpc::CreateActorReply &&reply) {
        callback(status, std::move(reply));
      });
}

void ActorInfoAccessor::AsyncReportActorOutOfScope(
    const ActorID &actor_id,
    uint64_t num_restarts_due_to_lineage_reconstruction,
    const StatusCallback &callback,
    int64_t timeout_ms) {
  rpc::ReportActorOutOfScopeRequest request;
  request.set_actor_id(actor_id.Binary());
  request.set_num_restarts_due_to_lineage_reconstruction(
      num_restarts_due_to_lineage_reconstruction);
  context_->GetGcsRpcClient().ReportActorOutOfScope(
      std::move(request),
      [callback](const Status &status, rpc::ReportActorOutOfScopeReply &&reply) {
        if (callback) {
          callback(status);
        }
      },
      timeout_ms);
}

void ActorInfoAccessor::AsyncSubscribe(
    const ActorID &actor_id,
    const SubscribeCallback<ActorID, rpc::ActorTableData> &subscribe,
    const StatusCallback &done) {
  RAY_LOG(DEBUG).WithField(actor_id).WithField(actor_id.JobId())
      << "Subscribing update operations of actor";
  RAY_CHECK(subscribe != nullptr) << "Failed to subscribe actor, actor id = " << actor_id;

  auto fetch_data_operation =
      [this, actor_id, subscribe](const StatusCallback &fetch_done) {
        auto callback = [actor_id, subscribe, fetch_done](
                            const Status &status,
                            std::optional<rpc::ActorTableData> &&result) {
          if (result) {
            subscribe(actor_id, std::move(*result));
          }
          if (fetch_done) {
            fetch_done(status);
          }
        };
        AsyncGet(actor_id, callback);
      };

  {
    absl::MutexLock lock(&mutex_);
    resubscribe_operations_[actor_id] = [this, actor_id, subscribe](
                                            const StatusCallback &subscribe_done) {
      context_->GetGcsSubscriber().SubscribeActor(actor_id, subscribe, subscribe_done);
    };
    fetch_data_operations_[actor_id] = fetch_data_operation;
  }

  context_->GetGcsSubscriber().SubscribeActor(
      actor_id, subscribe, [fetch_data_operation, done](const Status &) {
        fetch_data_operation(done);
      });
}

void ActorInfoAccessor::AsyncUnsubscribe(const ActorID &actor_id) {
  RAY_LOG(DEBUG).WithField(actor_id).WithField(actor_id.JobId())
      << "Cancelling subscription to an actor";
  context_->GetGcsSubscriber().UnsubscribeActor(actor_id);
  absl::MutexLock lock(&mutex_);
  resubscribe_operations_.erase(actor_id);
  fetch_data_operations_.erase(actor_id);
  RAY_LOG(DEBUG).WithField(actor_id).WithField(actor_id.JobId())
      << "Finished cancelling subscription to an actor";
}

void ActorInfoAccessor::AsyncResubscribe() {
  RAY_LOG(DEBUG) << "Reestablishing subscription for actor info.";
  // If only the GCS sever has restarted, we only need to fetch data from the GCS server.
  // If the pub-sub server has also restarted, we need to resubscribe to the pub-sub
  // server first, then fetch data from the GCS server.
  absl::MutexLock lock(&mutex_);
  for (auto &[actor_id, resubscribe_op] : resubscribe_operations_) {
    resubscribe_op([this, id = actor_id](const Status &status) {
      absl::MutexLock callback_lock(&mutex_);
      auto fetch_data_operation = fetch_data_operations_[id];
      // `fetch_data_operation` is called in the callback function of subscribe.
      // Before that, if the user calls `AsyncUnsubscribe` function, the corresponding
      // fetch function will be deleted, so we need to check if it's null.
      if (fetch_data_operation != nullptr) {
        fetch_data_operation(nullptr);
      }
    });
  }
}

bool ActorInfoAccessor::IsActorUnsubscribed(const ActorID &actor_id) {
  return context_->GetGcsSubscriber().IsActorUnsubscribed(actor_id);
}

}  // namespace gcs
}  // namespace ray
