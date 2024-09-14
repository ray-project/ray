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

#include "ray/gcs/gcs_client/accessor.h"

#include <future>

#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/common_protocol.h"
#include "ray/gcs/gcs_client/gcs_client.h"

namespace ray {
namespace gcs {

using namespace ray::rpc;

int64_t GetGcsTimeoutMs() {
  return absl::ToInt64Milliseconds(
      absl::Seconds(RayConfig::instance().gcs_server_request_timeout_seconds()));
}

JobInfoAccessor::JobInfoAccessor(GcsClient *client_impl) : client_impl_(client_impl) {}

Status JobInfoAccessor::AsyncAdd(const std::shared_ptr<JobTableData> &data_ptr,
                                 const StatusCallback &callback) {
  JobID job_id = JobID::FromBinary(data_ptr->job_id());
  RAY_LOG(DEBUG) << "Adding job, job id = " << job_id
                 << ", driver pid = " << data_ptr->driver_pid();
  rpc::AddJobRequest request;
  request.mutable_data()->CopyFrom(*data_ptr);
  client_impl_->GetGcsRpcClient().AddJob(
      request,
      [job_id, data_ptr, callback](const Status &status, rpc::AddJobReply &&reply) {
        if (callback) {
          callback(status);
        }
        RAY_LOG(DEBUG) << "Finished adding job, status = " << status
                       << ", job id = " << job_id
                       << ", driver pid = " << data_ptr->driver_pid();
      });
  return Status::OK();
}

Status JobInfoAccessor::AsyncMarkFinished(const JobID &job_id,
                                          const StatusCallback &callback) {
  RAY_LOG(DEBUG) << "Marking job state, job id = " << job_id;
  rpc::MarkJobFinishedRequest request;
  request.set_job_id(job_id.Binary());
  client_impl_->GetGcsRpcClient().MarkJobFinished(
      request,
      [job_id, callback](const Status &status, rpc::MarkJobFinishedReply &&reply) {
        if (callback) {
          callback(status);
        }
        RAY_LOG(DEBUG) << "Finished marking job state, status = " << status
                       << ", job id = " << job_id;
      });
  return Status::OK();
}

Status JobInfoAccessor::AsyncSubscribeAll(
    const SubscribeCallback<JobID, JobTableData> &subscribe, const StatusCallback &done) {
  RAY_CHECK(subscribe != nullptr);
  fetch_all_data_operation_ = [this, subscribe](const StatusCallback &done) {
    auto callback = [subscribe, done](const Status &status,
                                      std::vector<rpc::JobTableData> &&job_info_list) {
      for (auto &job_info : job_info_list) {
        subscribe(JobID::FromBinary(job_info.job_id()), std::move(job_info));
      }
      if (done) {
        done(status);
      }
    };
    RAY_CHECK_OK(AsyncGetAll(/*job_or_submission_id=*/std::nullopt,
                             /*skip_submission_job_info_field=*/true,
                             /*skip_is_running_tasks_field=*/true,
                             callback,
                             /*timeout_ms=*/-1));
  };
  subscribe_operation_ = [this, subscribe](const StatusCallback &done) {
    return client_impl_->GetGcsSubscriber().SubscribeAllJobs(subscribe, done);
  };
  return subscribe_operation_(
      [this, done](const Status &status) { fetch_all_data_operation_(done); });
}

void JobInfoAccessor::AsyncResubscribe() {
  RAY_LOG(DEBUG) << "Reestablishing subscription for job info.";
  auto fetch_all_done = [](const Status &status) {
    RAY_LOG(INFO) << "Finished fetching all job information from gcs server after gcs "
                     "server or pub-sub server is restarted.";
  };

  if (subscribe_operation_ != nullptr) {
    RAY_CHECK_OK(subscribe_operation_([this, fetch_all_done](const Status &status) {
      fetch_all_data_operation_(fetch_all_done);
    }));
  }
}

Status JobInfoAccessor::AsyncGetAll(
    const std::optional<std::string> &job_or_submission_id,
    bool skip_submission_job_info_field,
    bool skip_is_running_tasks_field,
    const MultiItemCallback<rpc::JobTableData> &callback,
    int64_t timeout_ms) {
  RAY_LOG(DEBUG) << "Getting all job info.";
  RAY_CHECK(callback);
  rpc::GetAllJobInfoRequest request;
  request.set_skip_submission_job_info_field(skip_submission_job_info_field);
  request.set_skip_is_running_tasks_field(skip_is_running_tasks_field);
  if (job_or_submission_id.has_value()) {
    request.set_job_or_submission_id(job_or_submission_id.value());
  }
  client_impl_->GetGcsRpcClient().GetAllJobInfo(
      request,
      [callback](const Status &status, rpc::GetAllJobInfoReply &&reply) {
        callback(status, VectorFromProtobuf(std::move(*reply.mutable_job_info_list())));
        RAY_LOG(DEBUG) << "Finished getting all job info.";
      },
      timeout_ms);
  return Status::OK();
}

Status JobInfoAccessor::GetAll(const std::optional<std::string> &job_or_submission_id,
                               bool skip_submission_job_info_field,
                               bool skip_is_running_tasks_field,
                               std::vector<rpc::JobTableData> &job_data_list,
                               int64_t timeout_ms) {
  rpc::GetAllJobInfoRequest request;
  request.set_skip_submission_job_info_field(skip_submission_job_info_field);
  request.set_skip_is_running_tasks_field(skip_is_running_tasks_field);
  if (job_or_submission_id.has_value()) {
    request.set_job_or_submission_id(job_or_submission_id.value());
  }
  rpc::GetAllJobInfoReply reply;
  RAY_RETURN_NOT_OK(
      client_impl_->GetGcsRpcClient().SyncGetAllJobInfo(request, &reply, timeout_ms));
  job_data_list = VectorFromProtobuf(std::move(*reply.mutable_job_info_list()));
  return Status::OK();
}

Status JobInfoAccessor::AsyncGetNextJobID(const ItemCallback<JobID> &callback) {
  RAY_LOG(DEBUG) << "Getting next job id";
  rpc::GetNextJobIDRequest request;
  client_impl_->GetGcsRpcClient().GetNextJobID(
      request, [callback](const Status &status, rpc::GetNextJobIDReply &&reply) {
        RAY_CHECK_OK(status);
        auto job_id = JobID::FromInt(reply.job_id());
        RAY_LOG(DEBUG) << "Finished getting next job id = " << job_id;
        callback(std::move(job_id));
      });
  return Status::OK();
}

ActorInfoAccessor::ActorInfoAccessor(GcsClient *client_impl)
    : client_impl_(client_impl) {}

Status ActorInfoAccessor::AsyncGet(
    const ActorID &actor_id, const OptionalItemCallback<rpc::ActorTableData> &callback) {
  RAY_LOG(DEBUG) << "Getting actor info, actor id = " << actor_id
                 << ", job id = " << actor_id.JobId();
  rpc::GetActorInfoRequest request;
  request.set_actor_id(actor_id.Binary());
  client_impl_->GetGcsRpcClient().GetActorInfo(
      request,
      [actor_id, callback](const Status &status, rpc::GetActorInfoReply &&reply) {
        if (reply.has_actor_table_data()) {
          callback(status, reply.actor_table_data());
        } else {
          callback(status, std::nullopt);
        }
        RAY_LOG(DEBUG) << "Finished getting actor info, status = " << status
                       << ", actor id = " << actor_id
                       << ", job id = " << actor_id.JobId();
      });
  return Status::OK();
}

Status ActorInfoAccessor::AsyncGetAllByFilter(
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
    rpc::ActorTableData::ActorState actor_state =
        StringToActorState(actor_state_name.value());
    request.mutable_filters()->set_state(actor_state);
  }

  client_impl_->GetGcsRpcClient().GetAllActorInfo(
      request,
      [callback](const Status &status, rpc::GetAllActorInfoReply &&reply) {
        callback(status,
                 VectorFromProtobuf(std::move(*reply.mutable_actor_table_data())));
        RAY_LOG(DEBUG) << "Finished getting all actor info, status = " << status;
      },
      timeout_ms);
  return Status::OK();
}

Status ActorInfoAccessor::AsyncGetByName(
    const std::string &name,
    const std::string &ray_namespace,
    const OptionalItemCallback<rpc::ActorTableData> &callback,
    int64_t timeout_ms) {
  RAY_LOG(DEBUG) << "Getting actor info, name = " << name;
  rpc::GetNamedActorInfoRequest request;
  request.set_name(name);
  request.set_ray_namespace(ray_namespace);
  client_impl_->GetGcsRpcClient().GetNamedActorInfo(
      request,
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
  return Status::OK();
}

Status ActorInfoAccessor::SyncGetByName(const std::string &name,
                                        const std::string &ray_namespace,
                                        rpc::ActorTableData &actor_table_data,
                                        rpc::TaskSpec &task_spec) {
  rpc::GetNamedActorInfoRequest request;
  rpc::GetNamedActorInfoReply reply;
  request.set_name(name);
  request.set_ray_namespace(ray_namespace);
  auto status = client_impl_->GetGcsRpcClient().SyncGetNamedActorInfo(
      request, &reply, GetGcsTimeoutMs());
  if (status.ok()) {
    actor_table_data = reply.actor_table_data();
    task_spec = reply.task_spec();
  }
  return status;
}

Status ActorInfoAccessor::AsyncListNamedActors(
    bool all_namespaces,
    const std::string &ray_namespace,
    const OptionalItemCallback<std::vector<rpc::NamedActorInfo>> &callback,
    int64_t timeout_ms) {
  RAY_LOG(DEBUG) << "Listing actors";
  rpc::ListNamedActorsRequest request;
  request.set_all_namespaces(all_namespaces);
  request.set_ray_namespace(ray_namespace);
  client_impl_->GetGcsRpcClient().ListNamedActors(
      request,
      [callback](const Status &status, rpc::ListNamedActorsReply &&reply) {
        if (!status.ok()) {
          callback(status, std::nullopt);
        } else {
          callback(status,
                   VectorFromProtobuf(std::move(*reply.mutable_named_actors_list())));
        }
        RAY_LOG(DEBUG) << "Finished getting named actor names, status = " << status;
      },
      timeout_ms);
  return Status::OK();
}

Status ActorInfoAccessor::SyncListNamedActors(
    bool all_namespaces,
    const std::string &ray_namespace,
    std::vector<std::pair<std::string, std::string>> &actors) {
  rpc::ListNamedActorsRequest request;
  request.set_all_namespaces(all_namespaces);
  request.set_ray_namespace(ray_namespace);
  rpc::ListNamedActorsReply reply;
  auto status = client_impl_->GetGcsRpcClient().SyncListNamedActors(
      request, &reply, GetGcsTimeoutMs());
  if (!status.ok()) {
    return status;
  }

  for (const auto &actor_info :
       VectorFromProtobuf(std::move(*reply.mutable_named_actors_list()))) {
    actors.push_back(std::make_pair(actor_info.ray_namespace(), actor_info.name()));
  }
  return status;
}

Status ActorInfoAccessor::AsyncRestartActor(const ray::ActorID &actor_id,
                                            uint64_t num_restarts,
                                            const ray::gcs::StatusCallback &callback,
                                            int64_t timeout_ms) {
  rpc::RestartActorRequest request;
  request.set_actor_id(actor_id.Binary());
  request.set_num_restarts(num_restarts);
  client_impl_->GetGcsRpcClient().RestartActor(
      request,
      [callback](const Status &status, rpc::RestartActorReply &&reply) {
        callback(status);
      },
      timeout_ms);
  return Status::OK();
}

Status ActorInfoAccessor::AsyncRegisterActor(const ray::TaskSpecification &task_spec,
                                             const ray::gcs::StatusCallback &callback,
                                             int64_t timeout_ms) {
  RAY_CHECK(task_spec.IsActorCreationTask() && callback);
  rpc::RegisterActorRequest request;
  request.mutable_task_spec()->CopyFrom(task_spec.GetMessage());
  client_impl_->GetGcsRpcClient().RegisterActor(
      request,
      [callback](const Status &status, rpc::RegisterActorReply &&reply) {
        callback(status);
      },
      timeout_ms);
  return Status::OK();
}

Status ActorInfoAccessor::SyncRegisterActor(const ray::TaskSpecification &task_spec) {
  RAY_CHECK(task_spec.IsActorCreationTask());
  rpc::RegisterActorRequest request;
  rpc::RegisterActorReply reply;
  request.mutable_task_spec()->CopyFrom(task_spec.GetMessage());
  auto status = client_impl_->GetGcsRpcClient().SyncRegisterActor(
      request, &reply, GetGcsTimeoutMs());
  return status;
}

Status ActorInfoAccessor::AsyncKillActor(const ActorID &actor_id,
                                         bool force_kill,
                                         bool no_restart,
                                         const ray::gcs::StatusCallback &callback,
                                         int64_t timeout_ms) {
  rpc::KillActorViaGcsRequest request;
  request.set_actor_id(actor_id.Binary());
  request.set_force_kill(force_kill);
  request.set_no_restart(no_restart);
  client_impl_->GetGcsRpcClient().KillActorViaGcs(
      request,
      [callback](const Status &status, rpc::KillActorViaGcsReply &&reply) {
        if (callback) {
          callback(status);
        }
      },
      timeout_ms);
  return Status::OK();
}

Status ActorInfoAccessor::AsyncCreateActor(
    const ray::TaskSpecification &task_spec,
    const rpc::ClientCallback<rpc::CreateActorReply> &callback) {
  RAY_CHECK(task_spec.IsActorCreationTask() && callback);
  rpc::CreateActorRequest request;
  request.mutable_task_spec()->CopyFrom(task_spec.GetMessage());
  client_impl_->GetGcsRpcClient().CreateActor(
      request, [callback](const Status &status, rpc::CreateActorReply &&reply) {
        callback(status, std::move(reply));
      });
  return Status::OK();
}

Status ActorInfoAccessor::AsyncReportActorOutOfScope(
    const ActorID &actor_id,
    uint64_t num_restarts_due_to_lineage_reconstruction,
    const StatusCallback &callback,
    int64_t timeout_ms) {
  rpc::ReportActorOutOfScopeRequest request;
  request.set_actor_id(actor_id.Binary());
  request.set_num_restarts_due_to_lineage_reconstruction(
      num_restarts_due_to_lineage_reconstruction);
  client_impl_->GetGcsRpcClient().ReportActorOutOfScope(
      request,
      [callback](const Status &status, rpc::ReportActorOutOfScopeReply &&reply) {
        if (callback) {
          callback(status);
        }
      },
      timeout_ms);
  return Status::OK();
}

Status ActorInfoAccessor::AsyncSubscribe(
    const ActorID &actor_id,
    const SubscribeCallback<ActorID, rpc::ActorTableData> &subscribe,
    const StatusCallback &done) {
  RAY_LOG(DEBUG) << "Subscribing update operations of actor, actor id = " << actor_id
                 << ", job id = " << actor_id.JobId();
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
        RAY_CHECK_OK(AsyncGet(actor_id, callback));
      };

  {
    absl::MutexLock lock(&mutex_);
    resubscribe_operations_[actor_id] =
        [this, actor_id, subscribe](const StatusCallback &subscribe_done) {
          return client_impl_->GetGcsSubscriber().SubscribeActor(
              actor_id, subscribe, subscribe_done);
        };
    fetch_data_operations_[actor_id] = fetch_data_operation;
  }

  return client_impl_->GetGcsSubscriber().SubscribeActor(
      actor_id, subscribe, [fetch_data_operation, done](const Status &) {
        fetch_data_operation(done);
      });
}

Status ActorInfoAccessor::AsyncUnsubscribe(const ActorID &actor_id) {
  RAY_LOG(DEBUG) << "Cancelling subscription to an actor, actor id = " << actor_id
                 << ", job id = " << actor_id.JobId();
  auto status = client_impl_->GetGcsSubscriber().UnsubscribeActor(actor_id);
  absl::MutexLock lock(&mutex_);
  resubscribe_operations_.erase(actor_id);
  fetch_data_operations_.erase(actor_id);
  RAY_LOG(DEBUG) << "Finished cancelling subscription to an actor, actor id = "
                 << actor_id << ", job id = " << actor_id.JobId();
  return status;
}

void ActorInfoAccessor::AsyncResubscribe() {
  RAY_LOG(DEBUG) << "Reestablishing subscription for actor info.";
  // If only the GCS sever has restarted, we only need to fetch data from the GCS server.
  // If the pub-sub server has also restarted, we need to resubscribe to the pub-sub
  // server first, then fetch data from the GCS server.
  absl::MutexLock lock(&mutex_);
  for (auto &[actor_id, resubscribe_op] : resubscribe_operations_) {
    RAY_CHECK_OK(resubscribe_op([this, actor_id = actor_id](const Status &status) {
      absl::MutexLock lock(&mutex_);
      auto fetch_data_operation = fetch_data_operations_[actor_id];
      // `fetch_data_operation` is called in the callback function of subscribe.
      // Before that, if the user calls `AsyncUnsubscribe` function, the corresponding
      // fetch function will be deleted, so we need to check if it's null.
      if (fetch_data_operation != nullptr) {
        fetch_data_operation(nullptr);
      }
    }));
  }
}

bool ActorInfoAccessor::IsActorUnsubscribed(const ActorID &actor_id) {
  return client_impl_->GetGcsSubscriber().IsActorUnsubscribed(actor_id);
}

NodeInfoAccessor::NodeInfoAccessor(GcsClient *client_impl) : client_impl_(client_impl) {}

Status NodeInfoAccessor::RegisterSelf(const GcsNodeInfo &local_node_info,
                                      const StatusCallback &callback) {
  auto node_id = NodeID::FromBinary(local_node_info.node_id());
  RAY_LOG(DEBUG) << "Registering node info, node id = " << node_id
                 << ", address is = " << local_node_info.node_manager_address();
  RAY_CHECK(local_node_id_.IsNil()) << "This node is already connected.";
  RAY_CHECK(local_node_info.state() == GcsNodeInfo::ALIVE);
  rpc::RegisterNodeRequest request;
  request.mutable_node_info()->CopyFrom(local_node_info);
  client_impl_->GetGcsRpcClient().RegisterNode(
      request,
      [this, node_id, local_node_info, callback](const Status &status,
                                                 rpc::RegisterNodeReply &&reply) {
        if (status.ok()) {
          local_node_info_.CopyFrom(local_node_info);
          local_node_id_ = NodeID::FromBinary(local_node_info.node_id());
        }
        if (callback) {
          callback(status);
        }
        RAY_LOG(DEBUG) << "Finished registering node info, status = " << status
                       << ", node id = " << node_id;
      });

  return Status::OK();
}

void NodeInfoAccessor::UnregisterSelf(const rpc::NodeDeathInfo &node_death_info,
                                      std::function<void()> unregister_done_callback) {
  if (local_node_id_.IsNil()) {
    RAY_LOG(INFO) << "The node is already unregistered.";
    return;
  }
  auto node_id = NodeID::FromBinary(local_node_info_.node_id());
  RAY_LOG(INFO) << "Unregistering node, node id = " << node_id;

  rpc::UnregisterNodeRequest request;
  request.set_node_id(local_node_info_.node_id());
  request.mutable_node_death_info()->CopyFrom(node_death_info);
  client_impl_->GetGcsRpcClient().UnregisterNode(
      request,
      [this, node_id, unregister_done_callback](const Status &status,
                                                rpc::UnregisterNodeReply &&reply) {
        if (status.ok()) {
          local_node_info_.set_state(GcsNodeInfo::DEAD);
          local_node_id_ = NodeID::Nil();
        }
        RAY_LOG(INFO) << "Finished unregistering node info, status = " << status
                      << ", node id = " << node_id;
        unregister_done_callback();
      });
}

const NodeID &NodeInfoAccessor::GetSelfId() const { return local_node_id_; }

const GcsNodeInfo &NodeInfoAccessor::GetSelfInfo() const { return local_node_info_; }

Status NodeInfoAccessor::AsyncRegister(const rpc::GcsNodeInfo &node_info,
                                       const StatusCallback &callback) {
  NodeID node_id = NodeID::FromBinary(node_info.node_id());
  RAY_LOG(DEBUG) << "Registering node info, node id = " << node_id;
  rpc::RegisterNodeRequest request;
  request.mutable_node_info()->CopyFrom(node_info);
  client_impl_->GetGcsRpcClient().RegisterNode(
      request, [node_id, callback](const Status &status, rpc::RegisterNodeReply &&reply) {
        if (callback) {
          callback(status);
        }
        RAY_LOG(DEBUG) << "Finished registering node info, status = " << status
                       << ", node id = " << node_id;
      });
  return Status::OK();
}

Status NodeInfoAccessor::AsyncCheckSelfAlive(
    const std::function<void(Status, bool)> &callback, int64_t timeout_ms = -1) {
  std::vector<std::string> raylet_addresses = {
      local_node_info_.node_manager_address() + ":" +
      std::to_string(local_node_info_.node_manager_port())};

  return AsyncCheckAlive(
      raylet_addresses,
      timeout_ms,
      [callback](const Status &status, const std::vector<bool> &nodes_alive) {
        if (!status.ok()) {
          callback(status, false);
          return;
        } else {
          RAY_CHECK_EQ(nodes_alive.size(), static_cast<size_t>(1));
          callback(status, nodes_alive[0]);
        }
      });
}

Status NodeInfoAccessor::AsyncCheckAlive(const std::vector<std::string> &raylet_addresses,
                                         int64_t timeout_ms,
                                         const MultiItemCallback<bool> &callback) {
  rpc::CheckAliveRequest request;
  for (const auto &raylet_address : raylet_addresses) {
    request.add_raylet_address(raylet_address);
  }
  size_t num_raylets = raylet_addresses.size();
  client_impl_->GetGcsRpcClient().CheckAlive(
      request,
      [num_raylets, callback](const Status &status, rpc::CheckAliveReply &&reply) {
        if (status.ok()) {
          RAY_CHECK_EQ(static_cast<size_t>(reply.raylet_alive().size()), num_raylets);
          std::vector<bool> is_alive;
          is_alive.reserve(num_raylets);
          for (const bool &alive : reply.raylet_alive()) {
            is_alive.push_back(alive);
          }
          callback(status, std::move(is_alive));
        } else {
          callback(status, {});
        }
      },
      timeout_ms);
  return Status::OK();
}

Status NodeInfoAccessor::AsyncDrainNode(const NodeID &node_id,
                                        const StatusCallback &callback) {
  RAY_LOG(DEBUG) << "Draining node, node id = " << node_id;
  rpc::DrainNodeRequest request;
  auto draining_request = request.add_drain_node_data();
  draining_request->set_node_id(node_id.Binary());
  client_impl_->GetGcsRpcClient().DrainNode(
      request, [node_id, callback](const Status &status, rpc::DrainNodeReply &&reply) {
        if (callback) {
          callback(status);
        }
        RAY_LOG(DEBUG) << "Finished draining node, status = " << status
                       << ", node id = " << node_id;
      });
  return Status::OK();
}

Status NodeInfoAccessor::DrainNodes(const std::vector<NodeID> &node_ids,
                                    int64_t timeout_ms,
                                    std::vector<std::string> &drained_node_ids) {
  RAY_LOG(DEBUG) << "Draining nodes, node id = " << debug_string(node_ids);
  rpc::DrainNodeRequest request;
  rpc::DrainNodeReply reply;
  for (const auto &node_id : node_ids) {
    auto draining_request = request.add_drain_node_data();
    draining_request->set_node_id(node_id.Binary());
  }
  RAY_RETURN_NOT_OK(
      client_impl_->GetGcsRpcClient().SyncDrainNode(request, &reply, timeout_ms));
  drained_node_ids.clear();
  for (const auto &s : reply.drain_node_status()) {
    drained_node_ids.push_back(s.node_id());
  }
  return Status::OK();
}

Status NodeInfoAccessor::AsyncGetAll(const MultiItemCallback<GcsNodeInfo> &callback,
                                     int64_t timeout_ms) {
  RAY_LOG(DEBUG) << "Getting information of all nodes.";
  rpc::GetAllNodeInfoRequest request;
  client_impl_->GetGcsRpcClient().GetAllNodeInfo(
      request,
      [callback](const Status &status, rpc::GetAllNodeInfoReply &&reply) {
        std::vector<GcsNodeInfo> result;
        result.reserve((reply.node_info_list_size()));
        for (int index = 0; index < reply.node_info_list_size(); ++index) {
          result.emplace_back(reply.node_info_list(index));
        }
        callback(status, std::move(result));
        RAY_LOG(DEBUG) << "Finished getting information of all nodes, status = "
                       << status;
      },
      timeout_ms);
  return Status::OK();
}

Status NodeInfoAccessor::AsyncSubscribeToNodeChange(
    const SubscribeCallback<NodeID, GcsNodeInfo> &subscribe, const StatusCallback &done) {
  RAY_CHECK(subscribe != nullptr);
  RAY_CHECK(node_change_callback_ == nullptr);
  node_change_callback_ = subscribe;

  fetch_node_data_operation_ = [this](const StatusCallback &done) {
    auto callback = [this, done](const Status &status,
                                 std::vector<GcsNodeInfo> &&node_info_list) {
      for (auto &node_info : node_info_list) {
        HandleNotification(std::move(node_info));
      }
      if (done) {
        done(status);
      }
    };
    RAY_CHECK_OK(AsyncGetAll(callback, /*timeout_ms=*/-1));
  };

  subscribe_node_operation_ = [this](const StatusCallback &done) {
    auto on_subscribe = [this](GcsNodeInfo &&data) {
      HandleNotification(std::move(data));
    };
    return client_impl_->GetGcsSubscriber().SubscribeAllNodeInfo(on_subscribe, done);
  };

  return subscribe_node_operation_([this, subscribe, done](const Status &status) {
    fetch_node_data_operation_(done);
  });
}

const GcsNodeInfo *NodeInfoAccessor::Get(const NodeID &node_id,
                                         bool filter_dead_nodes) const {
  RAY_CHECK(!node_id.IsNil());
  auto entry = node_cache_.find(node_id);
  if (entry != node_cache_.end()) {
    if (filter_dead_nodes && entry->second.state() == rpc::GcsNodeInfo::DEAD) {
      return nullptr;
    }
    return &entry->second;
  }
  return nullptr;
}

const absl::flat_hash_map<NodeID, GcsNodeInfo> &NodeInfoAccessor::GetAll() const {
  return node_cache_;
}

Status NodeInfoAccessor::GetAllNoCache(int64_t timeout_ms,
                                       std::vector<rpc::GcsNodeInfo> &nodes) {
  RAY_LOG(DEBUG) << "Getting information of all nodes.";
  rpc::GetAllNodeInfoRequest request;
  rpc::GetAllNodeInfoReply reply;
  RAY_RETURN_NOT_OK(
      client_impl_->GetGcsRpcClient().SyncGetAllNodeInfo(request, &reply, timeout_ms));
  nodes = VectorFromProtobuf(std::move(*reply.mutable_node_info_list()));
  return Status::OK();
}

Status NodeInfoAccessor::CheckAlive(const std::vector<std::string> &raylet_addresses,
                                    int64_t timeout_ms,
                                    std::vector<bool> &nodes_alive) {
  std::promise<Status> ret_promise;
  RAY_RETURN_NOT_OK(AsyncCheckAlive(
      raylet_addresses,
      timeout_ms,
      [&ret_promise, &nodes_alive](Status status, const std::vector<bool> &alive) {
        nodes_alive = alive;
        ret_promise.set_value(status);
      }));
  return ret_promise.get_future().get();
}

bool NodeInfoAccessor::IsRemoved(const NodeID &node_id) const {
  return removed_nodes_.count(node_id) == 1;
}

void NodeInfoAccessor::HandleNotification(GcsNodeInfo &&node_info) {
  NodeID node_id = NodeID::FromBinary(node_info.node_id());
  bool is_alive = (node_info.state() == GcsNodeInfo::ALIVE);
  auto entry = node_cache_.find(node_id);
  bool is_notif_new;
  if (entry == node_cache_.end()) {
    // If the entry is not in the cache, then the notification is new.
    is_notif_new = true;
  } else {
    // If the entry is in the cache, then the notification is new if the node
    // was alive and is now dead or resources have been updated.
    bool was_alive = (entry->second.state() == GcsNodeInfo::ALIVE);
    is_notif_new = was_alive && !is_alive;

    // Once a node with a given ID has been removed, it should never be added
    // again. If the entry was in the cache and the node was deleted, we should check
    // that this new notification is not an insertion.
    // However, when a new node(node-B) registers with GCS, it subscribes to all node
    // information. It will subscribe to redis and then get all node information from GCS
    // through RPC. If node-A fails after GCS replies to node-B, GCS will send another
    // message(node-A is dead) to node-B through redis publish. Because RPC and redis
    // subscribe are two different sessions, node-B may process node-A dead message first
    // and then node-A alive message. So we use `RAY_LOG` instead of `RAY_CHECK ` as a
    // workaround.
    if (!was_alive && is_alive) {
      RAY_LOG(INFO) << "Notification for addition of a node that was already removed:"
                    << node_id;
      return;
    }
  }

  // Add the notification to our cache.
  RAY_LOG(INFO) << "Received notification for node id = " << node_id
                << ", IsAlive = " << is_alive;

  auto &node = node_cache_[node_id];
  if (is_alive) {
    node = std::move(node_info);
  } else {
    node.set_node_id(node_info.node_id());
    node.set_state(rpc::GcsNodeInfo::DEAD);
    node.set_end_time_ms(node_info.end_time_ms());
  }

  // If the notification is new, call registered callback.
  if (is_notif_new) {
    if (is_alive) {
      RAY_CHECK(removed_nodes_.find(node_id) == removed_nodes_.end());
    } else {
      removed_nodes_.insert(node_id);
    }
    if (node_change_callback_) {
      // Copy happens!
      GcsNodeInfo cache_data_copied = node_cache_[node_id];
      node_change_callback_(node_id, std::move(cache_data_copied));
    }
  }
}

void NodeInfoAccessor::AsyncResubscribe() {
  RAY_LOG(DEBUG) << "Reestablishing subscription for node info.";
  auto fetch_all_done = [](const Status &status) {
    RAY_LOG(INFO) << "Finished fetching all node information from gcs server after gcs "
                     "server or pub-sub server is restarted.";
  };

  if (subscribe_node_operation_ != nullptr) {
    RAY_CHECK_OK(subscribe_node_operation_([this, fetch_all_done](const Status &status) {
      fetch_node_data_operation_(fetch_all_done);
    }));
  }
}

Status NodeInfoAccessor::AsyncGetInternalConfig(
    const OptionalItemCallback<std::string> &callback) {
  rpc::GetInternalConfigRequest request;
  client_impl_->GetGcsRpcClient().GetInternalConfig(
      request, [callback](const Status &status, rpc::GetInternalConfigReply &&reply) {
        if (status.ok()) {
          RAY_LOG(DEBUG) << "Fetched internal config: " << reply.config();
        } else {
          RAY_LOG(ERROR) << "Failed to get internal config: " << status.message();
        }
        callback(status, reply.config());
      });
  return Status::OK();
}

NodeResourceInfoAccessor::NodeResourceInfoAccessor(GcsClient *client_impl)
    : client_impl_(client_impl) {}

Status NodeResourceInfoAccessor::AsyncGetAllAvailableResources(
    const MultiItemCallback<rpc::AvailableResources> &callback) {
  rpc::GetAllAvailableResourcesRequest request;
  client_impl_->GetGcsRpcClient().GetAllAvailableResources(
      request,
      [callback](const Status &status, rpc::GetAllAvailableResourcesReply &&reply) {
        callback(status, VectorFromProtobuf(std::move(*reply.mutable_resources_list())));
        RAY_LOG(DEBUG) << "Finished getting available resources of all nodes, status = "
                       << status;
      });
  return Status::OK();
}

Status NodeResourceInfoAccessor::AsyncGetAllTotalResources(
    const MultiItemCallback<rpc::TotalResources> &callback) {
  rpc::GetAllTotalResourcesRequest request;
  client_impl_->GetGcsRpcClient().GetAllTotalResources(
      request, [callback](const Status &status, rpc::GetAllTotalResourcesReply &&reply) {
        callback(status, VectorFromProtobuf(std::move(*reply.mutable_resources_list())));
        RAY_LOG(DEBUG) << "Finished getting total resources of all nodes, status = "
                       << status;
      });
  return Status::OK();
}

Status NodeResourceInfoAccessor::AsyncGetDrainingNodes(
    const ItemCallback<std::unordered_map<NodeID, int64_t>> &callback) {
  rpc::GetDrainingNodesRequest request;
  client_impl_->GetGcsRpcClient().GetDrainingNodes(
      request, [callback](const Status &status, rpc::GetDrainingNodesReply &&reply) {
        RAY_CHECK_OK(status);
        std::unordered_map<NodeID, int64_t> draining_nodes;
        for (const auto &draining_node : reply.draining_nodes()) {
          draining_nodes[NodeID::FromBinary(draining_node.node_id())] =
              draining_node.draining_deadline_timestamp_ms();
        }
        callback(std::move(draining_nodes));
      });
  return Status::OK();
}

void NodeResourceInfoAccessor::AsyncResubscribe() {
  RAY_LOG(DEBUG) << "Reestablishing subscription for node resource info.";
  if (subscribe_resource_operation_ != nullptr) {
    RAY_CHECK_OK(subscribe_resource_operation_(nullptr));
  }
  if (subscribe_batch_resource_usage_operation_ != nullptr) {
    RAY_CHECK_OK(subscribe_batch_resource_usage_operation_(nullptr));
  }
}

Status NodeResourceInfoAccessor::AsyncGetAllResourceUsage(
    const ItemCallback<rpc::ResourceUsageBatchData> &callback) {
  rpc::GetAllResourceUsageRequest request;
  client_impl_->GetGcsRpcClient().GetAllResourceUsage(
      request, [callback](const Status &status, rpc::GetAllResourceUsageReply &&reply) {
        callback(std::move(*reply.mutable_resource_usage_data()));
        RAY_LOG(DEBUG) << "Finished getting resource usage of all nodes, status = "
                       << status;
      });
  return Status::OK();
}

Status NodeResourceInfoAccessor::GetAllResourceUsage(
    int64_t timeout_ms, rpc::GetAllResourceUsageReply &reply) {
  rpc::GetAllResourceUsageRequest request;
  return client_impl_->GetGcsRpcClient().SyncGetAllResourceUsage(
      request, &reply, timeout_ms);
}

Status TaskInfoAccessor::AsyncAddTaskEventData(
    std::unique_ptr<rpc::TaskEventData> data_ptr, StatusCallback callback) {
  rpc::AddTaskEventDataRequest request;
  // Prevent copy here
  request.mutable_data()->Swap(data_ptr.get());
  client_impl_->GetGcsRpcClient().AddTaskEventData(
      request, [callback](const Status &status, rpc::AddTaskEventDataReply &&reply) {
        if (callback) {
          callback(status);
        }
        RAY_LOG(DEBUG) << "Accessor added task events grpc OK";
      });
  return Status::OK();
}

Status TaskInfoAccessor::AsyncGetTaskEvents(
    const MultiItemCallback<rpc::TaskEvents> &callback) {
  RAY_LOG(DEBUG) << "Getting all task events info.";
  RAY_CHECK(callback);
  rpc::GetTaskEventsRequest request;
  client_impl_->GetGcsRpcClient().GetTaskEvents(
      request, [callback](const Status &status, rpc::GetTaskEventsReply &&reply) {
        callback(status, VectorFromProtobuf(std::move(*reply.mutable_events_by_task())));
      });

  return Status::OK();
}

ErrorInfoAccessor::ErrorInfoAccessor(GcsClient *client_impl)
    : client_impl_(client_impl) {}

Status ErrorInfoAccessor::AsyncReportJobError(
    const std::shared_ptr<rpc::ErrorTableData> &data_ptr,
    const StatusCallback &callback) {
  auto job_id = JobID::FromBinary(data_ptr->job_id());
  RAY_LOG(DEBUG) << "Publishing job error, job id = " << job_id;
  rpc::ReportJobErrorRequest request;
  request.mutable_job_error()->CopyFrom(*data_ptr);
  client_impl_->GetGcsRpcClient().ReportJobError(
      request,
      [job_id, callback](const Status &status, rpc::ReportJobErrorReply &&reply) {
        if (callback) {
          callback(status);
        }
        RAY_LOG(DEBUG) << "Finished publishing job error, job id = " << job_id;
      });
  return Status::OK();
}

WorkerInfoAccessor::WorkerInfoAccessor(GcsClient *client_impl)
    : client_impl_(client_impl) {}

Status WorkerInfoAccessor::AsyncSubscribeToWorkerFailures(
    const ItemCallback<rpc::WorkerDeltaData> &subscribe, const StatusCallback &done) {
  RAY_CHECK(subscribe != nullptr);
  subscribe_operation_ = [this, subscribe](const StatusCallback &done) {
    return client_impl_->GetGcsSubscriber().SubscribeAllWorkerFailures(subscribe, done);
  };
  return subscribe_operation_(done);
}

void WorkerInfoAccessor::AsyncResubscribe() {
  // TODO(iycheng): Fix the case where messages has been pushed to GCS but
  // resubscribe hasn't been done yet. In this case, we'll lose that message.
  RAY_LOG(DEBUG) << "Reestablishing subscription for worker failures.";
  // The pub-sub server has restarted, we need to resubscribe to the pub-sub server.
  if (subscribe_operation_ != nullptr) {
    RAY_CHECK_OK(subscribe_operation_(nullptr));
  }
}

Status WorkerInfoAccessor::AsyncReportWorkerFailure(
    const std::shared_ptr<rpc::WorkerTableData> &data_ptr,
    const StatusCallback &callback) {
  rpc::Address worker_address = data_ptr->worker_address();
  RAY_LOG(DEBUG) << "Reporting worker failure, " << worker_address.DebugString();
  rpc::ReportWorkerFailureRequest request;
  request.mutable_worker_failure()->CopyFrom(*data_ptr);
  client_impl_->GetGcsRpcClient().ReportWorkerFailure(
      request,
      [worker_address, callback](const Status &status,
                                 rpc::ReportWorkerFailureReply &&reply) {
        if (callback) {
          callback(status);
        }
        RAY_LOG(DEBUG) << "Finished reporting worker failure, "
                       << worker_address.DebugString() << ", status = " << status;
      });
  return Status::OK();
}

Status WorkerInfoAccessor::AsyncGet(
    const WorkerID &worker_id,
    const OptionalItemCallback<rpc::WorkerTableData> &callback) {
  RAY_LOG(DEBUG) << "Getting worker info, worker id = " << worker_id;
  rpc::GetWorkerInfoRequest request;
  request.set_worker_id(worker_id.Binary());
  client_impl_->GetGcsRpcClient().GetWorkerInfo(
      request,
      [worker_id, callback](const Status &status, rpc::GetWorkerInfoReply &&reply) {
        if (reply.has_worker_table_data()) {
          callback(status, reply.worker_table_data());
        } else {
          callback(status, std::nullopt);
        }
        RAY_LOG(DEBUG) << "Finished getting worker info, worker id = " << worker_id;
      });
  return Status::OK();
}

Status WorkerInfoAccessor::AsyncGetAll(
    const MultiItemCallback<rpc::WorkerTableData> &callback) {
  RAY_LOG(DEBUG) << "Getting all worker info.";
  rpc::GetAllWorkerInfoRequest request;
  client_impl_->GetGcsRpcClient().GetAllWorkerInfo(
      request, [callback](const Status &status, rpc::GetAllWorkerInfoReply &&reply) {
        callback(status,
                 VectorFromProtobuf(std::move(*reply.mutable_worker_table_data())));
        RAY_LOG(DEBUG) << "Finished getting all worker info, status = " << status;
      });
  return Status::OK();
}

Status WorkerInfoAccessor::AsyncAdd(const std::shared_ptr<rpc::WorkerTableData> &data_ptr,
                                    const StatusCallback &callback) {
  rpc::AddWorkerInfoRequest request;
  request.mutable_worker_data()->CopyFrom(*data_ptr);
  client_impl_->GetGcsRpcClient().AddWorkerInfo(
      request, [callback](const Status &status, rpc::AddWorkerInfoReply &&reply) {
        if (callback) {
          callback(status);
        }
      });
  return Status::OK();
}

Status WorkerInfoAccessor::AsyncUpdateDebuggerPort(const WorkerID &worker_id,
                                                   uint32_t debugger_port,
                                                   const StatusCallback &callback) {
  rpc::UpdateWorkerDebuggerPortRequest request;
  request.set_worker_id(worker_id.Binary());
  request.set_debugger_port(debugger_port);
  RAY_LOG(DEBUG) << "Updating the worker debugger port, worker id = " << worker_id
                 << ", port = " << debugger_port << ".";
  client_impl_->GetGcsRpcClient().UpdateWorkerDebuggerPort(
      request,
      [callback](const Status &status, rpc::UpdateWorkerDebuggerPortReply &&reply) {
        if (callback) {
          callback(status);
        }
      });
  return Status::OK();
}

Status WorkerInfoAccessor::AsyncUpdateWorkerNumPausedThreads(
    const WorkerID &worker_id,
    const int num_paused_threads_delta,
    const StatusCallback &callback) {
  rpc::UpdateWorkerNumPausedThreadsRequest request;
  request.set_worker_id(worker_id.Binary());
  request.set_num_paused_threads_delta(num_paused_threads_delta);
  RAY_LOG(DEBUG) << "Update the num paused threads on worker id = " << worker_id
                 << " by delta = " << num_paused_threads_delta << ".";
  client_impl_->GetGcsRpcClient().UpdateWorkerNumPausedThreads(
      request,
      [callback](const Status &status, rpc::UpdateWorkerNumPausedThreadsReply &&reply) {
        if (callback) {
          callback(status);
        }
      });
  return Status::OK();
}

PlacementGroupInfoAccessor::PlacementGroupInfoAccessor(GcsClient *client_impl)
    : client_impl_(client_impl) {}

Status PlacementGroupInfoAccessor::SyncCreatePlacementGroup(
    const ray::PlacementGroupSpecification &placement_group_spec) {
  rpc::CreatePlacementGroupRequest request;
  rpc::CreatePlacementGroupReply reply;
  request.mutable_placement_group_spec()->CopyFrom(placement_group_spec.GetMessage());
  auto status = client_impl_->GetGcsRpcClient().SyncCreatePlacementGroup(
      request, &reply, GetGcsTimeoutMs());
  if (status.ok()) {
    RAY_LOG(DEBUG) << "Finished registering placement group. placement group id = "
                   << placement_group_spec.PlacementGroupId();
  } else {
    RAY_LOG(ERROR) << "Placement group id = " << placement_group_spec.PlacementGroupId()
                   << " failed to be registered. " << status;
  }
  return status;
}

Status PlacementGroupInfoAccessor::SyncRemovePlacementGroup(
    const ray::PlacementGroupID &placement_group_id) {
  rpc::RemovePlacementGroupRequest request;
  rpc::RemovePlacementGroupReply reply;
  request.set_placement_group_id(placement_group_id.Binary());
  auto status = client_impl_->GetGcsRpcClient().SyncRemovePlacementGroup(
      request, &reply, GetGcsTimeoutMs());
  return status;
}

Status PlacementGroupInfoAccessor::AsyncGet(
    const PlacementGroupID &placement_group_id,
    const OptionalItemCallback<rpc::PlacementGroupTableData> &callback) {
  RAY_LOG(DEBUG) << "Getting placement group info, placement group id = "
                 << placement_group_id;
  rpc::GetPlacementGroupRequest request;
  request.set_placement_group_id(placement_group_id.Binary());
  client_impl_->GetGcsRpcClient().GetPlacementGroup(
      request,
      [placement_group_id, callback](const Status &status,
                                     rpc::GetPlacementGroupReply &&reply) {
        if (reply.has_placement_group_table_data()) {
          callback(status, reply.placement_group_table_data());
        } else {
          callback(status, std::nullopt);
        }
        RAY_LOG(DEBUG) << "Finished getting placement group info, placement group id = "
                       << placement_group_id;
      });
  return Status::OK();
}

Status PlacementGroupInfoAccessor::AsyncGetByName(
    const std::string &name,
    const std::string &ray_namespace,
    const OptionalItemCallback<rpc::PlacementGroupTableData> &callback,
    int64_t timeout_ms) {
  RAY_LOG(DEBUG) << "Getting named placement group info, name = " << name;
  rpc::GetNamedPlacementGroupRequest request;
  request.set_name(name);
  request.set_ray_namespace(ray_namespace);
  client_impl_->GetGcsRpcClient().GetNamedPlacementGroup(
      request,
      [name, callback](const Status &status, rpc::GetNamedPlacementGroupReply &&reply) {
        if (reply.has_placement_group_table_data()) {
          callback(status, reply.placement_group_table_data());
        } else {
          callback(status, std::nullopt);
        }
        RAY_LOG(DEBUG) << "Finished getting named placement group info, status = "
                       << status << ", name = " << name;
      },
      timeout_ms);
  return Status::OK();
}

Status PlacementGroupInfoAccessor::AsyncGetAll(
    const MultiItemCallback<rpc::PlacementGroupTableData> &callback) {
  RAY_LOG(DEBUG) << "Getting all placement group info.";
  rpc::GetAllPlacementGroupRequest request;
  client_impl_->GetGcsRpcClient().GetAllPlacementGroup(
      request, [callback](const Status &status, rpc::GetAllPlacementGroupReply &&reply) {
        callback(
            status,
            VectorFromProtobuf(std::move(*reply.mutable_placement_group_table_data())));
        RAY_LOG(DEBUG) << "Finished getting all placement group info, status = "
                       << status;
      });
  return Status::OK();
}

Status PlacementGroupInfoAccessor::SyncWaitUntilReady(
    const PlacementGroupID &placement_group_id, int64_t timeout_seconds) {
  rpc::WaitPlacementGroupUntilReadyRequest request;
  rpc::WaitPlacementGroupUntilReadyReply reply;
  request.set_placement_group_id(placement_group_id.Binary());
  auto status = client_impl_->GetGcsRpcClient().SyncWaitPlacementGroupUntilReady(
      request, &reply, absl::ToInt64Milliseconds(absl::Seconds(timeout_seconds)));
  RAY_LOG(DEBUG) << "Finished waiting placement group until ready, placement group id = "
                 << placement_group_id;
  return status;
}

InternalKVAccessor::InternalKVAccessor(GcsClient *client_impl)
    : client_impl_(client_impl) {}

Status InternalKVAccessor::AsyncInternalKVGet(
    const std::string &ns,
    const std::string &key,
    const int64_t timeout_ms,
    const OptionalItemCallback<std::string> &callback) {
  rpc::InternalKVGetRequest req;
  req.set_key(key);
  req.set_namespace_(ns);
  client_impl_->GetGcsRpcClient().InternalKVGet(
      req,
      [callback](const Status &status, rpc::InternalKVGetReply &&reply) {
        if (reply.status().code() == (int)StatusCode::NotFound) {
          callback(status, std::nullopt);
        } else {
          callback(status, reply.value());
        }
      },
      timeout_ms);
  return Status::OK();
}

Status InternalKVAccessor::AsyncInternalKVMultiGet(
    const std::string &ns,
    const std::vector<std::string> &keys,
    const int64_t timeout_ms,
    const OptionalItemCallback<std::unordered_map<std::string, std::string>> &callback) {
  rpc::InternalKVMultiGetRequest req;
  for (const auto &key : keys) {
    req.add_keys(key);
  }
  req.set_namespace_(ns);
  client_impl_->GetGcsRpcClient().InternalKVMultiGet(
      req,
      [callback](const Status &status, rpc::InternalKVMultiGetReply &&reply) {
        std::unordered_map<std::string, std::string> map;
        if (!status.ok()) {
          callback(status, map);
        } else {
          // TODO(ryw): reply.status() is not examined. It's never populated in
          // src/ray/gcs/gcs_server/gcs_kv_manager.cc either anyway so it's ok for now.
          // Investigate if we wanna remove that field.
          for (const auto &entry : reply.results()) {
            map[entry.key()] = entry.value();
          }
          callback(Status::OK(), map);
        }
      },
      timeout_ms);
  return Status::OK();
}

Status InternalKVAccessor::AsyncInternalKVPut(const std::string &ns,
                                              const std::string &key,
                                              const std::string &value,
                                              bool overwrite,
                                              const int64_t timeout_ms,
                                              const OptionalItemCallback<int> &callback) {
  rpc::InternalKVPutRequest req;
  req.set_namespace_(ns);
  req.set_key(key);
  req.set_value(value);
  req.set_overwrite(overwrite);
  client_impl_->GetGcsRpcClient().InternalKVPut(
      req,
      [callback](const Status &status, rpc::InternalKVPutReply &&reply) {
        callback(status, reply.added_num());
      },
      timeout_ms);
  return Status::OK();
}

Status InternalKVAccessor::AsyncInternalKVExists(
    const std::string &ns,
    const std::string &key,
    const int64_t timeout_ms,
    const OptionalItemCallback<bool> &callback) {
  rpc::InternalKVExistsRequest req;
  req.set_namespace_(ns);
  req.set_key(key);
  client_impl_->GetGcsRpcClient().InternalKVExists(
      req,
      [callback](const Status &status, rpc::InternalKVExistsReply &&reply) {
        callback(status, reply.exists());
      },
      timeout_ms);
  return Status::OK();
}

Status InternalKVAccessor::AsyncInternalKVDel(const std::string &ns,
                                              const std::string &key,
                                              bool del_by_prefix,
                                              const int64_t timeout_ms,
                                              const OptionalItemCallback<int> &callback) {
  rpc::InternalKVDelRequest req;
  req.set_namespace_(ns);
  req.set_key(key);
  req.set_del_by_prefix(del_by_prefix);
  client_impl_->GetGcsRpcClient().InternalKVDel(
      req,
      [callback](const Status &status, rpc::InternalKVDelReply &&reply) {
        callback(status, reply.deleted_num());
      },
      timeout_ms);
  return Status::OK();
}

Status InternalKVAccessor::AsyncInternalKVKeys(
    const std::string &ns,
    const std::string &prefix,
    const int64_t timeout_ms,
    const OptionalItemCallback<std::vector<std::string>> &callback) {
  rpc::InternalKVKeysRequest req;
  req.set_namespace_(ns);
  req.set_prefix(prefix);
  client_impl_->GetGcsRpcClient().InternalKVKeys(
      req,
      [callback](const Status &status, rpc::InternalKVKeysReply &&reply) {
        if (!status.ok()) {
          callback(status, std::nullopt);
        } else {
          callback(status, VectorFromProtobuf(std::move(*reply.mutable_results())));
        }
      },
      timeout_ms);
  return Status::OK();
}

Status InternalKVAccessor::Put(const std::string &ns,
                               const std::string &key,
                               const std::string &value,
                               bool overwrite,
                               const int64_t timeout_ms,
                               bool &added) {
  std::promise<Status> ret_promise;
  RAY_CHECK_OK(AsyncInternalKVPut(
      ns,
      key,
      value,
      overwrite,
      timeout_ms,
      [&ret_promise, &added](Status status, std::optional<int> added_num) {
        added = static_cast<bool>(added_num.value_or(0));
        ret_promise.set_value(status);
      }));
  return ret_promise.get_future().get();
}

Status InternalKVAccessor::Keys(const std::string &ns,
                                const std::string &prefix,
                                const int64_t timeout_ms,
                                std::vector<std::string> &value) {
  std::promise<Status> ret_promise;
  RAY_CHECK_OK(AsyncInternalKVKeys(
      ns,
      prefix,
      timeout_ms,
      [&ret_promise, &value](Status status,
                             std::optional<std::vector<std::string>> &&values) {
        if (values) {
          value = std::move(*values);
        } else {
          value = std::vector<std::string>();
        }
        ret_promise.set_value(status);
      }));
  return ret_promise.get_future().get();
}

Status InternalKVAccessor::Get(const std::string &ns,
                               const std::string &key,
                               const int64_t timeout_ms,
                               std::string &value) {
  std::promise<Status> ret_promise;
  RAY_CHECK_OK(AsyncInternalKVGet(
      ns,
      key,
      timeout_ms,
      [&ret_promise, &value](Status status, std::optional<std::string> &&v) {
        if (v) {
          value = std::move(v.value());
        } else {
          value.clear();
        }
        ret_promise.set_value(status);
      }));
  return ret_promise.get_future().get();
}

Status InternalKVAccessor::MultiGet(
    const std::string &ns,
    const std::vector<std::string> &keys,
    const int64_t timeout_ms,
    std::unordered_map<std::string, std::string> &values) {
  std::promise<Status> ret_promise;
  RAY_CHECK_OK(AsyncInternalKVMultiGet(
      ns,
      keys,
      timeout_ms,
      [&ret_promise, &values](
          Status status,
          std::optional<std::unordered_map<std::string, std::string>> &&vs) {
        values.clear();
        if (vs) {
          values = std::move(*vs);
        }
        ret_promise.set_value(status);
      }));
  return ret_promise.get_future().get();
}

Status InternalKVAccessor::Del(const std::string &ns,
                               const std::string &key,
                               bool del_by_prefix,
                               const int64_t timeout_ms,
                               int &num_deleted) {
  std::promise<Status> ret_promise;
  RAY_CHECK_OK(AsyncInternalKVDel(
      ns,
      key,
      del_by_prefix,
      timeout_ms,
      [&ret_promise, &num_deleted](Status status, std::optional<int> &&value) {
        num_deleted = value.value_or(0);
        ret_promise.set_value(status);
      }));
  return ret_promise.get_future().get();
}

Status InternalKVAccessor::Exists(const std::string &ns,
                                  const std::string &key,
                                  const int64_t timeout_ms,
                                  bool &exists) {
  std::promise<Status> ret_promise;
  RAY_CHECK_OK(AsyncInternalKVExists(
      ns,
      key,
      timeout_ms,
      [&ret_promise, &exists](Status status, std::optional<bool> &&value) {
        exists = value.value_or(false);
        ret_promise.set_value(status);
      }));
  return ret_promise.get_future().get();
}

RuntimeEnvAccessor::RuntimeEnvAccessor(GcsClient *client_impl)
    : client_impl_(client_impl) {}

Status RuntimeEnvAccessor::PinRuntimeEnvUri(const std::string &uri,
                                            int expiration_s,
                                            int64_t timeout_ms) {
  rpc::PinRuntimeEnvURIRequest request;
  request.set_uri(uri);
  request.set_expiration_s(expiration_s);
  rpc::PinRuntimeEnvURIReply reply;
  auto status =
      client_impl_->GetGcsRpcClient().SyncPinRuntimeEnvURI(request, &reply, timeout_ms);
  return status;
}

AutoscalerStateAccessor::AutoscalerStateAccessor(GcsClient *client_impl)
    : client_impl_(client_impl) {}

Status AutoscalerStateAccessor::RequestClusterResourceConstraint(
    int64_t timeout_ms,
    const std::vector<std::unordered_map<std::string, double>> &bundles,
    const std::vector<int64_t> &count_array) {
  rpc::autoscaler::RequestClusterResourceConstraintRequest request;
  rpc::autoscaler::RequestClusterResourceConstraintReply reply;
  RAY_CHECK_EQ(bundles.size(), count_array.size());
  for (size_t i = 0; i < bundles.size(); ++i) {
    const auto &bundle = bundles[i];
    auto count = count_array[i];

    auto new_resource_requests_by_count =
        request.mutable_cluster_resource_constraint()->add_min_bundles();

    new_resource_requests_by_count->mutable_request()->mutable_resources_bundle()->insert(
        bundle.begin(), bundle.end());
    new_resource_requests_by_count->set_count(count);
  }

  return client_impl_->GetGcsRpcClient().SyncRequestClusterResourceConstraint(
      request, &reply, timeout_ms);
}

Status AutoscalerStateAccessor::GetClusterResourceState(int64_t timeout_ms,
                                                        std::string &serialized_reply) {
  rpc::autoscaler::GetClusterResourceStateRequest request;
  rpc::autoscaler::GetClusterResourceStateReply reply;

  RAY_RETURN_NOT_OK(client_impl_->GetGcsRpcClient().SyncGetClusterResourceState(
      request, &reply, timeout_ms));

  if (!reply.SerializeToString(&serialized_reply)) {
    return Status::IOError("Failed to serialize GetClusterResourceState");
  }
  return Status::OK();
}

Status AutoscalerStateAccessor::GetClusterStatus(int64_t timeout_ms,
                                                 std::string &serialized_reply) {
  rpc::autoscaler::GetClusterStatusRequest request;
  rpc::autoscaler::GetClusterStatusReply reply;

  RAY_RETURN_NOT_OK(
      client_impl_->GetGcsRpcClient().SyncGetClusterStatus(request, &reply, timeout_ms));

  if (!reply.SerializeToString(&serialized_reply)) {
    return Status::IOError("Failed to serialize GetClusterStatusReply");
  }
  return Status::OK();
}

Status AutoscalerStateAccessor::ReportAutoscalingState(
    int64_t timeout_ms, const std::string &serialized_state) {
  rpc::autoscaler::ReportAutoscalingStateRequest request;
  rpc::autoscaler::ReportAutoscalingStateReply reply;

  if (!request.mutable_autoscaling_state()->ParseFromString(serialized_state)) {
    return Status::IOError("Failed to parse ReportAutoscalingState");
  }
  return client_impl_->GetGcsRpcClient().SyncReportAutoscalingState(
      request, &reply, timeout_ms);
}

Status AutoscalerStateAccessor::DrainNode(const std::string &node_id,
                                          int32_t reason,
                                          const std::string &reason_message,
                                          int64_t deadline_timestamp_ms,
                                          int64_t timeout_ms,
                                          bool &is_accepted,
                                          std::string &rejection_reason_message) {
  rpc::autoscaler::DrainNodeRequest request;
  request.set_node_id(NodeID::FromHex(node_id).Binary());
  request.set_reason(static_cast<rpc::autoscaler::DrainNodeReason>(reason));
  request.set_reason_message(reason_message);
  request.set_deadline_timestamp_ms(deadline_timestamp_ms);

  rpc::autoscaler::DrainNodeReply reply;

  RAY_RETURN_NOT_OK(
      client_impl_->GetGcsRpcClient().SyncDrainNode(request, &reply, timeout_ms));

  is_accepted = reply.is_accepted();
  if (!is_accepted) {
    rejection_reason_message = reply.rejection_reason_message();
  }
  return Status::OK();
}

}  // namespace gcs
}  // namespace ray
