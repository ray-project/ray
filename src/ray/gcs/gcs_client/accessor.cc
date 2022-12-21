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
#include "ray/gcs/gcs_client/gcs_client.h"

namespace {
inline int64_t GetGcsTimeoutMs() {
  return absl::ToInt64Milliseconds(
      absl::Seconds(RayConfig::instance().gcs_server_request_timeout_seconds()));
}
}  // namespace

namespace ray {
namespace gcs {

using namespace ray::rpc;

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
      [job_id, data_ptr, callback](const Status &status, const rpc::AddJobReply &reply) {
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
      [job_id, callback](const Status &status, const rpc::MarkJobFinishedReply &reply) {
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
    auto callback = [subscribe, done](
                        const Status &status,
                        const std::vector<rpc::JobTableData> &job_info_list) {
      for (auto &job_info : job_info_list) {
        subscribe(JobID::FromBinary(job_info.job_id()), job_info);
      }
      if (done) {
        done(status);
      }
    };
    RAY_CHECK_OK(AsyncGetAll(callback));
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
    const MultiItemCallback<rpc::JobTableData> &callback) {
  RAY_LOG(DEBUG) << "Getting all job info.";
  RAY_CHECK(callback);
  rpc::GetAllJobInfoRequest request;
  client_impl_->GetGcsRpcClient().GetAllJobInfo(
      request, [callback](const Status &status, const rpc::GetAllJobInfoReply &reply) {
        callback(status, VectorFromProtobuf(reply.job_info_list()));
        RAY_LOG(DEBUG) << "Finished getting all job info.";
      });
  return Status::OK();
}

Status JobInfoAccessor::AsyncGetNextJobID(const ItemCallback<JobID> &callback) {
  RAY_LOG(DEBUG) << "Getting next job id";
  rpc::GetNextJobIDRequest request;
  client_impl_->GetGcsRpcClient().GetNextJobID(
      request, [callback](const Status &status, const rpc::GetNextJobIDReply &reply) {
        RAY_CHECK_OK(status);
        auto job_id = JobID::FromInt(reply.job_id());
        callback(job_id);
        RAY_LOG(DEBUG) << "Finished getting next job id = " << job_id;
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
      [actor_id, callback](const Status &status, const rpc::GetActorInfoReply &reply) {
        if (reply.has_actor_table_data()) {
          callback(status, reply.actor_table_data());
        } else {
          callback(status, boost::none);
        }
        RAY_LOG(DEBUG) << "Finished getting actor info, status = " << status
                       << ", actor id = " << actor_id
                       << ", job id = " << actor_id.JobId();
      });
  return Status::OK();
}

Status ActorInfoAccessor::AsyncGetAll(
    const MultiItemCallback<rpc::ActorTableData> &callback) {
  RAY_LOG(DEBUG) << "Getting all actor info.";
  rpc::GetAllActorInfoRequest request;
  client_impl_->GetGcsRpcClient().GetAllActorInfo(
      request, [callback](const Status &status, const rpc::GetAllActorInfoReply &reply) {
        callback(status, VectorFromProtobuf(reply.actor_table_data()));
        RAY_LOG(DEBUG) << "Finished getting all actor info, status = " << status;
      });
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
      [name, callback](const Status &status, const rpc::GetNamedActorInfoReply &reply) {
        if (reply.has_actor_table_data()) {
          callback(status, reply.actor_table_data());
        } else {
          callback(status, boost::none);
        }
        RAY_LOG(DEBUG) << "Finished getting actor info, status = " << status
                       << ", name = " << name;
      },
      /*timeout_ms*/ timeout_ms);
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
      request, &reply, /*timeout_ms*/ GetGcsTimeoutMs());
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
      [callback](const Status &status, const rpc::ListNamedActorsReply &reply) {
        if (!status.ok()) {
          callback(status, boost::none);
        } else {
          callback(status, VectorFromProtobuf(reply.named_actors_list()));
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

  for (const auto &actor_info : VectorFromProtobuf(reply.named_actors_list())) {
    actors.push_back(std::make_pair(actor_info.ray_namespace(), actor_info.name()));
  }
  return status;
}

Status ActorInfoAccessor::AsyncRegisterActor(const ray::TaskSpecification &task_spec,
                                             const ray::gcs::StatusCallback &callback,
                                             int64_t timeout_ms) {
  RAY_CHECK(task_spec.IsActorCreationTask() && callback);
  rpc::RegisterActorRequest request;
  request.mutable_task_spec()->CopyFrom(task_spec.GetMessage());
  client_impl_->GetGcsRpcClient().RegisterActor(
      request,
      [callback](const Status & /*unused*/, const rpc::RegisterActorReply &reply) {
        auto status =
            reply.status().code() == (int)StatusCode::OK
                ? Status()
                : Status(StatusCode(reply.status().code()), reply.status().message());
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
                                         const ray::gcs::StatusCallback &callback) {
  rpc::KillActorViaGcsRequest request;
  request.set_actor_id(actor_id.Binary());
  request.set_force_kill(force_kill);
  request.set_no_restart(no_restart);
  client_impl_->GetGcsRpcClient().KillActorViaGcs(
      request,
      [callback](const Status & /*unused*/, const rpc::KillActorViaGcsReply &reply) {
        if (callback) {
          auto status =
              reply.status().code() == (int)StatusCode::OK
                  ? Status()
                  : Status(StatusCode(reply.status().code()), reply.status().message());
          callback(status);
        }
      });
  return Status::OK();
}

Status ActorInfoAccessor::AsyncCreateActor(
    const ray::TaskSpecification &task_spec,
    const rpc::ClientCallback<rpc::CreateActorReply> &callback) {
  RAY_CHECK(task_spec.IsActorCreationTask() && callback);
  rpc::CreateActorRequest request;
  request.mutable_task_spec()->CopyFrom(task_spec.GetMessage());
  client_impl_->GetGcsRpcClient().CreateActor(
      request, [callback](const Status & /*unused*/, const rpc::CreateActorReply &reply) {
        auto status =
            reply.status().code() == (int)StatusCode::OK
                ? Status()
                : Status(StatusCode(reply.status().code()), reply.status().message());
        callback(status, reply);
      });
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
                            const boost::optional<rpc::ActorTableData> &result) {
          if (result) {
            subscribe(actor_id, *result);
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
                                                 const rpc::RegisterNodeReply &reply) {
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

Status NodeInfoAccessor::DrainSelf() {
  if (local_node_id_.IsNil()) {
    RAY_LOG(INFO) << "The node is already drained.";
    // This node is already drained.
    return Status::OK();
  }
  NodeID node_id = NodeID::FromBinary(local_node_info_.node_id());
  RAY_LOG(INFO) << "Unregistering node info, node id = " << node_id;
  rpc::DrainNodeRequest request;
  auto draining_request = request.add_drain_node_data();
  draining_request->set_node_id(local_node_info_.node_id());
  client_impl_->GetGcsRpcClient().DrainNode(
      request, [this, node_id](const Status &status, const rpc::DrainNodeReply &reply) {
        if (status.ok()) {
          local_node_info_.set_state(GcsNodeInfo::DEAD);
          local_node_id_ = NodeID::Nil();
        }
        RAY_LOG(INFO) << "Finished unregistering node info, status = " << status
                      << ", node id = " << node_id;
      });
  return Status::OK();
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
      request,
      [node_id, callback](const Status &status, const rpc::RegisterNodeReply &reply) {
        if (callback) {
          callback(status);
        }
        RAY_LOG(DEBUG) << "Finished registering node info, status = " << status
                       << ", node id = " << node_id;
      });
  return Status::OK();
}

Status NodeInfoAccessor::AsyncDrainNode(const NodeID &node_id,
                                        const StatusCallback &callback) {
  RAY_LOG(DEBUG) << "Draining node, node id = " << node_id;
  rpc::DrainNodeRequest request;
  auto draining_request = request.add_drain_node_data();
  draining_request->set_node_id(node_id.Binary());
  client_impl_->GetGcsRpcClient().DrainNode(
      request,
      [node_id, callback](const Status &status, const rpc::DrainNodeReply &reply) {
        if (callback) {
          callback(status);
        }
        RAY_LOG(DEBUG) << "Finished draining node, status = " << status
                       << ", node id = " << node_id;
      });
  return Status::OK();
}

Status NodeInfoAccessor::AsyncGetAll(const MultiItemCallback<GcsNodeInfo> &callback) {
  RAY_LOG(DEBUG) << "Getting information of all nodes.";
  rpc::GetAllNodeInfoRequest request;
  client_impl_->GetGcsRpcClient().GetAllNodeInfo(
      request, [callback](const Status &status, const rpc::GetAllNodeInfoReply &reply) {
        std::vector<GcsNodeInfo> result;
        result.reserve((reply.node_info_list_size()));
        for (int index = 0; index < reply.node_info_list_size(); ++index) {
          result.emplace_back(reply.node_info_list(index));
        }
        callback(status, std::move(result));
        RAY_LOG(DEBUG) << "Finished getting information of all nodes, status = "
                       << status;
      });
  return Status::OK();
}

Status NodeInfoAccessor::AsyncSubscribeToNodeChange(
    const SubscribeCallback<NodeID, GcsNodeInfo> &subscribe, const StatusCallback &done) {
  RAY_CHECK(subscribe != nullptr);
  RAY_CHECK(node_change_callback_ == nullptr);
  node_change_callback_ = subscribe;

  fetch_node_data_operation_ = [this](const StatusCallback &done) {
    auto callback = [this, done](const Status &status,
                                 const std::vector<GcsNodeInfo> &node_info_list) {
      for (auto &node_info : node_info_list) {
        HandleNotification(node_info);
      }
      if (done) {
        done(status);
      }
    };
    RAY_CHECK_OK(AsyncGetAll(callback));
  };

  subscribe_node_operation_ = [this](const StatusCallback &done) {
    auto on_subscribe = [this](const GcsNodeInfo &data) { HandleNotification(data); };
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

bool NodeInfoAccessor::IsRemoved(const NodeID &node_id) const {
  return removed_nodes_.count(node_id) == 1;
}

Status NodeInfoAccessor::AsyncReportHeartbeat(
    const std::shared_ptr<rpc::HeartbeatTableData> &data_ptr,
    const StatusCallback &callback) {
  rpc::ReportHeartbeatRequest request;
  request.mutable_heartbeat()->CopyFrom(*data_ptr);
  static auto *rpc_client = [this]() -> rpc::GcsRpcClient * {
    auto io_service = new instrumented_io_context;
    auto client_call_manager = new rpc::ClientCallManager(*io_service);
    new boost::asio::io_service::work(*io_service);
    new std::thread([io_service]() { io_service->run(); });
    const auto addr = client_impl_->GetGcsServerAddress();
    return new rpc::GcsRpcClient(addr.first, addr.second, *client_call_manager);
  }();
  rpc_client->ReportHeartbeat(
      request, [callback](const Status &status, const rpc::ReportHeartbeatReply &reply) {
        if (callback) {
          callback(status);
        }
      });
  return Status::OK();
}

void NodeInfoAccessor::HandleNotification(const GcsNodeInfo &node_info) {
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
    node = node_info;
  } else {
    node.set_node_id(node_info.node_id());
    node.set_state(rpc::GcsNodeInfo::DEAD);
    node.set_timestamp(node_info.timestamp());
  }

  // If the notification is new, call registered callback.
  if (is_notif_new) {
    if (is_alive) {
      RAY_CHECK(removed_nodes_.find(node_id) == removed_nodes_.end());
    } else {
      removed_nodes_.insert(node_id);
    }
    GcsNodeInfo &cache_data = node_cache_[node_id];
    if (node_change_callback_) {
      node_change_callback_(node_id, cache_data);
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
      request,
      [callback](const Status &status, const rpc::GetInternalConfigReply &reply) {
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

Status NodeResourceInfoAccessor::AsyncGetResources(
    const NodeID &node_id, const OptionalItemCallback<ResourceMap> &callback) {
  RAY_LOG(DEBUG) << "Getting node resources, node id = " << node_id;
  rpc::GetResourcesRequest request;
  request.set_node_id(node_id.Binary());
  client_impl_->GetGcsRpcClient().GetResources(
      request,
      [node_id, callback](const Status &status, const rpc::GetResourcesReply &reply) {
        ResourceMap resource_map;
        for (const auto &resource : reply.resources()) {
          resource_map[resource.first] =
              std::make_shared<rpc::ResourceTableData>(resource.second);
        }
        callback(status, resource_map);
        RAY_LOG(DEBUG) << "Finished getting node resources, status = " << status
                       << ", node id = " << node_id;
      });
  return Status::OK();
}

Status NodeResourceInfoAccessor::AsyncGetAllAvailableResources(
    const MultiItemCallback<rpc::AvailableResources> &callback) {
  rpc::GetAllAvailableResourcesRequest request;
  client_impl_->GetGcsRpcClient().GetAllAvailableResources(
      request,
      [callback](const Status &status, const rpc::GetAllAvailableResourcesReply &reply) {
        callback(status, VectorFromProtobuf(reply.resources_list()));
        RAY_LOG(DEBUG) << "Finished getting available resources of all nodes, status = "
                       << status;
      });
  return Status::OK();
}

Status NodeResourceInfoAccessor::AsyncReportResourceUsage(
    const std::shared_ptr<rpc::ResourcesData> &data_ptr, const StatusCallback &callback) {
  absl::MutexLock lock(&mutex_);
  last_resource_usage_ = std::make_shared<NodeResources>(
      ResourceMapToNodeResources(MapFromProtobuf(data_ptr->resources_total()),
                                 MapFromProtobuf(data_ptr->resources_available())));
  cached_resource_usage_.mutable_resources()->CopyFrom(*data_ptr);
  client_impl_->GetGcsRpcClient().ReportResourceUsage(
      cached_resource_usage_,
      [callback](const Status &status, const rpc::ReportResourceUsageReply &reply) {
        if (callback) {
          callback(status);
        }
      });
  return Status::OK();
}

void NodeResourceInfoAccessor::AsyncReReportResourceUsage() {
  absl::MutexLock lock(&mutex_);
  if (cached_resource_usage_.has_resources()) {
    RAY_LOG(INFO) << "Rereport resource usage.";
    FillResourceUsageRequest(cached_resource_usage_);
    client_impl_->GetGcsRpcClient().ReportResourceUsage(
        cached_resource_usage_,
        [](const Status &status, const rpc::ReportResourceUsageReply &reply) {});
  }
}

void NodeResourceInfoAccessor::FillResourceUsageRequest(
    rpc::ReportResourceUsageRequest &resources) {
  NodeResources cached_resources = *GetLastResourceUsage();

  auto resources_data = resources.mutable_resources();
  resources_data->clear_resources_total();
  for (const auto &resource_pair : cached_resources.total.ToResourceMap()) {
    (*resources_data->mutable_resources_total())[resource_pair.first] =
        resource_pair.second;
  }

  resources_data->clear_resources_available();
  resources_data->set_resources_available_changed(true);
  for (const auto &resource_pair : cached_resources.available.ToResourceMap()) {
    (*resources_data->mutable_resources_available())[resource_pair.first] =
        resource_pair.second;
  }

  resources_data->clear_resource_load();
  resources_data->set_resource_load_changed(true);
  for (const auto &resource_pair : cached_resources.load.ToResourceMap()) {
    (*resources_data->mutable_resource_load())[resource_pair.first] =
        resource_pair.second;
  }
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
      request,
      [callback](const Status &status, const rpc::GetAllResourceUsageReply &reply) {
        callback(reply.resource_usage_data());
        RAY_LOG(DEBUG) << "Finished getting resource usage of all nodes, status = "
                       << status;
      });
  return Status::OK();
}

StatsInfoAccessor::StatsInfoAccessor(GcsClient *client_impl)
    : client_impl_(client_impl) {}

Status StatsInfoAccessor::AsyncAddProfileData(
    const std::shared_ptr<rpc::ProfileTableData> &data_ptr,
    const StatusCallback &callback) {
  NodeID node_id = NodeID::FromBinary(data_ptr->component_id());
  RAY_LOG(DEBUG) << "Adding profile data, component type = " << data_ptr->component_type()
                 << ", node id = " << node_id;
  rpc::AddProfileDataRequest request;
  request.mutable_profile_data()->CopyFrom(*data_ptr);
  client_impl_->GetGcsRpcClient().AddProfileData(
      request,
      [data_ptr, node_id, callback](const Status &status,
                                    const rpc::AddProfileDataReply &reply) {
        if (callback) {
          callback(status);
        }
        RAY_LOG(DEBUG) << "Finished adding profile data, status = " << status
                       << ", component type = " << data_ptr->component_type()
                       << ", node id = " << node_id;
      });
  return Status::OK();
}

Status StatsInfoAccessor::AsyncGetAll(
    const MultiItemCallback<rpc::ProfileTableData> &callback) {
  RAY_LOG(DEBUG) << "Getting all profile info.";
  RAY_CHECK(callback);
  rpc::GetAllProfileInfoRequest request;
  client_impl_->GetGcsRpcClient().GetAllProfileInfo(
      request,
      [callback](const Status &status, const rpc::GetAllProfileInfoReply &reply) {
        callback(status, VectorFromProtobuf(reply.profile_info_list()));
        RAY_LOG(DEBUG) << "Finished getting all job info.";
      });
  return Status::OK();
}

Status TaskInfoAccessor::AsyncAddTaskEventData(
    std::unique_ptr<rpc::TaskEventData> data_ptr, StatusCallback callback) {
  RAY_LOG(DEBUG) << "Adding task events." << data_ptr->DebugString();
  rpc::AddTaskEventDataRequest request;
  // Prevent copy here
  request.mutable_data()->Swap(data_ptr.get());
  client_impl_->GetGcsRpcClient().AddTaskEventData(
      request, [callback](const Status &status, const rpc::AddTaskEventDataReply &reply) {
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
      request, [callback](const Status &status, const rpc::GetTaskEventsReply &reply) {
        callback(status, VectorFromProtobuf(reply.events_by_task()));
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
      [job_id, callback](const Status &status, const rpc::ReportJobErrorReply &reply) {
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
                                 const rpc::ReportWorkerFailureReply &reply) {
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
      [worker_id, callback](const Status &status, const rpc::GetWorkerInfoReply &reply) {
        if (reply.has_worker_table_data()) {
          callback(status, reply.worker_table_data());
        } else {
          callback(status, boost::none);
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
      request, [callback](const Status &status, const rpc::GetAllWorkerInfoReply &reply) {
        callback(status, VectorFromProtobuf(reply.worker_table_data()));
        RAY_LOG(DEBUG) << "Finished getting all worker info, status = " << status;
      });
  return Status::OK();
}

Status WorkerInfoAccessor::AsyncAdd(const std::shared_ptr<rpc::WorkerTableData> &data_ptr,
                                    const StatusCallback &callback) {
  rpc::AddWorkerInfoRequest request;
  request.mutable_worker_data()->CopyFrom(*data_ptr);
  client_impl_->GetGcsRpcClient().AddWorkerInfo(
      request, [callback](const Status &status, const rpc::AddWorkerInfoReply &reply) {
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
                                     const rpc::GetPlacementGroupReply &reply) {
        if (reply.has_placement_group_table_data()) {
          callback(status, reply.placement_group_table_data());
        } else {
          callback(status, boost::none);
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
      [name, callback](const Status &status,
                       const rpc::GetNamedPlacementGroupReply &reply) {
        if (reply.has_placement_group_table_data()) {
          callback(status, reply.placement_group_table_data());
        } else {
          callback(status, boost::none);
        }
        RAY_LOG(DEBUG) << "Finished getting named placement group info, status = "
                       << status << ", name = " << name;
      },
      /*timeout_ms*/ timeout_ms);
  return Status::OK();
}

Status PlacementGroupInfoAccessor::AsyncGetAll(
    const MultiItemCallback<rpc::PlacementGroupTableData> &callback) {
  RAY_LOG(DEBUG) << "Getting all placement group info.";
  rpc::GetAllPlacementGroupRequest request;
  client_impl_->GetGcsRpcClient().GetAllPlacementGroup(
      request,
      [callback](const Status &status, const rpc::GetAllPlacementGroupReply &reply) {
        callback(status, VectorFromProtobuf(reply.placement_group_table_data()));
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
    const OptionalItemCallback<std::string> &callback) {
  rpc::InternalKVGetRequest req;
  req.set_key(key);
  req.set_namespace_(ns);
  client_impl_->GetGcsRpcClient().InternalKVGet(
      req,
      [callback](const Status &status, const rpc::InternalKVGetReply &reply) {
        if (reply.status().code() == (int)StatusCode::NotFound) {
          callback(status, boost::none);
        } else {
          callback(status, reply.value());
        }
      },
      /*timeout_ms*/ GetGcsTimeoutMs());
  return Status::OK();
}

Status InternalKVAccessor::AsyncInternalKVPut(const std::string &ns,
                                              const std::string &key,
                                              const std::string &value,
                                              bool overwrite,
                                              const OptionalItemCallback<int> &callback) {
  rpc::InternalKVPutRequest req;
  req.set_namespace_(ns);
  req.set_key(key);
  req.set_value(value);
  req.set_overwrite(overwrite);
  client_impl_->GetGcsRpcClient().InternalKVPut(
      req,
      [callback](const Status &status, const rpc::InternalKVPutReply &reply) {
        callback(status, reply.added_num());
      },
      /*timeout_ms*/ GetGcsTimeoutMs());
  return Status::OK();
}

Status InternalKVAccessor::AsyncInternalKVExists(
    const std::string &ns,
    const std::string &key,
    const OptionalItemCallback<bool> &callback) {
  rpc::InternalKVExistsRequest req;
  req.set_namespace_(ns);
  req.set_key(key);
  client_impl_->GetGcsRpcClient().InternalKVExists(
      req,
      [callback](const Status &status, const rpc::InternalKVExistsReply &reply) {
        callback(status, reply.exists());
      },
      /*timeout_ms*/ GetGcsTimeoutMs());
  return Status::OK();
}

Status InternalKVAccessor::AsyncInternalKVDel(const std::string &ns,
                                              const std::string &key,
                                              bool del_by_prefix,
                                              const StatusCallback &callback) {
  rpc::InternalKVDelRequest req;
  req.set_namespace_(ns);
  req.set_key(key);
  client_impl_->GetGcsRpcClient().InternalKVDel(
      req,
      [callback](const Status &status, const rpc::InternalKVDelReply &reply) {
        callback(status);
      },
      /*timeout_ms*/ GetGcsTimeoutMs());
  return Status::OK();
}

Status InternalKVAccessor::AsyncInternalKVKeys(
    const std::string &ns,
    const std::string &prefix,
    const OptionalItemCallback<std::vector<std::string>> &callback) {
  rpc::InternalKVKeysRequest req;
  req.set_namespace_(ns);
  req.set_prefix(prefix);
  client_impl_->GetGcsRpcClient().InternalKVKeys(
      req,
      [callback](const Status &status, const rpc::InternalKVKeysReply &reply) {
        if (!status.ok()) {
          callback(status, boost::none);
        } else {
          callback(status, VectorFromProtobuf(reply.results()));
        }
      },
      /*timeout_ms*/ GetGcsTimeoutMs());
  return Status::OK();
}

Status InternalKVAccessor::Put(const std::string &ns,
                               const std::string &key,
                               const std::string &value,
                               bool overwrite,
                               bool &added) {
  std::promise<Status> ret_promise;
  RAY_CHECK_OK(AsyncInternalKVPut(
      ns,
      key,
      value,
      overwrite,
      [&ret_promise, &added](Status status, boost::optional<int> added_num) {
        added = static_cast<bool>(added_num.value_or(0));
        ret_promise.set_value(status);
      }));
  return ret_promise.get_future().get();
}

Status InternalKVAccessor::Keys(const std::string &ns,
                                const std::string &prefix,
                                std::vector<std::string> &value) {
  std::promise<Status> ret_promise;
  RAY_CHECK_OK(AsyncInternalKVKeys(
      ns, prefix, [&ret_promise, &value](Status status, auto &values) {
        value = values.value_or(std::vector<std::string>());
        ret_promise.set_value(status);
      }));
  return ret_promise.get_future().get();
}

Status InternalKVAccessor::Get(const std::string &ns,
                               const std::string &key,
                               std::string &value) {
  std::promise<Status> ret_promise;
  RAY_CHECK_OK(
      AsyncInternalKVGet(ns, key, [&ret_promise, &value](Status status, auto &v) {
        if (v) {
          value = *v;
        }
        ret_promise.set_value(status);
      }));
  return ret_promise.get_future().get();
}

Status InternalKVAccessor::Del(const std::string &ns,
                               const std::string &key,
                               bool del_by_prefix) {
  std::promise<Status> ret_promise;
  RAY_CHECK_OK(AsyncInternalKVDel(ns, key, del_by_prefix, [&ret_promise](Status status) {
    ret_promise.set_value(status);
  }));
  return ret_promise.get_future().get();
}

Status InternalKVAccessor::Exists(const std::string &ns,
                                  const std::string &key,
                                  bool &exist) {
  std::promise<Status> ret_promise;
  RAY_CHECK_OK(AsyncInternalKVExists(
      ns, key, [&ret_promise, &exist](Status status, const boost::optional<bool> &value) {
        if (value) {
          exist = *value;
        }
        ret_promise.set_value(status);
      }));
  return ret_promise.get_future().get();
}

}  // namespace gcs
}  // namespace ray
