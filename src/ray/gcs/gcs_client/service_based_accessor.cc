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

#include "ray/gcs/gcs_client/service_based_accessor.h"

#include "ray/gcs/gcs_client/service_based_gcs_client.h"

namespace ray {
namespace gcs {

using namespace ray::rpc;

ServiceBasedJobInfoAccessor::ServiceBasedJobInfoAccessor(
    ServiceBasedGcsClient *client_impl)
    : client_impl_(client_impl) {}

Status ServiceBasedJobInfoAccessor::AsyncAdd(
    const std::shared_ptr<JobTableData> &data_ptr, const StatusCallback &callback) {
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

Status ServiceBasedJobInfoAccessor::AsyncMarkFinished(const JobID &job_id,
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

Status ServiceBasedJobInfoAccessor::AsyncSubscribeAll(
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
    auto on_subscribe = [subscribe](const std::string &id, const std::string &data) {
      JobTableData job_data;
      job_data.ParseFromString(data);
      subscribe(JobID::FromHex(id), job_data);
    };
    return client_impl_->GetGcsPubSub().SubscribeAll(JOB_CHANNEL, on_subscribe, done);
  };
  return subscribe_operation_(
      [this, done](const Status &status) { fetch_all_data_operation_(done); });
}

void ServiceBasedJobInfoAccessor::AsyncResubscribe(bool is_pubsub_server_restarted) {
  RAY_LOG(DEBUG) << "Reestablishing subscription for job info.";
  auto fetch_all_done = [](const Status &status) {
    RAY_LOG(INFO) << "Finished fetching all job information from gcs server after gcs "
                     "server or pub-sub server is restarted.";
  };

  // If only the GCS sever has restarted, we only need to fetch data from the GCS server.
  // If the pub-sub server has also restarted, we need to resubscribe to the pub-sub
  // server first, then fetch data from the GCS server.
  if (is_pubsub_server_restarted) {
    if (subscribe_operation_ != nullptr) {
      RAY_CHECK_OK(subscribe_operation_([this, fetch_all_done](const Status &status) {
        fetch_all_data_operation_(fetch_all_done);
      }));
    }
  } else {
    if (fetch_all_data_operation_ != nullptr) {
      fetch_all_data_operation_(fetch_all_done);
    }
  }
}

Status ServiceBasedJobInfoAccessor::AsyncGetAll(
    const MultiItemCallback<rpc::JobTableData> &callback) {
  RAY_LOG(DEBUG) << "Getting all job info.";
  RAY_CHECK(callback);
  rpc::GetAllJobInfoRequest request;
  client_impl_->GetGcsRpcClient().GetAllJobInfo(
      request, [callback](const Status &status, const rpc::GetAllJobInfoReply &reply) {
        auto result = VectorFromProtobuf(reply.job_info_list());
        callback(status, result);
        RAY_LOG(DEBUG) << "Finished getting all job info.";
      });
  return Status::OK();
}

ServiceBasedActorInfoAccessor::ServiceBasedActorInfoAccessor(
    ServiceBasedGcsClient *client_impl)
    : client_impl_(client_impl) {}

Status ServiceBasedActorInfoAccessor::AsyncGet(
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

Status ServiceBasedActorInfoAccessor::AsyncGetAll(
    const MultiItemCallback<rpc::ActorTableData> &callback) {
  RAY_LOG(DEBUG) << "Getting all actor info.";
  rpc::GetAllActorInfoRequest request;
  client_impl_->GetGcsRpcClient().GetAllActorInfo(
      request, [callback](const Status &status, const rpc::GetAllActorInfoReply &reply) {
        auto result = VectorFromProtobuf(reply.actor_table_data());
        callback(status, result);
        RAY_LOG(DEBUG) << "Finished getting all actor info, status = " << status;
      });
  return Status::OK();
}

Status ServiceBasedActorInfoAccessor::AsyncGetByName(
    const std::string &name, const std::string &ray_namespace,
    const OptionalItemCallback<rpc::ActorTableData> &callback) {
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
      });
  return Status::OK();
}

Status ServiceBasedActorInfoAccessor::AsyncRegisterActor(
    const ray::TaskSpecification &task_spec, const ray::gcs::StatusCallback &callback) {
  RAY_CHECK(task_spec.IsActorCreationTask() && callback);
  rpc::RegisterActorRequest request;
  request.mutable_task_spec()->CopyFrom(task_spec.GetMessage());
  client_impl_->GetGcsRpcClient().RegisterActor(
      request, [callback](const Status &, const rpc::RegisterActorReply &reply) {
        auto status =
            reply.status().code() == (int)StatusCode::OK
                ? Status()
                : Status(StatusCode(reply.status().code()), reply.status().message());
        callback(status);
      });
  return Status::OK();
}

Status ServiceBasedActorInfoAccessor::AsyncKillActor(
    const ActorID &actor_id, bool force_kill, bool no_restart,
    const ray::gcs::StatusCallback &callback) {
  rpc::KillActorViaGcsRequest request;
  request.set_actor_id(actor_id.Binary());
  request.set_force_kill(force_kill);
  request.set_no_restart(no_restart);
  client_impl_->GetGcsRpcClient().KillActorViaGcs(
      request, [callback](const Status &, const rpc::KillActorViaGcsReply &reply) {
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

Status ServiceBasedActorInfoAccessor::AsyncCreateActor(
    const ray::TaskSpecification &task_spec, const ray::gcs::StatusCallback &callback) {
  RAY_CHECK(task_spec.IsActorCreationTask() && callback);
  rpc::CreateActorRequest request;
  request.mutable_task_spec()->CopyFrom(task_spec.GetMessage());
  client_impl_->GetGcsRpcClient().CreateActor(
      request, [callback](const Status &, const rpc::CreateActorReply &reply) {
        auto status =
            reply.status().code() == (int)StatusCode::OK
                ? Status()
                : Status(StatusCode(reply.status().code()), reply.status().message());
        callback(status);
      });
  return Status::OK();
}

Status ServiceBasedActorInfoAccessor::AsyncSubscribeAll(
    const SubscribeCallback<ActorID, rpc::ActorTableData> &subscribe,
    const StatusCallback &done) {
  RAY_CHECK(subscribe != nullptr);
  fetch_all_data_operation_ = [this, subscribe](const StatusCallback &done) {
    auto callback = [subscribe, done](
                        const Status &status,
                        const std::vector<rpc::ActorTableData> &actor_info_list) {
      for (auto &actor_info : actor_info_list) {
        subscribe(ActorID::FromBinary(actor_info.actor_id()), actor_info);
      }
      if (done) {
        done(status);
      }
    };
    RAY_CHECK_OK(AsyncGetAll(callback));
  };

  subscribe_all_operation_ = [this, subscribe](const StatusCallback &done) {
    auto on_subscribe = [subscribe](const std::string &id, const std::string &data) {
      ActorTableData actor_data;
      actor_data.ParseFromString(data);
      subscribe(ActorID::FromBinary(actor_data.actor_id()), actor_data);
    };
    return client_impl_->GetGcsPubSub().SubscribeAll(ACTOR_CHANNEL, on_subscribe, done);
  };

  return subscribe_all_operation_(
      [this, done](const Status &status) { fetch_all_data_operation_(done); });
}

Status ServiceBasedActorInfoAccessor::AsyncSubscribe(
    const ActorID &actor_id,
    const SubscribeCallback<ActorID, rpc::ActorTableData> &subscribe,
    const StatusCallback &done) {
  RAY_LOG(DEBUG) << "Subscribing update operations of actor, actor id = " << actor_id
                 << ", job id = " << actor_id.JobId();
  RAY_CHECK(subscribe != nullptr) << "Failed to subscribe actor, actor id = " << actor_id;

  auto fetch_data_operation = [this, actor_id,
                               subscribe](const StatusCallback &fetch_done) {
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

  auto subscribe_operation = [this, actor_id,
                              subscribe](const StatusCallback &subscribe_done) {
    auto on_subscribe = [subscribe](const std::string &id, const std::string &data) {
      ActorTableData actor_data;
      actor_data.ParseFromString(data);
      subscribe(ActorID::FromHex(id), actor_data);
    };
    return client_impl_->GetGcsPubSub().Subscribe(ACTOR_CHANNEL, actor_id.Hex(),
                                                  on_subscribe, subscribe_done);
  };

  {
    absl::MutexLock lock(&mutex_);
    subscribe_operations_[actor_id] = subscribe_operation;
    fetch_data_operations_[actor_id] = fetch_data_operation;
  }
  return subscribe_operation(
      [fetch_data_operation, done](const Status &status) { fetch_data_operation(done); });
}

Status ServiceBasedActorInfoAccessor::AsyncUnsubscribe(const ActorID &actor_id) {
  RAY_LOG(DEBUG) << "Cancelling subscription to an actor, actor id = " << actor_id
                 << ", job id = " << actor_id.JobId();
  auto status = client_impl_->GetGcsPubSub().Unsubscribe(ACTOR_CHANNEL, actor_id.Hex());
  absl::MutexLock lock(&mutex_);
  subscribe_operations_.erase(actor_id);
  fetch_data_operations_.erase(actor_id);
  RAY_LOG(DEBUG) << "Finished cancelling subscription to an actor, actor id = "
                 << actor_id << ", job id = " << actor_id.JobId();
  return status;
}

void ServiceBasedActorInfoAccessor::AsyncResubscribe(bool is_pubsub_server_restarted) {
  RAY_LOG(DEBUG) << "Reestablishing subscription for actor info.";
  auto fetch_all_done = [](const Status &status) {
    RAY_LOG(INFO) << "Finished fetching all actor information from gcs server after gcs "
                     "server or pub-sub server is restarted.";
  };

  // If only the GCS sever has restarted, we only need to fetch data from the GCS server.
  // If the pub-sub server has also restarted, we need to resubscribe to the pub-sub
  // server first, then fetch data from the GCS server.
  absl::MutexLock lock(&mutex_);
  if (is_pubsub_server_restarted) {
    if (subscribe_all_operation_ != nullptr) {
      RAY_CHECK_OK(subscribe_all_operation_([this, fetch_all_done](const Status &status) {
        fetch_all_data_operation_(fetch_all_done);
      }));
    }
    for (auto &item : subscribe_operations_) {
      auto &actor_id = item.first;
      RAY_CHECK_OK(item.second([this, actor_id](const Status &status) {
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
  } else {
    if (fetch_all_data_operation_ != nullptr) {
      fetch_all_data_operation_(fetch_all_done);
    }
    for (auto &item : fetch_data_operations_) {
      item.second(nullptr);
    }
  }
}

bool ServiceBasedActorInfoAccessor::IsActorUnsubscribed(const ActorID &actor_id) {
  return client_impl_->GetGcsPubSub().IsUnsubscribed(ACTOR_CHANNEL, actor_id.Hex());
}

ServiceBasedNodeInfoAccessor::ServiceBasedNodeInfoAccessor(
    ServiceBasedGcsClient *client_impl)
    : client_impl_(client_impl) {}

Status ServiceBasedNodeInfoAccessor::RegisterSelf(const GcsNodeInfo &local_node_info,
                                                  const StatusCallback &callback) {
  auto node_id = NodeID::FromBinary(local_node_info.node_id());
  RAY_LOG(DEBUG) << "Registering node info, node id = " << node_id
                 << ", address is = " << local_node_info.node_manager_address();
  RAY_CHECK(local_node_id_.IsNil()) << "This node is already connected.";
  RAY_CHECK(local_node_info.state() == GcsNodeInfo::ALIVE);
  rpc::RegisterNodeRequest request;
  request.mutable_node_info()->CopyFrom(local_node_info);

  client_impl_->GetGcsRpcClient().RegisterNode(
      request, [this, node_id, local_node_info, callback](
                   const Status &status, const rpc::RegisterNodeReply &reply) {
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

Status ServiceBasedNodeInfoAccessor::UnregisterSelf() {
  RAY_CHECK(!local_node_id_.IsNil()) << "This node is disconnected.";
  NodeID node_id = NodeID::FromBinary(local_node_info_.node_id());
  RAY_LOG(INFO) << "Unregistering node info, node id = " << node_id;
  rpc::UnregisterNodeRequest request;
  request.set_node_id(local_node_info_.node_id());
  client_impl_->GetGcsRpcClient().UnregisterNode(
      request,
      [this, node_id](const Status &status, const rpc::UnregisterNodeReply &reply) {
        if (status.ok()) {
          local_node_info_.set_state(GcsNodeInfo::DEAD);
          local_node_id_ = NodeID::Nil();
        }
        RAY_LOG(INFO) << "Finished unregistering node info, status = " << status
                      << ", node id = " << node_id;
      });
  return Status::OK();
}

const NodeID &ServiceBasedNodeInfoAccessor::GetSelfId() const { return local_node_id_; }

const GcsNodeInfo &ServiceBasedNodeInfoAccessor::GetSelfInfo() const {
  return local_node_info_;
}

Status ServiceBasedNodeInfoAccessor::AsyncRegister(const rpc::GcsNodeInfo &node_info,
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

Status ServiceBasedNodeInfoAccessor::AsyncUnregister(const NodeID &node_id,
                                                     const StatusCallback &callback) {
  RAY_LOG(DEBUG) << "Unregistering node info, node id = " << node_id;
  rpc::UnregisterNodeRequest request;
  request.set_node_id(node_id.Binary());
  client_impl_->GetGcsRpcClient().UnregisterNode(
      request,
      [node_id, callback](const Status &status, const rpc::UnregisterNodeReply &reply) {
        if (callback) {
          callback(status);
        }
        RAY_LOG(DEBUG) << "Finished unregistering node info, status = " << status
                       << ", node id = " << node_id;
      });
  return Status::OK();
}

Status ServiceBasedNodeInfoAccessor::AsyncGetAll(
    const MultiItemCallback<GcsNodeInfo> &callback) {
  RAY_LOG(DEBUG) << "Getting information of all nodes.";
  rpc::GetAllNodeInfoRequest request;
  client_impl_->GetGcsRpcClient().GetAllNodeInfo(
      request, [callback](const Status &status, const rpc::GetAllNodeInfoReply &reply) {
        std::vector<GcsNodeInfo> result;
        result.reserve((reply.node_info_list_size()));
        for (int index = 0; index < reply.node_info_list_size(); ++index) {
          result.emplace_back(reply.node_info_list(index));
        }
        callback(status, result);
        RAY_LOG(DEBUG) << "Finished getting information of all nodes, status = "
                       << status;
      });
  return Status::OK();
}

Status ServiceBasedNodeInfoAccessor::AsyncSubscribeToNodeChange(
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
    auto on_subscribe = [this](const std::string &id, const std::string &data) {
      GcsNodeInfo node_info;
      node_info.ParseFromString(data);
      HandleNotification(node_info);
    };
    return client_impl_->GetGcsPubSub().SubscribeAll(NODE_CHANNEL, on_subscribe, done);
  };

  return subscribe_node_operation_([this, subscribe, done](const Status &status) {
    fetch_node_data_operation_(done);
  });
}

boost::optional<GcsNodeInfo> ServiceBasedNodeInfoAccessor::Get(
    const NodeID &node_id, bool filter_dead_nodes) const {
  RAY_CHECK(!node_id.IsNil());
  auto entry = node_cache_.find(node_id);
  if (entry != node_cache_.end()) {
    if (filter_dead_nodes && entry->second.state() == rpc::GcsNodeInfo::DEAD) {
      return boost::none;
    }
    return entry->second;
  }
  return boost::none;
}

const std::unordered_map<NodeID, GcsNodeInfo> &ServiceBasedNodeInfoAccessor::GetAll()
    const {
  return node_cache_;
}

bool ServiceBasedNodeInfoAccessor::IsRemoved(const NodeID &node_id) const {
  return removed_nodes_.count(node_id) == 1;
}

Status ServiceBasedNodeInfoAccessor::AsyncReportHeartbeat(
    const std::shared_ptr<rpc::HeartbeatTableData> &data_ptr,
    const StatusCallback &callback) {
  rpc::ReportHeartbeatRequest request;
  request.mutable_heartbeat()->CopyFrom(*data_ptr);
  client_impl_->GetGcsRpcClient().ReportHeartbeat(
      request, [callback](const Status &status, const rpc::ReportHeartbeatReply &reply) {
        if (callback) {
          callback(status);
        }
      });
  return Status::OK();
}

void ServiceBasedNodeInfoAccessor::HandleNotification(const GcsNodeInfo &node_info) {
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
  if (is_alive) {
    node_cache_[node_id] = node_info;
  } else {
    node_cache_[node_id].set_state(rpc::GcsNodeInfo::DEAD);
    node_cache_[node_id].set_timestamp(node_info.timestamp());
  }

  // If the notification is new, call registered callback.
  if (is_notif_new) {
    if (is_alive) {
      RAY_CHECK(removed_nodes_.find(node_id) == removed_nodes_.end());
    } else {
      removed_nodes_.insert(node_id);
    }
    GcsNodeInfo &cache_data = node_cache_[node_id];
    node_change_callback_(node_id, cache_data);
  }
}

void ServiceBasedNodeInfoAccessor::AsyncResubscribe(bool is_pubsub_server_restarted) {
  RAY_LOG(DEBUG) << "Reestablishing subscription for node info.";
  auto fetch_all_done = [](const Status &status) {
    RAY_LOG(INFO) << "Finished fetching all node information from gcs server after gcs "
                     "server or pub-sub server is restarted.";
  };

  // If only the GCS sever has restarted, we only need to fetch data from the GCS server.
  // If the pub-sub server has also restarted, we need to resubscribe to the pub-sub
  // server first, then fetch data from the GCS server.
  if (is_pubsub_server_restarted) {
    if (subscribe_node_operation_ != nullptr) {
      RAY_CHECK_OK(
          subscribe_node_operation_([this, fetch_all_done](const Status &status) {
            fetch_node_data_operation_(fetch_all_done);
          }));
    }
  } else {
    if (fetch_node_data_operation_ != nullptr) {
      fetch_node_data_operation_(fetch_all_done);
    }
  }
}

Status ServiceBasedNodeInfoAccessor::AsyncGetInternalConfig(
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

ServiceBasedNodeResourceInfoAccessor::ServiceBasedNodeResourceInfoAccessor(
    ServiceBasedGcsClient *client_impl)
    : client_impl_(client_impl) {}

Status ServiceBasedNodeResourceInfoAccessor::AsyncGetResources(
    const NodeID &node_id, const OptionalItemCallback<ResourceMap> &callback) {
  RAY_LOG(DEBUG) << "Getting node resources, node id = " << node_id;
  rpc::GetResourcesRequest request;
  request.set_node_id(node_id.Binary());
  client_impl_->GetGcsRpcClient().GetResources(
      request,
      [node_id, callback](const Status &status, const rpc::GetResourcesReply &reply) {
        ResourceMap resource_map;
        for (auto resource : reply.resources()) {
          resource_map[resource.first] =
              std::make_shared<rpc::ResourceTableData>(resource.second);
        }
        callback(status, resource_map);
        RAY_LOG(DEBUG) << "Finished getting node resources, status = " << status
                       << ", node id = " << node_id;
      });
  return Status::OK();
}

Status ServiceBasedNodeResourceInfoAccessor::AsyncGetAllAvailableResources(
    const MultiItemCallback<rpc::AvailableResources> &callback) {
  rpc::GetAllAvailableResourcesRequest request;
  client_impl_->GetGcsRpcClient().GetAllAvailableResources(
      request,
      [callback](const Status &status, const rpc::GetAllAvailableResourcesReply &reply) {
        std::vector<rpc::AvailableResources> result =
            VectorFromProtobuf(reply.resources_list());
        callback(status, result);
        RAY_LOG(DEBUG) << "Finished getting available resources of all nodes, status = "
                       << status;
      });
  return Status::OK();
}

Status ServiceBasedNodeResourceInfoAccessor::AsyncUpdateResources(
    const NodeID &node_id, const ResourceMap &resources, const StatusCallback &callback) {
  RAY_LOG(DEBUG) << "Updating node resources, node id = " << node_id;
  rpc::UpdateResourcesRequest request;
  request.set_node_id(node_id.Binary());
  for (auto &resource : resources) {
    (*request.mutable_resources())[resource.first] = *resource.second;
  }

  auto operation = [this, request, node_id,
                    callback](const SequencerDoneCallback &done_callback) {
    client_impl_->GetGcsRpcClient().UpdateResources(
        request, [node_id, callback, done_callback](
                     const Status &status, const rpc::UpdateResourcesReply &reply) {
          if (callback) {
            callback(status);
          }
          RAY_LOG(DEBUG) << "Finished updating node resources, status = " << status
                         << ", node id = " << node_id;
          done_callback();
        });
  };

  sequencer_.Post(node_id, operation);
  return Status::OK();
}

Status ServiceBasedNodeResourceInfoAccessor::AsyncReportResourceUsage(
    const std::shared_ptr<rpc::ResourcesData> &data_ptr, const StatusCallback &callback) {
  absl::MutexLock lock(&mutex_);
  last_resource_usage_->SetAvailableResources(
      ResourceSet(MapFromProtobuf(data_ptr->resources_available())));
  last_resource_usage_->SetTotalResources(
      ResourceSet(MapFromProtobuf(data_ptr->resources_total())));
  last_resource_usage_->SetLoadResources(
      ResourceSet(MapFromProtobuf(data_ptr->resource_load())));
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

void ServiceBasedNodeResourceInfoAccessor::AsyncReReportResourceUsage() {
  absl::MutexLock lock(&mutex_);
  if (cached_resource_usage_.has_resources()) {
    RAY_LOG(INFO) << "Rereport resource usage.";
    FillResourceUsageRequest(cached_resource_usage_);
    client_impl_->GetGcsRpcClient().ReportResourceUsage(
        cached_resource_usage_,
        [](const Status &status, const rpc::ReportResourceUsageReply &reply) {});
  }
}

void ServiceBasedNodeResourceInfoAccessor::FillResourceUsageRequest(
    rpc::ReportResourceUsageRequest &resources) {
  SchedulingResources cached_resources = SchedulingResources(*GetLastResourceUsage());

  auto resources_data = resources.mutable_resources();
  resources_data->clear_resources_total();
  for (const auto &resource_pair :
       cached_resources.GetTotalResources().GetResourceMap()) {
    (*resources_data->mutable_resources_total())[resource_pair.first] =
        resource_pair.second;
  }

  resources_data->clear_resources_available();
  resources_data->set_resources_available_changed(true);
  for (const auto &resource_pair :
       cached_resources.GetAvailableResources().GetResourceMap()) {
    (*resources_data->mutable_resources_available())[resource_pair.first] =
        resource_pair.second;
  }

  resources_data->clear_resource_load();
  resources_data->set_resource_load_changed(true);
  for (const auto &resource_pair : cached_resources.GetLoadResources().GetResourceMap()) {
    (*resources_data->mutable_resource_load())[resource_pair.first] =
        resource_pair.second;
  }
}

Status ServiceBasedNodeResourceInfoAccessor::AsyncSubscribeBatchedResourceUsage(
    const ItemCallback<rpc::ResourceUsageBatchData> &subscribe,
    const StatusCallback &done) {
  RAY_CHECK(subscribe != nullptr);
  subscribe_batch_resource_usage_operation_ = [this,
                                               subscribe](const StatusCallback &done) {
    auto on_subscribe = [subscribe](const std::string &id, const std::string &data) {
      rpc::ResourceUsageBatchData resources_batch_data;
      resources_batch_data.ParseFromString(data);
      subscribe(resources_batch_data);
    };
    return client_impl_->GetGcsPubSub().Subscribe(RESOURCES_BATCH_CHANNEL, "",
                                                  on_subscribe, done);
  };
  return subscribe_batch_resource_usage_operation_(done);
}

Status ServiceBasedNodeResourceInfoAccessor::AsyncDeleteResources(
    const NodeID &node_id, const std::vector<std::string> &resource_names,
    const StatusCallback &callback) {
  RAY_LOG(DEBUG) << "Deleting node resources, node id = " << node_id;
  rpc::DeleteResourcesRequest request;
  request.set_node_id(node_id.Binary());
  for (auto &resource_name : resource_names) {
    request.add_resource_name_list(resource_name);
  }

  auto operation = [this, request, node_id,
                    callback](const SequencerDoneCallback &done_callback) {
    client_impl_->GetGcsRpcClient().DeleteResources(
        request, [node_id, callback, done_callback](
                     const Status &status, const rpc::DeleteResourcesReply &reply) {
          if (callback) {
            callback(status);
          }
          RAY_LOG(DEBUG) << "Finished deleting node resources, status = " << status
                         << ", node id = " << node_id;
          done_callback();
        });
  };

  sequencer_.Post(node_id, operation);
  return Status::OK();
}

Status ServiceBasedNodeResourceInfoAccessor::AsyncSubscribeToResources(
    const ItemCallback<rpc::NodeResourceChange> &subscribe, const StatusCallback &done) {
  RAY_CHECK(subscribe != nullptr);
  subscribe_resource_operation_ = [this, subscribe](const StatusCallback &done) {
    auto on_subscribe = [subscribe](const std::string &id, const std::string &data) {
      rpc::NodeResourceChange node_resource_change;
      node_resource_change.ParseFromString(data);
      subscribe(node_resource_change);
    };
    return client_impl_->GetGcsPubSub().SubscribeAll(NODE_RESOURCE_CHANNEL, on_subscribe,
                                                     done);
  };
  return subscribe_resource_operation_(done);
}

void ServiceBasedNodeResourceInfoAccessor::AsyncResubscribe(
    bool is_pubsub_server_restarted) {
  RAY_LOG(DEBUG) << "Reestablishing subscription for node resource info.";
  // If the pub-sub server has also restarted, we need to resubscribe to the pub-sub
  // server.
  if (is_pubsub_server_restarted) {
    if (subscribe_resource_operation_ != nullptr) {
      RAY_CHECK_OK(subscribe_resource_operation_(nullptr));
    }
    if (subscribe_batch_resource_usage_operation_ != nullptr) {
      RAY_CHECK_OK(subscribe_batch_resource_usage_operation_(nullptr));
    }
  }
}

Status ServiceBasedNodeResourceInfoAccessor::AsyncGetAllResourceUsage(
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

ServiceBasedTaskInfoAccessor::ServiceBasedTaskInfoAccessor(
    ServiceBasedGcsClient *client_impl)
    : client_impl_(client_impl) {}

Status ServiceBasedTaskInfoAccessor::AsyncAdd(
    const std::shared_ptr<rpc::TaskTableData> &data_ptr, const StatusCallback &callback) {
  TaskID task_id = TaskID::FromBinary(data_ptr->task().task_spec().task_id());
  JobID job_id = JobID::FromBinary(data_ptr->task().task_spec().job_id());
  RAY_LOG(DEBUG) << "Adding task, task id = " << task_id << ", job id = " << job_id;
  rpc::AddTaskRequest request;
  request.mutable_task_data()->CopyFrom(*data_ptr);
  client_impl_->GetGcsRpcClient().AddTask(
      request,
      [task_id, job_id, callback](const Status &status, const rpc::AddTaskReply &reply) {
        if (callback) {
          callback(status);
        }
        RAY_LOG(DEBUG) << "Finished adding task, status = " << status
                       << ", task id = " << task_id << ", job id = " << job_id;
      });
  return Status::OK();
}

Status ServiceBasedTaskInfoAccessor::AsyncGet(
    const TaskID &task_id, const OptionalItemCallback<rpc::TaskTableData> &callback) {
  RAY_LOG(DEBUG) << "Getting task, task id = " << task_id
                 << ", job id = " << task_id.JobId();
  rpc::GetTaskRequest request;
  request.set_task_id(task_id.Binary());
  client_impl_->GetGcsRpcClient().GetTask(
      request, [task_id, callback](const Status &status, const rpc::GetTaskReply &reply) {
        if (reply.has_task_data()) {
          callback(status, reply.task_data());
        } else {
          callback(status, boost::none);
        }
        RAY_LOG(DEBUG) << "Finished getting task, status = " << status
                       << ", task id = " << task_id << ", job id = " << task_id.JobId();
      });
  return Status::OK();
}

Status ServiceBasedTaskInfoAccessor::AsyncAddTaskLease(
    const std::shared_ptr<rpc::TaskLeaseData> &data_ptr, const StatusCallback &callback) {
  TaskID task_id = TaskID::FromBinary(data_ptr->task_id());
  NodeID node_id = NodeID::FromBinary(data_ptr->node_manager_id());
  RAY_LOG(DEBUG) << "Adding task lease, task id = " << task_id
                 << ", node id = " << node_id << ", job id = " << task_id.JobId();
  rpc::AddTaskLeaseRequest request;
  request.mutable_task_lease_data()->CopyFrom(*data_ptr);
  client_impl_->GetGcsRpcClient().AddTaskLease(
      request, [task_id, node_id, callback](const Status &status,
                                            const rpc::AddTaskLeaseReply &reply) {
        if (callback) {
          callback(status);
        }
        RAY_LOG(DEBUG) << "Finished adding task lease, status = " << status
                       << ", task id = " << task_id << ", node id = " << node_id
                       << ", job id = " << task_id.JobId();
      });
  return Status::OK();
}

Status ServiceBasedTaskInfoAccessor::AsyncGetTaskLease(
    const TaskID &task_id, const OptionalItemCallback<rpc::TaskLeaseData> &callback) {
  RAY_LOG(DEBUG) << "Getting task lease, task id = " << task_id
                 << ", job id = " << task_id.JobId();
  rpc::GetTaskLeaseRequest request;
  request.set_task_id(task_id.Binary());
  client_impl_->GetGcsRpcClient().GetTaskLease(
      request,
      [task_id, callback](const Status &status, const rpc::GetTaskLeaseReply &reply) {
        if (reply.has_task_lease_data()) {
          callback(status, reply.task_lease_data());
        } else {
          callback(status, boost::none);
        }
        RAY_LOG(DEBUG) << "Finished getting task lease, status = " << status
                       << ", task id = " << task_id << ", job id = " << task_id.JobId();
      });
  return Status::OK();
}

Status ServiceBasedTaskInfoAccessor::AsyncSubscribeTaskLease(
    const TaskID &task_id,
    const SubscribeCallback<TaskID, boost::optional<rpc::TaskLeaseData>> &subscribe,
    const StatusCallback &done) {
  RAY_CHECK(subscribe != nullptr)
      << "Failed to subscribe task lease, task id = " << task_id
      << ", job id = " << task_id.JobId();

  auto fetch_data_operation = [this, task_id,
                               subscribe](const StatusCallback &fetch_done) {
    auto callback = [task_id, subscribe, fetch_done](
                        const Status &status,
                        const boost::optional<rpc::TaskLeaseData> &result) {
      subscribe(task_id, result);
      if (fetch_done) {
        fetch_done(status);
      }
    };
    RAY_CHECK_OK(AsyncGetTaskLease(task_id, callback));
  };

  auto subscribe_operation = [this, task_id,
                              subscribe](const StatusCallback &subscribe_done) {
    auto on_subscribe = [task_id, subscribe](const std::string &id,
                                             const std::string &data) {
      TaskLeaseData task_lease_data;
      task_lease_data.ParseFromString(data);
      subscribe(task_id, task_lease_data);
    };
    return client_impl_->GetGcsPubSub().Subscribe(TASK_LEASE_CHANNEL, task_id.Hex(),
                                                  on_subscribe, subscribe_done);
  };

  subscribe_task_lease_operations_[task_id] = subscribe_operation;
  fetch_task_lease_data_operations_[task_id] = fetch_data_operation;
  return subscribe_operation(
      [fetch_data_operation, done](const Status &status) { fetch_data_operation(done); });
}

Status ServiceBasedTaskInfoAccessor::AsyncUnsubscribeTaskLease(const TaskID &task_id) {
  RAY_LOG(DEBUG) << "Unsubscribing task lease, task id = " << task_id
                 << ", job id = " << task_id.JobId();
  auto status =
      client_impl_->GetGcsPubSub().Unsubscribe(TASK_LEASE_CHANNEL, task_id.Hex());
  subscribe_task_lease_operations_.erase(task_id);
  fetch_task_lease_data_operations_.erase(task_id);
  RAY_LOG(DEBUG) << "Finished unsubscribing task lease, task id = " << task_id
                 << ", job id = " << task_id.JobId();
  return status;
}

Status ServiceBasedTaskInfoAccessor::AttemptTaskReconstruction(
    const std::shared_ptr<rpc::TaskReconstructionData> &data_ptr,
    const StatusCallback &callback) {
  auto num_reconstructions = data_ptr->num_reconstructions();
  NodeID node_id = NodeID::FromBinary(data_ptr->node_manager_id());
  TaskID task_id = TaskID::FromBinary(data_ptr->task_id());
  RAY_LOG(DEBUG) << "Reconstructing task, reconstructions num = " << num_reconstructions
                 << ", node id = " << node_id << ", task id = " << task_id
                 << ", job id = " << task_id.JobId();
  rpc::AttemptTaskReconstructionRequest request;
  request.mutable_task_reconstruction()->CopyFrom(*data_ptr);
  client_impl_->GetGcsRpcClient().AttemptTaskReconstruction(
      request,
      [num_reconstructions, node_id, task_id, callback](
          const Status &status, const rpc::AttemptTaskReconstructionReply &reply) {
        if (callback) {
          callback(status);
        }
        RAY_LOG(DEBUG) << "Finished reconstructing task, status = " << status
                       << ", reconstructions num = " << num_reconstructions
                       << ", node id = " << node_id << ", task id = " << task_id
                       << ", job id = " << task_id.JobId();
      });
  return Status::OK();
}

void ServiceBasedTaskInfoAccessor::AsyncResubscribe(bool is_pubsub_server_restarted) {
  RAY_LOG(DEBUG) << "Reestablishing subscription for task info.";
  // If only the GCS sever has restarted, we only need to fetch data from the GCS server.
  // If the pub-sub server has also restarted, we need to resubscribe to the pub-sub
  // server first, then fetch data from the GCS server.
  if (is_pubsub_server_restarted) {
    for (auto &item : subscribe_task_lease_operations_) {
      auto &task_id = item.first;
      RAY_CHECK_OK(item.second([this, task_id](const Status &status) {
        fetch_task_lease_data_operations_[task_id](nullptr);
      }));
    }
  } else {
    for (auto &item : fetch_task_lease_data_operations_) {
      item.second(nullptr);
    }
  }
}

bool ServiceBasedTaskInfoAccessor::IsTaskLeaseUnsubscribed(const TaskID &task_id) {
  return client_impl_->GetGcsPubSub().IsUnsubscribed(TASK_LEASE_CHANNEL, task_id.Hex());
}

ServiceBasedObjectInfoAccessor::ServiceBasedObjectInfoAccessor(
    ServiceBasedGcsClient *client_impl)
    : client_impl_(client_impl) {}

Status ServiceBasedObjectInfoAccessor::AsyncGetLocations(
    const ObjectID &object_id,
    const OptionalItemCallback<rpc::ObjectLocationInfo> &callback) {
  RAY_LOG(DEBUG) << "Getting object locations, object id = " << object_id
                 << ", job id = " << object_id.TaskId().JobId();
  rpc::GetObjectLocationsRequest request;
  request.set_object_id(object_id.Binary());
  client_impl_->GetGcsRpcClient().GetObjectLocations(
      request, [object_id, callback](const Status &status,
                                     const rpc::GetObjectLocationsReply &reply) {
        callback(status, reply.location_info());
        RAY_LOG(DEBUG) << "Finished getting object locations, status = " << status
                       << ", object id = " << object_id
                       << ", job id = " << object_id.TaskId().JobId();
      });
  return Status::OK();
}

Status ServiceBasedObjectInfoAccessor::AsyncGetAll(
    const MultiItemCallback<rpc::ObjectLocationInfo> &callback) {
  RAY_LOG(DEBUG) << "Getting all object locations.";
  rpc::GetAllObjectLocationsRequest request;
  client_impl_->GetGcsRpcClient().GetAllObjectLocations(
      request,
      [callback](const Status &status, const rpc::GetAllObjectLocationsReply &reply) {
        std::vector<rpc::ObjectLocationInfo> result;
        result.reserve((reply.object_location_info_list_size()));
        for (int index = 0; index < reply.object_location_info_list_size(); ++index) {
          result.emplace_back(reply.object_location_info_list(index));
        }
        callback(status, result);
        RAY_LOG(DEBUG) << "Finished getting all object locations, status = " << status;
      });
  return Status::OK();
}

Status ServiceBasedObjectInfoAccessor::AsyncAddLocation(const ObjectID &object_id,
                                                        const NodeID &node_id,
                                                        size_t object_size,
                                                        const StatusCallback &callback) {
  RAY_LOG(DEBUG) << "Adding object location, object id = " << object_id
                 << ", node id = " << node_id
                 << ", job id = " << object_id.TaskId().JobId();
  rpc::AddObjectLocationRequest request;
  request.set_object_id(object_id.Binary());
  request.set_node_id(node_id.Binary());
  request.set_size(object_size);

  auto operation = [this, request, object_id, node_id,
                    callback](const SequencerDoneCallback &done_callback) {
    client_impl_->GetGcsRpcClient().AddObjectLocation(
        request, [object_id, node_id, callback, done_callback](
                     const Status &status, const rpc::AddObjectLocationReply &reply) {
          if (callback) {
            callback(status);
          }

          RAY_LOG(DEBUG) << "Finished adding object location, status = " << status
                         << ", object id = " << object_id << ", node id = " << node_id
                         << ", job id = " << object_id.TaskId().JobId();
          done_callback();
        });
  };

  sequencer_.Post(object_id, operation);
  return Status::OK();
}

Status ServiceBasedObjectInfoAccessor::AsyncAddSpilledUrl(
    const ObjectID &object_id, const std::string &spilled_url,
    const NodeID &spilled_node_id, size_t object_size, const StatusCallback &callback) {
  RAY_LOG(DEBUG) << "Adding object spilled location, object id = " << object_id
                 << ", spilled_url = " << spilled_url
                 << ", job id = " << object_id.TaskId().JobId();
  rpc::AddObjectLocationRequest request;
  request.set_object_id(object_id.Binary());
  request.set_spilled_url(spilled_url);
  request.set_spilled_node_id(spilled_node_id.Binary());
  request.set_size(object_size);

  auto operation = [this, request, callback](const SequencerDoneCallback &done_callback) {
    client_impl_->GetGcsRpcClient().AddObjectLocation(
        request, [callback, done_callback](const Status &status,
                                           const rpc::AddObjectLocationReply &reply) {
          if (callback) {
            callback(status);
          }

          done_callback();
        });
  };

  sequencer_.Post(object_id, operation);
  return Status::OK();
}

Status ServiceBasedObjectInfoAccessor::AsyncRemoveLocation(
    const ObjectID &object_id, const NodeID &node_id, const StatusCallback &callback) {
  RAY_LOG(DEBUG) << "Removing object location, object id = " << object_id
                 << ", node id = " << node_id
                 << ", job id = " << object_id.TaskId().JobId();
  rpc::RemoveObjectLocationRequest request;
  request.set_object_id(object_id.Binary());
  request.set_node_id(node_id.Binary());

  auto operation = [this, request, object_id, node_id,
                    callback](const SequencerDoneCallback &done_callback) {
    client_impl_->GetGcsRpcClient().RemoveObjectLocation(
        request, [object_id, node_id, callback, done_callback](
                     const Status &status, const rpc::RemoveObjectLocationReply &reply) {
          if (callback) {
            callback(status);
          }
          RAY_LOG(DEBUG) << "Finished removing object location, status = " << status
                         << ", object id = " << object_id << ", node id = " << node_id
                         << ", job id = " << object_id.TaskId().JobId();
          done_callback();
        });
  };

  sequencer_.Post(object_id, operation);
  return Status::OK();
}

Status ServiceBasedObjectInfoAccessor::AsyncSubscribeToLocations(
    const ObjectID &object_id,
    const SubscribeCallback<ObjectID, std::vector<rpc::ObjectLocationChange>> &subscribe,
    const StatusCallback &done) {
  RAY_CHECK(subscribe != nullptr)
      << "Failed to subscribe object location, object id = " << object_id
      << ", job id = " << object_id.TaskId().JobId();

  auto fetch_data_operation = [this, object_id,
                               subscribe](const StatusCallback &fetch_done) {
    auto callback = [object_id, subscribe, fetch_done](
                        const Status &status,
                        const boost::optional<rpc::ObjectLocationInfo> &result) {
      if (status.ok()) {
        std::vector<rpc::ObjectLocationChange> notification;
        for (const auto &loc : result->locations()) {
          rpc::ObjectLocationChange update;
          update.set_is_add(true);
          update.set_node_id(loc.manager());
          update.set_size(result->size());
          notification.push_back(update);
        }
        if (!result->spilled_url().empty()) {
          rpc::ObjectLocationChange update;
          update.set_spilled_url(result->spilled_url());
          update.set_spilled_node_id(result->spilled_node_id());
          update.set_size(result->size());
          notification.push_back(update);
        }
        subscribe(object_id, notification);
      }
      if (fetch_done) {
        fetch_done(status);
      }
    };
    RAY_CHECK_OK(AsyncGetLocations(object_id, callback));
  };

  auto subscribe_operation = [this, object_id,
                              subscribe](const StatusCallback &subscribe_done) {
    auto on_subscribe = [object_id, subscribe](const std::string &id,
                                               const std::string &data) {
      rpc::ObjectLocationChange object_location_change;
      object_location_change.ParseFromString(data);
      subscribe(object_id, {object_location_change});
    };
    return client_impl_->GetGcsPubSub().Subscribe(OBJECT_CHANNEL, object_id.Hex(),
                                                  on_subscribe, subscribe_done);
  };

  {
    absl::MutexLock lock(&mutex_);
    subscribe_object_operations_[object_id] = subscribe_operation;
    fetch_object_data_operations_[object_id] = fetch_data_operation;
  }
  return subscribe_operation(
      [fetch_data_operation, done](const Status &status) { fetch_data_operation(done); });
}

void ServiceBasedObjectInfoAccessor::AsyncResubscribe(bool is_pubsub_server_restarted) {
  RAY_LOG(DEBUG) << "Reestablishing subscription for object locations.";
  // If only the GCS sever has restarted, we only need to fetch data from the GCS server.
  // If the pub-sub server has also restarted, we need to resubscribe to the pub-sub
  // server first, then fetch data from the GCS server.
  absl::MutexLock lock(&mutex_);
  if (is_pubsub_server_restarted) {
    for (auto &item : subscribe_object_operations_) {
      RAY_CHECK_OK(item.second([this, item](const Status &status) {
        absl::MutexLock lock(&mutex_);
        auto fetch_object_data_operation = fetch_object_data_operations_[item.first];
        // `fetch_object_data_operation` is called in the callback function of subscribe.
        // Before that, if the user calls `AsyncUnsubscribeToLocations` function, the
        // corresponding fetch function will be deleted, so we need to check if it's null.
        if (fetch_object_data_operation != nullptr) {
          fetch_object_data_operation(nullptr);
        }
      }));
    }
  } else {
    for (auto &item : fetch_object_data_operations_) {
      item.second(nullptr);
    }
  }
}

Status ServiceBasedObjectInfoAccessor::AsyncUnsubscribeToLocations(
    const ObjectID &object_id) {
  RAY_LOG(DEBUG) << "Unsubscribing object location, object id = " << object_id
                 << ", job id = " << object_id.TaskId().JobId();
  auto status = client_impl_->GetGcsPubSub().Unsubscribe(OBJECT_CHANNEL, object_id.Hex());
  absl::MutexLock lock(&mutex_);
  subscribe_object_operations_.erase(object_id);
  fetch_object_data_operations_.erase(object_id);
  RAY_LOG(DEBUG) << "Finished unsubscribing object location, object id = " << object_id
                 << ", job id = " << object_id.TaskId().JobId();
  return status;
}

bool ServiceBasedObjectInfoAccessor::IsObjectUnsubscribed(const ObjectID &object_id) {
  return client_impl_->GetGcsPubSub().IsUnsubscribed(OBJECT_CHANNEL, object_id.Hex());
}

ServiceBasedStatsInfoAccessor::ServiceBasedStatsInfoAccessor(
    ServiceBasedGcsClient *client_impl)
    : client_impl_(client_impl) {}

Status ServiceBasedStatsInfoAccessor::AsyncAddProfileData(
    const std::shared_ptr<rpc::ProfileTableData> &data_ptr,
    const StatusCallback &callback) {
  NodeID node_id = NodeID::FromBinary(data_ptr->component_id());
  RAY_LOG(DEBUG) << "Adding profile data, component type = " << data_ptr->component_type()
                 << ", node id = " << node_id;
  rpc::AddProfileDataRequest request;
  request.mutable_profile_data()->CopyFrom(*data_ptr);
  client_impl_->GetGcsRpcClient().AddProfileData(
      request, [data_ptr, node_id, callback](const Status &status,
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

Status ServiceBasedStatsInfoAccessor::AsyncGetAll(
    const MultiItemCallback<rpc::ProfileTableData> &callback) {
  RAY_LOG(DEBUG) << "Getting all profile info.";
  RAY_CHECK(callback);
  rpc::GetAllProfileInfoRequest request;
  client_impl_->GetGcsRpcClient().GetAllProfileInfo(
      request,
      [callback](const Status &status, const rpc::GetAllProfileInfoReply &reply) {
        auto result = VectorFromProtobuf(reply.profile_info_list());
        callback(status, result);
        RAY_LOG(DEBUG) << "Finished getting all job info.";
      });
  return Status::OK();
}

ServiceBasedErrorInfoAccessor::ServiceBasedErrorInfoAccessor(
    ServiceBasedGcsClient *client_impl)
    : client_impl_(client_impl) {}

Status ServiceBasedErrorInfoAccessor::AsyncReportJobError(
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

ServiceBasedWorkerInfoAccessor::ServiceBasedWorkerInfoAccessor(
    ServiceBasedGcsClient *client_impl)
    : client_impl_(client_impl) {}

Status ServiceBasedWorkerInfoAccessor::AsyncSubscribeToWorkerFailures(
    const ItemCallback<rpc::WorkerDeltaData> &subscribe, const StatusCallback &done) {
  RAY_CHECK(subscribe != nullptr);
  subscribe_operation_ = [this, subscribe](const StatusCallback &done) {
    auto on_subscribe = [subscribe](const std::string &id, const std::string &data) {
      rpc::WorkerDeltaData worker_failure_data;
      worker_failure_data.ParseFromString(data);
      subscribe(worker_failure_data);
    };
    return client_impl_->GetGcsPubSub().SubscribeAll(WORKER_CHANNEL, on_subscribe, done);
  };
  return subscribe_operation_(done);
}

void ServiceBasedWorkerInfoAccessor::AsyncResubscribe(bool is_pubsub_server_restarted) {
  RAY_LOG(DEBUG) << "Reestablishing subscription for worker failures.";
  // If the pub-sub server has restarted, we need to resubscribe to the pub-sub server.
  if (subscribe_operation_ != nullptr && is_pubsub_server_restarted) {
    RAY_CHECK_OK(subscribe_operation_(nullptr));
  }
}

Status ServiceBasedWorkerInfoAccessor::AsyncReportWorkerFailure(
    const std::shared_ptr<rpc::WorkerTableData> &data_ptr,
    const StatusCallback &callback) {
  rpc::Address worker_address = data_ptr->worker_address();
  RAY_LOG(DEBUG) << "Reporting worker failure, " << worker_address.DebugString();
  rpc::ReportWorkerFailureRequest request;
  request.mutable_worker_failure()->CopyFrom(*data_ptr);
  client_impl_->GetGcsRpcClient().ReportWorkerFailure(
      request, [worker_address, callback](const Status &status,
                                          const rpc::ReportWorkerFailureReply &reply) {
        if (callback) {
          callback(status);
        }
        RAY_LOG(DEBUG) << "Finished reporting worker failure, "
                       << worker_address.DebugString() << ", status = " << status;
      });
  return Status::OK();
}

Status ServiceBasedWorkerInfoAccessor::AsyncGet(
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

Status ServiceBasedWorkerInfoAccessor::AsyncGetAll(
    const MultiItemCallback<rpc::WorkerTableData> &callback) {
  RAY_LOG(DEBUG) << "Getting all worker info.";
  rpc::GetAllWorkerInfoRequest request;
  client_impl_->GetGcsRpcClient().GetAllWorkerInfo(
      request, [callback](const Status &status, const rpc::GetAllWorkerInfoReply &reply) {
        auto result = VectorFromProtobuf(reply.worker_table_data());
        callback(status, result);
        RAY_LOG(DEBUG) << "Finished getting all worker info, status = " << status;
      });
  return Status::OK();
}

Status ServiceBasedWorkerInfoAccessor::AsyncAdd(
    const std::shared_ptr<rpc::WorkerTableData> &data_ptr,
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

ServiceBasedPlacementGroupInfoAccessor::ServiceBasedPlacementGroupInfoAccessor(
    ServiceBasedGcsClient *client_impl)
    : client_impl_(client_impl) {}

Status ServiceBasedPlacementGroupInfoAccessor::AsyncCreatePlacementGroup(
    const ray::PlacementGroupSpecification &placement_group_spec,
    const StatusCallback &callback) {
  rpc::CreatePlacementGroupRequest request;
  request.mutable_placement_group_spec()->CopyFrom(placement_group_spec.GetMessage());
  client_impl_->GetGcsRpcClient().CreatePlacementGroup(
      request, [placement_group_spec, callback](
                   const Status &, const rpc::CreatePlacementGroupReply &reply) {
        auto status =
            reply.status().code() == (int)StatusCode::OK
                ? Status()
                : Status(StatusCode(reply.status().code()), reply.status().message());
        if (status.ok()) {
          RAY_LOG(DEBUG) << "Finished registering placement group. placement group id = "
                         << placement_group_spec.PlacementGroupId();
        } else {
          RAY_LOG(ERROR) << "Placement group id = "
                         << placement_group_spec.PlacementGroupId()
                         << " failed to be registered. " << status;
        }
        if (callback) {
          callback(status);
        }
      });
  return Status::OK();
}

Status ServiceBasedPlacementGroupInfoAccessor::AsyncRemovePlacementGroup(
    const ray::PlacementGroupID &placement_group_id, const StatusCallback &callback) {
  rpc::RemovePlacementGroupRequest request;
  request.set_placement_group_id(placement_group_id.Binary());
  client_impl_->GetGcsRpcClient().RemovePlacementGroup(
      request,
      [callback](const Status &status, const rpc::RemovePlacementGroupReply &reply) {
        if (callback) {
          callback(status);
        }
      });
  return Status::OK();
}

Status ServiceBasedPlacementGroupInfoAccessor::AsyncGet(
    const PlacementGroupID &placement_group_id,
    const OptionalItemCallback<rpc::PlacementGroupTableData> &callback) {
  RAY_LOG(DEBUG) << "Getting placement group info, placement group id = "
                 << placement_group_id;
  rpc::GetPlacementGroupRequest request;
  request.set_placement_group_id(placement_group_id.Binary());
  client_impl_->GetGcsRpcClient().GetPlacementGroup(
      request, [placement_group_id, callback](const Status &status,
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

Status ServiceBasedPlacementGroupInfoAccessor::AsyncGetByName(
    const std::string &name,
    const OptionalItemCallback<rpc::PlacementGroupTableData> &callback) {
  RAY_LOG(DEBUG) << "Getting named placement group info, name = " << name;
  rpc::GetNamedPlacementGroupRequest request;
  request.set_name(name);
  client_impl_->GetGcsRpcClient().GetNamedPlacementGroup(
      request, [name, callback](const Status &status,
                                const rpc::GetNamedPlacementGroupReply &reply) {
        if (reply.has_placement_group_table_data()) {
          callback(status, reply.placement_group_table_data());
        } else {
          callback(status, boost::none);
        }
        RAY_LOG(DEBUG) << "Finished getting named placement group info, status = "
                       << status << ", name = " << name;
      });
  return Status::OK();
}

Status ServiceBasedPlacementGroupInfoAccessor::AsyncGetAll(
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

Status ServiceBasedPlacementGroupInfoAccessor::AsyncWaitUntilReady(
    const PlacementGroupID &placement_group_id, const StatusCallback &callback) {
  RAY_LOG(DEBUG) << "Waiting for placement group until ready, placement group id = "
                 << placement_group_id;
  rpc::WaitPlacementGroupUntilReadyRequest request;
  request.set_placement_group_id(placement_group_id.Binary());
  client_impl_->GetGcsRpcClient().WaitPlacementGroupUntilReady(
      request,
      [placement_group_id, callback](
          const Status &status, const rpc::WaitPlacementGroupUntilReadyReply &reply) {
        callback(status);
        RAY_LOG(DEBUG)
            << "Finished waiting placement group until ready, placement group id = "
            << placement_group_id;
      });
  return Status::OK();
}

ServiceBasedInternalKVAccessor::ServiceBasedInternalKVAccessor(
    ServiceBasedGcsClient *client_impl)
    : client_impl_(client_impl) {}

Status ServiceBasedInternalKVAccessor::AsyncInternalKVGet(
    const std::string &key, const OptionalItemCallback<std::string> &callback) {
  rpc::InternalKVGetRequest req;
  req.set_key(key);
  client_impl_->GetGcsRpcClient().InternalKVGet(
      req, [callback](const Status &status, const rpc::InternalKVGetReply &reply) {
        if (reply.status().code() == (int)StatusCode::NotFound) {
          callback(status, boost::none);
        } else {
          callback(status, reply.value());
        }
      });
  return Status::OK();
}

Status ServiceBasedInternalKVAccessor::AsyncInternalKVPut(
    const std::string &key, const std::string &value, bool overwrite,
    const OptionalItemCallback<int> &callback) {
  rpc::InternalKVPutRequest req;
  req.set_key(key);
  req.set_value(value);
  req.set_overwrite(overwrite);
  client_impl_->GetGcsRpcClient().InternalKVPut(
      req, [callback](const Status &status, const rpc::InternalKVPutReply &reply) {
        callback(status, reply.added_num());
      });
  return Status::OK();
}

Status ServiceBasedInternalKVAccessor::AsyncInternalKVExists(
    const std::string &key, const OptionalItemCallback<bool> &callback) {
  rpc::InternalKVExistsRequest req;
  req.set_key(key);
  client_impl_->GetGcsRpcClient().InternalKVExists(
      req, [callback](const Status &status, const rpc::InternalKVExistsReply &reply) {
        callback(status, reply.exists());
      });
  return Status::OK();
}

Status ServiceBasedInternalKVAccessor::AsyncInternalKVDel(
    const std::string &key, const StatusCallback &callback) {
  rpc::InternalKVDelRequest req;
  req.set_key(key);
  client_impl_->GetGcsRpcClient().InternalKVDel(
      req, [callback](const Status &status, const rpc::InternalKVDelReply &reply) {
        callback(status);
      });
  return Status::OK();
}

Status ServiceBasedInternalKVAccessor::AsyncInternalKVKeys(
    const std::string &prefix,
    const OptionalItemCallback<std::vector<std::string>> &callback) {
  rpc::InternalKVKeysRequest req;
  req.set_prefix(prefix);
  client_impl_->GetGcsRpcClient().InternalKVKeys(
      req, [callback](const Status &status, const rpc::InternalKVKeysReply &reply) {
        if (!status.ok()) {
          callback(status, boost::none);
        } else {
          callback(status, VectorFromProtobuf(reply.results()));
        }
      });
  return Status::OK();
}

}  // namespace gcs
}  // namespace ray
