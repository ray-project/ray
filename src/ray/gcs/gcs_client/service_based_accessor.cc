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

Status ServiceBasedJobInfoAccessor::AsyncSubscribeToFinishedJobs(
    const SubscribeCallback<JobID, JobTableData> &subscribe, const StatusCallback &done) {
  RAY_LOG(DEBUG) << "Subscribing finished job.";
  RAY_CHECK(subscribe != nullptr);
  auto on_subscribe = [subscribe](const std::string &id, const std::string &data) {
    JobTableData job_data;
    job_data.ParseFromString(data);
    if (job_data.is_dead()) {
      subscribe(JobID::FromBinary(id), job_data);
    }
  };
  Status status =
      client_impl_->GetGcsPubSub().SubscribeAll(JOB_CHANNEL, on_subscribe, done);
  RAY_LOG(DEBUG) << "Finished subscribing finished job.";
  return status;
}

ServiceBasedActorInfoAccessor::ServiceBasedActorInfoAccessor(
    ServiceBasedGcsClient *client_impl)
    : client_impl_(client_impl) {}

Status ServiceBasedActorInfoAccessor::GetAll(
    std::vector<ActorTableData> *actor_table_data_list) {
  return Status::Invalid("Not implemented");
}

Status ServiceBasedActorInfoAccessor::AsyncGet(
    const ActorID &actor_id, const OptionalItemCallback<rpc::ActorTableData> &callback) {
  RAY_LOG(DEBUG) << "Getting actor info, actor id = " << actor_id;
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
                       << ", actor id = " << actor_id;
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

Status ServiceBasedActorInfoAccessor::AsyncRegister(
    const std::shared_ptr<rpc::ActorTableData> &data_ptr,
    const StatusCallback &callback) {
  ActorID actor_id = ActorID::FromBinary(data_ptr->actor_id());
  RAY_LOG(DEBUG) << "Registering actor info, actor id = " << actor_id;
  rpc::RegisterActorInfoRequest request;
  request.mutable_actor_table_data()->CopyFrom(*data_ptr);

  auto operation = [this, request, actor_id,
                    callback](const SequencerDoneCallback &done_callback) {
    client_impl_->GetGcsRpcClient().RegisterActorInfo(
        request, [actor_id, callback, done_callback](
                     const Status &status, const rpc::RegisterActorInfoReply &reply) {
          if (callback) {
            callback(status);
          }
          RAY_LOG(DEBUG) << "Finished registering actor info, status = " << status
                         << ", actor id = " << actor_id;
          done_callback();
        });
  };

  sequencer_.Post(actor_id, operation);
  return Status::OK();
}

Status ServiceBasedActorInfoAccessor::AsyncUpdate(
    const ActorID &actor_id, const std::shared_ptr<rpc::ActorTableData> &data_ptr,
    const StatusCallback &callback) {
  RAY_LOG(DEBUG) << "Updating actor info, actor id = " << actor_id;
  rpc::UpdateActorInfoRequest request;
  request.set_actor_id(actor_id.Binary());
  request.mutable_actor_table_data()->CopyFrom(*data_ptr);

  auto operation = [this, request, actor_id,
                    callback](const SequencerDoneCallback &done_callback) {
    client_impl_->GetGcsRpcClient().UpdateActorInfo(
        request, [actor_id, callback, done_callback](
                     const Status &status, const rpc::UpdateActorInfoReply &reply) {
          if (callback) {
            callback(status);
          }
          RAY_LOG(DEBUG) << "Finished updating actor info, status = " << status
                         << ", actor id = " << actor_id;
          done_callback();
        });
  };

  sequencer_.Post(actor_id, operation);
  return Status::OK();
}

Status ServiceBasedActorInfoAccessor::AsyncSubscribeAll(
    const SubscribeCallback<ActorID, rpc::ActorTableData> &subscribe,
    const StatusCallback &done) {
  RAY_LOG(DEBUG) << "Subscribing register or update operations of actors.";
  RAY_CHECK(subscribe != nullptr);
  auto on_subscribe = [subscribe](const std::string &id, const std::string &data) {
    ActorTableData actor_data;
    actor_data.ParseFromString(data);
    subscribe(ActorID::FromBinary(id), actor_data);
  };
  auto status =
      client_impl_->GetGcsPubSub().SubscribeAll(ACTOR_CHANNEL, on_subscribe, done);
  RAY_LOG(DEBUG) << "Finished subscribing register or update operations of actors.";
  return status;
}

Status ServiceBasedActorInfoAccessor::AsyncSubscribe(
    const ActorID &actor_id,
    const SubscribeCallback<ActorID, rpc::ActorTableData> &subscribe,
    const StatusCallback &done) {
  RAY_LOG(DEBUG) << "Subscribing update operations of actor, actor id = " << actor_id;
  RAY_CHECK(subscribe != nullptr) << "Failed to subscribe actor, actor id = " << actor_id;
  auto on_subscribe = [subscribe](const std::string &id, const std::string &data) {
    ActorTableData actor_data;
    actor_data.ParseFromString(data);
    subscribe(ActorID::FromBinary(id), actor_data);
  };
  auto status = client_impl_->GetGcsPubSub().Subscribe(ACTOR_CHANNEL, actor_id.Binary(),
                                                       on_subscribe, done);
  RAY_LOG(DEBUG) << "Finished subscribing update operations of actor, actor id = "
                 << actor_id;
  return status;
}

Status ServiceBasedActorInfoAccessor::AsyncUnsubscribe(const ActorID &actor_id,
                                                       const StatusCallback &done) {
  RAY_LOG(DEBUG) << "Cancelling subscription to an actor, actor id = " << actor_id;
  auto status =
      client_impl_->GetGcsPubSub().Unsubscribe(ACTOR_CHANNEL, actor_id.Binary());
  if (done) {
    done(status);
  }
  RAY_LOG(DEBUG) << "Finished cancelling subscription to an actor, actor id = "
                 << actor_id;
  return status;
}

Status ServiceBasedActorInfoAccessor::AsyncAddCheckpoint(
    const std::shared_ptr<rpc::ActorCheckpointData> &data_ptr,
    const StatusCallback &callback) {
  ActorID actor_id = ActorID::FromBinary(data_ptr->actor_id());
  ActorCheckpointID checkpoint_id =
      ActorCheckpointID::FromBinary(data_ptr->checkpoint_id());
  RAY_LOG(DEBUG) << "Adding actor checkpoint, actor id = " << actor_id
                 << ", checkpoint id = " << checkpoint_id;
  rpc::AddActorCheckpointRequest request;
  request.mutable_checkpoint_data()->CopyFrom(*data_ptr);

  auto operation = [this, request, actor_id, checkpoint_id,
                    callback](const SequencerDoneCallback &done_callback) {
    client_impl_->GetGcsRpcClient().AddActorCheckpoint(
        request, [actor_id, checkpoint_id, callback, done_callback](
                     const Status &status, const rpc::AddActorCheckpointReply &reply) {
          if (callback) {
            callback(status);
          }
          RAY_LOG(DEBUG) << "Finished adding actor checkpoint, status = " << status
                         << ", actor id = " << actor_id
                         << ", checkpoint id = " << checkpoint_id;
          done_callback();
        });
  };

  sequencer_.Post(actor_id, operation);
  return Status::OK();
}

Status ServiceBasedActorInfoAccessor::AsyncGetCheckpoint(
    const ActorCheckpointID &checkpoint_id, const ActorID &actor_id,
    const OptionalItemCallback<rpc::ActorCheckpointData> &callback) {
  RAY_LOG(DEBUG) << "Getting actor checkpoint, checkpoint id = " << checkpoint_id;
  rpc::GetActorCheckpointRequest request;
  request.set_actor_id(actor_id.Binary());
  request.set_checkpoint_id(checkpoint_id.Binary());
  client_impl_->GetGcsRpcClient().GetActorCheckpoint(
      request, [checkpoint_id, callback](const Status &status,
                                         const rpc::GetActorCheckpointReply &reply) {
        if (reply.has_checkpoint_data()) {
          callback(status, reply.checkpoint_data());
        } else {
          callback(status, boost::none);
        }
        RAY_LOG(DEBUG) << "Finished getting actor checkpoint, status = " << status
                       << ", checkpoint id = " << checkpoint_id;
      });
  return Status::OK();
}

Status ServiceBasedActorInfoAccessor::AsyncGetCheckpointID(
    const ActorID &actor_id,
    const OptionalItemCallback<rpc::ActorCheckpointIdData> &callback) {
  RAY_LOG(DEBUG) << "Getting actor checkpoint id, actor id = " << actor_id;
  rpc::GetActorCheckpointIDRequest request;
  request.set_actor_id(actor_id.Binary());
  client_impl_->GetGcsRpcClient().GetActorCheckpointID(
      request, [actor_id, callback](const Status &status,
                                    const rpc::GetActorCheckpointIDReply &reply) {
        if (reply.has_checkpoint_id_data()) {
          callback(status, reply.checkpoint_id_data());
        } else {
          callback(status, boost::none);
        }
        RAY_LOG(DEBUG) << "Finished getting actor checkpoint id, status = " << status
                       << ", actor id = " << actor_id;
      });
  return Status::OK();
}

ServiceBasedNodeInfoAccessor::ServiceBasedNodeInfoAccessor(
    ServiceBasedGcsClient *client_impl)
    : client_impl_(client_impl) {}

Status ServiceBasedNodeInfoAccessor::RegisterSelf(const GcsNodeInfo &local_node_info) {
  auto node_id = ClientID::FromBinary(local_node_info.node_id());
  RAY_LOG(DEBUG) << "Registering node info, node id = " << node_id
                 << ", address is = " << local_node_info.node_manager_address();
  RAY_CHECK(local_node_id_.IsNil()) << "This node is already connected.";
  RAY_CHECK(local_node_info.state() == GcsNodeInfo::ALIVE);
  rpc::RegisterNodeRequest request;
  request.mutable_node_info()->CopyFrom(local_node_info);
  client_impl_->GetGcsRpcClient().RegisterNode(
      request, [this, node_id, &local_node_info](const Status &status,
                                                 const rpc::RegisterNodeReply &reply) {
        if (status.ok()) {
          local_node_info_.CopyFrom(local_node_info);
          local_node_id_ = ClientID::FromBinary(local_node_info.node_id());
        }
        RAY_LOG(DEBUG) << "Finished registering node info, status = " << status
                       << ", node id = " << node_id;
      });
  return Status::OK();
}

Status ServiceBasedNodeInfoAccessor::UnregisterSelf() {
  RAY_CHECK(!local_node_id_.IsNil()) << "This node is disconnected.";
  ClientID node_id = ClientID::FromBinary(local_node_info_.node_id());
  RAY_LOG(DEBUG) << "Unregistering node info, node id = " << node_id;
  rpc::UnregisterNodeRequest request;
  request.set_node_id(local_node_info_.node_id());
  std::promise<Status> promise;
  client_impl_->GetGcsRpcClient().UnregisterNode(
      request, [this, node_id, &promise](const Status &status,
                                         const rpc::UnregisterNodeReply &reply) {
        if (status.ok()) {
          local_node_info_.set_state(GcsNodeInfo::DEAD);
          local_node_id_ = ClientID::Nil();
        }
        RAY_LOG(DEBUG) << "Finished unregistering node info, status = " << status
                       << ", node id = " << node_id;
        promise.set_value(status);
      });
  return promise.get_future().get();
}

const ClientID &ServiceBasedNodeInfoAccessor::GetSelfId() const { return local_node_id_; }

const GcsNodeInfo &ServiceBasedNodeInfoAccessor::GetSelfInfo() const {
  return local_node_info_;
}

Status ServiceBasedNodeInfoAccessor::AsyncRegister(const rpc::GcsNodeInfo &node_info,
                                                   const StatusCallback &callback) {
  ClientID node_id = ClientID::FromBinary(node_info.node_id());
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

Status ServiceBasedNodeInfoAccessor::AsyncUnregister(const ClientID &node_id,
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
    const SubscribeCallback<ClientID, GcsNodeInfo> &subscribe,
    const StatusCallback &done) {
  RAY_LOG(DEBUG) << "Subscribing node change.";
  RAY_CHECK(subscribe != nullptr);
  auto on_subscribe = [this, subscribe](const std::string &id, const std::string &data) {
    GcsNodeInfo node_info;
    node_info.ParseFromString(data);
    HandleNotification(node_info);
  };

  // Callback to request notifications from the client table once we've
  // successfully subscribed.
  auto on_done = [this, subscribe, done](const Status &status) {
    RAY_CHECK_OK(status);
    if (done != nullptr) {
      done(status);
    }
    // Register node change callbacks after SubscribeAll finishes.
    RegisterNodeChangeCallback(subscribe);
  };

  auto status =
      client_impl_->GetGcsPubSub().SubscribeAll(NODE_CHANNEL, on_subscribe, on_done);
  RAY_LOG(DEBUG) << "Finished subscribing node change.";
  return status;
}

void ServiceBasedNodeInfoAccessor::RegisterNodeChangeCallback(
    const NodeChangeCallback &callback) {
  RAY_CHECK(node_change_callback_ == nullptr);
  node_change_callback_ = callback;
  // Call the callback for any added clients that are cached.
  for (const auto &entry : node_cache_) {
    if (!entry.first.IsNil()) {
      RAY_CHECK(entry.second.state() == GcsNodeInfo::ALIVE ||
                entry.second.state() == GcsNodeInfo::DEAD);
      node_change_callback_(entry.first, entry.second);
    }
  }
}

boost::optional<GcsNodeInfo> ServiceBasedNodeInfoAccessor::Get(
    const ClientID &node_id) const {
  RAY_CHECK(!node_id.IsNil());
  auto entry = node_cache_.find(node_id);
  auto found = (entry != node_cache_.end());

  boost::optional<GcsNodeInfo> optional_node;
  if (found) {
    GcsNodeInfo node_info = entry->second;
    optional_node = std::move(node_info);
  }
  return optional_node;
}

const std::unordered_map<ClientID, GcsNodeInfo> &ServiceBasedNodeInfoAccessor::GetAll()
    const {
  return node_cache_;
}

bool ServiceBasedNodeInfoAccessor::IsRemoved(const ClientID &node_id) const {
  return removed_nodes_.count(node_id) == 1;
}

Status ServiceBasedNodeInfoAccessor::AsyncGetResources(
    const ClientID &node_id, const OptionalItemCallback<ResourceMap> &callback) {
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

Status ServiceBasedNodeInfoAccessor::AsyncUpdateResources(
    const ClientID &node_id, const ResourceMap &resources,
    const StatusCallback &callback) {
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

Status ServiceBasedNodeInfoAccessor::AsyncDeleteResources(
    const ClientID &node_id, const std::vector<std::string> &resource_names,
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

Status ServiceBasedNodeInfoAccessor::AsyncSubscribeToResources(
    const SubscribeCallback<ClientID, ResourceChangeNotification> &subscribe,
    const StatusCallback &done) {
  RAY_LOG(DEBUG) << "Subscribing node resources change.";
  RAY_CHECK(subscribe != nullptr);
  auto on_subscribe = [subscribe](const std::string &id, const std::string &data) {
    rpc::ResourceChange resource_change;
    resource_change.ParseFromString(data);
    std::unordered_map<std::string, std::shared_ptr<rpc::ResourceTableData>> resource_map;
    for (auto &item : resource_change.data().items()) {
      resource_map[item.first] = std::make_shared<rpc::ResourceTableData>(item.second);
    }
    gcs::ResourceChangeNotification notification(resource_change.change_mode(),
                                                 resource_map);
    subscribe(ClientID::FromBinary(id), notification);
  };
  auto status = client_impl_->GetGcsPubSub().SubscribeAll(NODE_RESOURCE_CHANNEL,
                                                          on_subscribe, done);
  RAY_LOG(DEBUG) << "Finished subscribing node resources change.";
  return status;
}

Status ServiceBasedNodeInfoAccessor::AsyncReportHeartbeat(
    const std::shared_ptr<rpc::HeartbeatTableData> &data_ptr,
    const StatusCallback &callback) {
  ClientID node_id = ClientID::FromBinary(data_ptr->client_id());
  RAY_LOG(DEBUG) << "Reporting heartbeat, node id = " << node_id;
  rpc::ReportHeartbeatRequest request;
  request.mutable_heartbeat()->CopyFrom(*data_ptr);
  client_impl_->GetGcsRpcClient().ReportHeartbeat(
      request,
      [node_id, callback](const Status &status, const rpc::ReportHeartbeatReply &reply) {
        if (callback) {
          callback(status);
        }
        RAY_LOG(DEBUG) << "Finished reporting heartbeat, status = " << status
                       << ", node id = " << node_id;
      });
  return Status::OK();
}

Status ServiceBasedNodeInfoAccessor::AsyncSubscribeHeartbeat(
    const SubscribeCallback<ClientID, rpc::HeartbeatTableData> &subscribe,
    const StatusCallback &done) {
  const std::string error_msg =
      "Unsupported method of AsyncSubscribeHeartbeat in ServiceBasedNodeInfoAccessor.";
  RAY_LOG(FATAL) << error_msg;
  return Status::Invalid(error_msg);
}

Status ServiceBasedNodeInfoAccessor::AsyncReportBatchHeartbeat(
    const std::shared_ptr<rpc::HeartbeatBatchTableData> &data_ptr,
    const StatusCallback &callback) {
  const std::string error_msg =
      "Unsupported method of AsyncReportBatchHeartbeat in ServiceBasedNodeInfoAccessor.";
  RAY_LOG(FATAL) << error_msg;
  return Status::Invalid(error_msg);
}

Status ServiceBasedNodeInfoAccessor::AsyncSubscribeBatchHeartbeat(
    const ItemCallback<rpc::HeartbeatBatchTableData> &subscribe,
    const StatusCallback &done) {
  RAY_LOG(DEBUG) << "Subscribing batch heartbeat.";
  RAY_CHECK(subscribe != nullptr);
  auto on_subscribe = [subscribe](const std::string &id, const std::string &data) {
    HeartbeatBatchTableData heartbeat_batch;
    heartbeat_batch.ParseFromString(data);
    subscribe(heartbeat_batch);
  };
  auto status = client_impl_->GetGcsPubSub().SubscribeAll(HEARTBEAT_BATCH_CHANNEL,
                                                          on_subscribe, done);
  RAY_LOG(DEBUG) << "Finished subscribing batch heartbeat.";
  return status;
}

void ServiceBasedNodeInfoAccessor::HandleNotification(const GcsNodeInfo &node_info) {
  ClientID node_id = ClientID::FromBinary(node_info.node_id());
  bool is_alive = (node_info.state() == GcsNodeInfo::ALIVE);
  // It's possible to get duplicate notifications from the client table, so
  // check whether this notification is new.
  auto entry = node_cache_.find(node_id);
  bool is_notif_new;
  if (entry == node_cache_.end()) {
    // If the entry is not in the cache, then the notification is new.
    is_notif_new = true;
  } else {
    // If the entry is in the cache, then the notification is new if the client
    // was alive and is now dead or resources have been updated.
    bool was_alive = (entry->second.state() == GcsNodeInfo::ALIVE);
    is_notif_new = was_alive && !is_alive;
    // Once a client with a given ID has been removed, it should never be added
    // again. If the entry was in the cache and the client was deleted, check
    // that this new notification is not an insertion.
    if (!was_alive) {
      RAY_CHECK(!is_alive)
          << "Notification for addition of a client that was already removed:" << node_id;
    }
  }

  // Add the notification to our cache. Notifications are idempotent.
  RAY_LOG(INFO) << "[ClientTableNotification] ClientTable Insertion/Deletion "
                   "notification for client id "
                << node_id << ". IsAlive: " << is_alive
                << ". Setting the client cache to data.";
  node_cache_[node_id] = node_info;

  // If the notification is new, call any registered callbacks.
  GcsNodeInfo &cache_data = node_cache_[node_id];
  if (is_notif_new) {
    if (is_alive) {
      RAY_CHECK(removed_nodes_.find(node_id) == removed_nodes_.end());
    } else {
      // NOTE(swang): The node should be added to this data structure before
      // the callback gets called, in case the callback depends on the data
      // structure getting updated.
      removed_nodes_.insert(node_id);
    }
    if (node_change_callback_ != nullptr) {
      node_change_callback_(node_id, cache_data);
    }
  }
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
  RAY_LOG(DEBUG) << "Getting task, task id = " << task_id;
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
                       << ", task id = " << task_id;
      });
  return Status::OK();
}

Status ServiceBasedTaskInfoAccessor::AsyncDelete(const std::vector<TaskID> &task_ids,
                                                 const StatusCallback &callback) {
  RAY_LOG(DEBUG) << "Deleting tasks, task id list size = " << task_ids.size();
  rpc::DeleteTasksRequest request;
  for (auto &task_id : task_ids) {
    request.add_task_id_list(task_id.Binary());
  }
  client_impl_->GetGcsRpcClient().DeleteTasks(
      request,
      [task_ids, callback](const Status &status, const rpc::DeleteTasksReply &reply) {
        if (callback) {
          callback(status);
        }
        RAY_LOG(DEBUG) << "Finished deleting tasks, status = " << status
                       << ", task id list size = " << task_ids.size();
      });
  return Status::OK();
}

Status ServiceBasedTaskInfoAccessor::AsyncSubscribe(
    const TaskID &task_id, const SubscribeCallback<TaskID, rpc::TaskTableData> &subscribe,
    const StatusCallback &done) {
  RAY_LOG(DEBUG) << "Subscribing task, task id = " << task_id;
  RAY_CHECK(subscribe != nullptr) << "Failed to subscribe task, task id = " << task_id;
  auto on_subscribe = [subscribe](const std::string &id, const std::string &data) {
    TaskTableData task_data;
    task_data.ParseFromString(data);
    subscribe(TaskID::FromBinary(id), task_data);
  };
  auto status = client_impl_->GetGcsPubSub().Subscribe(TASK_CHANNEL, task_id.Binary(),
                                                       on_subscribe, done);
  RAY_LOG(DEBUG) << "Finished subscribing task, task id = " << task_id;
  return status;
}

Status ServiceBasedTaskInfoAccessor::AsyncUnsubscribe(const TaskID &task_id,
                                                      const StatusCallback &done) {
  RAY_LOG(DEBUG) << "Unsubscribing task, task id = " << task_id;
  auto status = client_impl_->GetGcsPubSub().Unsubscribe(TASK_CHANNEL, task_id.Binary());
  if (done) {
    done(status);
  }
  RAY_LOG(DEBUG) << "Finished unsubscribing task, task id = " << task_id;
  return status;
}

Status ServiceBasedTaskInfoAccessor::AsyncAddTaskLease(
    const std::shared_ptr<rpc::TaskLeaseData> &data_ptr, const StatusCallback &callback) {
  TaskID task_id = TaskID::FromBinary(data_ptr->task_id());
  ClientID node_id = ClientID::FromBinary(data_ptr->node_manager_id());
  RAY_LOG(DEBUG) << "Adding task lease, task id = " << task_id
                 << ", node id = " << node_id;
  rpc::AddTaskLeaseRequest request;
  request.mutable_task_lease_data()->CopyFrom(*data_ptr);
  client_impl_->GetGcsRpcClient().AddTaskLease(
      request, [task_id, node_id, callback](const Status &status,
                                            const rpc::AddTaskLeaseReply &reply) {
        if (callback) {
          callback(status);
        }
        RAY_LOG(DEBUG) << "Finished adding task lease, status = " << status
                       << ", task id = " << task_id << ", node id = " << node_id;
      });
  return Status::OK();
}

Status ServiceBasedTaskInfoAccessor::AsyncSubscribeTaskLease(
    const TaskID &task_id,
    const SubscribeCallback<TaskID, boost::optional<rpc::TaskLeaseData>> &subscribe,
    const StatusCallback &done) {
  RAY_LOG(DEBUG) << "Subscribing task lease, task id = " << task_id;
  RAY_CHECK(subscribe != nullptr)
      << "Failed to subscribe task lease, task id = " << task_id;
  auto on_subscribe = [subscribe](const std::string &id, const std::string &data) {
    TaskLeaseData task_lease_data;
    task_lease_data.ParseFromString(data);
    subscribe(TaskID::FromBinary(id), task_lease_data);
  };
  auto status = client_impl_->GetGcsPubSub().Subscribe(
      TASK_LEASE_CHANNEL, task_id.Binary(), on_subscribe, done);
  RAY_LOG(DEBUG) << "Finished subscribing task lease, task id = " << task_id;
  return status;
}

Status ServiceBasedTaskInfoAccessor::AsyncUnsubscribeTaskLease(
    const TaskID &task_id, const StatusCallback &done) {
  RAY_LOG(DEBUG) << "Unsubscribing task lease, task id = " << task_id;
  auto status =
      client_impl_->GetGcsPubSub().Unsubscribe(TASK_LEASE_CHANNEL, task_id.Binary());
  if (done) {
    done(status);
  }
  RAY_LOG(DEBUG) << "Finished unsubscribing task lease, task id = " << task_id;
  return status;
}

Status ServiceBasedTaskInfoAccessor::AttemptTaskReconstruction(
    const std::shared_ptr<rpc::TaskReconstructionData> &data_ptr,
    const StatusCallback &callback) {
  ClientID node_id = ClientID::FromBinary(data_ptr->node_manager_id());
  RAY_LOG(DEBUG) << "Reconstructing task, reconstructions num = "
                 << data_ptr->num_reconstructions() << ", node id = " << node_id;
  rpc::AttemptTaskReconstructionRequest request;
  request.mutable_task_reconstruction()->CopyFrom(*data_ptr);
  client_impl_->GetGcsRpcClient().AttemptTaskReconstruction(
      request,
      [data_ptr, node_id, callback](const Status &status,
                                    const rpc::AttemptTaskReconstructionReply &reply) {
        if (callback) {
          callback(status);
        }
        RAY_LOG(DEBUG) << "Finished reconstructing task, status = " << status
                       << ", reconstructions num = " << data_ptr->num_reconstructions()
                       << ", node id = " << node_id;
      });
  return Status::OK();
}

ServiceBasedObjectInfoAccessor::ServiceBasedObjectInfoAccessor(
    ServiceBasedGcsClient *client_impl)
    : client_impl_(client_impl) {}

Status ServiceBasedObjectInfoAccessor::AsyncGetLocations(
    const ObjectID &object_id, const MultiItemCallback<rpc::ObjectTableData> &callback) {
  RAY_LOG(DEBUG) << "Getting object locations, object id = " << object_id;
  rpc::GetObjectLocationsRequest request;
  request.set_object_id(object_id.Binary());
  client_impl_->GetGcsRpcClient().GetObjectLocations(
      request, [object_id, callback](const Status &status,
                                     const rpc::GetObjectLocationsReply &reply) {
        std::vector<ObjectTableData> result;
        result.reserve((reply.object_table_data_list_size()));
        for (int index = 0; index < reply.object_table_data_list_size(); ++index) {
          result.emplace_back(reply.object_table_data_list(index));
        }
        callback(status, result);
        RAY_LOG(DEBUG) << "Finished getting object locations, status = " << status
                       << ", object id = " << object_id;
      });
  return Status::OK();
}

Status ServiceBasedObjectInfoAccessor::AsyncAddLocation(const ObjectID &object_id,
                                                        const ClientID &node_id,
                                                        const StatusCallback &callback) {
  RAY_LOG(DEBUG) << "Adding object location, object id = " << object_id
                 << ", node id = " << node_id;
  rpc::AddObjectLocationRequest request;
  request.set_object_id(object_id.Binary());
  request.set_node_id(node_id.Binary());

  auto operation = [this, request, object_id, node_id,
                    callback](const SequencerDoneCallback &done_callback) {
    client_impl_->GetGcsRpcClient().AddObjectLocation(
        request, [object_id, node_id, callback, done_callback](
                     const Status &status, const rpc::AddObjectLocationReply &reply) {
          if (callback) {
            callback(status);
          }

          RAY_LOG(DEBUG) << "Finished adding object location, status = " << status
                         << ", object id = " << object_id << ", node id = " << node_id;
          done_callback();
        });
  };

  sequencer_.Post(object_id, operation);
  return Status::OK();
}

Status ServiceBasedObjectInfoAccessor::AsyncRemoveLocation(
    const ObjectID &object_id, const ClientID &node_id, const StatusCallback &callback) {
  RAY_LOG(DEBUG) << "Removing object location, object id = " << object_id
                 << ", node id = " << node_id;
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
                         << ", object id = " << object_id << ", node id = " << node_id;
          done_callback();
        });
  };

  sequencer_.Post(object_id, operation);
  return Status::OK();
}

Status ServiceBasedObjectInfoAccessor::AsyncSubscribeToLocations(
    const ObjectID &object_id,
    const SubscribeCallback<ObjectID, ObjectChangeNotification> &subscribe,
    const StatusCallback &done) {
  RAY_LOG(DEBUG) << "Subscribing object location, object id = " << object_id;
  RAY_CHECK(subscribe != nullptr)
      << "Failed to subscribe object location, object id = " << object_id;
  auto on_subscribe = [subscribe](const std::string &id, const std::string &data) {
    rpc::ObjectChange object_change;
    object_change.ParseFromString(data);
    auto object_data_vector = std::vector<rpc::ObjectTableData>();
    object_data_vector.emplace_back(object_change.data());
    gcs::ObjectChangeNotification notification(object_change.change_mode(),
                                               object_data_vector);
    subscribe(ObjectID::FromBinary(id), notification);
  };
  auto status = client_impl_->GetGcsPubSub().Subscribe(OBJECT_CHANNEL, object_id.Binary(),
                                                       on_subscribe, done);
  RAY_LOG(DEBUG) << "Finished subscribing object location, object id = " << object_id;
  return status;
}

Status ServiceBasedObjectInfoAccessor::AsyncUnsubscribeToLocations(
    const ObjectID &object_id, const StatusCallback &done) {
  RAY_LOG(DEBUG) << "Unsubscribing object location, object id = " << object_id;
  auto status =
      client_impl_->GetGcsPubSub().Unsubscribe(OBJECT_CHANNEL, object_id.Binary());
  if (done) {
    done(status);
  }
  RAY_LOG(DEBUG) << "Finished unsubscribing object location, object id = " << object_id;
  return status;
}

ServiceBasedStatsInfoAccessor::ServiceBasedStatsInfoAccessor(
    ServiceBasedGcsClient *client_impl)
    : client_impl_(client_impl) {}

Status ServiceBasedStatsInfoAccessor::AsyncAddProfileData(
    const std::shared_ptr<rpc::ProfileTableData> &data_ptr,
    const StatusCallback &callback) {
  ClientID node_id = ClientID::FromBinary(data_ptr->component_id());
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

ServiceBasedErrorInfoAccessor::ServiceBasedErrorInfoAccessor(
    ServiceBasedGcsClient *client_impl)
    : client_impl_(client_impl) {}

Status ServiceBasedErrorInfoAccessor::AsyncReportJobError(
    const std::shared_ptr<rpc::ErrorTableData> &data_ptr,
    const StatusCallback &callback) {
  JobID job_id = JobID::FromBinary(data_ptr->job_id());
  std::string type = data_ptr->type();
  RAY_LOG(DEBUG) << "Reporting job error, job id = " << job_id << ", type = " << type;
  rpc::ReportJobErrorRequest request;
  request.mutable_error_data()->CopyFrom(*data_ptr);
  client_impl_->GetGcsRpcClient().ReportJobError(
      request, [job_id, type, callback](const Status &status,
                                        const rpc::ReportJobErrorReply &reply) {
        if (callback) {
          callback(status);
        }
        RAY_LOG(DEBUG) << "Finished reporting job error, status = " << status
                       << ", job id = " << job_id << ", type = " << type;
      });
  return Status::OK();
}

ServiceBasedWorkerInfoAccessor::ServiceBasedWorkerInfoAccessor(
    ServiceBasedGcsClient *client_impl)
    : client_impl_(client_impl) {}

Status ServiceBasedWorkerInfoAccessor::AsyncSubscribeToWorkerFailures(
    const SubscribeCallback<WorkerID, rpc::WorkerFailureData> &subscribe,
    const StatusCallback &done) {
  RAY_LOG(DEBUG) << "Subscribing worker failures.";
  RAY_CHECK(subscribe != nullptr);
  auto on_subscribe = [subscribe](const std::string &id, const std::string &data) {
    rpc::WorkerFailureData worker_failure_data;
    worker_failure_data.ParseFromString(data);
    subscribe(WorkerID::FromBinary(id), worker_failure_data);
  };
  auto status = client_impl_->GetGcsPubSub().SubscribeAll(WORKER_FAILURE_CHANNEL,
                                                          on_subscribe, done);
  RAY_LOG(DEBUG) << "Finished subscribing worker failures.";
  return status;
}

Status ServiceBasedWorkerInfoAccessor::AsyncReportWorkerFailure(
    const std::shared_ptr<rpc::WorkerFailureData> &data_ptr,
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

Status ServiceBasedWorkerInfoAccessor::AsyncRegisterWorker(
    rpc::WorkerType worker_type, const WorkerID &worker_id,
    const std::unordered_map<std::string, std::string> &worker_info,
    const StatusCallback &callback) {
  RAY_LOG(DEBUG) << "Registering the worker. worker id = " << worker_id;
  rpc::RegisterWorkerRequest request;
  request.set_worker_type(worker_type);
  request.set_worker_id(worker_id.Binary());
  request.mutable_worker_info()->insert(worker_info.begin(), worker_info.end());
  client_impl_->GetGcsRpcClient().RegisterWorker(
      request,
      [worker_id, callback](const Status &status, const rpc::RegisterWorkerReply &reply) {
        if (callback) {
          callback(status);
        }
        RAY_LOG(DEBUG) << "Finished registering worker. worker id = " << worker_id;
      });
  return Status::OK();
}

}  // namespace gcs
}  // namespace ray
