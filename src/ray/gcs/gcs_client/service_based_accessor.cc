#include "ray/gcs/gcs_client/service_based_accessor.h"
#include "ray/gcs/gcs_client/service_based_gcs_client.h"

namespace ray {
namespace gcs {

ServiceBasedJobInfoAccessor::ServiceBasedJobInfoAccessor(
    ServiceBasedGcsClient *client_impl)
    : client_impl_(client_impl),
      job_sub_executor_(client_impl->GetRedisGcsClient().job_table()) {}

Status ServiceBasedJobInfoAccessor::AsyncAdd(
    const std::shared_ptr<JobTableData> &data_ptr, const StatusCallback &callback) {
  JobID job_id = JobID::FromBinary(data_ptr->job_id());
  RAY_LOG(INFO) << "Adding job, job id = " << job_id
                << ", driver pid = " << data_ptr->driver_pid();
  rpc::AddJobRequest request;
  request.mutable_data()->CopyFrom(*data_ptr);
  client_impl_->GetGcsRpcClient().AddJob(
      request, [callback](const Status &status, const rpc::AddJobReply &reply) {
        if (callback) {
          callback(status);
        }
      });
  RAY_LOG(INFO) << "Finished adding job, job id = " << job_id
                << ", driver pid = " << data_ptr->driver_pid();
  return Status::OK();
}

Status ServiceBasedJobInfoAccessor::AsyncMarkFinished(const JobID &job_id,
                                                      const StatusCallback &callback) {
  RAY_LOG(INFO) << "Marking job state, job id = " << job_id;
  rpc::MarkJobFinishedRequest request;
  request.set_job_id(job_id.Binary());
  client_impl_->GetGcsRpcClient().MarkJobFinished(
      request, [callback](const Status &status, const rpc::MarkJobFinishedReply &reply) {
        if (callback) {
          callback(status);
        }
      });
  RAY_LOG(INFO) << "Finished marking job state, job id = " << job_id;
  return Status::OK();
}

Status ServiceBasedJobInfoAccessor::AsyncSubscribeToFinishedJobs(
    const SubscribeCallback<JobID, JobTableData> &subscribe, const StatusCallback &done) {
  RAY_LOG(INFO) << "Subscribing finished job.";
  RAY_CHECK(subscribe != nullptr);
  auto on_subscribe = [subscribe](const JobID &job_id, const JobTableData &job_data) {
    if (job_data.is_dead()) {
      subscribe(job_id, job_data);
    }
  };
  Status status =
      job_sub_executor_.AsyncSubscribeAll(ClientID::Nil(), on_subscribe, done);
  RAY_LOG(INFO) << "Finished subscribing finished job.";
  return status;
}

ServiceBasedActorInfoAccessor::ServiceBasedActorInfoAccessor(
    ServiceBasedGcsClient *client_impl)
    : client_impl_(client_impl),
      actor_sub_executor_(client_impl->GetRedisGcsClient().actor_table()) {}

Status ServiceBasedActorInfoAccessor::AsyncGet(
    const ActorID &actor_id, const OptionalItemCallback<rpc::ActorTableData> &callback) {
  RAY_LOG(INFO) << "Getting actor info, actor id = " << actor_id;
  rpc::GetActorInfoRequest request;
  request.set_actor_id(actor_id.Binary());
  client_impl_->GetGcsRpcClient().GetActorInfo(
      request, [callback](const Status &status, const rpc::GetActorInfoReply &reply) {
        rpc::ActorTableData actor_table_data;
        actor_table_data.CopyFrom(reply.actor_table_data());
        callback(status, actor_table_data);
      });
  RAY_LOG(INFO) << "Finished getting actor info, actor id = " << actor_id;
  return Status::OK();
}

Status ServiceBasedActorInfoAccessor::AsyncRegister(
    const std::shared_ptr<rpc::ActorTableData> &data_ptr,
    const StatusCallback &callback) {
  ActorID actor_id = ActorID::FromBinary(data_ptr->actor_id());
  RAY_LOG(INFO) << "Registering actor info, actor id = " << actor_id;
  rpc::RegisterActorInfoRequest request;
  request.mutable_actor_table_data()->CopyFrom(*data_ptr);
  client_impl_->GetGcsRpcClient().RegisterActorInfo(
      request,
      [callback](const Status &status, const rpc::RegisterActorInfoReply &reply) {
        if (callback) {
          callback(status);
        }
      });
  RAY_LOG(INFO) << "Finished registering actor info, actor id = " << actor_id;
  return Status::OK();
}

Status ServiceBasedActorInfoAccessor::AsyncUpdate(
    const ActorID &actor_id, const std::shared_ptr<rpc::ActorTableData> &data_ptr,
    const StatusCallback &callback) {
  RAY_LOG(INFO) << "Updating actor info, actor id = " << actor_id;
  rpc::UpdateActorInfoRequest request;
  request.set_actor_id(actor_id.Binary());
  request.mutable_actor_table_data()->CopyFrom(*data_ptr);
  client_impl_->GetGcsRpcClient().UpdateActorInfo(
      request, [callback](const Status &status, const rpc::UpdateActorInfoReply &reply) {
        if (callback) {
          callback(status);
        }
      });
  RAY_LOG(INFO) << "Finished updating actor info, actor id = " << actor_id;
  return Status::OK();
}

Status ServiceBasedActorInfoAccessor::AsyncSubscribeAll(
    const SubscribeCallback<ActorID, rpc::ActorTableData> &subscribe,
    const StatusCallback &done) {
  RAY_LOG(INFO) << "Subscribing register or update operations of actors.";
  RAY_CHECK(subscribe != nullptr);
  auto status = actor_sub_executor_.AsyncSubscribeAll(ClientID::Nil(), subscribe, done);
  RAY_LOG(INFO) << "Finished subscribing register or update operations of actors.";
  return status;
}

Status ServiceBasedActorInfoAccessor::AsyncSubscribe(
    const ActorID &actor_id,
    const SubscribeCallback<ActorID, rpc::ActorTableData> &subscribe,
    const StatusCallback &done) {
  RAY_LOG(INFO) << "Subscribing update operations of actor, actor id = " << actor_id;
  RAY_CHECK(subscribe != nullptr);
  auto status =
      actor_sub_executor_.AsyncSubscribe(subscribe_id_, actor_id, subscribe, done);
  RAY_LOG(INFO) << "Finished subscribing update operations of actor, actor id = "
                << actor_id;
  return status;
}

Status ServiceBasedActorInfoAccessor::AsyncUnsubscribe(const ActorID &actor_id,
                                                       const StatusCallback &done) {
  RAY_LOG(INFO) << "Cancelling subscription to an actor, actor id = " << actor_id;
  auto status = actor_sub_executor_.AsyncUnsubscribe(subscribe_id_, actor_id, done);
  RAY_LOG(INFO) << "Finished cancelling subscription to an actor, actor id = "
                << actor_id;
  return status;
}

Status ServiceBasedActorInfoAccessor::AsyncAddCheckpoint(
    const std::shared_ptr<rpc::ActorCheckpointData> &data_ptr,
    const StatusCallback &callback) {
  ActorID actor_id = ActorID::FromBinary(data_ptr->actor_id());
  ActorCheckpointID checkpoint_id =
      ActorCheckpointID::FromBinary(data_ptr->checkpoint_id());
  RAY_LOG(INFO) << "Adding actor checkpoint, actor id = " << actor_id
                << ", checkpoint id = " << checkpoint_id;
  rpc::AddActorCheckpointRequest request;
  request.mutable_checkpoint_data()->CopyFrom(*data_ptr);
  client_impl_->GetGcsRpcClient().AddActorCheckpoint(
      request,
      [callback](const Status &status, const rpc::AddActorCheckpointReply &reply) {
        if (callback) {
          callback(status);
        }
      });
  RAY_LOG(INFO) << "Finished adding actor checkpoint, actor id = " << actor_id
                << ", checkpoint id = " << checkpoint_id;
  return Status::OK();
}

Status ServiceBasedActorInfoAccessor::AsyncGetCheckpoint(
    const ActorCheckpointID &checkpoint_id,
    const OptionalItemCallback<rpc::ActorCheckpointData> &callback) {
  RAY_LOG(INFO) << "Getting actor checkpoint, checkpoint id = " << checkpoint_id;
  rpc::GetActorCheckpointRequest request;
  request.set_checkpoint_id(checkpoint_id.Binary());
  client_impl_->GetGcsRpcClient().GetActorCheckpoint(
      request,
      [callback](const Status &status, const rpc::GetActorCheckpointReply &reply) {
        rpc::ActorCheckpointData checkpoint_data;
        checkpoint_data.CopyFrom(reply.checkpoint_data());
        callback(status, checkpoint_data);
      });
  RAY_LOG(INFO) << "Finished getting actor checkpoint, checkpoint id = " << checkpoint_id;
  return Status::OK();
}

Status ServiceBasedActorInfoAccessor::AsyncGetCheckpointID(
    const ActorID &actor_id,
    const OptionalItemCallback<rpc::ActorCheckpointIdData> &callback) {
  RAY_LOG(INFO) << "Getting actor checkpoint id, actor id = " << actor_id;
  rpc::GetActorCheckpointIDRequest request;
  request.set_actor_id(actor_id.Binary());
  client_impl_->GetGcsRpcClient().GetActorCheckpointID(
      request,
      [callback](const Status &status, const rpc::GetActorCheckpointIDReply &reply) {
        rpc::ActorCheckpointIdData checkpoint_id_data;
        checkpoint_id_data.CopyFrom(reply.checkpoint_id_data());
        callback(status, checkpoint_id_data);
      });
  RAY_LOG(INFO) << "Finished getting actor checkpoint id, actor id = " << actor_id;
  return Status::OK();
}

ServiceBasedNodeInfoAccessor::ServiceBasedNodeInfoAccessor(
    ServiceBasedGcsClient *client_impl)
    : client_impl_(client_impl),
      resource_sub_executor_(client_impl->GetRedisGcsClient().resource_table()),
      heartbeat_sub_executor_(client_impl->GetRedisGcsClient().heartbeat_table()),
      heartbeat_batch_sub_executor_(
          client_impl->GetRedisGcsClient().heartbeat_batch_table()) {}

Status ServiceBasedNodeInfoAccessor::RegisterSelf(const GcsNodeInfo &local_node_info) {
  auto node_id = ClientID::FromBinary(local_node_info.node_id());
  RAY_LOG(INFO) << "Registering node info, node id = " << node_id
                << ", address is = " << local_node_info.node_manager_address();
  RAY_CHECK(local_node_id_.IsNil()) << "This node is already connected.";
  RAY_CHECK(local_node_info.state() == GcsNodeInfo::ALIVE);
  rpc::RegisterNodeRequest request;
  request.mutable_node_info()->CopyFrom(local_node_info);
  std::promise<Status> promise;
  client_impl_->GetGcsRpcClient().RegisterNode(
      request, [&promise](const Status &status, const rpc::RegisterNodeReply &reply) {
        promise.set_value(status);
      });

  Status ret = promise.get_future().get();
  if (ret.ok()) {
    local_node_info_.CopyFrom(local_node_info);
    local_node_id_ = ClientID::FromBinary(local_node_info.node_id());
  }
  RAY_LOG(INFO) << "Finished registering node info, node id = " << node_id;
  return Status::OK();
}

Status ServiceBasedNodeInfoAccessor::UnregisterSelf() {
  RAY_CHECK(!local_node_id_.IsNil()) << "This node is disconnected.";
  ClientID node_id = ClientID::FromBinary(local_node_info_.node_id());
  RAY_LOG(INFO) << "Unregistering node info, node id = " << node_id;
  rpc::UnregisterNodeRequest request;
  request.set_node_id(local_node_info_.node_id());
  std::promise<Status> promise;
  client_impl_->GetGcsRpcClient().UnregisterNode(
      request, [&promise](const Status &status, const rpc::UnregisterNodeReply &reply) {
        promise.set_value(status);
      });
  Status ret = promise.get_future().get();
  if (ret.ok()) {
    local_node_info_.set_state(GcsNodeInfo::DEAD);
    local_node_id_ = ClientID::Nil();
  }
  RAY_LOG(INFO) << "Finished unregistering node info, node id = " << node_id;
  return ret;
}

const ClientID &ServiceBasedNodeInfoAccessor::GetSelfId() const { return local_node_id_; }

const GcsNodeInfo &ServiceBasedNodeInfoAccessor::GetSelfInfo() const {
  return local_node_info_;
}

Status ServiceBasedNodeInfoAccessor::AsyncRegister(const rpc::GcsNodeInfo &node_info,
                                                   const StatusCallback &callback) {
  ClientID node_id = ClientID::FromBinary(node_info.node_id());
  RAY_LOG(INFO) << "Registering node info, node id = " << node_id;
  rpc::RegisterNodeRequest request;
  request.mutable_node_info()->CopyFrom(node_info);
  client_impl_->GetGcsRpcClient().RegisterNode(
      request, [callback](const Status &status, const rpc::RegisterNodeReply &reply) {
        if (callback) {
          callback(status);
        }
      });
  RAY_LOG(INFO) << "Finished registering node info, node id = " << node_id;
  return Status::OK();
}

Status ServiceBasedNodeInfoAccessor::AsyncUnregister(const ClientID &node_id,
                                                     const StatusCallback &callback) {
  RAY_LOG(INFO) << "Unregistering node info, node id = " << node_id;
  rpc::UnregisterNodeRequest request;
  request.set_node_id(node_id.Binary());
  client_impl_->GetGcsRpcClient().UnregisterNode(
      request, [callback](const Status &status, const rpc::UnregisterNodeReply &reply) {
        if (callback) {
          callback(status);
        }
      });
  RAY_LOG(INFO) << "Finished unregistering node info, node id = " << node_id;
  return Status::OK();
}

Status ServiceBasedNodeInfoAccessor::AsyncGetAll(
    const MultiItemCallback<GcsNodeInfo> &callback) {
  RAY_LOG(INFO) << "Getting information of all nodes.";
  rpc::GetAllNodeInfoRequest request;
  client_impl_->GetGcsRpcClient().GetAllNodeInfo(
      request, [callback](const Status &status, const rpc::GetAllNodeInfoReply &reply) {
        std::vector<GcsNodeInfo> result;
        result.reserve((reply.node_info_list_size()));
        for (int index = 0; index < reply.node_info_list_size(); ++index) {
          result.emplace_back(reply.node_info_list(index));
        }
        callback(status, result);
      });
  RAY_LOG(INFO) << "Finished getting information of all nodes.";
  return Status::OK();
}

Status ServiceBasedNodeInfoAccessor::AsyncSubscribeToNodeChange(
    const SubscribeCallback<ClientID, GcsNodeInfo> &subscribe,
    const StatusCallback &done) {
  RAY_LOG(INFO) << "Subscribing node change.";
  RAY_CHECK(subscribe != nullptr);
  ClientTable &client_table = client_impl_->GetRedisGcsClient().client_table();
  auto status = client_table.SubscribeToNodeChange(subscribe, done);
  RAY_LOG(INFO) << "Finished subscribing node change.";
  return status;
}

boost::optional<GcsNodeInfo> ServiceBasedNodeInfoAccessor::Get(
    const ClientID &node_id) const {
  GcsNodeInfo node_info;
  ClientTable &client_table = client_impl_->GetRedisGcsClient().client_table();
  bool found = client_table.GetClient(node_id, &node_info);
  boost::optional<GcsNodeInfo> optional_node;
  if (found) {
    optional_node = std::move(node_info);
  }
  return optional_node;
}

const std::unordered_map<ClientID, GcsNodeInfo> &ServiceBasedNodeInfoAccessor::GetAll()
    const {
  ClientTable &client_table = client_impl_->GetRedisGcsClient().client_table();
  return client_table.GetAllClients();
}

bool ServiceBasedNodeInfoAccessor::IsRemoved(const ClientID &node_id) const {
  ClientTable &client_table = client_impl_->GetRedisGcsClient().client_table();
  return client_table.IsRemoved(node_id);
}

Status ServiceBasedNodeInfoAccessor::AsyncGetResources(
    const ClientID &node_id, const OptionalItemCallback<ResourceMap> &callback) {
  RAY_LOG(INFO) << "Getting node resources, node id = " << node_id;
  rpc::GetResourcesRequest request;
  request.set_node_id(node_id.Binary());
  client_impl_->GetGcsRpcClient().GetResources(
      request, [callback](const Status &status, const rpc::GetResourcesReply &reply) {
        ResourceMap resource_map;
        for (auto resource : reply.resources()) {
          resource_map[resource.first] =
              std::make_shared<rpc::ResourceTableData>(resource.second);
        }
        callback(status, resource_map);
      });
  RAY_LOG(INFO) << "Finished getting node resources, node id = " << node_id;
  return Status::OK();
}

Status ServiceBasedNodeInfoAccessor::AsyncUpdateResources(
    const ClientID &node_id, const ResourceMap &resources,
    const StatusCallback &callback) {
  RAY_LOG(INFO) << "Updating node resources, node id = " << node_id;
  rpc::UpdateResourcesRequest request;
  request.set_node_id(node_id.Binary());
  for (auto &resource : resources) {
    (*request.mutable_resources())[resource.first] = *resource.second;
  }
  client_impl_->GetGcsRpcClient().UpdateResources(
      request, [callback](const Status &status, const rpc::UpdateResourcesReply &reply) {
        if (callback) {
          callback(status);
        }
      });
  RAY_LOG(INFO) << "Finished updating node resources, node id = " << node_id;
  return Status::OK();
}

Status ServiceBasedNodeInfoAccessor::AsyncDeleteResources(
    const ClientID &node_id, const std::vector<std::string> &resource_names,
    const StatusCallback &callback) {
  RAY_LOG(INFO) << "Deleting node resources, node id = " << node_id;
  rpc::DeleteResourcesRequest request;
  request.set_node_id(node_id.Binary());
  for (auto &resource_name : resource_names) {
    request.add_resource_name_list(resource_name);
  }
  client_impl_->GetGcsRpcClient().DeleteResources(
      request, [callback](const Status &status, const rpc::DeleteResourcesReply &reply) {
        if (callback) {
          callback(status);
        }
      });
  RAY_LOG(INFO) << "Finished deleting node resources, node id = " << node_id;
  return Status::OK();
}

Status ServiceBasedNodeInfoAccessor::AsyncSubscribeToResources(
    const SubscribeCallback<ClientID, ResourceChangeNotification> &subscribe,
    const StatusCallback &done) {
  RAY_LOG(INFO) << "Subscribing node resources change.";
  RAY_CHECK(subscribe != nullptr);
  auto status =
      resource_sub_executor_.AsyncSubscribeAll(ClientID::Nil(), subscribe, done);
  RAY_LOG(INFO) << "Finished subscribing node resources change.";
  return status;
}

Status ServiceBasedNodeInfoAccessor::AsyncReportHeartbeat(
    const std::shared_ptr<rpc::HeartbeatTableData> &data_ptr,
    const StatusCallback &callback) {
  ClientID node_id = ClientID::FromBinary(data_ptr->client_id());
  RAY_LOG(INFO) << "Reporting heartbeat, node id = " << node_id;
  rpc::ReportHeartbeatRequest request;
  request.mutable_heartbeat()->CopyFrom(*data_ptr);
  client_impl_->GetGcsRpcClient().ReportHeartbeat(
      request, [callback](const Status &status, const rpc::ReportHeartbeatReply &reply) {
        if (callback) {
          callback(status);
        }
      });
  RAY_LOG(INFO) << "Finished reporting heartbeat, node id = " << node_id;
  return Status::OK();
}

Status ServiceBasedNodeInfoAccessor::AsyncSubscribeHeartbeat(
    const SubscribeCallback<ClientID, rpc::HeartbeatTableData> &subscribe,
    const StatusCallback &done) {
  RAY_LOG(INFO) << "Subscribing heartbeat.";
  RAY_CHECK(subscribe != nullptr);
  auto status =
      heartbeat_sub_executor_.AsyncSubscribeAll(ClientID::Nil(), subscribe, done);
  RAY_LOG(INFO) << "Finished subscribing heartbeat.";
  return status;
}

Status ServiceBasedNodeInfoAccessor::AsyncReportBatchHeartbeat(
    const std::shared_ptr<rpc::HeartbeatBatchTableData> &data_ptr,
    const StatusCallback &callback) {
  RAY_LOG(INFO) << "Reporting batch heartbeat, batch size = " << data_ptr->batch_size();
  rpc::ReportBatchHeartbeatRequest request;
  request.mutable_heartbeat_batch()->CopyFrom(*data_ptr);
  client_impl_->GetGcsRpcClient().ReportBatchHeartbeat(
      request,
      [callback](const Status &status, const rpc::ReportBatchHeartbeatReply &reply) {
        if (callback) {
          callback(status);
        }
      });
  RAY_LOG(INFO) << "Finished reporting batch heartbeat, batch size = "
                << data_ptr->batch_size();
  return Status::OK();
}

Status ServiceBasedNodeInfoAccessor::AsyncSubscribeBatchHeartbeat(
    const ItemCallback<rpc::HeartbeatBatchTableData> &subscribe,
    const StatusCallback &done) {
  RAY_LOG(INFO) << "Subscribing batch heartbeat.";
  RAY_CHECK(subscribe != nullptr);
  auto on_subscribe = [subscribe](const ClientID &node_id,
                                  const HeartbeatBatchTableData &data) {
    subscribe(data);
  };
  auto status = heartbeat_batch_sub_executor_.AsyncSubscribeAll(ClientID::Nil(),
                                                                on_subscribe, done);
  RAY_LOG(INFO) << "Finished subscribing batch heartbeat.";
  return status;
}

ServiceBasedTaskInfoAccessor::ServiceBasedTaskInfoAccessor(
    ServiceBasedGcsClient *client_impl)
    : client_impl_(client_impl),
      task_sub_executor_(client_impl->GetRedisGcsClient().raylet_task_table()),
      task_lease_sub_executor_(client_impl->GetRedisGcsClient().task_lease_table()) {}

Status ServiceBasedTaskInfoAccessor::AsyncAdd(
    const std::shared_ptr<rpc::TaskTableData> &data_ptr, const StatusCallback &callback) {
  TaskID task_id = TaskID::FromBinary(data_ptr->task().task_spec().task_id());
  JobID job_id = JobID::FromBinary(data_ptr->task().task_spec().job_id());
  RAY_LOG(INFO) << "Adding task, task id = " << task_id << ", job id = " << job_id;
  rpc::AddTaskRequest request;
  request.mutable_task_data()->CopyFrom(*data_ptr);
  client_impl_->GetGcsRpcClient().AddTask(
      request, [callback](const Status &status, const rpc::AddTaskReply &reply) {
        if (callback) {
          callback(status);
        }
      });
  RAY_LOG(INFO) << "Finished adding task, task id = " << task_id
                << ", job id = " << job_id;
  return Status::OK();
}

Status ServiceBasedTaskInfoAccessor::AsyncGet(
    const TaskID &task_id, const OptionalItemCallback<rpc::TaskTableData> &callback) {
  RAY_LOG(INFO) << "Getting task, task id = " << task_id;
  rpc::GetTaskRequest request;
  request.set_task_id(task_id.Binary());
  client_impl_->GetGcsRpcClient().GetTask(
      request, [callback](const Status &status, const rpc::GetTaskReply &reply) {
        TaskTableData task_table_data;
        task_table_data.CopyFrom(reply.task_data());
        callback(status, task_table_data);
      });
  RAY_LOG(INFO) << "Finished getting task, task id = " << task_id;
  return Status::OK();
}

Status ServiceBasedTaskInfoAccessor::AsyncDelete(const std::vector<TaskID> &task_ids,
                                                 const StatusCallback &callback) {
  RAY_LOG(INFO) << "Deleting tasks, task id list size = " << task_ids.size();
  rpc::DeleteTasksRequest request;
  for (auto &task_id : task_ids) {
    request.add_task_id_list(task_id.Binary());
  }
  client_impl_->GetGcsRpcClient().DeleteTasks(
      request, [callback](const Status &status, const rpc::DeleteTasksReply &reply) {
        if (callback) {
          callback(status);
        }
      });
  RAY_LOG(INFO) << "Finished deleting tasks, task id list size = " << task_ids.size();
  return Status::OK();
}

Status ServiceBasedTaskInfoAccessor::AsyncSubscribe(
    const TaskID &task_id, const SubscribeCallback<TaskID, rpc::TaskTableData> &subscribe,
    const StatusCallback &done) {
  RAY_LOG(INFO) << "Subscribing task, task id = " << task_id;
  RAY_CHECK(subscribe != nullptr);
  auto status =
      task_sub_executor_.AsyncSubscribe(subscribe_id_, task_id, subscribe, done);
  RAY_LOG(INFO) << "Finished subscribing task, task id = " << task_id;
  return status;
}

Status ServiceBasedTaskInfoAccessor::AsyncUnsubscribe(const TaskID &task_id,
                                                      const StatusCallback &done) {
  RAY_LOG(INFO) << "Unsubscribing task, task id = " << task_id;
  auto status = task_sub_executor_.AsyncUnsubscribe(subscribe_id_, task_id, done);
  RAY_LOG(INFO) << "Finished unsubscribing task, task id = " << task_id;
  return status;
}

Status ServiceBasedTaskInfoAccessor::AsyncAddTaskLease(
    const std::shared_ptr<rpc::TaskLeaseData> &data_ptr, const StatusCallback &callback) {
  TaskID task_id = TaskID::FromBinary(data_ptr->task_id());
  ClientID node_id = ClientID::FromBinary(data_ptr->node_manager_id());
  RAY_LOG(INFO) << "Adding task lease, task id = " << task_id
                << ", node id = " << node_id;
  rpc::AddTaskLeaseRequest request;
  request.mutable_task_lease_data()->CopyFrom(*data_ptr);
  client_impl_->GetGcsRpcClient().AddTaskLease(
      request, [callback](const Status &status, const rpc::AddTaskLeaseReply &reply) {
        if (callback) {
          callback(status);
        }
      });
  RAY_LOG(INFO) << "Finished adding task lease, task id = " << task_id
                << ", node id = " << node_id;
  return Status::OK();
}

Status ServiceBasedTaskInfoAccessor::AsyncSubscribeTaskLease(
    const TaskID &task_id,
    const SubscribeCallback<TaskID, boost::optional<rpc::TaskLeaseData>> &subscribe,
    const StatusCallback &done) {
  RAY_LOG(INFO) << "Subscribing task lease, task id = " << task_id;
  RAY_CHECK(subscribe != nullptr);
  auto status =
      task_lease_sub_executor_.AsyncSubscribe(subscribe_id_, task_id, subscribe, done);
  RAY_LOG(INFO) << "Finished subscribing task lease, task id = " << task_id;
  return status;
}

Status ServiceBasedTaskInfoAccessor::AsyncUnsubscribeTaskLease(
    const TaskID &task_id, const StatusCallback &done) {
  RAY_LOG(INFO) << "Unsubscribing task lease, task id = " << task_id;
  auto status = task_lease_sub_executor_.AsyncUnsubscribe(subscribe_id_, task_id, done);
  RAY_LOG(INFO) << "Finished unsubscribing task lease, task id = " << task_id;
  return status;
}

Status ServiceBasedTaskInfoAccessor::AttemptTaskReconstruction(
    const std::shared_ptr<rpc::TaskReconstructionData> &data_ptr,
    const StatusCallback &callback) {
  ClientID node_id = ClientID::FromBinary(data_ptr->node_manager_id());
  RAY_LOG(INFO) << "Reconstructing task, reconstructions num = "
                << data_ptr->num_reconstructions() << ", node id = " << node_id;
  rpc::AttemptTaskReconstructionRequest request;
  request.mutable_task_reconstruction()->CopyFrom(*data_ptr);
  client_impl_->GetGcsRpcClient().AttemptTaskReconstruction(
      request,
      [callback](const Status &status, const rpc::AttemptTaskReconstructionReply &reply) {
        if (callback) {
          callback(status);
        }
      });
  RAY_LOG(INFO) << "Finished reconstructing task, reconstructions num = "
                << data_ptr->num_reconstructions() << ", node id = " << node_id;
  return Status::OK();
}

ServiceBasedObjectInfoAccessor::ServiceBasedObjectInfoAccessor(
    ServiceBasedGcsClient *client_impl)
    : client_impl_(client_impl),
      object_sub_executor_(client_impl->GetRedisGcsClient().object_table()) {}

Status ServiceBasedObjectInfoAccessor::AsyncGetLocations(
    const ObjectID &object_id, const MultiItemCallback<rpc::ObjectTableData> &callback) {
  RAY_LOG(INFO) << "Getting object locations, object id = " << object_id;
  rpc::GetObjectLocationsRequest request;
  request.set_object_id(object_id.Binary());
  client_impl_->GetGcsRpcClient().GetObjectLocations(
      request,
      [callback](const Status &status, const rpc::GetObjectLocationsReply &reply) {
        std::vector<ObjectTableData> result;
        result.reserve((reply.object_table_data_list_size()));
        for (int index = 0; index < reply.object_table_data_list_size(); ++index) {
          result.emplace_back(reply.object_table_data_list(index));
        }
        callback(status, result);
      });
  RAY_LOG(INFO) << "Finished getting object locations, object id = " << object_id;
  return Status::OK();
}

Status ServiceBasedObjectInfoAccessor::AsyncAddLocation(const ObjectID &object_id,
                                                        const ClientID &node_id,
                                                        const StatusCallback &callback) {
  RAY_LOG(INFO) << "Adding object location, object id = " << object_id
                << ", node id = " << node_id;
  rpc::AddObjectLocationRequest request;
  request.set_object_id(object_id.Binary());
  request.set_node_id(node_id.Binary());
  client_impl_->GetGcsRpcClient().AddObjectLocation(
      request,
      [callback](const Status &status, const rpc::AddObjectLocationReply &reply) {
        if (callback) {
          callback(status);
        }
      });
  RAY_LOG(INFO) << "Finished adding object location, object id = " << object_id
                 << ", node id = " << node_id;
  return Status::OK();
}

Status ServiceBasedObjectInfoAccessor::AsyncRemoveLocation(
    const ObjectID &object_id, const ClientID &node_id, const StatusCallback &callback) {
  RAY_LOG(INFO) << "Removing object location, object id = " << object_id
                << ", node id = " << node_id;
  rpc::RemoveObjectLocationRequest request;
  request.set_object_id(object_id.Binary());
  request.set_node_id(node_id.Binary());
  client_impl_->GetGcsRpcClient().RemoveObjectLocation(
      request,
      [callback](const Status &status, const rpc::RemoveObjectLocationReply &reply) {
        if (callback) {
          callback(status);
        }
      });
  RAY_LOG(INFO) << "Finished removing object location, object id = " << object_id
                 << ", node id = " << node_id;
  return Status::OK();
}

Status ServiceBasedObjectInfoAccessor::AsyncSubscribeToLocations(
    const ObjectID &object_id,
    const SubscribeCallback<ObjectID, ObjectChangeNotification> &subscribe,
    const StatusCallback &done) {
  RAY_LOG(INFO) << "Subscribing object location, object id = " << object_id;
  RAY_CHECK(subscribe != nullptr);
  auto status =
      object_sub_executor_.AsyncSubscribe(subscribe_id_, object_id, subscribe, done);
  RAY_LOG(INFO) << "Finished subscribing object location, object id = " << object_id;
  return status;
}

Status ServiceBasedObjectInfoAccessor::AsyncUnsubscribeToLocations(
    const ObjectID &object_id, const StatusCallback &done) {
  RAY_LOG(INFO) << "Unsubscribing object location, object id = " << object_id;
  auto status = object_sub_executor_.AsyncUnsubscribe(subscribe_id_, object_id, done);
  RAY_LOG(INFO) << "Finished unsubscribing object location, object id = " << object_id;
  return status;
}

}  // namespace gcs
}  // namespace ray
