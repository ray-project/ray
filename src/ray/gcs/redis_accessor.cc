#include "ray/gcs/redis_accessor.h"
#include <boost/none.hpp>
#include "ray/gcs/pb_util.h"
#include "ray/gcs/redis_gcs_client.h"
#include "ray/util/logging.h"

namespace ray {

namespace gcs {

RedisActorInfoAccessor::RedisActorInfoAccessor(RedisGcsClient *client_impl)
    : client_impl_(client_impl), actor_sub_executor_(client_impl_->actor_table()) {}

Status RedisActorInfoAccessor::AsyncGet(
    const ActorID &actor_id, const OptionalItemCallback<ActorTableData> &callback) {
  RAY_CHECK(callback != nullptr);
  auto on_done = [callback](RedisGcsClient *client, const ActorID &actor_id,
                            const std::vector<ActorTableData> &data) {
    boost::optional<ActorTableData> result;
    if (!data.empty()) {
      result = data.back();
    }
    callback(Status::OK(), result);
  };

  return client_impl_->actor_table().Lookup(JobID::Nil(), actor_id, on_done);
}

Status RedisActorInfoAccessor::AsyncRegister(
    const std::shared_ptr<ActorTableData> &data_ptr, const StatusCallback &callback) {
  auto on_success = [callback](RedisGcsClient *client, const ActorID &actor_id,
                               const ActorTableData &data) {
    if (callback != nullptr) {
      callback(Status::OK());
    }
  };

  auto on_failure = [callback](RedisGcsClient *client, const ActorID &actor_id,
                               const ActorTableData &data) {
    if (callback != nullptr) {
      callback(Status::Invalid("Adding actor failed."));
    }
  };

  ActorID actor_id = ActorID::FromBinary(data_ptr->actor_id());
  return client_impl_->actor_table().AppendAt(JobID::Nil(), actor_id, data_ptr,
                                              on_success, on_failure,
                                              /*log_length*/ 0);
}

Status RedisActorInfoAccessor::AsyncUpdate(
    const ActorID &actor_id, const std::shared_ptr<ActorTableData> &data_ptr,
    const StatusCallback &callback) {
  // The actor log starts with an ALIVE entry. This is followed by 0 to N pairs
  // of (RECONSTRUCTING, ALIVE) entries, where N is the maximum number of
  // reconstructions. This is followed optionally by a DEAD entry.
  int log_length =
      2 * (data_ptr->max_reconstructions() - data_ptr->remaining_reconstructions());
  if (data_ptr->state() != ActorTableData::ALIVE) {
    // RECONSTRUCTING or DEAD entries have an odd index.
    log_length += 1;
  }
  RAY_LOG(DEBUG) << "AsyncUpdate actor state to " << data_ptr->state()
                 << ", actor id: " << actor_id << ", log_length: " << log_length;
  auto on_success = [callback](RedisGcsClient *client, const ActorID &actor_id,
                               const ActorTableData &data) {
    // If we successfully appended a record to the GCS table of the actor that
    // has died, signal this to anyone receiving signals from this actor.
    if (data.state() == ActorTableData::DEAD ||
        data.state() == ActorTableData::RECONSTRUCTING) {
      std::vector<std::string> args = {"XADD", actor_id.Hex(), "*", "signal",
                                       "ACTOR_DIED_SIGNAL"};
      auto redis_context = client->primary_context();
      RAY_CHECK_OK(redis_context->RunArgvAsync(args));
    }

    if (callback != nullptr) {
      callback(Status::OK());
    }
  };

  auto on_failure = [callback](RedisGcsClient *client, const ActorID &actor_id,
                               const ActorTableData &data) {
    if (callback != nullptr) {
      callback(Status::Invalid("Updating actor failed."));
    }
  };

  return client_impl_->actor_table().AppendAt(JobID::Nil(), actor_id, data_ptr,
                                              on_success, on_failure, log_length);
}

Status RedisActorInfoAccessor::AsyncSubscribeAll(
    const SubscribeCallback<ActorID, ActorTableData> &subscribe,
    const StatusCallback &done) {
  RAY_CHECK(subscribe != nullptr);
  return actor_sub_executor_.AsyncSubscribeAll(ClientID::Nil(), subscribe, done);
}

Status RedisActorInfoAccessor::AsyncSubscribe(
    const ActorID &actor_id, const SubscribeCallback<ActorID, ActorTableData> &subscribe,
    const StatusCallback &done) {
  RAY_CHECK(subscribe != nullptr);
  return actor_sub_executor_.AsyncSubscribe(subscribe_id_, actor_id, subscribe, done);
}

Status RedisActorInfoAccessor::AsyncUnsubscribe(const ActorID &actor_id,
                                                const StatusCallback &done) {
  return actor_sub_executor_.AsyncUnsubscribe(subscribe_id_, actor_id, done);
}

Status RedisActorInfoAccessor::AsyncAddCheckpoint(
    const std::shared_ptr<ActorCheckpointData> &data_ptr,
    const StatusCallback &callback) {
  auto on_add_data_done = [callback, data_ptr, this](
                              RedisGcsClient *client,
                              const ActorCheckpointID &checkpoint_id,
                              const ActorCheckpointData &data) {
    ActorID actor_id = ActorID::FromBinary(data_ptr->actor_id());
    Status status = AsyncAddCheckpointID(actor_id, checkpoint_id, callback);
    if (!status.ok()) {
      callback(status);
    }
  };

  ActorCheckpointID checkpoint_id =
      ActorCheckpointID::FromBinary(data_ptr->checkpoint_id());
  ActorCheckpointTable &actor_cp_table = client_impl_->actor_checkpoint_table();
  return actor_cp_table.Add(JobID::Nil(), checkpoint_id, data_ptr, on_add_data_done);
}

Status RedisActorInfoAccessor::AsyncGetCheckpoint(
    const ActorCheckpointID &checkpoint_id,
    const OptionalItemCallback<ActorCheckpointData> &callback) {
  RAY_CHECK(callback != nullptr);
  auto on_success = [callback](RedisGcsClient *client,
                               const ActorCheckpointID &checkpoint_id,
                               const ActorCheckpointData &checkpoint_data) {
    boost::optional<ActorCheckpointData> optional(checkpoint_data);
    callback(Status::OK(), std::move(optional));
  };

  auto on_failure = [callback](RedisGcsClient *client,
                               const ActorCheckpointID &checkpoint_id) {
    boost::optional<ActorCheckpointData> optional;
    callback(Status::Invalid("Invalid checkpoint id."), std::move(optional));
  };

  ActorCheckpointTable &actor_cp_table = client_impl_->actor_checkpoint_table();
  return actor_cp_table.Lookup(JobID::Nil(), checkpoint_id, on_success, on_failure);
}

Status RedisActorInfoAccessor::AsyncGetCheckpointID(
    const ActorID &actor_id,
    const OptionalItemCallback<ActorCheckpointIdData> &callback) {
  RAY_CHECK(callback != nullptr);
  auto on_success = [callback](RedisGcsClient *client, const ActorID &actor_id,
                               const ActorCheckpointIdData &data) {
    boost::optional<ActorCheckpointIdData> optional(data);
    callback(Status::OK(), std::move(optional));
  };

  auto on_failure = [callback](RedisGcsClient *client, const ActorID &actor_id) {
    boost::optional<ActorCheckpointIdData> optional;
    callback(Status::Invalid("Checkpoint not found."), std::move(optional));
  };

  ActorCheckpointIdTable &cp_id_table = client_impl_->actor_checkpoint_id_table();
  return cp_id_table.Lookup(JobID::Nil(), actor_id, on_success, on_failure);
}

Status RedisActorInfoAccessor::AsyncAddCheckpointID(
    const ActorID &actor_id, const ActorCheckpointID &checkpoint_id,
    const StatusCallback &callback) {
  ActorCheckpointIdTable::WriteCallback on_done = nullptr;
  if (callback != nullptr) {
    on_done = [callback](RedisGcsClient *client, const ActorID &actor_id,
                         const ActorCheckpointIdData &data) { callback(Status::OK()); };
  }

  ActorCheckpointIdTable &cp_id_table = client_impl_->actor_checkpoint_id_table();
  return cp_id_table.AddCheckpointId(JobID::Nil(), actor_id, checkpoint_id, on_done);
}

RedisJobInfoAccessor::RedisJobInfoAccessor(RedisGcsClient *client_impl)
    : client_impl_(client_impl), job_sub_executor_(client_impl->job_table()) {}

Status RedisJobInfoAccessor::AsyncAdd(const std::shared_ptr<JobTableData> &data_ptr,
                                      const StatusCallback &callback) {
  return DoAsyncAppend(data_ptr, callback);
}

Status RedisJobInfoAccessor::AsyncMarkFinished(const JobID &job_id,
                                               const StatusCallback &callback) {
  std::shared_ptr<JobTableData> data_ptr =
      CreateJobTableData(job_id, /*is_dead*/ true, /*time_stamp*/ std::time(nullptr),
                         /*node_manager_address*/ "", /*driver_pid*/ -1);
  return DoAsyncAppend(data_ptr, callback);
}

Status RedisJobInfoAccessor::DoAsyncAppend(const std::shared_ptr<JobTableData> &data_ptr,
                                           const StatusCallback &callback) {
  JobTable::WriteCallback on_done = nullptr;
  if (callback != nullptr) {
    on_done = [callback](RedisGcsClient *client, const JobID &job_id,
                         const JobTableData &data) { callback(Status::OK()); };
  }

  JobID job_id = JobID::FromBinary(data_ptr->job_id());
  return client_impl_->job_table().Append(job_id, job_id, data_ptr, on_done);
}

Status RedisJobInfoAccessor::AsyncSubscribeToFinishedJobs(
    const SubscribeCallback<JobID, JobTableData> &subscribe, const StatusCallback &done) {
  RAY_CHECK(subscribe != nullptr);
  auto on_subscribe = [subscribe](const JobID &job_id, const JobTableData &job_data) {
    if (job_data.is_dead()) {
      subscribe(job_id, job_data);
    }
  };
  return job_sub_executor_.AsyncSubscribeAll(ClientID::Nil(), on_subscribe, done);
}

RedisTaskInfoAccessor::RedisTaskInfoAccessor(RedisGcsClient *client_impl)
    : client_impl_(client_impl),
      task_sub_executor_(client_impl->raylet_task_table()),
      task_lease_sub_executor_(client_impl->task_lease_table()) {}

Status RedisTaskInfoAccessor::AsyncAdd(const std::shared_ptr<TaskTableData> &data_ptr,
                                       const StatusCallback &callback) {
  raylet::TaskTable::WriteCallback on_done = nullptr;
  if (callback != nullptr) {
    on_done = [callback](RedisGcsClient *client, const TaskID &task_id,
                         const TaskTableData &data) { callback(Status::OK()); };
  }

  TaskID task_id = TaskID::FromBinary(data_ptr->task().task_spec().task_id());
  raylet::TaskTable &task_table = client_impl_->raylet_task_table();
  return task_table.Add(JobID::Nil(), task_id, data_ptr, on_done);
}

Status RedisTaskInfoAccessor::AsyncGet(
    const TaskID &task_id, const OptionalItemCallback<TaskTableData> &callback) {
  RAY_CHECK(callback != nullptr);
  auto on_success = [callback](RedisGcsClient *client, const TaskID &task_id,
                               const TaskTableData &data) {
    boost::optional<TaskTableData> result(data);
    callback(Status::OK(), result);
  };

  auto on_failure = [callback](RedisGcsClient *client, const TaskID &task_id) {
    boost::optional<TaskTableData> result;
    callback(Status::Invalid("Task not exist."), result);
  };

  raylet::TaskTable &task_table = client_impl_->raylet_task_table();
  return task_table.Lookup(JobID::Nil(), task_id, on_success, on_failure);
}

Status RedisTaskInfoAccessor::AsyncDelete(const std::vector<TaskID> &task_ids,
                                          const StatusCallback &callback) {
  raylet::TaskTable &task_table = client_impl_->raylet_task_table();
  task_table.Delete(JobID::Nil(), task_ids);
  if (callback) {
    callback(Status::OK());
  }
  // TODO(micafan) Always return OK here.
  // Confirm if we need to handle the deletion failure and how to handle it.
  return Status::OK();
}

Status RedisTaskInfoAccessor::AsyncSubscribe(
    const TaskID &task_id, const SubscribeCallback<TaskID, TaskTableData> &subscribe,
    const StatusCallback &done) {
  RAY_CHECK(subscribe != nullptr);
  return task_sub_executor_.AsyncSubscribe(subscribe_id_, task_id, subscribe, done);
}

Status RedisTaskInfoAccessor::AsyncUnsubscribe(const TaskID &task_id,
                                               const StatusCallback &done) {
  return task_sub_executor_.AsyncUnsubscribe(subscribe_id_, task_id, done);
}

Status RedisTaskInfoAccessor::AsyncAddTaskLease(
    const std::shared_ptr<TaskLeaseData> &data_ptr, const StatusCallback &callback) {
  TaskLeaseTable::WriteCallback on_done = nullptr;
  if (callback != nullptr) {
    on_done = [callback](RedisGcsClient *client, const TaskID &id,
                         const TaskLeaseData &data) { callback(Status::OK()); };
  }
  TaskID task_id = TaskID::FromBinary(data_ptr->task_id());
  TaskLeaseTable &task_lease_table = client_impl_->task_lease_table();
  return task_lease_table.Add(JobID::Nil(), task_id, data_ptr, on_done);
}

Status RedisTaskInfoAccessor::AsyncSubscribeTaskLease(
    const TaskID &task_id,
    const SubscribeCallback<TaskID, boost::optional<TaskLeaseData>> &subscribe,
    const StatusCallback &done) {
  RAY_CHECK(subscribe != nullptr);
  return task_lease_sub_executor_.AsyncSubscribe(subscribe_id_, task_id, subscribe, done);
}

Status RedisTaskInfoAccessor::AsyncUnsubscribeTaskLease(const TaskID &task_id,
                                                        const StatusCallback &done) {
  return task_lease_sub_executor_.AsyncUnsubscribe(subscribe_id_, task_id, done);
}

Status RedisTaskInfoAccessor::AttemptTaskReconstruction(
    const std::shared_ptr<TaskReconstructionData> &data_ptr,
    const StatusCallback &callback) {
  TaskReconstructionLog::WriteCallback on_success = nullptr;
  TaskReconstructionLog::WriteCallback on_failure = nullptr;
  if (callback != nullptr) {
    on_success = [callback](RedisGcsClient *client, const TaskID &id,
                            const TaskReconstructionData &data) {
      callback(Status::OK());
    };
    on_failure = [callback](RedisGcsClient *client, const TaskID &id,
                            const TaskReconstructionData &data) {
      callback(Status::Invalid("Updating task reconstruction failed."));
    };
  }

  TaskID task_id = TaskID::FromBinary(data_ptr->task_id());
  int reconstruction_attempt = data_ptr->num_reconstructions();
  TaskReconstructionLog &task_reconstruction_log =
      client_impl_->task_reconstruction_log();
  return task_reconstruction_log.AppendAt(JobID::Nil(), task_id, data_ptr, on_success,
                                          on_failure, reconstruction_attempt);
}

RedisObjectInfoAccessor::RedisObjectInfoAccessor(RedisGcsClient *client_impl)
    : client_impl_(client_impl), object_sub_executor_(client_impl->object_table()) {}

Status RedisObjectInfoAccessor::AsyncGetLocations(
    const ObjectID &object_id, const MultiItemCallback<ObjectTableData> &callback) {
  RAY_CHECK(callback != nullptr);
  auto on_done = [callback](RedisGcsClient *client, const ObjectID &object_id,
                            const std::vector<ObjectTableData> &data) {
    callback(Status::OK(), data);
  };

  ObjectTable &object_table = client_impl_->object_table();
  return object_table.Lookup(JobID::Nil(), object_id, on_done);
}

Status RedisObjectInfoAccessor::AsyncAddLocation(const ObjectID &object_id,
                                                 const ClientID &node_id,
                                                 const StatusCallback &callback) {
  std::function<void(RedisGcsClient * client, const ObjectID &id,
                     const ObjectTableData &data)>
      on_done = nullptr;
  if (callback != nullptr) {
    on_done = [callback](RedisGcsClient *client, const ObjectID &object_id,
                         const ObjectTableData &data) { callback(Status::OK()); };
  }

  std::shared_ptr<ObjectTableData> data_ptr = std::make_shared<ObjectTableData>();
  data_ptr->set_manager(node_id.Binary());

  ObjectTable &object_table = client_impl_->object_table();
  return object_table.Add(JobID::Nil(), object_id, data_ptr, on_done);
}

Status RedisObjectInfoAccessor::AsyncRemoveLocation(const ObjectID &object_id,
                                                    const ClientID &node_id,
                                                    const StatusCallback &callback) {
  std::function<void(RedisGcsClient * client, const ObjectID &id,
                     const ObjectTableData &data)>
      on_done = nullptr;
  if (callback != nullptr) {
    on_done = [callback](RedisGcsClient *client, const ObjectID &object_id,
                         const ObjectTableData &data) { callback(Status::OK()); };
  }

  std::shared_ptr<ObjectTableData> data_ptr = std::make_shared<ObjectTableData>();
  data_ptr->set_manager(node_id.Binary());

  ObjectTable &object_table = client_impl_->object_table();
  return object_table.Remove(JobID::Nil(), object_id, data_ptr, on_done);
}

Status RedisObjectInfoAccessor::AsyncSubscribeToLocations(
    const ObjectID &object_id,
    const SubscribeCallback<ObjectID, ObjectChangeNotification> &subscribe,
    const StatusCallback &done) {
  RAY_CHECK(subscribe != nullptr);
  return object_sub_executor_.AsyncSubscribe(subscribe_id_, object_id, subscribe, done);
}

Status RedisObjectInfoAccessor::AsyncUnsubscribeToLocations(const ObjectID &object_id,
                                                            const StatusCallback &done) {
  return object_sub_executor_.AsyncUnsubscribe(subscribe_id_, object_id, done);
}

RedisNodeInfoAccessor::RedisNodeInfoAccessor(RedisGcsClient *client_impl)
    : client_impl_(client_impl),
      resource_sub_executor_(client_impl_->resource_table()),
      heartbeat_sub_executor_(client_impl->heartbeat_table()),
      heartbeat_batch_sub_executor_(client_impl->heartbeat_batch_table()) {}

Status RedisNodeInfoAccessor::RegisterSelf(const GcsNodeInfo &local_node_info) {
  ClientTable &client_table = client_impl_->client_table();
  return client_table.Connect(local_node_info);
}

Status RedisNodeInfoAccessor::UnregisterSelf() {
  ClientTable &client_table = client_impl_->client_table();
  return client_table.Disconnect();
}

const ClientID &RedisNodeInfoAccessor::GetSelfId() const {
  ClientTable &client_table = client_impl_->client_table();
  return client_table.GetLocalClientId();
}

const GcsNodeInfo &RedisNodeInfoAccessor::GetSelfInfo() const {
  ClientTable &client_table = client_impl_->client_table();
  return client_table.GetLocalClient();
}

Status RedisNodeInfoAccessor::AsyncRegister(const GcsNodeInfo &node_info,
                                            const StatusCallback &callback) {
  ClientTable::WriteCallback on_done = nullptr;
  if (callback != nullptr) {
    on_done = [callback](RedisGcsClient *client, const ClientID &id,
                         const GcsNodeInfo &data) { callback(Status::OK()); };
  }
  ClientTable &client_table = client_impl_->client_table();
  return client_table.MarkConnected(node_info, on_done);
}

Status RedisNodeInfoAccessor::AsyncUnregister(const ClientID &node_id,
                                              const StatusCallback &callback) {
  ClientTable::WriteCallback on_done = nullptr;
  if (callback != nullptr) {
    on_done = [callback](RedisGcsClient *client, const ClientID &id,
                         const GcsNodeInfo &data) { callback(Status::OK()); };
  }
  ClientTable &client_table = client_impl_->client_table();
  return client_table.MarkDisconnected(node_id, on_done);
}

Status RedisNodeInfoAccessor::AsyncSubscribeToNodeChange(
    const SubscribeCallback<ClientID, GcsNodeInfo> &subscribe,
    const StatusCallback &done) {
  RAY_CHECK(subscribe != nullptr);
  ClientTable &client_table = client_impl_->client_table();
  return client_table.SubscribeToNodeChange(subscribe, done);
}

Status RedisNodeInfoAccessor::AsyncGetAll(
    const MultiItemCallback<GcsNodeInfo> &callback) {
  RAY_CHECK(callback != nullptr);
  auto on_done = [callback](RedisGcsClient *client, const ClientID &id,
                            const std::vector<GcsNodeInfo> &data) {
    std::vector<GcsNodeInfo> result;
    std::set<std::string> node_ids;
    for (int index = data.size() - 1; index >= 0; --index) {
      if (node_ids.insert(data[index].node_id()).second) {
        result.emplace_back(data[index]);
      }
    }
    callback(Status::OK(), result);
  };
  ClientTable &client_table = client_impl_->client_table();
  return client_table.Lookup(on_done);
}

boost::optional<GcsNodeInfo> RedisNodeInfoAccessor::Get(const ClientID &node_id) const {
  GcsNodeInfo node_info;
  ClientTable &client_table = client_impl_->client_table();
  bool found = client_table.GetClient(node_id, &node_info);
  boost::optional<GcsNodeInfo> optional_node;
  if (found) {
    optional_node = std::move(node_info);
  }
  return optional_node;
}

const std::unordered_map<ClientID, GcsNodeInfo> &RedisNodeInfoAccessor::GetAll() const {
  ClientTable &client_table = client_impl_->client_table();
  return client_table.GetAllClients();
}

bool RedisNodeInfoAccessor::IsRemoved(const ClientID &node_id) const {
  ClientTable &client_table = client_impl_->client_table();
  return client_table.IsRemoved(node_id);
}
Status RedisNodeInfoAccessor::AsyncReportHeartbeat(
    const std::shared_ptr<HeartbeatTableData> &data_ptr, const StatusCallback &callback) {
  HeartbeatTable::WriteCallback on_done = nullptr;
  if (callback != nullptr) {
    on_done = [callback](RedisGcsClient *client, const ClientID &node_id,
                         const HeartbeatTableData &data) { callback(Status::OK()); };
  }

  ClientID node_id = ClientID::FromBinary(data_ptr->client_id());
  HeartbeatTable &heartbeat_table = client_impl_->heartbeat_table();
  return heartbeat_table.Add(JobID::Nil(), node_id, data_ptr, on_done);
}

Status RedisNodeInfoAccessor::AsyncSubscribeHeartbeat(
    const SubscribeCallback<ClientID, HeartbeatTableData> &subscribe,
    const StatusCallback &done) {
  RAY_CHECK(subscribe != nullptr);
  auto on_subscribe = [subscribe](const ClientID &node_id,
                                  const HeartbeatTableData &data) {
    subscribe(node_id, data);
  };

  return heartbeat_sub_executor_.AsyncSubscribeAll(ClientID::Nil(), on_subscribe, done);
}

Status RedisNodeInfoAccessor::AsyncReportBatchHeartbeat(
    const std::shared_ptr<HeartbeatBatchTableData> &data_ptr,
    const StatusCallback &callback) {
  HeartbeatBatchTable::WriteCallback on_done = nullptr;
  if (callback != nullptr) {
    on_done = [callback](RedisGcsClient *client, const ClientID &node_id,
                         const HeartbeatBatchTableData &data) { callback(Status::OK()); };
  }

  HeartbeatBatchTable &hb_batch_table = client_impl_->heartbeat_batch_table();
  return hb_batch_table.Add(JobID::Nil(), ClientID::Nil(), data_ptr, on_done);
}

Status RedisNodeInfoAccessor::AsyncSubscribeBatchHeartbeat(
    const ItemCallback<HeartbeatBatchTableData> &subscribe, const StatusCallback &done) {
  RAY_CHECK(subscribe != nullptr);
  auto on_subscribe = [subscribe](const ClientID &node_id,
                                  const HeartbeatBatchTableData &data) {
    subscribe(data);
  };

  return heartbeat_batch_sub_executor_.AsyncSubscribeAll(ClientID::Nil(), on_subscribe,
                                                         done);
}

Status RedisNodeInfoAccessor::AsyncGetResources(
    const ClientID &node_id, const OptionalItemCallback<ResourceMap> &callback) {
  RAY_CHECK(callback != nullptr);
  auto on_done = [callback](RedisGcsClient *client, const ClientID &id,
                            const ResourceMap &data) {
    boost::optional<ResourceMap> result;
    if (!data.empty()) {
      result = data;
    }
    callback(Status::OK(), result);
  };

  DynamicResourceTable &resource_table = client_impl_->resource_table();
  return resource_table.Lookup(JobID::Nil(), node_id, on_done);
}

Status RedisNodeInfoAccessor::AsyncUpdateResources(const ClientID &node_id,
                                                   const ResourceMap &resources,
                                                   const StatusCallback &callback) {
  Hash<ClientID, ResourceTableData>::HashCallback on_done = nullptr;
  if (callback != nullptr) {
    on_done = [callback](RedisGcsClient *client, const ClientID &node_id,
                         const ResourceMap &resources) { callback(Status::OK()); };
  }

  DynamicResourceTable &resource_table = client_impl_->resource_table();
  return resource_table.Update(JobID::Nil(), node_id, resources, on_done);
}

Status RedisNodeInfoAccessor::AsyncDeleteResources(
    const ClientID &node_id, const std::vector<std::string> &resource_names,
    const StatusCallback &callback) {
  Hash<ClientID, ResourceTableData>::HashRemoveCallback on_done = nullptr;
  if (callback != nullptr) {
    on_done = [callback](RedisGcsClient *client, const ClientID &node_id,
                         const std::vector<std::string> &resource_names) {
      callback(Status::OK());
    };
  }

  DynamicResourceTable &resource_table = client_impl_->resource_table();
  return resource_table.RemoveEntries(JobID::Nil(), node_id, resource_names, on_done);
}

Status RedisNodeInfoAccessor::AsyncSubscribeToResources(
    const SubscribeCallback<ClientID, ResourceChangeNotification> &subscribe,
    const StatusCallback &done) {
  RAY_CHECK(subscribe != nullptr);
  return resource_sub_executor_.AsyncSubscribeAll(ClientID::Nil(), subscribe, done);
}

RedisErrorInfoAccessor::RedisErrorInfoAccessor(RedisGcsClient *client_impl)
    : client_impl_(client_impl) {}

Status RedisErrorInfoAccessor::AsyncReportJobError(
    const std::shared_ptr<ErrorTableData> &data_ptr, const StatusCallback &callback) {
  ErrorTable::WriteCallback on_done = nullptr;
  if (callback != nullptr) {
    on_done = [callback](RedisGcsClient *client, const JobID &job_id,
                         const ErrorTableData &data) { callback(Status::OK()); };
  }

  JobID job_id = JobID::FromBinary(data_ptr->job_id());
  ErrorTable &error_table = client_impl_->error_table();
  return error_table.Append(job_id, job_id, data_ptr, on_done);
}

RedisStatsInfoAccessor::RedisStatsInfoAccessor(RedisGcsClient *client_impl)
    : client_impl_(client_impl) {}

Status RedisStatsInfoAccessor::AsyncAddProfileData(
    const std::shared_ptr<ProfileTableData> &data_ptr, const StatusCallback &callback) {
  ProfileTable::WriteCallback on_done = nullptr;
  if (callback != nullptr) {
    on_done = [callback](RedisGcsClient *client, const UniqueID &id,
                         const ProfileTableData &data) { callback(Status::OK()); };
  }

  ProfileTable &profile_table = client_impl_->profile_table();
  return profile_table.Append(JobID::Nil(), UniqueID::FromRandom(), data_ptr, on_done);
}

RedisWorkerInfoAccessor::RedisWorkerInfoAccessor(RedisGcsClient *client_impl)
    : client_impl_(client_impl),
      worker_failure_sub_executor_(client_impl->worker_failure_table()) {}

Status RedisWorkerInfoAccessor::AsyncSubscribeToWorkerFailures(
    const SubscribeCallback<WorkerID, WorkerFailureData> &subscribe,
    const StatusCallback &done) {
  RAY_CHECK(subscribe != nullptr);
  return worker_failure_sub_executor_.AsyncSubscribeAll(ClientID::Nil(), subscribe, done);
}

Status RedisWorkerInfoAccessor::AsyncReportWorkerFailure(
    const std::shared_ptr<WorkerFailureData> &data_ptr, const StatusCallback &callback) {
  WorkerFailureTable::WriteCallback on_done = nullptr;
  if (callback != nullptr) {
    on_done = [callback](RedisGcsClient *client, const WorkerID &id,
                         const WorkerFailureData &data) { callback(Status::OK()); };
  }

  WorkerID worker_id = WorkerID::FromBinary(data_ptr->worker_address().worker_id());
  WorkerFailureTable &worker_failure_table = client_impl_->worker_failure_table();
  return worker_failure_table.Add(JobID::Nil(), worker_id, data_ptr, on_done);
}

}  // namespace gcs

}  // namespace ray
