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

#include "ray/gcs/redis_accessor.h"

#include <boost/none.hpp>

#include "ray/gcs/pb_util.h"
#include "ray/gcs/redis_gcs_client.h"
#include "ray/util/logging.h"

namespace ray {

namespace gcs {

RedisLogBasedActorInfoAccessor::RedisLogBasedActorInfoAccessor(
    RedisGcsClient *client_impl)
    : client_impl_(client_impl),
      log_based_actor_sub_executor_(client_impl_->log_based_actor_table()) {}

std::vector<ActorID> RedisLogBasedActorInfoAccessor::GetAllActorID() const {
  return client_impl_->log_based_actor_table().GetAllActorID();
}

Status RedisLogBasedActorInfoAccessor::Get(const ActorID &actor_id,
                                           ActorTableData *actor_table_data) const {
  return client_impl_->log_based_actor_table().Get(actor_id, actor_table_data);
}

Status RedisLogBasedActorInfoAccessor::GetAll(
    std::vector<ActorTableData> *actor_table_data_list) {
  RAY_CHECK(actor_table_data_list);
  auto actor_id_list = GetAllActorID();
  actor_table_data_list->resize(actor_id_list.size());
  for (size_t i = 0; i < actor_id_list.size(); ++i) {
    RAY_CHECK_OK(Get(actor_id_list[i], &(*actor_table_data_list)[i]));
  }
  return Status::OK();
}

Status RedisLogBasedActorInfoAccessor::AsyncGet(
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

  return client_impl_->log_based_actor_table().Lookup(actor_id.JobId(), actor_id,
                                                      on_done);
}

Status RedisLogBasedActorInfoAccessor::AsyncRegisterActor(
    const ray::TaskSpecification &task_spec, const ray::gcs::StatusCallback &callback) {
  const std::string error_msg =
      "Unsupported method of AsyncRegisterActor in RedisLogBasedActorInfoAccessor.";
  RAY_LOG(FATAL) << error_msg;
  return Status::Invalid(error_msg);
}

Status RedisLogBasedActorInfoAccessor::AsyncCreateActor(
    const ray::TaskSpecification &task_spec, const ray::gcs::StatusCallback &callback) {
  const std::string error_msg =
      "Unsupported method of AsyncCreateActor in "
      "RedisLogBasedActorInfoAccessor.";
  RAY_LOG(FATAL) << error_msg;
  return Status::Invalid(error_msg);
}

Status RedisLogBasedActorInfoAccessor::AsyncRegister(
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
  return client_impl_->log_based_actor_table().AppendAt(actor_id.JobId(), actor_id,
                                                        data_ptr, on_success, on_failure,
                                                        /*log_length*/ 0);
}

Status RedisLogBasedActorInfoAccessor::AsyncUpdate(
    const ActorID &actor_id, const std::shared_ptr<ActorTableData> &data_ptr,
    const StatusCallback &callback) {
  // The actor log starts with an ALIVE entry. This is followed by 0 to N pairs
  // of (RESTARTING, ALIVE) entries, where N is the maximum number of
  // reconstructions. This is followed optionally by a DEAD entry.
  int log_length = 2 * (data_ptr->num_restarts());
  if (data_ptr->state() != ActorTableData::ALIVE) {
    // RESTARTING or DEAD entries have an odd index.
    log_length += 1;
  }
  RAY_LOG(DEBUG) << "AsyncUpdate actor state to " << data_ptr->state()
                 << ", actor id: " << actor_id << ", log_length: " << log_length;
  auto on_success = [callback](RedisGcsClient *client, const ActorID &actor_id,
                               const ActorTableData &data) {
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

  return client_impl_->log_based_actor_table().AppendAt(
      actor_id.JobId(), actor_id, data_ptr, on_success, on_failure, log_length);
}

Status RedisLogBasedActorInfoAccessor::AsyncSubscribeAll(
    const SubscribeCallback<ActorID, ActorTableData> &subscribe,
    const StatusCallback &done) {
  RAY_CHECK(subscribe != nullptr);
  return log_based_actor_sub_executor_.AsyncSubscribeAll(ClientID::Nil(), subscribe,
                                                         done);
}

Status RedisLogBasedActorInfoAccessor::AsyncSubscribe(
    const ActorID &actor_id, const SubscribeCallback<ActorID, ActorTableData> &subscribe,
    const StatusCallback &done) {
  RAY_CHECK(subscribe != nullptr);
  return log_based_actor_sub_executor_.AsyncSubscribe(subscribe_id_, actor_id, subscribe,
                                                      done);
}

Status RedisLogBasedActorInfoAccessor::AsyncUnsubscribe(const ActorID &actor_id) {
  return log_based_actor_sub_executor_.AsyncUnsubscribe(subscribe_id_, actor_id, nullptr);
}

Status RedisLogBasedActorInfoAccessor::AsyncAddCheckpoint(
    const std::shared_ptr<ActorCheckpointData> &data_ptr,
    const StatusCallback &callback) {
  ActorID actor_id = ActorID::FromBinary(data_ptr->actor_id());
  auto on_add_data_done = [actor_id, callback, data_ptr, this](
                              RedisGcsClient *client,
                              const ActorCheckpointID &checkpoint_id,
                              const ActorCheckpointData &data) {
    Status status = AsyncAddCheckpointID(actor_id, checkpoint_id, callback);
    if (!status.ok()) {
      callback(status);
    }
  };

  ActorCheckpointID checkpoint_id =
      ActorCheckpointID::FromBinary(data_ptr->checkpoint_id());
  ActorCheckpointTable &actor_cp_table = client_impl_->actor_checkpoint_table();
  return actor_cp_table.Add(actor_id.JobId(), checkpoint_id, data_ptr, on_add_data_done);
}

Status RedisLogBasedActorInfoAccessor::AsyncGetCheckpoint(
    const ActorCheckpointID &checkpoint_id, const ActorID &actor_id,
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
  return actor_cp_table.Lookup(actor_id.JobId(), checkpoint_id, on_success, on_failure);
}

Status RedisLogBasedActorInfoAccessor::AsyncGetCheckpointID(
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
  return cp_id_table.Lookup(actor_id.JobId(), actor_id, on_success, on_failure);
}

Status RedisLogBasedActorInfoAccessor::AsyncAddCheckpointID(
    const ActorID &actor_id, const ActorCheckpointID &checkpoint_id,
    const StatusCallback &callback) {
  ActorCheckpointIdTable::WriteCallback on_done = nullptr;
  if (callback != nullptr) {
    on_done = [callback](RedisGcsClient *client, const ActorID &actor_id,
                         const ActorCheckpointIdData &data) { callback(Status::OK()); };
  }

  ActorCheckpointIdTable &cp_id_table = client_impl_->actor_checkpoint_id_table();
  return cp_id_table.AddCheckpointId(actor_id.JobId(), actor_id, checkpoint_id, on_done);
}

RedisActorInfoAccessor::RedisActorInfoAccessor(RedisGcsClient *client_impl)
    : RedisLogBasedActorInfoAccessor(client_impl),
      actor_sub_executor_(client_impl_->actor_table()) {}

std::vector<ActorID> RedisActorInfoAccessor::GetAllActorID() const {
  return client_impl_->actor_table().GetAllActorID();
}

Status RedisActorInfoAccessor::Get(const ActorID &actor_id,
                                   ActorTableData *actor_table_data) const {
  return client_impl_->actor_table().Get(actor_id, actor_table_data);
}

Status RedisActorInfoAccessor::AsyncGet(
    const ActorID &actor_id, const OptionalItemCallback<ActorTableData> &callback) {
  RAY_CHECK(callback != nullptr);
  auto on_done = [callback](RedisGcsClient *client, const ActorID &actor_id,
                            const ActorTableData &data) { callback(Status::OK(), data); };

  auto on_failure = [callback](RedisGcsClient *client, const ActorID &actor_id) {
    if (callback != nullptr) {
      callback(Status::Invalid("Get actor failed."), boost::none);
    }
  };

  return client_impl_->actor_table().Lookup(JobID::Nil(), actor_id, on_done, on_failure);
}

Status RedisActorInfoAccessor::AsyncGetAll(
    const MultiItemCallback<rpc::ActorTableData> &callback) {
  RAY_CHECK(callback != nullptr);
  auto actor_id_list = GetAllActorID();
  if (actor_id_list.empty()) {
    callback(Status::OK(), std::vector<rpc::ActorTableData>());
    return Status::OK();
  }

  auto finished_count = std::make_shared<int>(0);
  auto result = std::make_shared<std::vector<ActorTableData>>();
  int size = actor_id_list.size();
  for (auto &actor_id : actor_id_list) {
    auto on_done = [finished_count, size, result, callback](
                       const Status &status,
                       const boost::optional<ActorTableData> &data) {
      ++(*finished_count);
      if (data) {
        result->push_back(*data);
      }
      if (*finished_count == size) {
        callback(Status::OK(), *result);
      }
    };
    RAY_CHECK_OK(AsyncGet(actor_id, on_done));
  }

  return Status::OK();
}

Status RedisActorInfoAccessor::AsyncRegister(
    const std::shared_ptr<ActorTableData> &data_ptr, const StatusCallback &callback) {
  auto on_register_done = [callback](RedisGcsClient *client, const ActorID &actor_id,
                                     const ActorTableData &data) {
    if (callback != nullptr) {
      callback(Status::OK());
    }
  };
  ActorID actor_id = ActorID::FromBinary(data_ptr->actor_id());
  return client_impl_->actor_table().Add(JobID::Nil(), actor_id, data_ptr,
                                         on_register_done);
}

Status RedisActorInfoAccessor::AsyncUpdate(
    const ActorID &actor_id, const std::shared_ptr<ActorTableData> &data_ptr,
    const StatusCallback &callback) {
  auto on_update_done = [callback](RedisGcsClient *client, const ActorID &actor_id,
                                   const ActorTableData &data) {
    if (callback != nullptr) {
      callback(Status::OK());
    }
  };
  return client_impl_->actor_table().Add(JobID::Nil(), actor_id, data_ptr,
                                         on_update_done);
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

Status RedisActorInfoAccessor::AsyncUnsubscribe(const ActorID &actor_id) {
  return actor_sub_executor_.AsyncUnsubscribe(subscribe_id_, actor_id, nullptr);
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
                         /*driver_ip_address*/ "", /*driver_pid*/ -1);
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

Status RedisJobInfoAccessor::AsyncSubscribeAll(
    const SubscribeCallback<JobID, JobTableData> &subscribe, const StatusCallback &done) {
  RAY_CHECK(subscribe != nullptr);
  return job_sub_executor_.AsyncSubscribeAll(ClientID::Nil(), subscribe, done);
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
  return task_table.Add(task_id.JobId(), task_id, data_ptr, on_done);
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
  return task_table.Lookup(task_id.JobId(), task_id, on_success, on_failure);
}

Status RedisTaskInfoAccessor::AsyncDelete(const std::vector<TaskID> &task_ids,
                                          const StatusCallback &callback) {
  raylet::TaskTable &task_table = client_impl_->raylet_task_table();
  JobID job_id = task_ids.empty() ? JobID::Nil() : task_ids[0].JobId();
  task_table.Delete(job_id, task_ids);
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

Status RedisTaskInfoAccessor::AsyncUnsubscribe(const TaskID &task_id) {
  return task_sub_executor_.AsyncUnsubscribe(subscribe_id_, task_id, nullptr);
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
  return task_lease_table.Add(task_id.JobId(), task_id, data_ptr, on_done);
}

Status RedisTaskInfoAccessor::AsyncGetTaskLease(
    const TaskID &task_id, const OptionalItemCallback<TaskLeaseData> &callback) {
  RAY_CHECK(callback != nullptr);
  auto on_success = [callback](RedisGcsClient *client, const TaskID &task_id,
                               const TaskLeaseData &data) {
    boost::optional<TaskLeaseData> result(data);
    callback(Status::OK(), result);
  };

  auto on_failure = [callback](RedisGcsClient *client, const TaskID &task_id) {
    boost::optional<TaskLeaseData> result;
    callback(Status::Invalid("Task lease not exist."), result);
  };

  TaskLeaseTable &task_lease_table = client_impl_->task_lease_table();
  return task_lease_table.Lookup(task_id.JobId(), task_id, on_success, on_failure);
}

Status RedisTaskInfoAccessor::AsyncSubscribeTaskLease(
    const TaskID &task_id,
    const SubscribeCallback<TaskID, boost::optional<TaskLeaseData>> &subscribe,
    const StatusCallback &done) {
  RAY_CHECK(subscribe != nullptr);
  return task_lease_sub_executor_.AsyncSubscribe(subscribe_id_, task_id, subscribe, done);
}

Status RedisTaskInfoAccessor::AsyncUnsubscribeTaskLease(const TaskID &task_id) {
  return task_lease_sub_executor_.AsyncUnsubscribe(subscribe_id_, task_id, nullptr);
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
  return task_reconstruction_log.AppendAt(task_id.JobId(), task_id, data_ptr, on_success,
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
  return object_table.Lookup(object_id.TaskId().JobId(), object_id, on_done);
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
  return object_table.Add(object_id.TaskId().JobId(), object_id, data_ptr, on_done);
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
  return object_table.Remove(object_id.TaskId().JobId(), object_id, data_ptr, on_done);
}

Status RedisObjectInfoAccessor::AsyncSubscribeToLocations(
    const ObjectID &object_id,
    const SubscribeCallback<ObjectID, ObjectChangeNotification> &subscribe,
    const StatusCallback &done) {
  RAY_CHECK(subscribe != nullptr);
  return object_sub_executor_.AsyncSubscribe(subscribe_id_, object_id, subscribe, done);
}

Status RedisObjectInfoAccessor::AsyncUnsubscribeToLocations(const ObjectID &object_id) {
  return object_sub_executor_.AsyncUnsubscribe(subscribe_id_, object_id, nullptr);
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
    const ItemCallback<rpc::NodeResourceChange> &subscribe, const StatusCallback &done) {
  RAY_CHECK(subscribe != nullptr);
  auto on_subscribe = [subscribe](const ClientID &id,
                                  const ResourceChangeNotification &result) {
    rpc::NodeResourceChange node_resource_change;
    node_resource_change.set_node_id(id.Binary());
    if (result.IsAdded()) {
      for (auto &it : result.GetData()) {
        (*node_resource_change.mutable_updated_resources())[it.first] =
            it.second->resource_capacity();
      }
    } else {
      for (auto &it : result.GetData()) {
        node_resource_change.add_deleted_resources(it.first);
      }
    }
    subscribe(node_resource_change);
  };
  return resource_sub_executor_.AsyncSubscribeAll(ClientID::Nil(), on_subscribe, done);
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
      worker_failure_sub_executor_(client_impl->worker_table()) {}

Status RedisWorkerInfoAccessor::AsyncSubscribeToWorkerFailures(
    const SubscribeCallback<WorkerID, WorkerTableData> &subscribe,
    const StatusCallback &done) {
  RAY_CHECK(subscribe != nullptr);
  return worker_failure_sub_executor_.AsyncSubscribeAll(ClientID::Nil(), subscribe, done);
}

Status RedisWorkerInfoAccessor::AsyncReportWorkerFailure(
    const std::shared_ptr<WorkerTableData> &data_ptr, const StatusCallback &callback) {
  WorkerTable::WriteCallback on_done = nullptr;
  if (callback != nullptr) {
    on_done = [callback](RedisGcsClient *client, const WorkerID &id,
                         const WorkerTableData &data) { callback(Status::OK()); };
  }

  WorkerID worker_id = WorkerID::FromBinary(data_ptr->worker_address().worker_id());
  WorkerTable &worker_failure_table = client_impl_->worker_table();
  return worker_failure_table.Add(JobID::Nil(), worker_id, data_ptr, on_done);
}

Status RedisWorkerInfoAccessor::AsyncGet(
    const WorkerID &worker_id,
    const OptionalItemCallback<rpc::WorkerTableData> &callback) {
  return Status::Invalid("Not implemented");
}

Status RedisWorkerInfoAccessor::AsyncGetAll(
    const MultiItemCallback<rpc::WorkerTableData> &callback) {
  return Status::Invalid("Not implemented");
}

Status RedisWorkerInfoAccessor::AsyncAdd(
    const std::shared_ptr<rpc::WorkerTableData> &data_ptr,
    const StatusCallback &callback) {
  return Status::Invalid("Not implemented");
}

Status RedisPlacementGroupInfoAccessor::AsyncCreatePlacementGroup(
    const PlacementGroupSpecification &placement_group_spec) {
  return Status::Invalid("Not implemented");
}

}  // namespace gcs

}  // namespace ray
