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

#pragma once

#include "absl/container/flat_hash_map.h"
#include "ray/gcs/accessor.h"
#include "ray/gcs/subscription_executor.h"

namespace ray {

namespace gcs {

class MockActorInfoAccessor : public ActorInfoAccessor {
 public:
  MockActorInfoAccessor() {}

  MockActorInfoAccessor(RedisGcsClient *client) : client_impl_(client) {}

  ~MockActorInfoAccessor() {}

  ray::Status AsyncSubscribe(
      const ActorID &actor_id,
      const SubscribeCallback<ActorID, rpc::ActorTableData> &subscribe,
      const StatusCallback &done) override {
    auto callback_entry = std::make_pair(actor_id, subscribe);
    callback_map_.emplace(actor_id, subscribe);
    return Status::OK();
  }

  bool ActorStateNotificationPublished(const ActorID &actor_id,
                                       const ActorTableData &actor_data) {
    auto it = callback_map_.find(actor_id);
    if (it == callback_map_.end()) return false;
    auto actor_state_notification_callback = it->second;
    actor_state_notification_callback(actor_id, actor_data);
    return true;
  }

  bool CheckSubscriptionRequested(const ActorID &actor_id) {
    return callback_map_.find(actor_id) != callback_map_.end();
  }

  Status GetAll(std::vector<rpc::ActorTableData> *actor_table_data_list) override {
    return Status::OK();
  }

  Status AsyncGet(const ActorID &actor_id,
                  const OptionalItemCallback<rpc::ActorTableData> &callback) override {
    return Status::OK();
  }

  Status AsyncGetAll(const MultiItemCallback<rpc::ActorTableData> &callback) override {
    return Status::OK();
  }

  Status AsyncGetByName(
      const std::string &name,
      const OptionalItemCallback<rpc::ActorTableData> &callback) override {
    return Status::OK();
  }

  Status AsyncRegisterActor(const TaskSpecification &task_spec,
                            const StatusCallback &callback) override {
    return Status::OK();
  }

  Status AsyncCreateActor(const TaskSpecification &task_spec,
                          const StatusCallback &callback) override {
    return Status::OK();
  }

  Status AsyncRegister(const std::shared_ptr<rpc::ActorTableData> &data_ptr,
                       const StatusCallback &callback) override {
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

  Status AsyncUpdate(const ActorID &actor_id,
                     const std::shared_ptr<rpc::ActorTableData> &data_ptr,
                     const StatusCallback &callback) override {
    return Status::OK();
  }

  Status AsyncSubscribeAll(
      const SubscribeCallback<ActorID, rpc::ActorTableData> &subscribe,
      const StatusCallback &done) override {
    return Status::OK();
  }

  Status AsyncUnsubscribe(const ActorID &actor_id) override { return Status::OK(); }

  Status AsyncAddCheckpoint(const std::shared_ptr<rpc::ActorCheckpointData> &data_ptr,
                            const StatusCallback &callback) override {
    return Status::OK();
  }

  Status AsyncGetCheckpoint(
      const ActorCheckpointID &checkpoint_id, const ActorID &actor_id,
      const OptionalItemCallback<rpc::ActorCheckpointData> &callback) override {
    return Status::OK();
  }

  Status AsyncGetCheckpointID(
      const ActorID &actor_id,
      const OptionalItemCallback<rpc::ActorCheckpointIdData> &callback) override {
    return Status::OK();
  }

  void AsyncResubscribe(bool is_pubsub_server_restarted) override {}

  ::absl::flat_hash_map<ActorID, SubscribeCallback<ActorID, rpc::ActorTableData>>
      callback_map_;

 private:
  RedisGcsClient *client_impl_{nullptr};
};

class MockNodeInfoAccessor : public NodeInfoAccessor {
 public:
  MockNodeInfoAccessor() {}

  MockNodeInfoAccessor(RedisGcsClient *client) : client_impl_(client) {}

  MockNodeInfoAccessor(const ClientID &node_id) : node_id_(node_id) {}

  bool IsRemoved(const ClientID &node_id) const override {
    ClientTable &client_table = client_impl_->client_table();
    return client_table.IsRemoved(node_id);
  }

  Status RegisterSelf(const rpc::GcsNodeInfo &local_node_info) override {
    ClientTable &client_table = client_impl_->client_table();
    return client_table.Connect(local_node_info);
  }

  Status UnregisterSelf() override {
    ClientTable &client_table = client_impl_->client_table();
    return client_table.Disconnect();
  }

  const ClientID &GetSelfId() const override {
    ClientTable &client_table = client_impl_->client_table();
    return client_table.GetLocalClientId();
  }

  const rpc::GcsNodeInfo &GetSelfInfo() const override { return node_info_; }

  Status AsyncRegister(const rpc::GcsNodeInfo &node_info,
                       const StatusCallback &callback) override {
    return Status::OK();
  }

  Status AsyncUnregister(const ClientID &node_id,
                         const StatusCallback &callback) override {
    ClientTable &client_table = client_impl_->client_table();
    return client_table.Disconnect();
  }

  Status AsyncGetAll(const MultiItemCallback<rpc::GcsNodeInfo> &callback) override {
    return Status::OK();
  }

  Status AsyncSubscribeToNodeChange(
      const SubscribeCallback<ClientID, rpc::GcsNodeInfo> &subscribe,
      const StatusCallback &done) override {
    RAY_CHECK(subscribe != nullptr);
    ClientTable &client_table = client_impl_->client_table();
    return client_table.SubscribeToNodeChange(subscribe, done);
  }

  boost::optional<rpc::GcsNodeInfo> Get(const ClientID &node_id) const override {
    rpc::GcsNodeInfo node_info;
    ClientTable &client_table = client_impl_->client_table();
    bool found = client_table.GetClient(node_id, &node_info);
    boost::optional<rpc::GcsNodeInfo> optional_node;
    if (found) {
      optional_node = std::move(node_info);
    }
    return optional_node;
  }

  const std::unordered_map<ClientID, rpc::GcsNodeInfo> &GetAll() const override {
    return map_info_;
  }

  Status AsyncGetResources(const ClientID &node_id,
                           const OptionalItemCallback<ResourceMap> &callback) override {
    return Status::OK();
  }

  Status AsyncUpdateResources(const ClientID &node_id, const ResourceMap &resources,
                              const StatusCallback &callback) override {
    return Status::OK();
  }

  Status AsyncDeleteResources(const ClientID &node_id,
                              const std::vector<std::string> &resource_names,
                              const StatusCallback &callback) override {
    return Status::OK();
  }

  Status AsyncSubscribeToResources(const ItemCallback<rpc::NodeResourceChange> &subscribe,
                                   const StatusCallback &done) override {
    return Status::OK();
  }

  Status AsyncReportHeartbeat(const std::shared_ptr<rpc::HeartbeatTableData> &data_ptr,
                              const StatusCallback &callback) override {
    return Status::OK();
  }

  Status AsyncSubscribeHeartbeat(
      const SubscribeCallback<ClientID, rpc::HeartbeatTableData> &subscribe,
      const StatusCallback &done) override {
    return Status::OK();
  }

  Status AsyncReportBatchHeartbeat(
      const std::shared_ptr<rpc::HeartbeatBatchTableData> &data_ptr,
      const StatusCallback &callback) override {
    return Status::OK();
  }

  Status AsyncSubscribeBatchHeartbeat(
      const ItemCallback<rpc::HeartbeatBatchTableData> &subscribe,
      const StatusCallback &done) override {
    return Status::OK();
  }

  void AsyncResubscribe(bool is_pubsub_server_restarted) override {}

  Status AsyncSetInternalConfig(
      std::unordered_map<std::string, std::string> &config) override {
    return Status::OK();
  }

  Status AsyncGetInternalConfig(
      const OptionalItemCallback<std::unordered_map<std::string, std::string>> &callback)
      override {
    return Status::OK();
  }

 private:
  ClientID node_id_;
  rpc::GcsNodeInfo node_info_;
  std::unordered_map<ClientID, rpc::GcsNodeInfo> map_info_;
  RedisGcsClient *client_impl_{nullptr};
};

class MockObjectAccessor : public ObjectInfoAccessor {
 public:
  MockObjectAccessor(RedisGcsClient *client)
      : client_impl_(client), object_sub_executor_(client_impl_->object_table()) {}

  Status AsyncGetLocations(const ObjectID &object_id,
                           const MultiItemCallback<ObjectTableData> &callback) override {
    RAY_CHECK(callback != nullptr);
    auto on_done = [callback](RedisGcsClient *client, const ObjectID &object_id,
                              const std::vector<ObjectTableData> &data) {
      callback(Status::OK(), data);
    };

    ObjectTable &object_table = client_impl_->object_table();
    return object_table.Lookup(object_id.TaskId().JobId(), object_id, on_done);
  }

  Status AsyncGetAll(
      const MultiItemCallback<rpc::ObjectLocationInfo> &callback) override {
    return Status::OK();
  }

  Status AsyncAddLocation(const ObjectID &object_id, const ClientID &node_id,
                          const StatusCallback &callback) override {
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
    // return Status::NotImplemented("AsyncGetAll not implemented");
  }

  Status AsyncRemoveLocation(const ObjectID &object_id, const ClientID &node_id,
                             const StatusCallback &callback) override {
    return Status::OK();
  }

  Status AsyncSubscribeToLocations(
      const ObjectID &object_id,
      const SubscribeCallback<ObjectID, ObjectChangeNotification> &subscribe,
      const StatusCallback &done) override {
    RAY_CHECK(subscribe != nullptr);
    return object_sub_executor_.AsyncSubscribe(subscribe_id_, object_id, subscribe, done);
  }

  Status AsyncUnsubscribeToLocations(const ObjectID &object_id) override {
    return object_sub_executor_.AsyncUnsubscribe(subscribe_id_, object_id, nullptr);
  }

  void AsyncResubscribe(bool is_pubsub_server_restarted) override {}

 private:
  RedisGcsClient *client_impl_{nullptr};
  ClientID subscribe_id_{ClientID::FromRandom()};

  typedef SubscriptionExecutor<ObjectID, ObjectChangeNotification, ObjectTable>
      ObjectSubscriptionExecutor;
  ObjectSubscriptionExecutor object_sub_executor_;
};

class MockTaskInfoAccessor : public TaskInfoAccessor {
 public:
  MockTaskInfoAccessor() {}

  virtual ~MockTaskInfoAccessor() {}

  void RegisterSubscribeCallback(
      const SubscribeCallback<TaskID, rpc::TaskTableData> &notification_callback) {
    notification_callback_ = notification_callback;
  }

  Status AsyncAdd(const std::shared_ptr<TaskTableData> &task_data,
                  const StatusCallback &done) override {
    TaskID task_id = TaskID::FromBinary(task_data->task().task_spec().task_id());
    task_table_[task_id] = task_data;
    auto callback = done;
    // If we requested notifications for this task ID, send the notification as
    // part of the callback.
    if (subscribed_tasks_.count(task_id) == 1) {
      callback = [this, done, task_id, task_data](Status status) {
        done(status);
        // If we're subscribed to the task to be added, also send a
        // subscription notification.
        notification_callback_(task_id, *task_data);
      };
    }

    callbacks_.push_back({callback, task_id});
    num_task_adds_++;
    return ray::Status::OK();
  }

  Status RemoteAdd(std::shared_ptr<TaskTableData> task_data) {
    TaskID task_id = TaskID::FromBinary(task_data->task().task_spec().task_id());
    task_table_[task_id] = task_data;
    // Send a notification after the add if the lineage cache requested
    // notifications for this key.
    bool send_notification = (subscribed_tasks_.count(task_id) == 1);
    auto callback = [this, send_notification, task_id, task_data](Status status) {
      if (send_notification) {
        notification_callback_(task_id, *task_data);
      }
    };
    return AsyncAdd(task_data, callback);
  }

  Status AsyncSubscribe(
      const TaskID &task_id,
      const SubscribeCallback<TaskID, rpc::TaskTableData> &notification_callback,
      const StatusCallback &done) override {
    subscribed_tasks_.insert(task_id);
    if (task_table_.count(task_id) == 1) {
      notification_callbacks_.push_back({notification_callback_, task_id});
    }
    num_requested_notifications_ += 1;
    return ray::Status::OK();
  }

  Status AsyncUnsubscribe(const TaskID &task_id) override {
    subscribed_tasks_.erase(task_id);
    return ray::Status::OK();
  }

  void Flush() {
    auto callbacks = std::move(callbacks_);
    callbacks_.clear();
    for (const auto &callback : callbacks) {
      callback.first(Status::OK());
    }
    for (const auto &callback : notification_callbacks_) {
      callback.first(callback.second, *task_table_[callback.second]);
    }
  }

  const std::unordered_map<TaskID, std::shared_ptr<TaskTableData>> &TaskTable() const {
    return task_table_;
  }

  const std::unordered_set<TaskID> &SubscribedTasks() const { return subscribed_tasks_; }

  const int NumRequestedNotifications() const { return num_requested_notifications_; }

  const int NumTaskAdds() const { return num_task_adds_; }

  Status AsyncGet(const TaskID &task_id,
                  const OptionalItemCallback<rpc::TaskTableData> &callback) override {
    return Status::OK();
  }

  Status AsyncDelete(const std::vector<TaskID> &task_ids,
                     const StatusCallback &callback) override {
    return Status::OK();
  }

  Status AsyncAddTaskLease(const std::shared_ptr<TaskLeaseData> &task_lease_data,
                           const StatusCallback &done) override {
    TaskID task_id = TaskID::FromBinary(task_lease_data->task_id());
    task_lease_table_[task_id] = task_lease_data;
    if (subscribed_tasks_.count(task_id) == 1) {
      boost::optional<TaskLeaseData> result(*task_lease_data);
      subscribe_callback_(task_id, result);
    }
    return Status::OK();
  }

  Status AsyncGetTaskLease(
      const TaskID &task_id,
      const OptionalItemCallback<rpc::TaskLeaseData> &callback) override {
    auto iter = task_lease_table_.find(task_id);
    if (iter != task_lease_table_.end()) {
      callback(Status::OK(), *iter->second);
    } else {
      callback(Status::OK(), boost::none);
    }
    return Status::OK();
  }

  Status AsyncSubscribeTaskLease(
      const TaskID &task_id,
      const SubscribeCallback<TaskID, boost::optional<TaskLeaseData>> &subscribe,
      const StatusCallback &done) override {
    subscribe_callback_ = subscribe;
    subscribed_tasks_.insert(task_id);
    auto entry = task_lease_table_.find(task_id);
    if (entry == task_lease_table_.end()) {
      boost::optional<TaskLeaseData> result;
      subscribe(task_id, result);
    } else {
      boost::optional<TaskLeaseData> result(*entry->second);
      subscribe(task_id, result);
    }
    return ray::Status::OK();
  }

  Status AsyncUnsubscribeTaskLease(const TaskID &task_id) override {
    subscribed_tasks_.erase(task_id);
    return ray::Status::OK();
  }

  Status AttemptTaskReconstruction(
      const std::shared_ptr<TaskReconstructionData> &task_data,
      const StatusCallback &done) override {
    int log_index = task_data->num_reconstructions();
    TaskID task_id = TaskID::FromBinary(task_data->task_id());
    if (task_reconstruction_log_[task_id].size() == static_cast<size_t>(log_index)) {
      task_reconstruction_log_[task_id].push_back(*task_data);
      if (done != nullptr) {
        done(Status::OK());
      }
    } else {
      if (done != nullptr) {
        done(Status::Invalid("Updating task reconstruction failed."));
      }
    }
    return Status::OK();
  }

  void AsyncResubscribe(bool is_pubsub_server_restarted) override {}

 private:
  std::unordered_map<TaskID, std::shared_ptr<TaskTableData>> task_table_;
  std::vector<std::pair<StatusCallback, TaskID>> callbacks_;

  typedef SubscribeCallback<TaskID, rpc::TaskTableData> TaskSubscribeCallback;
  TaskSubscribeCallback notification_callback_;
  std::vector<std::pair<TaskSubscribeCallback, TaskID>> notification_callbacks_;

  std::unordered_set<TaskID> subscribed_tasks_;
  int num_requested_notifications_ = 0;
  int num_task_adds_ = 0;

  SubscribeCallback<TaskID, boost::optional<TaskLeaseData>> subscribe_callback_;
  std::unordered_map<TaskID, std::shared_ptr<TaskLeaseData>> task_lease_table_;
  std::unordered_map<TaskID, std::vector<TaskReconstructionData>>
      task_reconstruction_log_;
};

}  // namespace gcs

}  // namespace ray
