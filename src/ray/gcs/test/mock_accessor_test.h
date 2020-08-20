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

  void AsyncReReportHeartbeat() override {}

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

}  // namespace gcs

}  // namespace ray
