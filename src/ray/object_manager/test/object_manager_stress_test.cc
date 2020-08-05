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

#include <chrono>
#include <iostream>
#include <random>
#include <thread>

#include "gtest/gtest.h"
#include "ray/common/status.h"
#include "ray/common/test_util.h"
#include "ray/gcs/redis_gcs_client.h"
#include "ray/gcs/subscription_executor.h"
#include "ray/object_manager/object_manager.h"
#include "ray/util/filesystem.h"

extern "C" {
#include "hiredis/hiredis.h"
}

namespace ray {

using rpc::GcsNodeInfo;

static inline void flushall_redis(void) {
  redisContext *context = redisConnect("127.0.0.1", 6379);
  freeReplyObject(redisCommand(context, "FLUSHALL"));
  redisFree(context);
}

int64_t current_time_ms() {
  std::chrono::milliseconds ms_since_epoch =
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::steady_clock::now().time_since_epoch());
  return ms_since_epoch.count();
}

class MockNodeInfoAccessor : public gcs::NodeInfoAccessor {
 public:
  MockNodeInfoAccessor(gcs::RedisGcsClient *client) : client_impl_(client) {}

  bool IsRemoved(const ClientID &node_id) const override {
    gcs::ClientTable &client_table = client_impl_->client_table();
    return client_table.IsRemoved(node_id);
  }

  Status RegisterSelf(const rpc::GcsNodeInfo &local_node_info) override {
    gcs::ClientTable &client_table = client_impl_->client_table();
    return client_table.Connect(local_node_info);
  }

  Status UnregisterSelf() override {
    gcs::ClientTable &client_table = client_impl_->client_table();
    return client_table.Disconnect();
  }

  const ClientID &GetSelfId() const override {
    gcs::ClientTable &client_table = client_impl_->client_table();
    return client_table.GetLocalClientId();
  }

  const rpc::GcsNodeInfo &GetSelfInfo() const override { return node_info_; }

  Status AsyncRegister(const rpc::GcsNodeInfo &node_info,
                       const gcs::StatusCallback &callback) override {
    return Status::OK();
  }

  Status AsyncUnregister(const ClientID &node_id,
                         const gcs::StatusCallback &callback) override {
    gcs::ClientTable &client_table = client_impl_->client_table();
    return client_table.Disconnect();
  }

  Status AsyncGetAll(const gcs::MultiItemCallback<rpc::GcsNodeInfo> &callback) override {
    return Status::OK();
  }

  Status AsyncSubscribeToNodeChange(
      const gcs::SubscribeCallback<ClientID, rpc::GcsNodeInfo> &subscribe,
      const gcs::StatusCallback &done) override {
    RAY_CHECK(subscribe != nullptr);
    gcs::ClientTable &client_table = client_impl_->client_table();
    return client_table.SubscribeToNodeChange(subscribe, done);
  }

  boost::optional<rpc::GcsNodeInfo> Get(const ClientID &node_id) const override {
    rpc::GcsNodeInfo node_info;
    gcs::ClientTable &client_table = client_impl_->client_table();
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

  Status AsyncGetResources(
      const ClientID &node_id,
      const gcs::OptionalItemCallback<ResourceMap> &callback) override {
    return Status::OK();
  }

  Status AsyncUpdateResources(const ClientID &node_id, const ResourceMap &resources,
                              const gcs::StatusCallback &callback) override {
    return Status::OK();
  }

  Status AsyncDeleteResources(const ClientID &node_id,
                              const std::vector<std::string> &resource_names,
                              const gcs::StatusCallback &callback) override {
    return Status::OK();
  }

  Status AsyncSubscribeToResources(
      const gcs::ItemCallback<rpc::NodeResourceChange> &subscribe,
      const gcs::StatusCallback &done) override {
    return Status::OK();
  }

  Status AsyncReportHeartbeat(const std::shared_ptr<rpc::HeartbeatTableData> &data_ptr,
                              const gcs::StatusCallback &callback) override {
    return Status::OK();
  }

  Status AsyncSubscribeHeartbeat(
      const gcs::SubscribeCallback<ClientID, rpc::HeartbeatTableData> &subscribe,
      const gcs::StatusCallback &done) override {
    return Status::OK();
  }

  Status AsyncReportBatchHeartbeat(
      const std::shared_ptr<rpc::HeartbeatBatchTableData> &data_ptr,
      const gcs::StatusCallback &callback) override {
    return Status::OK();
  }

  Status AsyncSubscribeBatchHeartbeat(
      const gcs::ItemCallback<rpc::HeartbeatBatchTableData> &subscribe,
      const gcs::StatusCallback &done) override {
    return Status::OK();
  }

  void AsyncResubscribe(bool is_pubsub_server_restarted) override {}

  Status AsyncSetInternalConfig(
      std::unordered_map<std::string, std::string> &config) override {
    return Status::OK();
  }

  Status AsyncGetInternalConfig(
      const gcs::OptionalItemCallback<std::unordered_map<std::string, std::string>>
          &callback) override {
    return Status::OK();
  }

 private:
  rpc::GcsNodeInfo node_info_;
  std::unordered_map<ClientID, rpc::GcsNodeInfo> map_info_;
  gcs::RedisGcsClient *client_impl_{nullptr};
};

class MockObjectAccessor : public gcs::ObjectInfoAccessor {
 public:
  MockObjectAccessor(gcs::RedisGcsClient *client)
      : client_impl_(client), object_sub_executor_(client_impl_->object_table()) {}

  Status AsyncGetLocations(
      const ObjectID &object_id,
      const gcs::MultiItemCallback<gcs::ObjectTableData> &callback) override {
    return Status::OK();
  }

  Status AsyncGetAll(
      const gcs::MultiItemCallback<rpc::ObjectLocationInfo> &callback) override {
    return Status::OK();
  }

  Status AsyncAddLocation(const ObjectID &object_id, const ClientID &node_id,
                          const gcs::StatusCallback &callback) override {
    std::function<void(gcs::RedisGcsClient * client, const ObjectID &id,
                       const gcs::ObjectTableData &data)>
        on_done = nullptr;
    if (callback != nullptr) {
      on_done = [callback](gcs::RedisGcsClient *client, const ObjectID &object_id,
                           const gcs::ObjectTableData &data) { callback(Status::OK()); };
    }

    std::shared_ptr<gcs::ObjectTableData> data_ptr =
        std::make_shared<gcs::ObjectTableData>();
    data_ptr->set_manager(node_id.Binary());

    gcs::ObjectTable &object_table = client_impl_->object_table();
    return object_table.Add(object_id.TaskId().JobId(), object_id, data_ptr, on_done);
    // return Status::NotImplemented("AsyncGetAll not implemented");
  }

  Status AsyncRemoveLocation(const ObjectID &object_id, const ClientID &node_id,
                             const gcs::StatusCallback &callback) override {
    return Status::OK();
  }

  Status AsyncSubscribeToLocations(
      const ObjectID &object_id,
      const gcs::SubscribeCallback<ObjectID, gcs::ObjectChangeNotification> &subscribe,
      const gcs::StatusCallback &done) override {
    RAY_CHECK(subscribe != nullptr);
    return object_sub_executor_.AsyncSubscribe(subscribe_id_, object_id, subscribe, done);
  }

  Status AsyncUnsubscribeToLocations(const ObjectID &object_id) override {
    return object_sub_executor_.AsyncUnsubscribe(subscribe_id_, object_id, nullptr);
  }

  void AsyncResubscribe(bool is_pubsub_server_restarted) override {}

 private:
  gcs::RedisGcsClient *client_impl_{nullptr};
  ClientID subscribe_id_{ClientID::FromRandom()};

  typedef gcs::SubscriptionExecutor<ObjectID, gcs::ObjectChangeNotification,
                                    gcs::ObjectTable>
      ObjectSubscriptionExecutor;
  ObjectSubscriptionExecutor object_sub_executor_;
};

class MockGcsClient : public gcs::RedisGcsClient {
 public:
  MockGcsClient(gcs::GcsClientOptions option) : gcs::RedisGcsClient(option){};

  void Init(gcs::NodeInfoAccessor *node_accessor, MockObjectAccessor *object_accessor) {
    node_accessor_.reset(node_accessor);
    object_accessor_.reset(object_accessor);
  }
};

class MockServer {
 public:
  MockServer(boost::asio::io_service &main_service,
             const ObjectManagerConfig &object_manager_config,
             std::shared_ptr<gcs::GcsClient> gcs_client,
             std::shared_ptr<MockGcsClient> redis_client)
      : node_id_(ClientID::FromRandom()),
        config_(object_manager_config),
        gcs_client_(redis_client),
        object_manager_(main_service, node_id_, object_manager_config,
                        std::make_shared<ObjectDirectory>(main_service, gcs_client)),
        node_accessor_(new MockNodeInfoAccessor(redis_client.get())),
        object_accessor_(new MockObjectAccessor(redis_client.get())) {
    gcs_client_->Init(node_accessor_, object_accessor_);

    RAY_CHECK_OK(RegisterGcs(main_service));
  }

  ~MockServer() { RAY_CHECK_OK(gcs_client_->Nodes().UnregisterSelf()); }

 private:
  ray::Status RegisterGcs(boost::asio::io_service &io_service) {
    auto object_manager_port = object_manager_.GetServerPort();
    GcsNodeInfo node_info;
    node_info.set_node_id(node_id_.Binary());
    node_info.set_node_manager_address("127.0.0.1");
    node_info.set_node_manager_port(object_manager_port);
    node_info.set_object_manager_port(object_manager_port);

    ray::Status status = gcs_client_->Nodes().RegisterSelf(node_info);
    return status;
  }

  friend class StressTestObjectManager;

  ClientID node_id_;
  ObjectManagerConfig config_;
  std::shared_ptr<MockGcsClient> gcs_client_;
  ObjectManager object_manager_;
  MockNodeInfoAccessor *node_accessor_;
  MockObjectAccessor *object_accessor_;
};

class TestObjectManagerBase : public ::testing::Test {
 public:
  void SetUp() {
    flushall_redis();

    // start store
    socket_name_1 = TestSetupUtil::StartObjectStore();
    socket_name_2 = TestSetupUtil::StartObjectStore();

    unsigned int pull_timeout_ms = 1000;
    uint64_t object_chunk_size = static_cast<uint64_t>(std::pow(10, 3));
    int push_timeout_ms = 10000;

    // start first server
    gcs::GcsClientOptions client_options("127.0.0.1", 6379, /*password*/ "",
                                         /*is_test_client=*/true);
    gcs_client_1 = std::make_shared<MockGcsClient>(client_options);
    RAY_CHECK_OK(gcs_client_1->Connect(main_service));
    ObjectManagerConfig om_config_1;
    om_config_1.store_socket_name = socket_name_1;
    om_config_1.pull_timeout_ms = pull_timeout_ms;
    om_config_1.object_chunk_size = object_chunk_size;
    om_config_1.push_timeout_ms = push_timeout_ms;
    om_config_1.object_manager_port = 0;
    om_config_1.rpc_service_threads_number = 3;
    server1.reset(new MockServer(main_service, om_config_1, gcs_client_1, gcs_client_1));

    // start second server
    gcs_client_2 = std::make_shared<MockGcsClient>(client_options);
    RAY_CHECK_OK(gcs_client_2->Connect(main_service));
    ObjectManagerConfig om_config_2;
    om_config_2.store_socket_name = socket_name_2;
    om_config_2.pull_timeout_ms = pull_timeout_ms;
    om_config_2.object_chunk_size = object_chunk_size;
    om_config_2.push_timeout_ms = push_timeout_ms;
    om_config_2.object_manager_port = 0;
    om_config_2.rpc_service_threads_number = 3;
    server2.reset(new MockServer(main_service, om_config_2, gcs_client_2, gcs_client_2));

    // connect to stores.
    RAY_CHECK_OK(client1.Connect(socket_name_1));
    RAY_CHECK_OK(client2.Connect(socket_name_2));
  }

  void TearDown() {
    Status client1_status = client1.Disconnect();
    Status client2_status = client2.Disconnect();
    ASSERT_TRUE(client1_status.ok() && client2_status.ok());

    gcs_client_1->Disconnect();
    gcs_client_2->Disconnect();

    this->server1.reset();
    this->server2.reset();

    TestSetupUtil::StopObjectStore(socket_name_1);
    TestSetupUtil::StopObjectStore(socket_name_2);
  }

  ObjectID WriteDataToClient(plasma::PlasmaClient &client, int64_t data_size) {
    ObjectID object_id = ObjectID::FromRandom();
    RAY_LOG(DEBUG) << "ObjectID Created: " << object_id;
    uint8_t metadata[] = {5};
    int64_t metadata_size = sizeof(metadata);
    std::shared_ptr<arrow::Buffer> data;
    RAY_CHECK_OK(client.Create(object_id, data_size, metadata, metadata_size, &data));
    RAY_CHECK_OK(client.Seal(object_id));
    return object_id;
  }

  void object_added_handler_1(ObjectID object_id) { v1.push_back(object_id); };

  void object_added_handler_2(ObjectID object_id) { v2.push_back(object_id); };

 protected:
  std::thread p;
  boost::asio::io_service main_service;
  std::shared_ptr<MockGcsClient> gcs_client_1;
  std::shared_ptr<MockGcsClient> gcs_client_2;
  std::unique_ptr<MockServer> server1;
  std::unique_ptr<MockServer> server2;

  plasma::PlasmaClient client1;
  plasma::PlasmaClient client2;
  std::vector<ObjectID> v1;
  std::vector<ObjectID> v2;

  std::string socket_name_1;
  std::string socket_name_2;
};

class StressTestObjectManager : public TestObjectManagerBase {
 public:
  enum class TransferPattern {
    PUSH_A_B,
    PUSH_B_A,
    BIDIRECTIONAL_PUSH,
    PULL_A_B,
    PULL_B_A,
    BIDIRECTIONAL_PULL,
    BIDIRECTIONAL_PULL_VARIABLE_DATA_SIZE,
  };

  int async_loop_index = -1;
  size_t num_expected_objects;

  std::vector<TransferPattern> async_loop_patterns = {
      TransferPattern::PUSH_A_B,
      TransferPattern::PUSH_B_A,
      TransferPattern::BIDIRECTIONAL_PUSH,
      TransferPattern::PULL_A_B,
      TransferPattern::PULL_B_A,
      TransferPattern::BIDIRECTIONAL_PULL,
      TransferPattern::BIDIRECTIONAL_PULL_VARIABLE_DATA_SIZE};

  int num_connected_clients = 0;

  ClientID node_id_1;
  ClientID node_id_2;

  int64_t start_time;

  void WaitConnections() {
    node_id_1 = gcs_client_1->Nodes().GetSelfId();
    node_id_2 = gcs_client_2->Nodes().GetSelfId();
    RAY_CHECK_OK(gcs_client_1->Nodes().AsyncSubscribeToNodeChange(
        [this](const ClientID &node_id, const GcsNodeInfo &data) {
          if (node_id == node_id_1 || node_id == node_id_2) {
            num_connected_clients += 1;
          }
          if (num_connected_clients == 4) {
            StartTests();
          }
        },
        nullptr));
    RAY_CHECK_OK(gcs_client_2->Nodes().AsyncSubscribeToNodeChange(
        [this](const ClientID &node_id, const GcsNodeInfo &data) {
          if (node_id == node_id_1 || node_id == node_id_2) {
            num_connected_clients += 1;
          }
          if (num_connected_clients == 4) {
            StartTests();
          }
        },
        nullptr));
  }

  void StartTests() {
    TestConnections();
    AddTransferTestHandlers();
    TransferTestNext();
  }

  void AddTransferTestHandlers() {
    ray::Status status = ray::Status::OK();
    status = server1->object_manager_.SubscribeObjAdded(
        [this](const object_manager::protocol::ObjectInfoT &object_info) {
          object_added_handler_1(ObjectID::FromBinary(object_info.object_id));
          if (v1.size() == num_expected_objects && v1.size() == v2.size()) {
            TransferTestComplete();
          }
        });
    RAY_CHECK_OK(status);
    status = server2->object_manager_.SubscribeObjAdded(
        [this](const object_manager::protocol::ObjectInfoT &object_info) {
          object_added_handler_2(ObjectID::FromBinary(object_info.object_id));
          if (v2.size() == num_expected_objects && v1.size() == v2.size()) {
            TransferTestComplete();
          }
        });
    RAY_CHECK_OK(status);
  }

  void TransferTestNext() {
    async_loop_index += 1;
    if ((size_t)async_loop_index < async_loop_patterns.size()) {
      TransferPattern pattern = async_loop_patterns[async_loop_index];
      TransferTestExecute(100, 3 * std::pow(10, 3) - 1, pattern);
    } else {
      main_service.stop();
    }
  }

  plasma::ObjectBuffer GetObject(plasma::PlasmaClient &client, ObjectID &object_id) {
    plasma::ObjectBuffer object_buffer;
    RAY_CHECK_OK(client.Get(&object_id, 1, 0, &object_buffer));
    return object_buffer;
  }

  void CompareObjects(ObjectID &object_id_1, ObjectID &object_id_2) {
    plasma::ObjectBuffer object_buffer_1 = GetObject(client1, object_id_1);
    plasma::ObjectBuffer object_buffer_2 = GetObject(client2, object_id_2);
    uint8_t *data_1 = const_cast<uint8_t *>(object_buffer_1.data->data());
    uint8_t *data_2 = const_cast<uint8_t *>(object_buffer_2.data->data());
    ASSERT_EQ(object_buffer_1.data->size(), object_buffer_2.data->size());
    ASSERT_EQ(object_buffer_1.metadata->size(), object_buffer_2.metadata->size());
    int64_t total_size = object_buffer_1.data->size() + object_buffer_1.metadata->size();
    RAY_LOG(DEBUG) << "total_size " << total_size;
    for (int i = -1; ++i < total_size;) {
      ASSERT_TRUE(data_1[i] == data_2[i]);
    }
  }

  void TransferTestComplete() {
    int64_t elapsed = current_time_ms() - start_time;
    RAY_LOG(INFO) << "TransferTestComplete: "
                  << static_cast<int>(async_loop_patterns[async_loop_index]) << " "
                  << v1.size() << " " << elapsed;
    ASSERT_TRUE(v1.size() == v2.size());
    for (size_t i = 0; i < v1.size(); ++i) {
      ASSERT_TRUE(std::find(v1.begin(), v1.end(), v2[i]) != v1.end());
    }

    // Compare objects and their hashes.
    for (size_t i = 0; i < v1.size(); ++i) {
      ObjectID object_id_2 = v2[i];
      ObjectID object_id_1 =
          v1[std::distance(v1.begin(), std::find(v1.begin(), v1.end(), v2[i]))];
      CompareObjects(object_id_1, object_id_2);
    }

    v1.clear();
    v2.clear();
    TransferTestNext();
  }

  void TransferTestExecute(int num_trials, int64_t data_size,
                           TransferPattern transfer_pattern) {
    ClientID node_id_1 = gcs_client_1->Nodes().GetSelfId();
    ClientID node_id_2 = gcs_client_2->Nodes().GetSelfId();

    ray::Status status = ray::Status::OK();

    if (transfer_pattern == TransferPattern::BIDIRECTIONAL_PULL ||
        transfer_pattern == TransferPattern::BIDIRECTIONAL_PUSH ||
        transfer_pattern == TransferPattern::BIDIRECTIONAL_PULL_VARIABLE_DATA_SIZE) {
      num_expected_objects = (size_t)2 * num_trials;
    } else {
      num_expected_objects = (size_t)num_trials;
    }

    start_time = current_time_ms();

    switch (transfer_pattern) {
    case TransferPattern::PUSH_A_B: {
      for (int i = -1; ++i < num_trials;) {
        ObjectID oid1 = WriteDataToClient(client1, data_size);
        server1->object_manager_.Push(oid1, node_id_2);
      }
    } break;
    case TransferPattern::PUSH_B_A: {
      for (int i = -1; ++i < num_trials;) {
        ObjectID oid2 = WriteDataToClient(client2, data_size);
        server2->object_manager_.Push(oid2, node_id_1);
      }
    } break;
    case TransferPattern::BIDIRECTIONAL_PUSH: {
      for (int i = -1; ++i < num_trials;) {
        ObjectID oid1 = WriteDataToClient(client1, data_size);
        server1->object_manager_.Push(oid1, node_id_2);
        ObjectID oid2 = WriteDataToClient(client2, data_size);
        server2->object_manager_.Push(oid2, node_id_1);
      }
    } break;
    case TransferPattern::PULL_A_B: {
      for (int i = -1; ++i < num_trials;) {
        ObjectID oid1 = WriteDataToClient(client1, data_size);
        status = server2->object_manager_.Pull(oid1);
      }
    } break;
    case TransferPattern::PULL_B_A: {
      for (int i = -1; ++i < num_trials;) {
        ObjectID oid2 = WriteDataToClient(client2, data_size);
        status = server1->object_manager_.Pull(oid2);
      }
    } break;
    case TransferPattern::BIDIRECTIONAL_PULL: {
      for (int i = -1; ++i < num_trials;) {
        ObjectID oid1 = WriteDataToClient(client1, data_size);
        status = server2->object_manager_.Pull(oid1);
        ObjectID oid2 = WriteDataToClient(client2, data_size);
        status = server1->object_manager_.Pull(oid2);
      }
    } break;
    case TransferPattern::BIDIRECTIONAL_PULL_VARIABLE_DATA_SIZE: {
      std::random_device rd;
      std::mt19937 gen(rd());
      std::uniform_int_distribution<> dis(1, 50);
      for (int i = -1; ++i < num_trials;) {
        ObjectID oid1 = WriteDataToClient(client1, data_size + dis(gen));
        status = server2->object_manager_.Pull(oid1);
        ObjectID oid2 = WriteDataToClient(client2, data_size + dis(gen));
        status = server1->object_manager_.Pull(oid2);
      }
    } break;
    default: {
      RAY_LOG(FATAL) << "No case for transfer_pattern "
                     << static_cast<int>(transfer_pattern);
    } break;
    }
  }

  void TestConnections() {
    RAY_LOG(DEBUG) << "\n"
                   << "Server node ids:"
                   << "\n";
    ClientID node_id_1 = gcs_client_1->Nodes().GetSelfId();
    ClientID node_id_2 = gcs_client_2->Nodes().GetSelfId();
    RAY_LOG(DEBUG) << "Server 1: " << node_id_1 << "\n"
                   << "Server 2: " << node_id_2;

    RAY_LOG(DEBUG) << "\n"
                   << "All connected nodes:"
                   << "\n";
    auto data = gcs_client_1->Nodes().Get(node_id_1);
    RAY_LOG(DEBUG) << "NodeID=" << ClientID::FromBinary(data->node_id()) << "\n"
                   << "NodeIp=" << data->node_manager_address() << "\n"
                   << "NodePort=" << data->node_manager_port();
    auto data2 = gcs_client_1->Nodes().Get(node_id_2);
    RAY_LOG(DEBUG) << "NodeID=" << ClientID::FromBinary(data2->node_id()) << "\n"
                   << "NodeIp=" << data2->node_manager_address() << "\n"
                   << "NodePort=" << data2->node_manager_port();
  }
};

TEST_F(StressTestObjectManager, StartStressTestObjectManager) {
  auto AsyncStartTests = main_service.wrap([this]() { WaitConnections(); });
  AsyncStartTests();
  main_service.run();
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  ray::TEST_STORE_EXEC_PATH = std::string(argv[1]);
  return RUN_ALL_TESTS();
}
