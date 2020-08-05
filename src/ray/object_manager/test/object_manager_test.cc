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

#include "ray/object_manager/object_manager.h"

#include <iostream>
#include <thread>

#include "gtest/gtest.h"
#include "ray/common/status.h"
#include "ray/common/test_util.h"
#include "ray/gcs/redis_gcs_client.h"
#include "ray/gcs/subscription_executor.h"
#include "ray/util/filesystem.h"

extern "C" {
#include "hiredis/hiredis.h"
}

namespace {
int64_t wait_timeout_ms;
}  // namespace

namespace ray {

using rpc::GcsNodeInfo;

static inline void flushall_redis(void) {
  redisContext *context = redisConnect("127.0.0.1", 6379);
  freeReplyObject(redisCommand(context, "FLUSHALL"));
  redisFree(context);
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
    RAY_CHECK(callback != nullptr);
    auto on_done = [callback](gcs::RedisGcsClient *client, const ObjectID &object_id,
                              const std::vector<gcs::ObjectTableData> &data) {
      callback(Status::OK(), data);
    };

    gcs::ObjectTable &object_table = client_impl_->object_table();
    return object_table.Lookup(object_id.TaskId().JobId(), object_id, on_done);
  }

  Status AsyncGetAll(
      const gcs::MultiItemCallback<rpc::ObjectLocationInfo> &callback) override {
    return Status::NotImplemented("AsyncGetAll not implemented");
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

  friend class TestObjectManager;

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

    unsigned int pull_timeout_ms = 1;
    push_timeout_ms = 1000;

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
    return WriteDataToClient(client, data_size, ObjectID::FromRandom());
  }

  ObjectID WriteDataToClient(plasma::PlasmaClient &client, int64_t data_size,
                             ObjectID object_id) {
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

  unsigned int push_timeout_ms;

  uint64_t object_chunk_size = static_cast<uint64_t>(std::pow(10, 3));
};

class TestObjectManager : public TestObjectManagerBase {
 public:
  int current_wait_test = -1;
  int num_connected_clients = 0;
  ClientID node_id_1;
  ClientID node_id_2;

  ObjectID created_object_id1;
  ObjectID created_object_id2;

  std::unique_ptr<boost::asio::deadline_timer> timer;

  void WaitConnections() {
    node_id_1 = gcs_client_1->Nodes().GetSelfId();
    node_id_2 = gcs_client_2->Nodes().GetSelfId();
    RAY_CHECK_OK(gcs_client_1->Nodes().AsyncSubscribeToNodeChange(
        [this](const ClientID &node_id, const GcsNodeInfo &data) {
          if (node_id == node_id_1 || node_id == node_id_2) {
            num_connected_clients += 1;
          }
          if (num_connected_clients == 2) {
            StartTests();
          }
        },
        nullptr));
  }

  void StartTests() {
    TestConnections();
    TestNotifications();
  }

  void TestNotifications() {
    ray::Status status = ray::Status::OK();
    status = server1->object_manager_.SubscribeObjAdded(
        [this](const object_manager::protocol::ObjectInfoT &object_info) {
          object_added_handler_1(ObjectID::FromBinary(object_info.object_id));
          NotificationTestCompleteIfSatisfied();
        });
    RAY_CHECK_OK(status);
    status = server2->object_manager_.SubscribeObjAdded(
        [this](const object_manager::protocol::ObjectInfoT &object_info) {
          object_added_handler_2(ObjectID::FromBinary(object_info.object_id));
          NotificationTestCompleteIfSatisfied();
        });
    RAY_CHECK_OK(status);

    size_t data_size = 1000000;

    // dummy_id is not local. The push function will timeout.
    ObjectID dummy_id = ObjectID::FromRandom();
    server1->object_manager_.Push(dummy_id, gcs_client_2->Nodes().GetSelfId());

    created_object_id1 = ObjectID::FromRandom();
    WriteDataToClient(client1, data_size, created_object_id1);
    // Server1 holds Object1 so this Push call will success.
    server1->object_manager_.Push(created_object_id1, gcs_client_2->Nodes().GetSelfId());

    // This timer is used to guarantee that the Push function for dummy_id will timeout.
    timer.reset(new boost::asio::deadline_timer(main_service));
    auto period = boost::posix_time::milliseconds(push_timeout_ms + 10);
    timer->expires_from_now(period);
    created_object_id2 = ObjectID::FromRandom();
    timer->async_wait([this, data_size](const boost::system::error_code &error) {
      WriteDataToClient(client2, data_size, created_object_id2);
    });
  }

  void NotificationTestCompleteIfSatisfied() {
    size_t num_expected_objects1 = 1;
    size_t num_expected_objects2 = 2;
    if (v1.size() == num_expected_objects1 && v2.size() == num_expected_objects2) {
      SubscribeObjectThenWait();
    }
  }

  void SubscribeObjectThenWait() {
    int data_size = 100;
    // Test to ensure Wait works properly during an active subscription to the same
    // object.
    ObjectID object_1 = WriteDataToClient(client2, data_size);
    ObjectID object_2 = WriteDataToClient(client2, data_size);
    UniqueID sub_id = ray::UniqueID::FromRandom();

    RAY_CHECK_OK(server1->object_manager_.object_directory_->SubscribeObjectLocations(
        sub_id, object_1,
        [this, sub_id, object_1, object_2](
            const ray::ObjectID &object_id,
            const std::unordered_set<ray::ClientID> &clients) {
          if (!clients.empty()) {
            TestWaitWhileSubscribed(sub_id, object_1, object_2);
          }
        }));
  }

  void TestWaitWhileSubscribed(UniqueID sub_id, ObjectID object_1, ObjectID object_2) {
    int required_objects = 1;
    int timeout_ms = 1000;

    std::vector<ObjectID> object_ids = {object_1, object_2};
    boost::posix_time::ptime start_time = boost::posix_time::second_clock::local_time();

    UniqueID wait_id = UniqueID::FromRandom();

    RAY_CHECK_OK(server1->object_manager_.AddWaitRequest(
        wait_id, object_ids, timeout_ms, required_objects, false,
        [this, sub_id, object_1, object_ids, start_time](
            const std::vector<ray::ObjectID> &found,
            const std::vector<ray::ObjectID> &remaining) {
          int64_t elapsed = (boost::posix_time::second_clock::local_time() - start_time)
                                .total_milliseconds();
          RAY_LOG(DEBUG) << "elapsed " << elapsed;
          RAY_LOG(DEBUG) << "found " << found.size();
          RAY_LOG(DEBUG) << "remaining " << remaining.size();
          RAY_CHECK(found.size() == 1);
          // There's nothing more to test. A check will fail if unexpected behavior is
          // triggered.
          RAY_CHECK_OK(
              server1->object_manager_.object_directory_->UnsubscribeObjectLocations(
                  sub_id, object_1));
          NextWaitTest();
        }));

    // Skip lookups and rely on Subscribe only to test subscribe interaction.
    server1->object_manager_.SubscribeRemainingWaitObjects(wait_id);
  }

  void NextWaitTest() {
    int data_size = 600;
    current_wait_test += 1;
    switch (current_wait_test) {
    case 0: {
      // Ensure timeout_ms = 0 is handled correctly.
      // Out of 5 objects, we expect 3 ready objects and 2 remaining objects.
      TestWait(data_size, 5, 3, /*timeout_ms=*/0, false, false);
    } break;
    case 1: {
      // Ensure timeout_ms = 1000 is handled correctly.
      // Out of 5 objects, we expect 3 ready objects and 2 remaining objects.
      TestWait(data_size, 5, 3, wait_timeout_ms, false, false);
    } break;
    case 2: {
      // Generate objects locally to ensure local object code-path works properly.
      // Out of 5 objects, we expect 3 ready objects and 2 remaining objects.
      TestWait(data_size, 5, 3, wait_timeout_ms, false, /*test_local=*/true);
    } break;
    case 3: {
      // Wait on an object that's never registered with GCS to ensure timeout works
      // properly.
      TestWait(data_size, /*num_objects=*/5, /*required_objects=*/6, wait_timeout_ms,
               /*include_nonexistent=*/true, false);
    } break;
    case 4: {
      // Ensure infinite time code-path works properly.
      TestWait(data_size, 5, 5, /*timeout_ms=*/-1, false, false);
    } break;
    }
  }

  void TestWait(int data_size, int num_objects, uint64_t required_objects, int timeout_ms,
                bool include_nonexistent, bool test_local) {
    std::vector<ObjectID> object_ids;
    for (int i = -1; ++i < num_objects;) {
      ObjectID oid;
      if (test_local) {
        oid = WriteDataToClient(client1, data_size);
      } else {
        oid = WriteDataToClient(client2, data_size);
      }
      object_ids.push_back(oid);
    }
    if (include_nonexistent) {
      num_objects += 1;
      object_ids.push_back(ObjectID::FromRandom());
    }

    boost::posix_time::ptime start_time = boost::posix_time::second_clock::local_time();
    RAY_CHECK_OK(server1->object_manager_.Wait(
        object_ids, timeout_ms, required_objects, false,
        [this, object_ids, num_objects, timeout_ms, required_objects, start_time](
            const std::vector<ray::ObjectID> &found,
            const std::vector<ray::ObjectID> &remaining) {
          int64_t elapsed = (boost::posix_time::second_clock::local_time() - start_time)
                                .total_milliseconds();
          RAY_LOG(DEBUG) << "elapsed " << elapsed;
          RAY_LOG(DEBUG) << "found " << found.size();
          RAY_LOG(DEBUG) << "remaining " << remaining.size();

          // Ensure object order is preserved for all invocations.
          size_t j = 0;
          size_t k = 0;
          for (size_t i = 0; i < object_ids.size(); ++i) {
            ObjectID oid = object_ids[i];
            // Make sure the object is in either the found vector or the remaining vector.
            if (j < found.size() && found[j] == oid) {
              j += 1;
            }
            if (k < remaining.size() && remaining[k] == oid) {
              k += 1;
            }
          }
          if (!found.empty()) {
            ASSERT_EQ(j, found.size());
          }
          if (!remaining.empty()) {
            ASSERT_EQ(k, remaining.size());
          }

          switch (current_wait_test) {
          case 0: {
            // Ensure timeout_ms = 0 returns expected number of found and remaining
            // objects.
            ASSERT_TRUE(found.size() <= required_objects);
            ASSERT_TRUE(static_cast<int>(found.size() + remaining.size()) == num_objects);
            NextWaitTest();
          } break;
          case 1: {
            // Ensure lookup succeeds as expected when timeout_ms = 1000.
            ASSERT_TRUE(found.size() >= required_objects);
            ASSERT_TRUE(static_cast<int>(found.size() + remaining.size()) == num_objects);
            NextWaitTest();
          } break;
          case 2: {
            // Ensure lookup succeeds as expected when objects are local.
            ASSERT_TRUE(found.size() >= required_objects);
            ASSERT_TRUE(static_cast<int>(found.size() + remaining.size()) == num_objects);
            NextWaitTest();
          } break;
          case 3: {
            // Ensure lookup returns after timeout_ms elapses when one object doesn't
            // exist.
            ASSERT_TRUE(elapsed >= timeout_ms);
            ASSERT_TRUE(static_cast<int>(found.size() + remaining.size()) == num_objects);
            NextWaitTest();
          } break;
          case 4: {
            // Ensure timeout_ms = -1 works properly.
            ASSERT_TRUE(static_cast<int>(found.size()) == num_objects);
            ASSERT_TRUE(remaining.size() == 0);
            TestWaitComplete();
          } break;
          }
        }));
  }

  void TestWaitComplete() { main_service.stop(); }

  void TestConnections() {
    RAY_LOG(DEBUG) << "\n"
                   << "Server node ids:"
                   << "\n";
    auto data = gcs_client_1->Nodes().Get(node_id_1);
    RAY_LOG(DEBUG) << (ClientID::FromBinary(data->node_id()).IsNil());
    RAY_LOG(DEBUG) << "Server 1 NodeID=" << ClientID::FromBinary(data->node_id());
    RAY_LOG(DEBUG) << "Server 1 NodeIp=" << data->node_manager_address();
    RAY_LOG(DEBUG) << "Server 1 NodePort=" << data->node_manager_port();
    ASSERT_EQ(node_id_1, ClientID::FromBinary(data->node_id()));
    auto data2 = gcs_client_1->Nodes().Get(node_id_2);
    RAY_LOG(DEBUG) << "Server 2 NodeID=" << ClientID::FromBinary(data2->node_id());
    RAY_LOG(DEBUG) << "Server 2 NodeIp=" << data2->node_manager_address();
    RAY_LOG(DEBUG) << "Server 2 NodePort=" << data2->node_manager_port();
    ASSERT_EQ(node_id_2, ClientID::FromBinary(data2->node_id()));
  }
};

TEST_F(TestObjectManager, StartTestObjectManager) {
  // TODO: Break this test suite into unit tests.
  auto AsyncStartTests = main_service.wrap([this]() { WaitConnections(); });
  AsyncStartTests();
  main_service.run();
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  ray::TEST_STORE_EXEC_PATH = std::string(argv[1]);
  wait_timeout_ms = std::stoi(std::string(argv[2]));
  return RUN_ALL_TESTS();
}
