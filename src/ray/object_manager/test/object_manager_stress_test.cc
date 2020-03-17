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

#include <unistd.h>

#include <chrono>
#include <iostream>
#include <random>
#include <thread>

#include "gtest/gtest.h"
#include "ray/common/status.h"
#include "ray/object_manager/object_manager.h"
#include "ray/util/filesystem.h"

namespace ray {

using rpc::GcsNodeInfo;

std::string store_executable;

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

class MockServer {
 public:
  MockServer(boost::asio::io_service &main_service,
             const ObjectManagerConfig &object_manager_config,
             std::shared_ptr<gcs::GcsClient> gcs_client)
      : node_id_(ClientID::FromRandom()),
        config_(object_manager_config),
        gcs_client_(gcs_client),
        object_manager_(main_service, node_id_, object_manager_config,
                        std::make_shared<ObjectDirectory>(main_service, gcs_client_)) {
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
  std::shared_ptr<gcs::GcsClient> gcs_client_;
  ObjectManager object_manager_;
};

class TestObjectManagerBase : public ::testing::Test {
 public:
  TestObjectManagerBase() {
#ifdef _WIN32
    RAY_CHECK(false) << "port system() calls to Windows before running this test";
#endif
  }

  std::string StartStore(const std::string &id) {
    std::string store_id = ray::JoinPaths(ray::GetUserTempDir(), "store");
    store_id = store_id + id;
    std::string store_pid = store_id + ".pid";
    std::string plasma_command = store_executable + " -m 1000000000 -s " + store_id +
                                 " 1> /dev/null 2> /dev/null &" + " echo $! > " +
                                 store_pid;

    RAY_LOG(DEBUG) << plasma_command;
    int ec = system(plasma_command.c_str());
    RAY_CHECK(ec == 0);
    sleep(1);
    return store_id;
  }

  void StopStore(const std::string &store_id) {
    std::string store_pid = store_id + ".pid";
    std::string kill_1 = "kill -9 `cat " + store_pid + "`";
    int s = system(kill_1.c_str());
    ASSERT_TRUE(!s);
  }

  void SetUp() {
    flushall_redis();

    // start store
    store_id_1 = StartStore(UniqueID::FromRandom().Hex());
    store_id_2 = StartStore(UniqueID::FromRandom().Hex());

    unsigned int pull_timeout_ms = 1000;
    uint64_t object_chunk_size = static_cast<uint64_t>(std::pow(10, 3));
    int push_timeout_ms = 10000;

    // start first server
    gcs::GcsClientOptions client_options("127.0.0.1", 6379, /*password*/ "",
                                         /*is_test_client=*/true);
    gcs_client_1 = std::make_shared<gcs::RedisGcsClient>(client_options);
    RAY_CHECK_OK(gcs_client_1->Connect(main_service));
    ObjectManagerConfig om_config_1;
    om_config_1.store_socket_name = store_id_1;
    om_config_1.pull_timeout_ms = pull_timeout_ms;
    om_config_1.object_chunk_size = object_chunk_size;
    om_config_1.push_timeout_ms = push_timeout_ms;
    om_config_1.object_manager_port = 0;
    om_config_1.rpc_service_threads_number = 3;
    server1.reset(new MockServer(main_service, om_config_1, gcs_client_1));

    // start second server
    gcs_client_2 = std::make_shared<gcs::RedisGcsClient>(client_options);
    RAY_CHECK_OK(gcs_client_2->Connect(main_service));
    ObjectManagerConfig om_config_2;
    om_config_2.store_socket_name = store_id_2;
    om_config_2.pull_timeout_ms = pull_timeout_ms;
    om_config_2.object_chunk_size = object_chunk_size;
    om_config_2.push_timeout_ms = push_timeout_ms;
    om_config_2.object_manager_port = 0;
    om_config_2.rpc_service_threads_number = 3;
    server2.reset(new MockServer(main_service, om_config_2, gcs_client_2));

    // connect to stores.
    RAY_ARROW_CHECK_OK(client1.Connect(store_id_1));
    RAY_ARROW_CHECK_OK(client2.Connect(store_id_2));
  }

  void TearDown() {
    arrow::Status client1_status = client1.Disconnect();
    arrow::Status client2_status = client2.Disconnect();
    ASSERT_TRUE(client1_status.ok() && client2_status.ok());

    gcs_client_1->Disconnect();
    gcs_client_2->Disconnect();

    this->server1.reset();
    this->server2.reset();

    StopStore(store_id_1);
    StopStore(store_id_2);
  }

  ObjectID WriteDataToClient(plasma::PlasmaClient &client, int64_t data_size) {
    ObjectID object_id = ObjectID::FromRandom();
    RAY_LOG(DEBUG) << "ObjectID Created: " << object_id;
    uint8_t metadata[] = {5};
    int64_t metadata_size = sizeof(metadata);
    std::shared_ptr<Buffer> data;
    RAY_ARROW_CHECK_OK(
        client.Create(object_id.ToPlasmaId(), data_size, metadata, metadata_size, &data));
    RAY_ARROW_CHECK_OK(client.Seal(object_id.ToPlasmaId()));
    return object_id;
  }

  void object_added_handler_1(ObjectID object_id) { v1.push_back(object_id); };

  void object_added_handler_2(ObjectID object_id) { v2.push_back(object_id); };

 protected:
  std::thread p;
  boost::asio::io_service main_service;
  std::shared_ptr<gcs::GcsClient> gcs_client_1;
  std::shared_ptr<gcs::GcsClient> gcs_client_2;
  std::unique_ptr<MockServer> server1;
  std::unique_ptr<MockServer> server2;

  plasma::PlasmaClient client1;
  plasma::PlasmaClient client2;
  std::vector<ObjectID> v1;
  std::vector<ObjectID> v2;

  std::string store_id_1;
  std::string store_id_2;
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
    plasma::ObjectID plasma_id = object_id.ToPlasmaId();
    RAY_ARROW_CHECK_OK(client.Get(&plasma_id, 1, 0, &object_buffer));
    return object_buffer;
  }

  static unsigned char *GetDigest(plasma::PlasmaClient &client, ObjectID &object_id) {
    const int64_t size = sizeof(uint64_t);
    static unsigned char digest_1[size];
    RAY_ARROW_CHECK_OK(client.Hash(object_id.ToPlasmaId(), &digest_1[0]));
    return digest_1;
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

  void CompareHashes(ObjectID &object_id_1, ObjectID &object_id_2) {
    const int64_t size = sizeof(uint64_t);
    static unsigned char *digest_1 = GetDigest(client1, object_id_1);
    static unsigned char *digest_2 = GetDigest(client2, object_id_2);
    for (int i = -1; ++i < size;) {
      ASSERT_TRUE(digest_1[i] == digest_2[i]);
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
      CompareHashes(object_id_1, object_id_2);
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
  ray::store_executable = std::string(argv[1]);
  return RUN_ALL_TESTS();
}
