// Copyright 2025 The Ray Authors.
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

#include <boost/endian/conversion.hpp>
#include <fstream>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "mock/ray/gcs_client/gcs_client.h"
#include "mock/ray/object_manager/object_directory.h"
#include "ray/asio/instrumented_io_context.h"
#include "ray/common/id.h"
#include "ray/common/ray_config.h"
#include "ray/common/ray_object.h"
#include "ray/common/status.h"
#include "ray/object_manager/common.h"
#include "ray/object_manager/plasma/fake_plasma_client.h"
#include "ray/object_manager_rpc_client/fake_object_manager_client.h"
#include "ray/util/filesystem.h"
#include "ray/util/path_utils.h"

namespace ray {

using ::testing::_;
using ::testing::Invoke;
using ::testing::Return;

namespace {

// Serializes an object into the on-disk spill layout that SpilledObjectReader
// expects. Copied from ContructObjectString in spilled_object_test.cc.
std::string ContructObjectString(uint64_t object_offset,
                                 std::string data,
                                 std::string metadata,
                                 rpc::Address owner_address) {
  std::string result(object_offset, '\0');
  std::string address_str;
  owner_address.SerializeToString(&address_str);
  uint64_t address_size = boost::endian::native_to_little(address_str.size());
  uint64_t data_size = boost::endian::native_to_little(data.size());
  uint64_t metadata_size = boost::endian::native_to_little(metadata.size());

  result.append(reinterpret_cast<char *>(&address_size), 8);
  result.append(reinterpret_cast<char *>(&metadata_size), 8);
  result.append(reinterpret_cast<char *>(&data_size), 8);
  result.append(address_str);
  result.append(metadata);
  result.append(data);
  return result;
}

/// Writes a spill file to the local filesystem and returns the
/// "<path>?offset=&size=" URL that LocalObjectManager would hand out for it.
std::string WriteSpilledObject(const std::string &data,
                               const std::string &metadata,
                               const rpc::Address &owner_address) {
  auto contents =
      ContructObjectString(/*object_offset=*/0, data, metadata, owner_address);
  std::string path = ray::JoinPaths(ray::GetUserTempDir(),
                                    "object_manager_test" + ObjectID::FromRandom().Hex());
  std::ofstream f(path, std::ios::binary);
  RAY_CHECK(f.write(contents.c_str(), contents.size()));
  f.close();
  return path + "?offset=0&size=" + std::to_string(contents.size());
}

}  // namespace

class ObjectManagerTest : public ::testing::Test {
 protected:
  ObjectManagerTest()
      : io_work_(boost::asio::make_work_guard(io_context_.get_executor())),
        rpc_work_(boost::asio::make_work_guard(rpc_context_.get_executor())) {
    ObjectManagerConfig config_;
    config_.object_manager_address = "127.0.0.1";
    config_.object_manager_port = 0;
    config_.timer_freq_ms = RayConfig::instance().object_manager_timer_freq_ms();
    config_.pull_timeout_ms = RayConfig::instance().object_manager_pull_timeout_ms();
    config_.object_chunk_size = RayConfig::instance().object_manager_default_chunk_size();
    config_.max_bytes_in_flight =
        RayConfig::instance().object_manager_max_bytes_in_flight();
    config_.store_socket_name = "test_store_socket";
    config_.push_timeout_ms = RayConfig::instance().object_manager_push_timeout_ms();
    config_.rpc_service_threads_number = 1;
    config_.huge_pages = false;

    local_node_id_ = NodeID::FromRandom();
    mock_gcs_client_ = std::make_unique<gcs::MockGcsClient>();
    mock_object_directory_ = std::make_unique<MockObjectDirectory>();
    fake_plasma_client_ = std::make_shared<plasma::FakePlasmaClient>();

    object_manager_ = std::make_unique<ObjectManager>(
        io_context_,
        local_node_id_,
        config_,
        *mock_gcs_client_,
        mock_object_directory_.get(),
        // RestoreSpilledObjectCallback
        [](const ObjectID &object_id,
           int64_t object_size,
           const std::string &object_url,
           std::function<void(const Status &)> callback) {},
        // get_spilled_object_url
        [this](const ObjectID &object_id) -> std::string { return spilled_object_url_; },
        // pin_object
        [](const ObjectID &object_id) -> std::unique_ptr<RayObject> { return nullptr; },
        fake_plasma_client_,
        nullptr,
        [](const std::string &address,
           const int port,
           ray::rpc::ClientCallManager &client_call_manager) {
          return std::make_shared<ray::rpc::FakeObjectManagerClient>(
              address, port, client_call_manager);
        },
        rpc_context_);
  }

  void InstallPullPlaceholder(const ObjectID &object_id, int64_t size) {
    rpc::Address owner;
    ASSERT_TRUE(
        object_manager_->buffer_pool_.CreateChunk(object_id, owner, size, 0, 0).ok());
  }

  /// Delivers the store's "object added" notification, the only thing that
  /// populates ObjectManager's local_plasma_objects_ mirror of plasma. Never delivering
  /// the matching delete notification is what leaves the mirror stale.
  void NotifyObjectAdded(const ObjectID &object_id,
                         int64_t data_size,
                         int64_t metadata_size) {
    ObjectInfo object_info;
    object_info.object_id = object_id;
    object_info.data_size = data_size;
    object_info.metadata_size = metadata_size;
    object_info.owner_node_id = NodeID::FromRandom();
    object_info.owner_ip_address = "127.0.0.1";
    object_info.owner_port = 9999;
    object_info.owner_worker_id = WorkerID::FromRandom();
    EXPECT_CALL(*mock_object_directory_, ReportObjectAdded(object_id, _, _));
    object_manager_->HandleObjectAdded(object_info);
  }

  /// Makes GetRpcClient() able to resolve `node_id` to a FakeObjectManagerClient.
  void RegisterRemoteNode(const NodeID &node_id) {
    rpc::GcsNodeAddressAndLiveness node_info;
    node_info.set_node_id(node_id.Binary());
    node_info.set_node_manager_address("127.0.0.1");
    node_info.set_object_manager_port(8076);
    EXPECT_CALL(*mock_gcs_client_->mock_node_accessor,
                GetNodeAddressAndLiveness(node_id, _))
        .WillRepeatedly(Return(node_info));
  }

  /// Whether Push() took the queue-and-wait path for this object.
  bool HasQueuedPush(const ObjectID &object_id) const {
    return object_manager_->unfulfilled_push_requests_.contains(object_id);
  }

  rpc::FakeObjectManagerClient *GetRemoteClient(const NodeID &node_id) {
    auto it = object_manager_->remote_object_manager_clients_.find(node_id);
    if (it == object_manager_->remote_object_manager_clients_.end()) {
      return nullptr;
    }
    return static_cast<rpc::FakeObjectManagerClient *>(it->second.get());
  }

  /// Drains the two services the spill-file push path hops between: the rpc
  /// service does the (blocking) file read and the chunk sends, the main service
  /// starts the push.
  void DrainServices() {
    for (int i = 0; i < 3; i++) {
      rpc_context_.poll();
      io_context_.poll();
    }
  }

  NodeID local_node_id_;

  /// Stands in for LocalObjectManager's spilled-object table: the URL that
  /// Push() sees. Empty means the object was never spilled.
  std::string spilled_object_url_;

  instrumented_io_context io_context_{/*enable_lag_probe=*/false,
                                      /*running_on_single_thread=*/true};
  instrumented_io_context rpc_context_{/*enable_lag_probe=*/false,
                                       /*running_on_single_thread=*/true};
  boost::asio::executor_work_guard<boost::asio::io_context::executor_type> io_work_;
  boost::asio::executor_work_guard<boost::asio::io_context::executor_type> rpc_work_;

  std::unique_ptr<gcs::MockGcsClient> mock_gcs_client_;
  std::unique_ptr<MockObjectDirectory> mock_object_directory_;
  std::unique_ptr<ObjectManager> object_manager_;
  std::shared_ptr<plasma::FakePlasmaClient> fake_plasma_client_;
};

TEST_F(ObjectManagerTest, MarkObjectFailedReleasesPlaceholderAndWritesSentinel) {
  // While a pull is in flight, we put an unsealed buffer at that
  // ObjectID slot in plasma (so we can stream chunks into it). If the
  // pull fails (e.g., the owner dies or it times out), we must release
  // the slot before writing the error sentinel; otherwise the write
  // collides with the slot and we pull forever.
  ObjectID id = ObjectID::FromRandom();
  InstallPullPlaceholder(id, 100);
  ASSERT_TRUE(fake_plasma_client_->objects_in_plasma_.contains(id));
  ASSERT_TRUE(fake_plasma_client_->objects_in_plasma_[id].second.empty());

  object_manager_->MarkObjectFailed(id, rpc::ErrorType::OWNER_DIED);

  ASSERT_TRUE(fake_plasma_client_->objects_in_plasma_.contains(id));
  std::string expected_meta =
      std::to_string(static_cast<int>(rpc::ErrorType::OWNER_DIED));
  const auto &actual_meta = fake_plasma_client_->objects_in_plasma_[id].second;
  EXPECT_EQ(std::string(actual_meta.begin(), actual_meta.end()), expected_meta);
}

/// Push() when local_plasma_objects_ -- ObjectManager's mirror of plasma -- is stale:
/// the object was spilled and its plasma copy evicted, but the mirror hasn't
/// processed the store's delete notification yet, so it still claims the object
/// is in plasma. A pull landing in that window must not trust the mirror.
class StaleMirrorPushTest : public ObjectManagerTest {
 protected:
  void SetUp() override {
    // The mirror says resident, but the fake store is empty, so the read fails.
    NotifyObjectAdded(object_id_, data_.size(), metadata_.size());
    ASSERT_FALSE(fake_plasma_client_->objects_in_plasma_.contains(object_id_));
    RegisterRemoteNode(remote_node_id_);
  }

  const ObjectID object_id_ = ObjectID::FromRandom();
  const NodeID remote_node_id_ = NodeID::FromRandom();
  const std::string data_ = "spilled-object-data";
  const std::string metadata_ = "meta";
};

TEST_F(StaleMirrorPushTest, ServesSpilledCopy) {
  // The spilled copy is on the local filesystem, so serve the push from there
  // instead of dropping it (which strands the puller until its pull timeout).
  spilled_object_url_ = WriteSpilledObject(data_, metadata_, rpc::Address());

  object_manager_->Push(object_id_, remote_node_id_);
  DrainServices();

  // Plasma is empty, so the spill file was the only possible source.
  auto *remote_client = GetRemoteClient(remote_node_id_);
  ASSERT_NE(remote_client, nullptr);
  EXPECT_EQ(remote_client->num_push_requests, 1);
  // And we did not fall through to the queue-and-wait path.
  EXPECT_FALSE(HasQueuedPush(object_id_));
}

TEST_F(StaleMirrorPushTest, DropsPushWhenSpillFileIsDeleted) {
  // The spill URL can be stale too: it can name a file that was already
  // deleted. The read fails off the main thread and the push is dropped, same
  // as for any other already-deleted object.
  spilled_object_url_ =
      ray::JoinPaths(ray::GetUserTempDir(), "object_manager_test_deleted_spill_file") +
      "?offset=0&size=100";

  object_manager_->Push(object_id_, remote_node_id_);
  DrainServices();

  EXPECT_EQ(GetRemoteClient(remote_node_id_), nullptr);
  EXPECT_FALSE(HasQueuedPush(object_id_));
}

TEST_F(StaleMirrorPushTest, DropsPushWhenObjectIsDeleted) {
  // No plasma copy and no spill copy at all: the object is gone, so drop the push
  // rather than arming a push timer for something that won't reappear.
  ASSERT_TRUE(spilled_object_url_.empty());

  object_manager_->Push(object_id_, remote_node_id_);
  DrainServices();

  EXPECT_EQ(GetRemoteClient(remote_node_id_), nullptr);
  EXPECT_FALSE(HasQueuedPush(object_id_));
}

}  // namespace ray
