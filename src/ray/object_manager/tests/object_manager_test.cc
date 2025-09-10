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

#include <unistd.h>

#include <cstdlib>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "fakes/ray/object_manager/plasma/fake_plasma_client.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "mock/ray/gcs/gcs_client/gcs_client.h"
#include "mock/ray/object_manager/object_directory.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/buffer.h"
#include "ray/common/id.h"
#include "ray/common/ray_object.h"
#include "ray/common/status.h"
#include "ray/object_manager/common.h"
#include "ray/rpc/object_manager/fake_object_manager_client.h"
#include "ray/util/temporary_directory.h"

namespace ray {

using ::testing::_;
using ::testing::Invoke;
using ::testing::Return;

class ObjectManagerTest : public ::testing::Test {
 protected:
  ObjectManagerTest()
      : io_work_(boost::asio::make_work_guard(io_context_.get_executor())),
        rpc_work_(boost::asio::make_work_guard(rpc_context_.get_executor())) {
    ObjectManagerConfig config_;
    config_.object_manager_address = "127.0.0.1";
    config_.object_manager_port = 0;
    config_.store_socket_name = "test_store_socket";
    config_.rpc_service_threads_number = 1;

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
        [](const ObjectID &object_id) -> std::string { return ""; },
        // pin_object
        [](const ObjectID &object_id) -> std::unique_ptr<RayObject> { return nullptr; },
        // fail_pull_request
        [](const ObjectID &object_id, rpc::ErrorType error_type) {},
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

  NodeID local_node_id_;

  std::unique_ptr<gcs::MockGcsClient> mock_gcs_client_;
  std::unique_ptr<MockObjectDirectory> mock_object_directory_;
  std::unique_ptr<ObjectManager> object_manager_;
  std::shared_ptr<plasma::FakePlasmaClient> fake_plasma_client_;

  instrumented_io_context io_context_{/*enable_lag_probe=*/false,
                                      /*running_on_single_thread=*/true};
  instrumented_io_context rpc_context_{/*enable_lag_probe=*/false,
                                       /*running_on_single_thread=*/true};
  boost::asio::executor_work_guard<boost::asio::io_context::executor_type> io_work_;
  boost::asio::executor_work_guard<boost::asio::io_context::executor_type> rpc_work_;
};

TEST_F(ObjectManagerTest, TestHandleFreeObjectsIdempotent) {
  auto object_id = ObjectID::FromRandom();
  rpc::FreeObjectsRequest request;
  request.add_object_ids(object_id.Binary());
  fake_plasma_client_->objects_in_plasma_[object_id] =
      std::make_pair(std::vector<uint8_t>(1), std::vector<uint8_t>(1));
  ASSERT_TRUE(fake_plasma_client_->objects_in_plasma_.contains(object_id));
  ASSERT_EQ(fake_plasma_client_->objects_in_plasma_.size(), 1);
  rpc::FreeObjectsReply reply1;
  object_manager_->HandleFreeObjects(
      request,
      &reply1,
      [](Status status, std::function<void()> success, std::function<void()> failure) {
        EXPECT_TRUE(status.ok());
      });
  ASSERT_EQ(fake_plasma_client_->num_free_objects_requests, 1);
  ASSERT_TRUE(!fake_plasma_client_->objects_in_plasma_.contains(object_id));
  ASSERT_EQ(fake_plasma_client_->objects_in_plasma_.size(), 0);
  rpc::FreeObjectsReply reply2;
  object_manager_->HandleFreeObjects(
      request,
      &reply2,
      [](Status status, std::function<void()> success, std::function<void()> failure) {
        EXPECT_TRUE(status.ok());
      });
  ASSERT_EQ(fake_plasma_client_->num_free_objects_requests, 2);
  ASSERT_TRUE(!fake_plasma_client_->objects_in_plasma_.contains(object_id));
  ASSERT_EQ(fake_plasma_client_->objects_in_plasma_.size(), 0);
}

}  // namespace ray
