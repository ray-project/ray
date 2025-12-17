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

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "ray/common/asio/asio_util.h"
#include "ray/common/id.h"
#include "ray/common/ray_config.h"
#include "ray/raylet_rpc_client/raylet_client.h"
#include "ray/rpc/client_call.h"
#include "ray/rpc/grpc_server.h"
#include "src/ray/protobuf/node_manager.grpc.pb.h"
#include "src/ray/protobuf/node_manager.pb.h"

namespace ray {
namespace rpc {

/// Mock NodeManager service handler for testing
/// Note: Must be named NodeManagerServiceHandler to work with
/// RPC_SERVICE_HANDLER_CUSTOM_AUTH macro
class NodeManagerServiceHandler {
 public:
  virtual ~NodeManagerServiceHandler() = default;

  virtual void HandlePushMutableObject(rpc::PushMutableObjectRequest request,
                                       rpc::PushMutableObjectReply *reply,
                                       SendReplyCallback send_reply_callback) {
    std::lock_guard<std::mutex> lock(mutex_);

    std::string object_id = request.writer_object_id();
    uint64_t offset = request.offset();
    uint64_t chunk_size = request.chunk_size();
    uint64_t total_data_size = request.total_data_size();

    total_requests_++;

    RAY_LOG(INFO) << "Received chunk for object " << ObjectID::FromBinary(object_id).Hex()
                  << " offset=" << offset << " chunk_size=" << chunk_size
                  << " total_size=" << total_data_size << " (request #" << total_requests_
                  << ")";

    // Simulate retry behavior: fail first N requests if configured
    if (fail_first_n_requests_ > 0 && total_requests_ <= fail_first_n_requests_) {
      RAY_LOG(INFO) << "Failing request " << total_requests_
                    << " with UNAVAILABLE (fail_first_n_requests_="
                    << fail_first_n_requests_ << ")";
      failed_requests_++;
      // Return RpcError with UNAVAILABLE to trigger retries
      send_reply_callback(
          ray::Status::RpcError("Simulated transient failure - server unavailable",
                                grpc::StatusCode::UNAVAILABLE),
          /*reply_success=*/[]() {},
          /*reply_failure=*/[]() {});
      return;
    }

    // Simulate fault tolerance: fail chunks based on pattern if configured
    if (fail_chunk_pattern_ > 0) {
      // Track chunk attempts
      std::string chunk_key = object_id + "_" + std::to_string(offset);
      chunk_attempts_[chunk_key]++;
      int attempt = chunk_attempts_[chunk_key];

      RAY_LOG(INFO) << "Chunk at offset " << offset << ", attempt " << attempt;

      // Fail non-first chunks on first attempt (pattern 1)
      if (fail_chunk_pattern_ == 1 && offset > 0 && attempt == 1) {
        RAY_LOG(INFO) << "Failing chunk at offset " << offset
                      << " (first attempt, pattern 1)";
        failed_requests_++;
        // Return RpcError with UNAVAILABLE to trigger retries
        send_reply_callback(
            ray::Status::RpcError("Simulated chunk failure - network unavailable",
                                  grpc::StatusCode::UNAVAILABLE),
            /*reply_success=*/[]() {},
            /*reply_failure=*/[]() {});
        return;
      }
    }

    // Initialize object state if first chunk
    if (objects_.find(object_id) == objects_.end()) {
      ObjectState state;
      state.total_data_size = total_data_size;
      state.total_metadata_size = request.total_metadata_size();
      state.data.resize(total_data_size);
      state.metadata = request.metadata();
      state.chunks_received = 0;
      objects_[object_id] = state;
    }

    // Copy chunk data
    ObjectState &state = objects_[object_id];
    const std::string &chunk_data = request.data();
    std::memcpy(state.data.data() + offset, chunk_data.data(), chunk_size);
    state.chunks_received++;
    state.bytes_received += chunk_size;

    // Check if all chunks received
    bool done = (state.bytes_received >= state.total_data_size);

    RAY_LOG(INFO) << "Object " << ObjectID::FromBinary(object_id).Hex()
                  << " bytes_received=" << state.bytes_received << "/"
                  << state.total_data_size << " done=" << done;

    reply->set_done(done);

    if (done) {
      completed_objects_.push_back(object_id);
    }

    send_reply_callback(
        ray::Status::OK(),
        /*reply_success=*/
        [object_id]() {
          RAY_LOG(INFO) << "Reply sent for object "
                        << ObjectID::FromBinary(object_id).Hex();
        },
        /*reply_failure=*/
        [this, object_id]() {
          RAY_LOG(WARNING) << "Reply failed for object "
                           << ObjectID::FromBinary(object_id).Hex();
          reply_failure_count_++;
        });
  }

  struct ObjectState {
    uint64_t total_data_size;
    uint64_t total_metadata_size;
    std::vector<uint8_t> data;
    std::string metadata;
    int chunks_received;
    uint64_t bytes_received = 0;
  };

  std::atomic<int> total_requests_{0};
  std::atomic<int> reply_failure_count_{0};
  std::atomic<int> failed_requests_{0};
  std::atomic<int> fail_first_n_requests_{0};  // Config: fail first N requests
  std::atomic<int> fail_chunk_pattern_{0};     // Config: chunk failure pattern
  std::map<std::string, ObjectState> objects_;
  std::map<std::string, int> chunk_attempts_;  // Track attempts per chunk
  std::vector<std::string> completed_objects_;
  std::mutex mutex_;
};

/// Mock NodeManager gRPC service
class MockNodeManagerService : public GrpcService {
 public:
  explicit MockNodeManagerService(instrumented_io_context &handler_io_service,
                                  NodeManagerServiceHandler &handler)
      : GrpcService(handler_io_service), service_handler_(handler) {}

 protected:
  grpc::Service &GetGrpcService() override { return service_; }

  void InitServerCallFactories(
      const std::unique_ptr<grpc::ServerCompletionQueue> &cq,
      std::vector<std::unique_ptr<ServerCallFactory>> *server_call_factories,
      const ClusterID &cluster_id,
      const std::optional<AuthenticationToken> &auth_token) override {
    RPC_SERVICE_HANDLER_CUSTOM_AUTH(NodeManagerService,
                                    PushMutableObject,
                                    /*max_active_rpcs=*/-1,
                                    ClusterIdAuthType::NO_AUTH);
  }

 private:
  NodeManagerService::AsyncService service_;
  NodeManagerServiceHandler &service_handler_;
};

/// Integration test fixture with real gRPC server
class PushMutableObjectTest : public ::testing::Test {
 public:
  void SetUp() override {
    // Start handler thread for server
    handler_thread_ = std::make_unique<std::thread>([this]() {
      boost::asio::executor_work_guard<boost::asio::io_context::executor_type>
          handler_io_service_work_(handler_io_service_.get_executor());
      handler_io_service_.run();
    });

    // Create and start gRPC server
    grpc_server_ = std::make_unique<GrpcServer>("test-raylet", 0, true);
    grpc_server_->RegisterService(std::make_unique<MockNodeManagerService>(
                                      handler_io_service_, mock_service_handler_),
                                  false);
    grpc_server_->Run();

    // Wait for server to start
    while (grpc_server_->GetPort() == 0) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    RAY_LOG(INFO) << "Test server started on port " << grpc_server_->GetPort();

    // Start client thread
    client_thread_ = std::make_unique<std::thread>([this]() {
      boost::asio::executor_work_guard<boost::asio::io_context::executor_type>
          client_io_service_work_(client_io_service_.get_executor());
      client_io_service_.run();
    });

    // Create client
    client_call_manager_ = std::make_unique<ClientCallManager>(
        client_io_service_, /*record_stats=*/false, /*local_address=*/"127.0.0.1");

    test_address_.set_ip_address("127.0.0.1");
    test_address_.set_port(grpc_server_->GetPort());

    raylet_unavailable_callback_ = []() {
      RAY_LOG(WARNING) << "Raylet unavailable callback invoked";
    };

    raylet_client_ = std::make_unique<RayletClient>(
        test_address_, *client_call_manager_, raylet_unavailable_callback_);
  }

  void TearDown() override {
    // Cleanup client
    raylet_client_.reset();
    client_call_manager_.reset();
    client_io_service_.stop();
    if (client_thread_ && client_thread_->joinable()) {
      client_thread_->join();
    }

    // Cleanup server
    grpc_server_->Shutdown();
    handler_io_service_.stop();
    if (handler_thread_ && handler_thread_->joinable()) {
      handler_thread_->join();
    }
  }

 protected:
  // Server side
  NodeManagerServiceHandler mock_service_handler_;
  instrumented_io_context handler_io_service_;
  std::unique_ptr<std::thread> handler_thread_;
  std::unique_ptr<GrpcServer> grpc_server_;

  // Client side
  instrumented_io_context client_io_service_;
  std::unique_ptr<std::thread> client_thread_;
  std::unique_ptr<ClientCallManager> client_call_manager_;
  rpc::Address test_address_;
  std::function<void()> raylet_unavailable_callback_;
  std::unique_ptr<RayletClient> raylet_client_;
};

// Test successful single chunk transfer
TEST_F(PushMutableObjectTest, TestSingleChunkTransferSuccess) {
  ObjectID writer_object_id = ObjectID::FromRandom();

  uint64_t data_size = 1024;  // Small data, single chunk
  uint64_t metadata_size = 100;

  std::vector<uint8_t> data(data_size);
  std::vector<uint8_t> metadata(metadata_size);

  // Fill with recognizable pattern
  for (size_t i = 0; i < data_size; i++) {
    data[i] = static_cast<uint8_t>(i % 256);
  }
  for (size_t i = 0; i < metadata_size; i++) {
    metadata[i] = static_cast<uint8_t>(0xCD);
  }

  bool callback_called = false;
  Status callback_status;
  rpc::PushMutableObjectReply callback_reply;

  auto callback = [&callback_called, &callback_status, &callback_reply](
                      const Status &status, rpc::PushMutableObjectReply &&reply) {
    callback_called = true;
    callback_status = status;
    callback_reply = std::move(reply);
  };

  raylet_client_->PushMutableObject(
      writer_object_id, data_size, metadata_size, data.data(), metadata.data(), callback);

  // Wait for callback
  auto start_time = std::chrono::steady_clock::now();
  while (!callback_called &&
         (std::chrono::steady_clock::now() - start_time) < std::chrono::seconds(5)) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  // Verify success
  ASSERT_TRUE(callback_called) << "Callback should be called";
  EXPECT_TRUE(callback_status.ok()) << "Status should be OK: " << callback_status;
  EXPECT_TRUE(callback_reply.done()) << "Reply should indicate done";

  // Verify server received the data
  std::lock_guard<std::mutex> lock(mock_service_handler_.mutex_);
  ASSERT_EQ(mock_service_handler_.completed_objects_.size(), 1);

  std::string object_id_binary = writer_object_id.Binary();
  ASSERT_TRUE(mock_service_handler_.objects_.find(object_id_binary) !=
              mock_service_handler_.objects_.end());

  const auto &received_object = mock_service_handler_.objects_[object_id_binary];
  EXPECT_EQ(received_object.bytes_received, data_size);
  EXPECT_EQ(received_object.data.size(), data_size);

  // Verify data integrity
  EXPECT_EQ(std::memcmp(received_object.data.data(), data.data(), data_size), 0)
      << "Received data should match sent data";
}

// Test successful multi-chunk transfer (large object)
TEST_F(PushMutableObjectTest, TestMultiChunkTransferSuccess) {
  ObjectID writer_object_id = ObjectID::FromRandom();

  // Create data larger than max chunk size to force chunking
  uint64_t data_size = RayConfig::instance().max_grpc_message_size() * 2;
  uint64_t metadata_size = 200;

  std::vector<uint8_t> data(data_size);
  std::vector<uint8_t> metadata(metadata_size, 0xAB);

  // Fill with recognizable pattern
  for (size_t i = 0; i < data_size; i++) {
    data[i] = static_cast<uint8_t>((i / 1024) % 256);
  }

  bool callback_called = false;
  Status callback_status;
  rpc::PushMutableObjectReply callback_reply;

  auto callback = [&callback_called, &callback_status, &callback_reply](
                      const Status &status, rpc::PushMutableObjectReply &&reply) {
    callback_called = true;
    callback_status = status;
    callback_reply = std::move(reply);
  };

  raylet_client_->PushMutableObject(
      writer_object_id, data_size, metadata_size, data.data(), metadata.data(), callback);

  // Wait for callback (may take longer for large object)
  auto start_time = std::chrono::steady_clock::now();
  while (!callback_called &&
         (std::chrono::steady_clock::now() - start_time) < std::chrono::seconds(30)) {
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }

  // Verify success
  ASSERT_TRUE(callback_called) << "Callback should be called";
  EXPECT_TRUE(callback_status.ok()) << "Status should be OK: " << callback_status;
  EXPECT_TRUE(callback_reply.done()) << "Reply should indicate done";

  // Verify server received all chunks
  std::lock_guard<std::mutex> lock(mock_service_handler_.mutex_);
  ASSERT_EQ(mock_service_handler_.completed_objects_.size(), 1);

  std::string object_id_binary = writer_object_id.Binary();
  ASSERT_TRUE(mock_service_handler_.objects_.find(object_id_binary) !=
              mock_service_handler_.objects_.end());

  const auto &received_object = mock_service_handler_.objects_[object_id_binary];
  EXPECT_EQ(received_object.bytes_received, data_size);
  EXPECT_GE(received_object.chunks_received, 2) << "Should have received multiple chunks";

  // Verify data integrity across all chunks
  EXPECT_EQ(std::memcmp(received_object.data.data(), data.data(), data_size), 0)
      << "Received data should match sent data across all chunks";

  RAY_LOG(INFO) << "Successfully transferred " << data_size << " bytes in "
                << received_object.chunks_received << " chunks";
}

// Test that callback is only called once even with multiple chunks
TEST_F(PushMutableObjectTest, TestCallbackCalledOnlyOnce) {
  ObjectID writer_object_id = ObjectID::FromRandom();

  uint64_t data_size = RayConfig::instance().max_grpc_message_size() * 1.5;
  uint64_t metadata_size = 50;

  std::vector<uint8_t> data(data_size, 0xFF);
  std::vector<uint8_t> metadata(metadata_size, 0xEE);

  std::atomic<int> callback_count{0};
  bool callback_called = false;
  Status callback_status;

  auto callback = [&callback_count, &callback_called, &callback_status](
                      const Status &status, rpc::PushMutableObjectReply &&reply) {
    callback_count++;
    callback_called = true;
    callback_status = status;
  };

  raylet_client_->PushMutableObject(
      writer_object_id, data_size, metadata_size, data.data(), metadata.data(), callback);

  // Wait for completion
  auto start_time = std::chrono::steady_clock::now();
  while (!callback_called &&
         (std::chrono::steady_clock::now() - start_time) < std::chrono::seconds(30)) {
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }

  // Give it extra time to catch any duplicate callbacks
  std::this_thread::sleep_for(std::chrono::milliseconds(500));

  ASSERT_TRUE(callback_called) << "Callback should be called";
  EXPECT_EQ(callback_count.load(), 1) << "Callback should be called exactly once";
  EXPECT_TRUE(callback_status.ok()) << "Status should be OK";
}

// Test that retry actually happens by temporarily making server unavailable
TEST_F(PushMutableObjectTest, TestRetryHappens) {
  ObjectID writer_object_id = ObjectID::FromRandom();

  // Use multi-chunk transfer to have time to interrupt it
  uint64_t data_size = RayConfig::instance().max_grpc_message_size() * 1.5;
  uint64_t metadata_size = 50;
  std::vector<uint8_t> data(data_size, 0xAA);
  std::vector<uint8_t> metadata(metadata_size, 0xBB);

  bool callback_called = false;
  Status callback_status;
  rpc::PushMutableObjectReply callback_reply;

  auto callback = [&callback_called, &callback_status, &callback_reply](
                      const Status &status, rpc::PushMutableObjectReply &&reply) {
    callback_called = true;
    callback_status = status;
    callback_reply = std::move(reply);
    RAY_LOG(INFO) << "Client callback invoked with status: " << status
                  << ", done=" << reply.done();
  };

  // Shut down server BEFORE starting transfer to force initial failure
  int old_port = grpc_server_->GetPort();
  RAY_LOG(INFO) << "Shutting down server before transfer to force retry";
  grpc_server_->Shutdown();

  // Start the transfer (will fail initially due to UNAVAILABLE)
  raylet_client_->PushMutableObject(
      writer_object_id, data_size, metadata_size, data.data(), metadata.data(), callback);

  // Wait a bit to let initial attempts fail
  std::this_thread::sleep_for(std::chrono::milliseconds(500));

  // Track requests before restart
  int requests_before = mock_service_handler_.total_requests_.load();
  RAY_LOG(INFO) << "Requests before restart: " << requests_before;

  // Restart the server - this allows retries to succeed
  RAY_LOG(INFO) << "Restarting server to allow retry to succeed";
  grpc_server_ = std::make_unique<GrpcServer>("test-raylet", old_port, true);
  grpc_server_->RegisterService(std::make_unique<MockNodeManagerService>(
                                    handler_io_service_, mock_service_handler_),
                                false);
  grpc_server_->Run();

  // Wait for server to be ready
  while (grpc_server_->GetPort() == 0) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  RAY_LOG(INFO) << "Server restarted on port " << grpc_server_->GetPort();

  // Wait for completion (the client should retry and succeed)
  auto start_time = std::chrono::steady_clock::now();
  while (!callback_called &&
         (std::chrono::steady_clock::now() - start_time) < std::chrono::seconds(15)) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  ASSERT_TRUE(callback_called) << "Callback should be called after server recovery";
  EXPECT_TRUE(callback_status.ok())
      << "Status should be OK after retry: " << callback_status;
  EXPECT_TRUE(callback_reply.done()) << "Transfer should complete after retry";

  // Verify the data was eventually received after retry
  std::lock_guard<std::mutex> lock(mock_service_handler_.mutex_);
  ASSERT_EQ(mock_service_handler_.completed_objects_.size(), 1);

  int requests_after = mock_service_handler_.total_requests_.load();
  RAY_LOG(INFO) << "Requests after restart: " << requests_after;
  RAY_LOG(INFO) << "Successfully completed transfer after server restart - retry worked! "
                << "Server processed " << requests_after << " requests";
}

}  // namespace rpc
}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
