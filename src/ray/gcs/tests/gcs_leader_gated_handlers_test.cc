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

#include "ray/gcs/gcs_leader_gated_handlers.h"

#include <memory>
#include <string>

#include "gtest/gtest.h"

namespace ray {
namespace gcs {

class MockJobInfoGcsServiceHandler : public rpc::JobInfoGcsServiceHandler {
 public:
  void HandleAddJob(rpc::AddJobRequest request,
                    rpc::AddJobReply *reply,
                    rpc::SendReplyCallback send_reply_callback) override {
    called_ = true;
    send_reply_callback(Status::OK(), nullptr, nullptr);
  }

  void HandleMarkJobFinished(rpc::MarkJobFinishedRequest request,
                             rpc::MarkJobFinishedReply *reply,
                             rpc::SendReplyCallback send_reply_callback) override {
    called_ = true;
    send_reply_callback(Status::OK(), nullptr, nullptr);
  }

  void HandleGetAllJobInfo(rpc::GetAllJobInfoRequest request,
                           rpc::GetAllJobInfoReply *reply,
                           rpc::SendReplyCallback send_reply_callback) override {
    called_ = true;
    send_reply_callback(Status::OK(), nullptr, nullptr);
  }

  void AddJobFinishedListener(JobFinishListenerCallback listener) override {}

  void HandleGetNextJobID(rpc::GetNextJobIDRequest request,
                          rpc::GetNextJobIDReply *reply,
                          rpc::SendReplyCallback send_reply_callback) override {
    called_ = true;
    send_reply_callback(Status::OK(), nullptr, nullptr);
  }

  bool called_ = false;
};

TEST(GcsLeaderGatedHandlersTest, TestJobGating) {
  MockJobInfoGcsServiceHandler underlying;
  bool is_leader = false;
  auto is_leader_fn = [&is_leader]() { return is_leader; };

  LeaderGatedJobInfoHandler proxy(underlying, is_leader_fn);

  // 1. In passive mode (is_leader = false), mutating RPCs must be rejected with
  // GcsPassive.
  {
    rpc::AddJobRequest request;
    rpc::AddJobReply reply;
    bool callback_called = false;
    auto send_reply_callback = [&callback_called, &reply](Status status,
                                                          std::function<void()> f1,
                                                          std::function<void()> f2) {
      EXPECT_TRUE(status.ok());
      Status logical_status =
          Status(StatusCode(reply.status().code()), reply.status().message());
      EXPECT_TRUE(logical_status.IsGcsPassive());
      callback_called = true;
    };
    proxy.HandleAddJob(request, &reply, send_reply_callback);
    EXPECT_TRUE(callback_called);
    EXPECT_FALSE(underlying.called_);
  }

  // 2. In passive mode, read-only RPCs must be forwarded directly.
  {
    rpc::GetAllJobInfoRequest request;
    rpc::GetAllJobInfoReply reply;
    bool callback_called = false;
    auto send_reply_callback = [&callback_called](Status status,
                                                  std::function<void()> f1,
                                                  std::function<void()> f2) {
      EXPECT_TRUE(status.ok());
      callback_called = true;
    };
    proxy.HandleGetAllJobInfo(request, &reply, send_reply_callback);
    EXPECT_TRUE(callback_called);
    EXPECT_TRUE(underlying.called_);
    underlying.called_ = false;
  }

  // 3. In leader mode (is_leader = true), mutating RPCs must be forwarded directly.
  {
    is_leader = true;
    rpc::AddJobRequest request;
    rpc::AddJobReply reply;
    bool callback_called = false;
    auto send_reply_callback = [&callback_called](Status status,
                                                  std::function<void()> f1,
                                                  std::function<void()> f2) {
      EXPECT_TRUE(status.ok());
      callback_called = true;
    };
    proxy.HandleAddJob(request, &reply, send_reply_callback);
    EXPECT_TRUE(callback_called);
    EXPECT_TRUE(underlying.called_);
  }
}

class MockInternalKVGcsServiceHandler : public rpc::InternalKVGcsServiceHandler {
 public:
  void HandleInternalKVKeys(rpc::InternalKVKeysRequest request,
                            rpc::InternalKVKeysReply *reply,
                            rpc::SendReplyCallback send_reply_callback) override {
    called_ = true;
    send_reply_callback(Status::OK(), nullptr, nullptr);
  }

  void HandleInternalKVGet(rpc::InternalKVGetRequest request,
                           rpc::InternalKVGetReply *reply,
                           rpc::SendReplyCallback send_reply_callback) override {
    called_ = true;
    send_reply_callback(Status::OK(), nullptr, nullptr);
  }

  void HandleInternalKVMultiGet(rpc::InternalKVMultiGetRequest request,
                                rpc::InternalKVMultiGetReply *reply,
                                rpc::SendReplyCallback send_reply_callback) override {
    called_ = true;
    send_reply_callback(Status::OK(), nullptr, nullptr);
  }

  void HandleInternalKVPut(rpc::InternalKVPutRequest request,
                           rpc::InternalKVPutReply *reply,
                           rpc::SendReplyCallback send_reply_callback) override {
    called_ = true;
    send_reply_callback(Status::OK(), nullptr, nullptr);
  }

  void HandleInternalKVDel(rpc::InternalKVDelRequest request,
                           rpc::InternalKVDelReply *reply,
                           rpc::SendReplyCallback send_reply_callback) override {
    called_ = true;
    send_reply_callback(Status::OK(), nullptr, nullptr);
  }

  void HandleInternalKVExists(rpc::InternalKVExistsRequest request,
                              rpc::InternalKVExistsReply *reply,
                              rpc::SendReplyCallback send_reply_callback) override {
    called_ = true;
    send_reply_callback(Status::OK(), nullptr, nullptr);
  }

  void HandleGetInternalConfig(rpc::GetInternalConfigRequest request,
                               rpc::GetInternalConfigReply *reply,
                               rpc::SendReplyCallback send_reply_callback) override {
    called_ = true;
    send_reply_callback(Status::OK(), nullptr, nullptr);
  }

  bool called_ = false;
};

TEST(GcsLeaderGatedHandlersTest, TestKVGating) {
  MockInternalKVGcsServiceHandler underlying;
  bool is_leader = false;
  auto is_leader_fn = [&is_leader]() { return is_leader; };

  LeaderGatedInternalKVHandler proxy(underlying, is_leader_fn);

  // 1. In passive mode (is_leader = false), mutating KV Put must be rejected.
  {
    rpc::InternalKVPutRequest request;
    rpc::InternalKVPutReply reply;
    bool callback_called = false;
    auto send_reply_callback = [&callback_called, &reply](Status status,
                                                          std::function<void()> f1,
                                                          std::function<void()> f2) {
      EXPECT_TRUE(status.ok());
      Status logical_status =
          Status(StatusCode(reply.status().code()), reply.status().message());
      EXPECT_TRUE(logical_status.IsGcsPassive());
      callback_called = true;
    };
    proxy.HandleInternalKVPut(request, &reply, send_reply_callback);
    EXPECT_TRUE(callback_called);
    EXPECT_FALSE(underlying.called_);
  }

  // 2. In passive mode, read-only KV Get must be forwarded.
  {
    rpc::InternalKVGetRequest request;
    rpc::InternalKVGetReply reply;
    bool callback_called = false;
    auto send_reply_callback = [&callback_called](Status status,
                                                  std::function<void()> f1,
                                                  std::function<void()> f2) {
      EXPECT_TRUE(status.ok());
      callback_called = true;
    };
    proxy.HandleInternalKVGet(request, &reply, send_reply_callback);
    EXPECT_TRUE(callback_called);
    EXPECT_TRUE(underlying.called_);
    underlying.called_ = false;
  }
}

class MockNodeInfoGcsServiceHandler : public rpc::NodeInfoGcsServiceHandler {
 public:
  void HandleGetClusterId(rpc::GetClusterIdRequest request,
                          rpc::GetClusterIdReply *reply,
                          rpc::SendReplyCallback send_reply_callback) override {
    called_ = true;
    send_reply_callback(Status::OK(), nullptr, nullptr);
  }

  void HandleRegisterNode(rpc::RegisterNodeRequest request,
                          rpc::RegisterNodeReply *reply,
                          rpc::SendReplyCallback send_reply_callback) override {
    called_ = true;
    send_reply_callback(Status::OK(), nullptr, nullptr);
  }

  void HandleUnregisterNode(rpc::UnregisterNodeRequest request,
                            rpc::UnregisterNodeReply *reply,
                            rpc::SendReplyCallback send_reply_callback,
                            const std::string &grpc_peer) override {
    called_ = true;
    send_reply_callback(Status::OK(), nullptr, nullptr);
  }

  void HandleCheckAlive(rpc::CheckAliveRequest request,
                        rpc::CheckAliveReply *reply,
                        rpc::SendReplyCallback send_reply_callback) override {
    called_ = true;
    send_reply_callback(Status::OK(), nullptr, nullptr);
  }

  void HandleDrainNode(rpc::DrainNodeRequest request,
                       rpc::DrainNodeReply *reply,
                       rpc::SendReplyCallback send_reply_callback) override {
    called_ = true;
    send_reply_callback(Status::OK(), nullptr, nullptr);
  }

  void HandleGetAllNodeInfo(rpc::GetAllNodeInfoRequest request,
                            rpc::GetAllNodeInfoReply *reply,
                            rpc::SendReplyCallback send_reply_callback) override {
    called_ = true;
    send_reply_callback(Status::OK(), nullptr, nullptr);
  }

  void HandleGetAllNodeAddressAndLiveness(
      rpc::GetAllNodeAddressAndLivenessRequest request,
      rpc::GetAllNodeAddressAndLivenessReply *reply,
      rpc::SendReplyCallback send_reply_callback) override {
    called_ = true;
    send_reply_callback(Status::OK(), nullptr, nullptr);
  }

  bool called_ = false;
};

TEST(GcsLeaderGatedHandlersTest, TestNodeRegistrationBypass) {
  MockNodeInfoGcsServiceHandler underlying;
  bool is_leader = false;
  auto is_leader_fn = [&is_leader]() { return is_leader; };

  LeaderGatedNodeInfoHandler proxy(underlying, is_leader_fn);

  // 1. Passive GCS (is_leader = false): RegisterNode with is_head_node = true must be
  // ALLOWED.
  {
    rpc::RegisterNodeRequest request;
    request.mutable_node_info()->set_is_head_node(true);
    rpc::RegisterNodeReply reply;
    bool callback_called = false;
    auto send_reply_callback = [&callback_called](Status status,
                                                  std::function<void()> f1,
                                                  std::function<void()> f2) {
      EXPECT_TRUE(status.ok());
      callback_called = true;
    };
    proxy.HandleRegisterNode(request, &reply, send_reply_callback);
    EXPECT_TRUE(callback_called);
    EXPECT_TRUE(underlying.called_);
    underlying.called_ = false;
  }

  // 2. Passive GCS: RegisterNode with is_head_node = false must be BLOCKED.
  {
    rpc::RegisterNodeRequest request;
    request.mutable_node_info()->set_is_head_node(false);
    rpc::RegisterNodeReply reply;
    bool callback_called = false;
    auto send_reply_callback = [&callback_called, &reply](Status status,
                                                          std::function<void()> f1,
                                                          std::function<void()> f2) {
      EXPECT_TRUE(status.ok());
      Status logical_status =
          Status(StatusCode(reply.status().code()), reply.status().message());
      EXPECT_TRUE(logical_status.IsGcsPassive());
      callback_called = true;
    };
    proxy.HandleRegisterNode(request, &reply, send_reply_callback);
    EXPECT_TRUE(callback_called);
    EXPECT_FALSE(underlying.called_);
    underlying.called_ = false;
  }

  // 3. Passive GCS: RegisterNode with remote node IP must be BLOCKED.
  {
    rpc::RegisterNodeRequest request;
    request.mutable_node_info()->set_node_manager_address("10.244.10.20");
    rpc::RegisterNodeReply reply;
    bool callback_called = false;
    auto send_reply_callback = [&callback_called, &reply](Status status,
                                                          std::function<void()> f1,
                                                          std::function<void()> f2) {
      EXPECT_TRUE(status.ok());
      Status logical_status =
          Status(StatusCode(reply.status().code()), reply.status().message());
      EXPECT_TRUE(logical_status.IsGcsPassive());
      callback_called = true;
    };
    proxy.HandleRegisterNode(request, &reply, send_reply_callback);
    EXPECT_TRUE(callback_called);
    EXPECT_FALSE(underlying.called_);
  }
}

}  // namespace gcs
}  // namespace ray
