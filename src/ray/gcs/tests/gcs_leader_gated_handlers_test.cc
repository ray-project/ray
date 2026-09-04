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

// =========================================================================
// 1. JobInfo Service Gating Tests
// =========================================================================

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

  // 1. Passive mode: unallowed mutating and read RPCs must be BLOCKED.
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
  {
    rpc::GetAllJobInfoRequest request;
    rpc::GetAllJobInfoReply reply;
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
    proxy.HandleGetAllJobInfo(request, &reply, send_reply_callback);
    EXPECT_TRUE(callback_called);
    EXPECT_FALSE(underlying.called_);
    underlying.called_ = false;
  }

  // 2. Passive mode: allowlisted RPCs (None for JobInfo service).

  // 3. Leader mode: all RPCs work.
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
    underlying.called_ = false;
  }
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
  }
}

// =========================================================================
// 2. InternalKV Service Gating Tests
// =========================================================================

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

  // 1. Passive mode: unallowed mutating KV Put must be BLOCKED.
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

  // 2. Passive mode: allowlisted KV Get must be FORWARDED.
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

  // 3. Leader mode: all RPCs work.
  {
    is_leader = true;
    rpc::InternalKVPutRequest request;
    rpc::InternalKVPutReply reply;
    bool callback_called = false;
    auto send_reply_callback = [&callback_called](Status status,
                                                  std::function<void()> f1,
                                                  std::function<void()> f2) {
      EXPECT_TRUE(status.ok());
      callback_called = true;
    };
    proxy.HandleInternalKVPut(request, &reply, send_reply_callback);
    EXPECT_TRUE(callback_called);
    EXPECT_TRUE(underlying.called_);
    underlying.called_ = false;
  }
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
  }
}

// =========================================================================
// 3. NodeInfo Service Gating Tests
// =========================================================================

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

TEST(GcsLeaderGatedHandlersTest, TestNodeRegistrationAndGating) {
  MockNodeInfoGcsServiceHandler underlying;
  bool is_leader = false;
  auto is_leader_fn = [&is_leader]() { return is_leader; };

  // Mock storage for passive node caching.
  std::shared_ptr<rpc::GcsNodeInfo> cached_passive_node;
  auto cache_local_node_fn = [&cached_passive_node](const rpc::GcsNodeInfo &node_info) {
    cached_passive_node = std::make_shared<rpc::GcsNodeInfo>(node_info);
  };

  LeaderGatedNodeInfoHandler proxy(underlying, is_leader_fn, cache_local_node_fn);

  // 1. Passive mode: unallowed worker node registration & unregister must be BLOCKED.
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
  }
  {
    rpc::UnregisterNodeRequest request;
    rpc::UnregisterNodeReply reply;
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
    proxy.HandleUnregisterNode(request, &reply, send_reply_callback, "peer");
    EXPECT_TRUE(callback_called);
    EXPECT_FALSE(underlying.called_);
    underlying.called_ = false;
  }

  // 2. Passive mode: allowlisted local head node registration is HANDLED IN PROXY,
  // CheckAlive is FORWARDED.
  {
    rpc::RegisterNodeRequest request;
    request.mutable_node_info()->set_is_head_node(true);
    rpc::RegisterNodeReply reply;
    bool callback_called = false;
    auto send_reply_callback = [&callback_called, &reply](Status status,
                                                          std::function<void()> f1,
                                                          std::function<void()> f2) {
      EXPECT_TRUE(status.ok());
      // Verify the reply status code is 0 (OK)
      EXPECT_EQ(reply.status().code(), static_cast<int>(StatusCode::OK));
      callback_called = true;
    };
    proxy.HandleRegisterNode(request, &reply, send_reply_callback);
    EXPECT_TRUE(callback_called);
    // Should NOT call underlying handler for head node in passive mode.
    EXPECT_FALSE(underlying.called_);
    // The head node should be cached in-memory via the cache callback.
    ASSERT_NE(cached_passive_node, nullptr);
    EXPECT_TRUE(cached_passive_node->is_head_node());
    underlying.called_ = false;
    cached_passive_node = nullptr;
  }
  {
    rpc::CheckAliveRequest request;
    rpc::CheckAliveReply reply;
    bool callback_called = false;
    auto send_reply_callback = [&callback_called](Status status,
                                                  std::function<void()> f1,
                                                  std::function<void()> f2) {
      EXPECT_TRUE(status.ok());
      callback_called = true;
    };
    proxy.HandleCheckAlive(request, &reply, send_reply_callback);
    EXPECT_TRUE(callback_called);
    EXPECT_TRUE(underlying.called_);
    underlying.called_ = false;
  }

  // 3. Leader mode: all RPCs work.
  {
    is_leader = true;
    rpc::RegisterNodeRequest request;
    request.mutable_node_info()->set_is_head_node(false);
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
  {
    rpc::UnregisterNodeRequest request;
    rpc::UnregisterNodeReply reply;
    bool callback_called = false;
    auto send_reply_callback = [&callback_called](Status status,
                                                  std::function<void()> f1,
                                                  std::function<void()> f2) {
      EXPECT_TRUE(status.ok());
      callback_called = true;
    };
    proxy.HandleUnregisterNode(request, &reply, send_reply_callback, "peer");
    EXPECT_TRUE(callback_called);
    EXPECT_TRUE(underlying.called_);
  }
}

// =========================================================================
// 4. ActorInfo Service Gating Tests
// =========================================================================

class MockActorInfoGcsServiceHandler : public rpc::ActorInfoGcsServiceHandler {
 public:
  void HandleRegisterActor(rpc::RegisterActorRequest request,
                           rpc::RegisterActorReply *reply,
                           rpc::SendReplyCallback send_reply_callback) override {
    called_ = true;
    send_reply_callback(Status::OK(), nullptr, nullptr);
  }
  void HandleRestartActorForLineageReconstruction(
      rpc::RestartActorForLineageReconstructionRequest request,
      rpc::RestartActorForLineageReconstructionReply *reply,
      rpc::SendReplyCallback send_reply_callback) override {
    called_ = true;
    send_reply_callback(Status::OK(), nullptr, nullptr);
  }
  void HandleCreateActor(rpc::CreateActorRequest request,
                         rpc::CreateActorReply *reply,
                         rpc::SendReplyCallback send_reply_callback) override {
    called_ = true;
    send_reply_callback(Status::OK(), nullptr, nullptr);
  }
  void HandleGetActorInfo(rpc::GetActorInfoRequest request,
                          rpc::GetActorInfoReply *reply,
                          rpc::SendReplyCallback send_reply_callback) override {
    called_ = true;
    send_reply_callback(Status::OK(), nullptr, nullptr);
  }
  void HandleGetNamedActorInfo(rpc::GetNamedActorInfoRequest request,
                               rpc::GetNamedActorInfoReply *reply,
                               rpc::SendReplyCallback send_reply_callback) override {
    called_ = true;
    send_reply_callback(Status::OK(), nullptr, nullptr);
  }
  void HandleListNamedActors(rpc::ListNamedActorsRequest request,
                             rpc::ListNamedActorsReply *reply,
                             rpc::SendReplyCallback send_reply_callback) override {
    called_ = true;
    send_reply_callback(Status::OK(), nullptr, nullptr);
  }
  void HandleGetAllActorInfo(rpc::GetAllActorInfoRequest request,
                             rpc::GetAllActorInfoReply *reply,
                             rpc::SendReplyCallback send_reply_callback) override {
    called_ = true;
    send_reply_callback(Status::OK(), nullptr, nullptr);
  }
  void HandleKillActorViaGcs(rpc::KillActorViaGcsRequest request,
                             rpc::KillActorViaGcsReply *reply,
                             rpc::SendReplyCallback send_reply_callback) override {
    called_ = true;
    send_reply_callback(Status::OK(), nullptr, nullptr);
  }
  void HandleReportActorOutOfScope(rpc::ReportActorOutOfScopeRequest request,
                                   rpc::ReportActorOutOfScopeReply *reply,
                                   rpc::SendReplyCallback send_reply_callback) override {
    called_ = true;
    send_reply_callback(Status::OK(), nullptr, nullptr);
  }

  bool called_ = false;
};

TEST(GcsLeaderGatedHandlersTest, TestActorGating) {
  MockActorInfoGcsServiceHandler underlying;
  bool is_leader = false;
  auto is_leader_fn = [&is_leader]() { return is_leader; };

  LeaderGatedActorInfoHandler proxy(underlying, is_leader_fn);

  // 1. Passive mode: unallowed RPCs must be BLOCKED.
  {
    rpc::GetActorInfoRequest request;
    rpc::GetActorInfoReply reply;
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
    proxy.HandleGetActorInfo(request, &reply, send_reply_callback);
    EXPECT_TRUE(callback_called);
    EXPECT_FALSE(underlying.called_);
  }
  {
    rpc::CreateActorRequest request;
    rpc::CreateActorReply reply;
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
    proxy.HandleCreateActor(request, &reply, send_reply_callback);
    EXPECT_TRUE(callback_called);
    EXPECT_FALSE(underlying.called_);
    underlying.called_ = false;
  }

  // 2. Passive mode: allowlisted RPCs (None for ActorInfo service).

  // 3. Leader mode: all RPCs work.
  {
    is_leader = true;
    rpc::GetActorInfoRequest request;
    rpc::GetActorInfoReply reply;
    bool callback_called = false;
    auto send_reply_callback = [&callback_called](Status status,
                                                  std::function<void()> f1,
                                                  std::function<void()> f2) {
      EXPECT_TRUE(status.ok());
      callback_called = true;
    };
    proxy.HandleGetActorInfo(request, &reply, send_reply_callback);
    EXPECT_TRUE(callback_called);
    EXPECT_TRUE(underlying.called_);
    underlying.called_ = false;
  }
  {
    rpc::CreateActorRequest request;
    rpc::CreateActorReply reply;
    bool callback_called = false;
    auto send_reply_callback = [&callback_called](Status status,
                                                  std::function<void()> f1,
                                                  std::function<void()> f2) {
      EXPECT_TRUE(status.ok());
      callback_called = true;
    };
    proxy.HandleCreateActor(request, &reply, send_reply_callback);
    EXPECT_TRUE(callback_called);
    EXPECT_TRUE(underlying.called_);
  }
}

// =========================================================================
// 5. PlacementGroupInfo Service Gating Tests
// =========================================================================

class MockPlacementGroupInfoGcsServiceHandler
    : public rpc::PlacementGroupInfoGcsServiceHandler {
 public:
  void HandleCreatePlacementGroup(rpc::CreatePlacementGroupRequest request,
                                  rpc::CreatePlacementGroupReply *reply,
                                  rpc::SendReplyCallback send_reply_callback) override {
    called_ = true;
    send_reply_callback(Status::OK(), nullptr, nullptr);
  }
  void HandleRemovePlacementGroup(rpc::RemovePlacementGroupRequest request,
                                  rpc::RemovePlacementGroupReply *reply,
                                  rpc::SendReplyCallback send_reply_callback) override {
    called_ = true;
    send_reply_callback(Status::OK(), nullptr, nullptr);
  }
  void HandleGetPlacementGroup(rpc::GetPlacementGroupRequest request,
                               rpc::GetPlacementGroupReply *reply,
                               rpc::SendReplyCallback send_reply_callback) override {
    called_ = true;
    send_reply_callback(Status::OK(), nullptr, nullptr);
  }
  void HandleGetAllPlacementGroup(rpc::GetAllPlacementGroupRequest request,
                                  rpc::GetAllPlacementGroupReply *reply,
                                  rpc::SendReplyCallback send_reply_callback) override {
    called_ = true;
    send_reply_callback(Status::OK(), nullptr, nullptr);
  }
  void HandleWaitPlacementGroupUntilReady(
      rpc::WaitPlacementGroupUntilReadyRequest request,
      rpc::WaitPlacementGroupUntilReadyReply *reply,
      rpc::SendReplyCallback send_reply_callback) override {
    called_ = true;
    send_reply_callback(Status::OK(), nullptr, nullptr);
  }
  void HandleGetNamedPlacementGroup(rpc::GetNamedPlacementGroupRequest request,
                                    rpc::GetNamedPlacementGroupReply *reply,
                                    rpc::SendReplyCallback send_reply_callback) override {
    called_ = true;
    send_reply_callback(Status::OK(), nullptr, nullptr);
  }

  bool called_ = false;
};

TEST(GcsLeaderGatedHandlersTest, TestPlacementGroupGating) {
  MockPlacementGroupInfoGcsServiceHandler underlying;
  bool is_leader = false;
  auto is_leader_fn = [&is_leader]() { return is_leader; };

  LeaderGatedPlacementGroupInfoHandler proxy(underlying, is_leader_fn);

  // 1. Passive mode: unallowed RPCs must be BLOCKED.
  {
    rpc::GetPlacementGroupRequest request;
    rpc::GetPlacementGroupReply reply;
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
    proxy.HandleGetPlacementGroup(request, &reply, send_reply_callback);
    EXPECT_TRUE(callback_called);
    EXPECT_FALSE(underlying.called_);
  }
  {
    rpc::CreatePlacementGroupRequest request;
    rpc::CreatePlacementGroupReply reply;
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
    proxy.HandleCreatePlacementGroup(request, &reply, send_reply_callback);
    EXPECT_TRUE(callback_called);
    EXPECT_FALSE(underlying.called_);
    underlying.called_ = false;
  }

  // 2. Passive mode: allowlisted RPCs (None for PlacementGroupInfo service).

  // 3. Leader mode: all RPCs work.
  {
    is_leader = true;
    rpc::GetPlacementGroupRequest request;
    rpc::GetPlacementGroupReply reply;
    bool callback_called = false;
    auto send_reply_callback = [&callback_called](Status status,
                                                  std::function<void()> f1,
                                                  std::function<void()> f2) {
      EXPECT_TRUE(status.ok());
      callback_called = true;
    };
    proxy.HandleGetPlacementGroup(request, &reply, send_reply_callback);
    EXPECT_TRUE(callback_called);
    EXPECT_TRUE(underlying.called_);
    underlying.called_ = false;
  }
  {
    rpc::CreatePlacementGroupRequest request;
    rpc::CreatePlacementGroupReply reply;
    bool callback_called = false;
    auto send_reply_callback = [&callback_called](Status status,
                                                  std::function<void()> f1,
                                                  std::function<void()> f2) {
      EXPECT_TRUE(status.ok());
      callback_called = true;
    };
    proxy.HandleCreatePlacementGroup(request, &reply, send_reply_callback);
    EXPECT_TRUE(callback_called);
    EXPECT_TRUE(underlying.called_);
  }
}

// =========================================================================
// 6. AutoscalerState Service Gating Tests
// =========================================================================

class MockAutoscalerStateServiceHandler
    : public rpc::autoscaler::AutoscalerStateServiceHandler {
 public:
  void HandleGetClusterResourceState(
      rpc::autoscaler::GetClusterResourceStateRequest request,
      rpc::autoscaler::GetClusterResourceStateReply *reply,
      rpc::SendReplyCallback send_reply_callback) override {
    called_ = true;
    send_reply_callback(Status::OK(), nullptr, nullptr);
  }
  void HandleReportAutoscalingState(
      rpc::autoscaler::ReportAutoscalingStateRequest request,
      rpc::autoscaler::ReportAutoscalingStateReply *reply,
      rpc::SendReplyCallback send_reply_callback) override {
    called_ = true;
    send_reply_callback(Status::OK(), nullptr, nullptr);
  }
  void HandleRequestClusterResourceConstraint(
      rpc::autoscaler::RequestClusterResourceConstraintRequest request,
      rpc::autoscaler::RequestClusterResourceConstraintReply *reply,
      rpc::SendReplyCallback send_reply_callback) override {
    called_ = true;
    send_reply_callback(Status::OK(), nullptr, nullptr);
  }
  void HandleGetClusterStatus(rpc::autoscaler::GetClusterStatusRequest request,
                              rpc::autoscaler::GetClusterStatusReply *reply,
                              rpc::SendReplyCallback send_reply_callback) override {
    called_ = true;
    send_reply_callback(Status::OK(), nullptr, nullptr);
  }
  void HandleDrainNode(rpc::autoscaler::DrainNodeRequest request,
                       rpc::autoscaler::DrainNodeReply *reply,
                       rpc::SendReplyCallback send_reply_callback,
                       const std::string &grpc_peer) override {
    called_ = true;
    send_reply_callback(Status::OK(), nullptr, nullptr);
  }
  void HandleResizeRayletResourceInstances(
      rpc::autoscaler::ResizeRayletResourceInstancesRequest request,
      rpc::autoscaler::ResizeRayletResourceInstancesReply *reply,
      rpc::SendReplyCallback send_reply_callback) override {
    called_ = true;
    send_reply_callback(Status::OK(), nullptr, nullptr);
  }
  void HandleReportClusterConfig(rpc::autoscaler::ReportClusterConfigRequest request,
                                 rpc::autoscaler::ReportClusterConfigReply *reply,
                                 rpc::SendReplyCallback send_reply_callback) override {
    called_ = true;
    send_reply_callback(Status::OK(), nullptr, nullptr);
  }

  bool called_ = false;
};

TEST(GcsLeaderGatedHandlersTest, TestAutoscalerGating) {
  MockAutoscalerStateServiceHandler underlying;
  bool is_leader = false;
  auto is_leader_fn = [&is_leader]() { return is_leader; };

  LeaderGatedAutoscalerStateHandler proxy(underlying, is_leader_fn);

  // 1. Passive mode: unallowed RPCs must be BLOCKED (callback status is GcsPassive).
  {
    rpc::autoscaler::GetClusterStatusRequest request;
    rpc::autoscaler::GetClusterStatusReply reply;
    bool callback_called = false;
    auto send_reply_callback = [&callback_called](Status status,
                                                  std::function<void()> f1,
                                                  std::function<void()> f2) {
      EXPECT_TRUE(status.IsGcsPassive());
      callback_called = true;
    };
    proxy.HandleGetClusterStatus(request, &reply, send_reply_callback);
    EXPECT_TRUE(callback_called);
    EXPECT_FALSE(underlying.called_);
  }
  {
    rpc::autoscaler::ReportAutoscalingStateRequest request;
    rpc::autoscaler::ReportAutoscalingStateReply reply;
    bool callback_called = false;
    auto send_reply_callback = [&callback_called](Status status,
                                                  std::function<void()> f1,
                                                  std::function<void()> f2) {
      EXPECT_TRUE(status.IsGcsPassive());
      callback_called = true;
    };
    proxy.HandleReportAutoscalingState(request, &reply, send_reply_callback);
    EXPECT_TRUE(callback_called);
    EXPECT_FALSE(underlying.called_);
    underlying.called_ = false;
  }

  // 2. Passive mode: allowlisted RPCs (None for AutoscalerState service).

  // 3. Leader mode: all RPCs work.
  {
    is_leader = true;
    rpc::autoscaler::GetClusterStatusRequest request;
    rpc::autoscaler::GetClusterStatusReply reply;
    bool callback_called = false;
    auto send_reply_callback = [&callback_called](Status status,
                                                  std::function<void()> f1,
                                                  std::function<void()> f2) {
      EXPECT_TRUE(status.ok());
      callback_called = true;
    };
    proxy.HandleGetClusterStatus(request, &reply, send_reply_callback);
    EXPECT_TRUE(callback_called);
    EXPECT_TRUE(underlying.called_);
    underlying.called_ = false;
  }
  {
    rpc::autoscaler::ReportAutoscalingStateRequest request;
    rpc::autoscaler::ReportAutoscalingStateReply reply;
    bool callback_called = false;
    auto send_reply_callback = [&callback_called](Status status,
                                                  std::function<void()> f1,
                                                  std::function<void()> f2) {
      EXPECT_TRUE(status.ok());
      callback_called = true;
    };
    proxy.HandleReportAutoscalingState(request, &reply, send_reply_callback);
    EXPECT_TRUE(callback_called);
    EXPECT_TRUE(underlying.called_);
  }
}

// =========================================================================
// Helpers for body-reply gating assertions.
// =========================================================================

namespace {

// Asserts that a gated RPC replied with Status::GcsPassive written into the reply
// body, and that the underlying handler was NOT invoked.
template <typename Reply>
void ExpectGatedInBody(const Reply &reply, bool underlying_called) {
  Status logical_status =
      Status(StatusCode(reply.status().code()), reply.status().message());
  EXPECT_TRUE(logical_status.IsGcsPassive());
  EXPECT_FALSE(underlying_called);
}

}  // namespace

// =========================================================================
// NodeResourceInfo Service Gating Tests (all RPCs gated).
// =========================================================================

class MockNodeResourceInfoGcsServiceHandler
    : public rpc::NodeResourceInfoGcsServiceHandler {
 public:
  void HandleGetAllAvailableResources(
      rpc::GetAllAvailableResourcesRequest request,
      rpc::GetAllAvailableResourcesReply *reply,
      rpc::SendReplyCallback send_reply_callback) override {
    called_ = true;
    send_reply_callback(Status::OK(), nullptr, nullptr);
  }

  void HandleGetAllTotalResources(rpc::GetAllTotalResourcesRequest request,
                                  rpc::GetAllTotalResourcesReply *reply,
                                  rpc::SendReplyCallback send_reply_callback) override {
    called_ = true;
    send_reply_callback(Status::OK(), nullptr, nullptr);
  }

  void HandleGetDrainingNodes(rpc::GetDrainingNodesRequest request,
                              rpc::GetDrainingNodesReply *reply,
                              rpc::SendReplyCallback send_reply_callback) override {
    called_ = true;
    send_reply_callback(Status::OK(), nullptr, nullptr);
  }

  void HandleGetAllResourceUsage(rpc::GetAllResourceUsageRequest request,
                                 rpc::GetAllResourceUsageReply *reply,
                                 rpc::SendReplyCallback send_reply_callback) override {
    called_ = true;
    send_reply_callback(Status::OK(), nullptr, nullptr);
  }

  bool called_ = false;
};

TEST(GcsLeaderGatedHandlersTest, TestNodeResourceInfoGating) {
  MockNodeResourceInfoGcsServiceHandler underlying;
  bool is_leader = false;
  LeaderGatedNodeResourceInfoHandler proxy(underlying,
                                           [&is_leader]() { return is_leader; });

  auto noop_cb = [](Status, std::function<void()>, std::function<void()>) {};

  // Passive: every RPC is gated.
  {
    rpc::GetAllResourceUsageReply reply;
    proxy.HandleGetAllResourceUsage({}, &reply, noop_cb);
    ExpectGatedInBody(reply, underlying.called_);
  }
  {
    rpc::GetAllAvailableResourcesReply reply;
    proxy.HandleGetAllAvailableResources({}, &reply, noop_cb);
    ExpectGatedInBody(reply, underlying.called_);
  }

  // Leader: forwarded to underlying handler.
  is_leader = true;
  {
    rpc::GetAllResourceUsageReply reply;
    proxy.HandleGetAllResourceUsage({}, &reply, noop_cb);
    EXPECT_TRUE(underlying.called_);
  }
}

// =========================================================================
// WorkerInfo Service Gating Tests (all RPCs gated, incl. reads).
// =========================================================================

class MockWorkerInfoGcsServiceHandler : public rpc::WorkerInfoGcsServiceHandler {
 public:
  void HandleReportWorkerFailure(rpc::ReportWorkerFailureRequest request,
                                 rpc::ReportWorkerFailureReply *reply,
                                 rpc::SendReplyCallback send_reply_callback) override {
    called_ = true;
    send_reply_callback(Status::OK(), nullptr, nullptr);
  }
  void HandleGetWorkerInfo(rpc::GetWorkerInfoRequest request,
                           rpc::GetWorkerInfoReply *reply,
                           rpc::SendReplyCallback send_reply_callback) override {
    called_ = true;
    send_reply_callback(Status::OK(), nullptr, nullptr);
  }
  void HandleGetAllWorkerInfo(rpc::GetAllWorkerInfoRequest request,
                              rpc::GetAllWorkerInfoReply *reply,
                              rpc::SendReplyCallback send_reply_callback) override {
    called_ = true;
    send_reply_callback(Status::OK(), nullptr, nullptr);
  }
  void HandleAddWorkerInfo(rpc::AddWorkerInfoRequest request,
                           rpc::AddWorkerInfoReply *reply,
                           rpc::SendReplyCallback send_reply_callback) override {
    called_ = true;
    send_reply_callback(Status::OK(), nullptr, nullptr);
  }
  void HandleUpdateWorkerDebuggerPort(
      rpc::UpdateWorkerDebuggerPortRequest request,
      rpc::UpdateWorkerDebuggerPortReply *reply,
      rpc::SendReplyCallback send_reply_callback) override {
    called_ = true;
    send_reply_callback(Status::OK(), nullptr, nullptr);
  }
  void HandleUpdateWorkerNumPausedThreads(
      rpc::UpdateWorkerNumPausedThreadsRequest request,
      rpc::UpdateWorkerNumPausedThreadsReply *reply,
      rpc::SendReplyCallback send_reply_callback) override {
    called_ = true;
    send_reply_callback(Status::OK(), nullptr, nullptr);
  }

  bool called_ = false;
};

TEST(GcsLeaderGatedHandlersTest, TestWorkerInfoGating) {
  MockWorkerInfoGcsServiceHandler underlying;
  bool is_leader = false;
  LeaderGatedWorkerInfoHandler proxy(underlying, [&is_leader]() { return is_leader; });

  auto noop_cb = [](Status, std::function<void()>, std::function<void()>) {};

  // Passive: writes are gated.
  {
    rpc::AddWorkerInfoReply reply;
    proxy.HandleAddWorkerInfo({}, &reply, noop_cb);
    ExpectGatedInBody(reply, underlying.called_);
  }
  // Passive: reads are also gated (no workers run on a passive head).
  {
    rpc::GetAllWorkerInfoReply reply;
    proxy.HandleGetAllWorkerInfo({}, &reply, noop_cb);
    ExpectGatedInBody(reply, underlying.called_);
  }

  // Leader: forwarded.
  is_leader = true;
  {
    rpc::AddWorkerInfoReply reply;
    proxy.HandleAddWorkerInfo({}, &reply, noop_cb);
    EXPECT_TRUE(underlying.called_);
  }
}

// =========================================================================
// TaskInfo Service Gating Tests (all RPCs gated, incl. reads).
// =========================================================================

class MockTaskInfoGcsServiceHandler : public rpc::TaskInfoGcsServiceHandler {
 public:
  void HandleAddTaskEventData(rpc::AddTaskEventDataRequest request,
                              rpc::AddTaskEventDataReply *reply,
                              rpc::SendReplyCallback send_reply_callback) override {
    called_ = true;
    send_reply_callback(Status::OK(), nullptr, nullptr);
  }
  void HandleGetTaskEvents(rpc::GetTaskEventsRequest request,
                           rpc::GetTaskEventsReply *reply,
                           rpc::SendReplyCallback send_reply_callback) override {
    called_ = true;
    send_reply_callback(Status::OK(), nullptr, nullptr);
  }

  bool called_ = false;
};

TEST(GcsLeaderGatedHandlersTest, TestTaskInfoGating) {
  MockTaskInfoGcsServiceHandler underlying;
  bool is_leader = false;
  LeaderGatedTaskInfoHandler proxy(underlying, [&is_leader]() { return is_leader; });

  auto noop_cb = [](Status, std::function<void()>, std::function<void()>) {};

  // Passive: write gated.
  {
    rpc::AddTaskEventDataReply reply;
    proxy.HandleAddTaskEventData({}, &reply, noop_cb);
    ExpectGatedInBody(reply, underlying.called_);
  }
  // Passive: read gated.
  {
    rpc::GetTaskEventsReply reply;
    proxy.HandleGetTaskEvents({}, &reply, noop_cb);
    ExpectGatedInBody(reply, underlying.called_);
  }

  // Leader: forwarded.
  is_leader = true;
  {
    rpc::GetTaskEventsReply reply;
    proxy.HandleGetTaskEvents({}, &reply, noop_cb);
    EXPECT_TRUE(underlying.called_);
  }
}

// =========================================================================
// RuntimeEnv Service Gating Tests (single write RPC gated).
// =========================================================================

class MockRuntimeEnvGcsServiceHandler : public rpc::RuntimeEnvGcsServiceHandler {
 public:
  void HandlePinRuntimeEnvURI(rpc::PinRuntimeEnvURIRequest request,
                              rpc::PinRuntimeEnvURIReply *reply,
                              rpc::SendReplyCallback send_reply_callback) override {
    called_ = true;
    send_reply_callback(Status::OK(), nullptr, nullptr);
  }

  bool called_ = false;
};

TEST(GcsLeaderGatedHandlersTest, TestRuntimeEnvGating) {
  MockRuntimeEnvGcsServiceHandler underlying;
  bool is_leader = false;
  LeaderGatedRuntimeEnvHandler proxy(underlying, [&is_leader]() { return is_leader; });

  auto noop_cb = [](Status, std::function<void()>, std::function<void()>) {};

  // Passive: gated.
  {
    rpc::PinRuntimeEnvURIReply reply;
    proxy.HandlePinRuntimeEnvURI({}, &reply, noop_cb);
    ExpectGatedInBody(reply, underlying.called_);
  }

  // Leader: forwarded.
  is_leader = true;
  {
    rpc::PinRuntimeEnvURIReply reply;
    proxy.HandlePinRuntimeEnvURI({}, &reply, noop_cb);
    EXPECT_TRUE(underlying.called_);
  }
}

// =========================================================================
// ControlPlanePubSub Service Gating Tests (publish gated, subscribe allowed).
// =========================================================================

class MockControlPlanePubSubGcsServiceHandler
    : public rpc::ControlPlanePubSubGcsServiceHandler {
 public:
  void HandleGcsPublish(rpc::GcsPublishRequest request,
                        rpc::GcsPublishReply *reply,
                        rpc::SendReplyCallback send_reply_callback) override {
    publish_called_ = true;
    send_reply_callback(Status::OK(), nullptr, nullptr);
  }
  void HandleGcsSubscriberPoll(rpc::GcsSubscriberPollRequest request,
                               rpc::GcsSubscriberPollReply *reply,
                               rpc::SendReplyCallback send_reply_callback) override {
    poll_called_ = true;
    send_reply_callback(Status::OK(), nullptr, nullptr);
  }
  void HandleGcsSubscriberCommandBatch(
      rpc::GcsSubscriberCommandBatchRequest request,
      rpc::GcsSubscriberCommandBatchReply *reply,
      rpc::SendReplyCallback send_reply_callback) override {
    command_batch_called_ = true;
    send_reply_callback(Status::OK(), nullptr, nullptr);
  }

  bool publish_called_ = false;
  bool poll_called_ = false;
  bool command_batch_called_ = false;
};

TEST(GcsLeaderGatedHandlersTest, TestControlPlanePubSubGating) {
  MockControlPlanePubSubGcsServiceHandler underlying;
  bool is_leader = false;
  LeaderGatedControlPlanePubSubHandler proxy(underlying,
                                             [&is_leader]() { return is_leader; });

  auto noop_cb = [](Status, std::function<void()>, std::function<void()>) {};

  // Passive: publish is gated.
  {
    rpc::GcsPublishReply reply;
    proxy.HandleGcsPublish({}, &reply, noop_cb);
    ExpectGatedInBody(reply, underlying.publish_called_);
  }

  // Passive: subscribe poll and command batch are allowed (forwarded),
  // required by the local raylet's node address/liveness subscription during
  // startup.
  {
    rpc::GcsSubscriberPollReply reply;
    proxy.HandleGcsSubscriberPoll({}, &reply, noop_cb);
    EXPECT_TRUE(underlying.poll_called_);
  }
  {
    rpc::GcsSubscriberCommandBatchReply reply;
    proxy.HandleGcsSubscriberCommandBatch({}, &reply, noop_cb);
    EXPECT_TRUE(underlying.command_batch_called_);
  }

  // Leader: publish is forwarded.
  is_leader = true;
  {
    rpc::GcsPublishReply reply;
    proxy.HandleGcsPublish({}, &reply, noop_cb);
    EXPECT_TRUE(underlying.publish_called_);
  }
}

// =========================================================================
// ObservabilityPubSub Service Gating Tests
// (publish + report-job-error gated, subscribe allowed).
// =========================================================================

class MockObservabilityPubSubServiceHandler
    : public rpc::ObservabilityPubSubServiceHandler {
 public:
  void HandleGcsPublish(rpc::GcsPublishRequest request,
                        rpc::GcsPublishReply *reply,
                        rpc::SendReplyCallback send_reply_callback) override {
    publish_called_ = true;
    send_reply_callback(Status::OK(), nullptr, nullptr);
  }
  void HandleReportJobError(rpc::ReportJobErrorRequest request,
                            rpc::ReportJobErrorReply *reply,
                            rpc::SendReplyCallback send_reply_callback) override {
    report_job_error_called_ = true;
    send_reply_callback(Status::OK(), nullptr, nullptr);
  }
  void HandleGcsSubscriberPoll(rpc::GcsSubscriberPollRequest request,
                               rpc::GcsSubscriberPollReply *reply,
                               rpc::SendReplyCallback send_reply_callback) override {
    poll_called_ = true;
    send_reply_callback(Status::OK(), nullptr, nullptr);
  }
  void HandleGcsSubscriberCommandBatch(
      rpc::GcsSubscriberCommandBatchRequest request,
      rpc::GcsSubscriberCommandBatchReply *reply,
      rpc::SendReplyCallback send_reply_callback) override {
    command_batch_called_ = true;
    send_reply_callback(Status::OK(), nullptr, nullptr);
  }

  bool publish_called_ = false;
  bool report_job_error_called_ = false;
  bool poll_called_ = false;
  bool command_batch_called_ = false;
};

TEST(GcsLeaderGatedHandlersTest, TestObservabilityPubSubGating) {
  MockObservabilityPubSubServiceHandler underlying;
  bool is_leader = false;
  LeaderGatedObservabilityPubSubHandler proxy(underlying,
                                              [&is_leader]() { return is_leader; });

  auto noop_cb = [](Status, std::function<void()>, std::function<void()>) {};

  // Passive: publish and report-job-error are gated.
  {
    rpc::GcsPublishReply reply;
    proxy.HandleGcsPublish({}, &reply, noop_cb);
    ExpectGatedInBody(reply, underlying.publish_called_);
  }
  {
    rpc::ReportJobErrorReply reply;
    proxy.HandleReportJobError({}, &reply, noop_cb);
    ExpectGatedInBody(reply, underlying.report_job_error_called_);
  }

  // Passive: subscribe poll and command batch are allowed (forwarded), mirroring
  // the control-plane pubsub allowlist so local observability subscribers attach.
  {
    rpc::GcsSubscriberPollReply reply;
    proxy.HandleGcsSubscriberPoll({}, &reply, noop_cb);
    EXPECT_TRUE(underlying.poll_called_);
  }
  {
    rpc::GcsSubscriberCommandBatchReply reply;
    proxy.HandleGcsSubscriberCommandBatch({}, &reply, noop_cb);
    EXPECT_TRUE(underlying.command_batch_called_);
  }

  // Leader: publish and report-job-error are forwarded.
  is_leader = true;
  {
    rpc::GcsPublishReply reply;
    proxy.HandleGcsPublish({}, &reply, noop_cb);
    EXPECT_TRUE(underlying.publish_called_);
  }
  {
    rpc::ReportJobErrorReply reply;
    proxy.HandleReportJobError({}, &reply, noop_cb);
    EXPECT_TRUE(underlying.report_job_error_called_);
  }
}

// =========================================================================
// RayEventExport Service Gating Tests (single ingest RPC gated).
// =========================================================================

class MockRayEventExportGcsServiceHandler
    : public rpc::events::RayEventExportGcsServiceHandler {
 public:
  void HandleAddEvents(rpc::events::AddEventsRequest request,
                       rpc::events::AddEventsReply *reply,
                       rpc::SendReplyCallback send_reply_callback) override {
    called_ = true;
    send_reply_callback(Status::OK(), nullptr, nullptr);
  }

  bool called_ = false;
};

TEST(GcsLeaderGatedHandlersTest, TestRayEventExportGating) {
  MockRayEventExportGcsServiceHandler underlying;
  bool is_leader = false;
  LeaderGatedRayEventExportHandler proxy(underlying,
                                         [&is_leader]() { return is_leader; });

  auto noop_cb = [](Status, std::function<void()>, std::function<void()>) {};

  // Passive: event ingest is gated.
  {
    rpc::events::AddEventsReply reply;
    proxy.HandleAddEvents({}, &reply, noop_cb);
    ExpectGatedInBody(reply, underlying.called_);
  }

  // Leader: event ingest is forwarded.
  is_leader = true;
  {
    rpc::events::AddEventsReply reply;
    proxy.HandleAddEvents({}, &reply, noop_cb);
    EXPECT_TRUE(underlying.called_);
  }
}

// =========================================================================
// RaySyncer Service Gating Tests (stream RPC ALLOWED even when passive).
// Unlike every other gated handler, StartSync must be forwarded regardless of
// leadership: it is a side-effect-free stream already scoped upstream at node
// registration. This test guards that contract against a future change that
// wrongly gates it.
// =========================================================================

class MockRaySyncerStreamHandler : public syncer::RaySyncerStreamHandler {
 public:
  syncer::SyncStreamReactor *StartSync(grpc::CallbackServerContext *context) override {
    called_ = true;
    return nullptr;
  }

  bool called_ = false;
};

TEST(GcsLeaderGatedHandlersTest, TestRaySyncerAllowedRegardlessOfLeadership) {
  MockRaySyncerStreamHandler underlying;
  bool is_leader = false;
  LeaderGatedRaySyncerHandler proxy(underlying, [&is_leader]() { return is_leader; });

  // Passive: StartSync is still forwarded (NOT gated, unlike other handlers).
  underlying.called_ = false;
  proxy.StartSync(/*context=*/nullptr);
  EXPECT_TRUE(underlying.called_);

  // Leader: also forwarded.
  is_leader = true;
  underlying.called_ = false;
  proxy.StartSync(/*context=*/nullptr);
  EXPECT_TRUE(underlying.called_);
}

}  // namespace gcs
}  // namespace ray
