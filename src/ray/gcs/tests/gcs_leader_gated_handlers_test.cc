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

  LeaderGatedNodeInfoHandler proxy(underlying, is_leader_fn);

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

  // 2. Passive mode: allowlisted local head node registration & CheckAlive must be
  // FORWARDED.
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

}  // namespace gcs
}  // namespace ray
