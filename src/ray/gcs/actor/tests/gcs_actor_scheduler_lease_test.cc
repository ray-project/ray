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
#include <gtest/gtest.h>

#include <chrono>
#include <deque>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "ray/common/test_utils.h"
#include "ray/core_worker_rpc_client/fake_core_worker_client.h"
#include "ray/gcs/actor/gcs_actor_scheduler.h"
#include "ray/gcs/store_client/in_memory_store_client.h"
#include "ray/observability/fake_metric.h"
#include "ray/observability/fake_ray_event_recorder.h"
#include "ray/pubsub/fake_publisher.h"
#include "ray/raylet_rpc_client/fake_raylet_client.h"

namespace ray::gcs {
namespace {

// Grant and RPC delivery are separate events. Cancellation cannot revoke a grant.
class LeaseRayletClient : public rpc::FakeRayletClient {
 public:
  struct Request {
    rpc::RequestWorkerLeaseRequest request;
    rpc::ClientCallback<rpc::RequestWorkerLeaseReply> callback;
    rpc::RequestWorkerLeaseReply reply;
    bool ready = false;
  };

  void RequestWorkerLease(
      rpc::RequestWorkerLeaseRequest &&request,
      const rpc::ClientCallback<rpc::RequestWorkerLeaseReply> &callback) override {
    requests.push_back({std::move(request), callback, {}});
  }

  LeaseID Id(size_t index) const {
    return LeaseID::FromBinary(requests.at(index).request.lease_spec().lease_id());
  }

  void Grant(size_t index, const rpc::GcsNodeInfo &node, const WorkerID &worker) {
    auto &request = requests.at(index);
    ASSERT_FALSE(request.ready);
    request.ready = true;
    auto *address = request.reply.mutable_worker_address();
    address->set_node_id(node.node_id());
    address->set_ip_address(node.node_manager_address());
    address->set_port(1234);
    address->set_worker_id(worker.Binary());
    auto *resource = request.reply.add_resource_mapping();
    resource->set_name("CPU");
    for (int cpu = 0; cpu < 4; ++cpu) {
      auto *instance = resource->add_resource_ids();
      instance->set_index((index % 2) * 4 + cpu);
      instance->set_quantity(1);
    }
  }

  void Deliver(size_t index, Status status = Status::OK()) {
    auto &request = requests.at(index);
    ASSERT_TRUE(request.callback);
    ASSERT_TRUE(request.ready || !status.ok());
    auto callback = std::move(request.callback);
    auto reply = request.reply;
    callback(status, std::move(reply));
  }

  void CancelWorkerLease(
      const LeaseID &id,
      const rpc::ClientCallback<rpc::CancelWorkerLeaseReply> &callback) override {
    cancellations.push_back(id);
    cancel_callbacks.push_back(callback);
    if (!delay_cancellation) {
      ProcessCancel(cancellations.size() - 1);
    }
  }

  void ProcessCancel(size_t index) {
    const auto id = cancellations.at(index);
    for (auto &request : requests) {
      if (request.request.lease_spec().lease_id() == id.Binary() && !request.ready) {
        request.ready = true;
        request.reply.set_canceled(true);
      }
    }
    auto callback = std::move(cancel_callbacks.at(index));
    ASSERT_TRUE(callback);
    callback(Status::OK(), {});
  }

  void KillLocalActor(
      const rpc::KillLocalActorRequest &request,
      const rpc::ClientCallback<rpc::KillLocalActorReply> &callback) override {
    kills.push_back(request);
    callback(Status::OK(), {});  // RPC ACK only, not process termination.
  }

  std::deque<Request> requests;
  std::vector<LeaseID> cancellations;
  std::vector<rpc::KillLocalActorRequest> kills;
  std::vector<rpc::ClientCallback<rpc::CancelWorkerLeaseReply>> cancel_callbacks;
  bool delay_cancellation = false;
};

class LeaseWorkerClient : public rpc::FakeCoreWorkerClient {
 public:
  void PushNormalTask(std::unique_ptr<rpc::PushTaskRequest> request,
                      const rpc::ClientCallback<rpc::PushTaskReply> &callback) override {
    creations.push_back(*request);
    rpc::FakeCoreWorkerClient::PushNormalTask(std::move(request), callback);
  }
  std::vector<rpc::PushTaskRequest> creations;
};

class LeaseScheduler : public GcsActorScheduler {
 public:
  using GcsActorScheduler::GcsActorScheduler;
  bool IsLeasing(const NodeID &node, const ActorID &actor) const {
    auto it = node_to_actors_when_leasing_.find(node);
    return it != node_to_actors_when_leasing_.end() && it->second.contains(actor);
  }
};

// All asynchronous storage callbacks and retry timers run on this test's manually
// polled event loop. The scheduler itself (including its retry timer) is real.
class GcsActorSchedulerLeaseTest : public ::testing::Test {
 public:
  void SetUp() override {
    RayConfig::instance().initialize(R"({"gcs_lease_worker_retry_interval_ms":0})");
    nodes_ = std::make_unique<GcsNodeManager>(&publisher_,
                                              &storage_,
                                              io_,
                                              &raylet_pool_,
                                              ClusterID::Nil(),
                                              recorder_,
                                              "",
                                              &observability_publisher_,
                                              clock_);
    scheduler_ = std::make_unique<LeaseScheduler>(
        io_,
        storage_.ActorTable(),
        *nodes_,
        [this](auto, auto, const std::string &) { ++failures_; },
        [this](auto actor, const auto &) {
          ++successes_;
          actor->UpdateState(rpc::ActorTableData::ALIVE);
        },
        raylet_pool_,
        worker_pool_,
        placement_time_,
        clock_);
    node_ = GenNodeInfo();
    (*node_->mutable_resources_total())["CPU"] = 8;
    nodes_->AddNode(node_);
    rpc::Address owner;
    owner.set_node_id(NodeID::FromRandom().Binary());
    owner.set_worker_id(WorkerID::FromRandom().Binary());
    auto task = GenActorCreationTask(
        JobID::FromInt(1), 3, false, "", "", owner, {{"CPU", 4}}, {{"CPU", 4}});
    actor_ = std::make_shared<GcsActor>(task.GetMessage(), "", counter_, recorder_, "");
    actor_->UpdateState(rpc::ActorTableData::PENDING_CREATION);
    scheduler_->Schedule(actor_);
    ASSERT_EQ(raylet_->requests.size(), 1u);
  }

  void TearDown() override {
    io_.stop();
    RayConfig::instance().initialize("{}");
  }

  void Drain() {
    io_.restart();
    io_.poll();
  }

  template <typename Condition>
  std::size_t RunUntil(Condition condition) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(1);
    std::size_t handlers = 0;
    io_.restart();
    while (!condition()) {
      const auto dispatched = io_.run_one_until(deadline);
      handlers += dispatched;
      if (dispatched == 0) {
        break;
      }
    }
    EXPECT_TRUE(condition()) << "Timed out waiting for the event-loop condition.";
    return handlers;
  }

  std::size_t RunUntilIdle() {
    io_.restart();
    const auto handlers = io_.run_for(std::chrono::seconds(1));
    EXPECT_TRUE(io_.stopped()) << "Timed out before the event loop became idle.";
    return handlers;
  }

  // Reproduce the scheduler boundary of GcsActorManager::RestartActor. Manager
  // registration/kill behavior is covered separately by gcs_actor_manager_test.
  void Restart(bool leasing) {
    if (leasing) {
      scheduler_->CancelOnLeasing(NodeID::FromBinary(node_->node_id()),
                                  actor_->GetActorID(),
                                  actor_->GetLeaseSpecification().LeaseId());
    }
    auto *data = actor_->GetMutableActorTableData();
    data->set_num_restarts(data->num_restarts() + 1);
    actor_->UpdateState(rpc::ActorTableData::RESTARTING);
    actor_->GetMutableTaskSpec()->set_attempt_number(data->num_restarts());
    actor_->UpdateAddress(rpc::Address());
    scheduler_->Schedule(actor_);
  }

  void PrepareReplacement() {
    raylet_->Grant(0, *node_, w1_);
    Restart(true);
    ASSERT_EQ(raylet_->requests.size(), 2u);
    ASSERT_NE(raylet_->Id(0), raylet_->Id(1));
    ASSERT_EQ(raylet_->cancellations, std::vector<LeaseID>{raylet_->Id(0)});
    EXPECT_EQ(LeaseSpecification(raylet_->requests[1].request.lease_spec()).ActorId(),
              actor_->GetActorID());
    EXPECT_EQ(actor_->GetNodeID(), NodeID::FromBinary(node_->node_id()));
  }

  void Finish(size_t index, const WorkerID &worker, int attempt) {
    raylet_->Deliver(index);
    Drain();
    ASSERT_FALSE(worker_->creations.empty());
    EXPECT_EQ(worker_->creations.back().intended_worker_id(), worker.Binary());
    EXPECT_EQ(worker_->creations.back().task_spec().attempt_number(), attempt);
    ASSERT_EQ(worker_->creations.back().resource_mapping_size(), 1);
    EXPECT_EQ(worker_->creations.back().resource_mapping(0).SerializeAsString(),
              raylet_->requests[index].reply.resource_mapping(0).SerializeAsString());
    EXPECT_EQ(actor_->GetActorTableData().resource_mapping(0).SerializeAsString(),
              raylet_->requests[index].reply.resource_mapping(0).SerializeAsString());
    ASSERT_TRUE(worker_->ReplyPushTask());
    Drain();
    EXPECT_EQ(actor_->GetWorkerID(), worker);
    EXPECT_EQ(actor_->GetState(), rpc::ActorTableData::ALIVE);
  }

  instrumented_io_context io_;
  Clock clock_;
  observability::FakeRayEventRecorder recorder_;
  observability::FakeHistogram placement_time_;
  std::shared_ptr<CounterMap<std::pair<rpc::ActorTableData::ActorState, std::string>>>
      counter_ = std::make_shared<
          CounterMap<std::pair<rpc::ActorTableData::ActorState, std::string>>>();
  std::shared_ptr<LeaseRayletClient> raylet_ = std::make_shared<LeaseRayletClient>();
  std::shared_ptr<LeaseWorkerClient> worker_ = std::make_shared<LeaseWorkerClient>();
  rpc::RayletClientPool raylet_pool_{[this](const auto &) { return raylet_; }};
  rpc::CoreWorkerClientPool worker_pool_{[this](const auto &) { return worker_; }};
  pubsub::GcsPublisher publisher_{std::make_unique<pubsub::FakePublisher>()};
  pubsub::ObservabilityPublisher observability_publisher_{
      std::make_unique<pubsub::FakePublisher>()};
  GcsTableStorage storage_{std::make_unique<InMemoryStoreClient>()};
  std::unique_ptr<GcsNodeManager> nodes_;
  std::unique_ptr<LeaseScheduler> scheduler_;
  std::shared_ptr<rpc::GcsNodeInfo> node_;
  std::shared_ptr<GcsActor> actor_;
  WorkerID w1_ = WorkerID::FromRandom();
  WorkerID w2_ = WorkerID::FromRandom();
  int successes_ = 0;
  int failures_ = 0;
};

TEST_F(GcsActorSchedulerLeaseTest, AdoptionCancelsExactReplacement) {
  PrepareReplacement();
  ASSERT_FALSE(HasFatalFailure());
  Finish(0, w1_, 1);
  EXPECT_EQ(raylet_->cancellations,
            (std::vector<LeaseID>{raylet_->Id(0), raylet_->Id(1)}));
  EXPECT_TRUE(raylet_->kills.empty());
  ASSERT_TRUE(raylet_->requests[1].ready);
  EXPECT_TRUE(raylet_->requests[1].reply.canceled());
  raylet_->Deliver(1);
  Drain();
  EXPECT_EQ(actor_->GetWorkerID(), w1_);
  EXPECT_EQ(worker_->creations.size(), 1u);
  EXPECT_EQ(successes_, 1);
  EXPECT_EQ(failures_, 0);
}

TEST_F(GcsActorSchedulerLeaseTest, GrantCrossingCancellationCleansOnlyUnusedWorker) {
  PrepareReplacement();
  ASSERT_FALSE(HasFatalFailure());
  raylet_->delay_cancellation = true;
  Finish(0, w1_, 1);
  ASSERT_EQ(raylet_->cancellations.size(), 2u);
  EXPECT_EQ(raylet_->cancellations[1], raylet_->Id(1));
  // L2 grants after cancellation was sent but before raylet processes it.
  raylet_->Grant(1, *node_, w2_);
  raylet_->ProcessCancel(1);
  raylet_->Deliver(1);
  Drain();
  ASSERT_EQ(raylet_->kills.size(), 1u);
  EXPECT_EQ(raylet_->kills[0].worker_id(), w2_.Binary());
  EXPECT_EQ(raylet_->kills[0].intended_actor_id(), ActorID::Nil().Binary());
  EXPECT_TRUE(raylet_->kills[0].force_kill());
  EXPECT_EQ(actor_->GetWorkerID(), w1_);
  EXPECT_EQ(worker_->creations.size(), 1u);
}

class RetiredLeaseReplyTest : public GcsActorSchedulerLeaseTest,
                              public ::testing::WithParamInterface<int> {};

TEST_P(RetiredLeaseReplyTest, CannotAffectNewAttempt) {
  PrepareReplacement();
  ASSERT_FALSE(HasFatalFailure());
  if (GetParam() == 0) {
    raylet_->Grant(1, *node_, w2_);  // Grant before cancellation is sent.
  }
  Finish(0, w1_, 1);
  // Accepted W1 subsequently dies; the manager starts the next incarnation.
  Restart(false);
  ASSERT_EQ(raylet_->requests.size(), 3u);
  const auto l3 = raylet_->Id(2);
  const auto cancellations = raylet_->cancellations;
  raylet_->Deliver(1, GetParam() == 2 ? Status::IOError("transport") : Status::OK());
  Drain();
  EXPECT_EQ(raylet_->cancellations, cancellations);
  EXPECT_EQ(raylet_->requests.size(), 3u);
  EXPECT_EQ(failures_, 0);
  EXPECT_TRUE(
      scheduler_->IsLeasing(NodeID::FromBinary(node_->node_id()), actor_->GetActorID()));
  EXPECT_EQ(actor_->GetLeaseSpecification().LeaseId(), l3);
  EXPECT_TRUE(actor_->GetWorkerID().IsNil());
  EXPECT_EQ(worker_->creations.size(), 1u);
  if (GetParam() == 0) {
    ASSERT_EQ(raylet_->kills.size(), 1u);
    EXPECT_EQ(raylet_->kills[0].worker_id(), w2_.Binary());
    EXPECT_EQ(raylet_->kills[0].intended_actor_id(), ActorID::Nil().Binary());
  } else {
    EXPECT_TRUE(raylet_->kills.empty());
  }
  auto w3 = WorkerID::FromRandom();
  raylet_->Grant(2, *node_, w3);
  Finish(2, w3, 2);
  EXPECT_EQ(worker_->creations.size(), 2u);
  EXPECT_EQ(successes_, 2);
}
INSTANTIATE_TEST_SUITE_P(GrantCanceledTransportError,
                         RetiredLeaseReplyTest,
                         ::testing::Values(0, 1, 2));

TEST_F(GcsActorSchedulerLeaseTest, ObsoleteRetryTimerCannotIssueLease) {
  const auto obsolete_lease_id = actor_->GetLeaseSpecification().LeaseId();
  const auto obsolete_attempt =
      actor_->GetCreationTaskSpecification().GetMessage().attempt_number();
  raylet_->Deliver(0, Status::IOError("transport"));
  Restart(true);  // The ready retry timer has not run yet.
  ASSERT_NE(actor_->GetLeaseSpecification().LeaseId(), obsolete_lease_id);
  ASSERT_NE(actor_->GetCreationTaskSpecification().GetMessage().attempt_number(),
            obsolete_attempt);
  ASSERT_EQ(raylet_->requests.size(), 2u);
  const auto handlers = RunUntilIdle();
  ASSERT_GT(handlers, 0u);
  ASSERT_EQ(raylet_->requests.size(), 2u);
  EXPECT_EQ(actor_->GetLeaseSpecification().LeaseId(), raylet_->Id(1));
  raylet_->Grant(1, *node_, w2_);
  Finish(1, w2_, 1);
  EXPECT_EQ(worker_->creations.size(), 1u);
}

TEST_F(GcsActorSchedulerLeaseTest, CurrentRequestStillRetries) {
  raylet_->Deliver(0, Status::IOError("transport"));
  ASSERT_EQ(raylet_->requests.size(), 1u);
  const auto handlers = RunUntil([this] { return raylet_->requests.size() == 2; });
  ASSERT_GT(handlers, 0u);
  ASSERT_EQ(raylet_->requests.size(), 2u);
  EXPECT_NE(raylet_->Id(0), raylet_->Id(1));
  raylet_->Grant(1, *node_, w1_);
  Finish(1, w1_, 0);
  EXPECT_EQ(worker_->creations.size(), 1u);
  EXPECT_TRUE(raylet_->kills.empty());
}

TEST_F(GcsActorSchedulerLeaseTest, CurrentGrantCreatesOnce) {
  raylet_->Grant(0, *node_, w1_);
  Finish(0, w1_, 0);
  EXPECT_TRUE(raylet_->cancellations.empty());
  EXPECT_TRUE(raylet_->kills.empty());
  EXPECT_EQ(worker_->creations.size(), 1u);
  EXPECT_EQ(successes_, 1);
}

}  // namespace
}  // namespace ray::gcs
