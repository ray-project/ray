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

#include <ray/gcs/gcs_server/test/gcs_test_util.h>
#include <ray/gcs/redis_gcs_client.h>

#include <memory>
#include "gtest/gtest.h"

namespace ray {
class GcsActorSchedulerTest : public RedisServiceManagerForTest {
 public:
  explicit GcsActorSchedulerTest(size_t node_count = 1)
      : node_count_(node_count), to_be_scheduled_actors_(actor_count_) {}

  void SetUp() override {
    std::promise<bool> initialized;
    io_thread_.reset(new std::thread([this, &initialized] {
      Init();
      initialized.set_value(true);
      boost::asio::io_context::work worker(io_service_);
      io_service_.run();
    }));
    Mocker::WaitForReady(initialized.get_future(), timeout_ms_);
  }

  void TearDown() override {
    io_service_.stop();
    io_thread_->join();
  }

 protected:
  void Init() {
    gcs::GcsClientOptions options("127.0.0.1", REDIS_SERVER_PORT, "", true);
    redis_gcs_client_ = std::make_shared<gcs::RedisGcsClient>(options);
    RAY_CHECK_OK(redis_gcs_client_->Connect(io_service_));

    gcs_node_manager_ =
        std::make_shared<gcs::GcsNodeManager>(io_service_, redis_gcs_client_);

    gcs_actor_scheduler_ = std::make_shared<Mocker::MockedGcsActorScheduler>(
        io_service_, redis_gcs_client_, *gcs_node_manager_);
    // Set schedule failed handler.
    gcs_actor_scheduler_->SetScheduleFailureHandler(
        [this](std::shared_ptr<gcs::GcsActor> actor) {
          failed_actors_.emplace_back(std::move(actor));
          TryNotify();
        });

    // Set schedule success handler.
    gcs_actor_scheduler_->SetScheduleSuccessHandler(
        [this](std::shared_ptr<gcs::GcsActor> actor) {
          succeed_actors_.emplace_back(std::move(actor));
          TryNotify();
        });

    // Init actors.
    JobID job_id = JobID::FromInt(1);
    for (size_t i = 0; i < actor_count_; ++i) {
      auto actor_table_data = Mocker::GenActorTableData(job_id);
      to_be_scheduled_actors_[i] =
          std::make_shared<gcs::GcsActor>(std::move(*actor_table_data));
    }

    // Init nodes.
    for (size_t i = 0; i < node_count_; ++i) {
      gcs_node_manager_->AddNode(Mocker::GenNodeInfo());
    }
  }

  void Run() {
    FlushAll();

    // Schedule asynchronously.
    io_service_.post([this] {
      for (auto &actor : to_be_scheduled_actors_) {
        gcs_actor_scheduler_->Schedule(actor);
      }
    });
  }

  void Wait() { Mocker::WaitForReady(promise_.get_future(), timeout_ms_); }

  void RunAndWait() {
    Run();
    Wait();
  }

  void CancelOnNode(ClientID node_id) {
    auto actor_ids = gcs_actor_scheduler_->CancelOnNode(node_id);
    canceled_actors_.insert(canceled_actors_.end(), actor_ids.begin(), actor_ids.end());
    TryNotify();
  }

  void CancelOnWorker(ClientID node_id, WorkerID worker_id) {
    auto actor_id = gcs_actor_scheduler_->CancelOnWorker(node_id, worker_id);
    if (!actor_id.IsNil()) {
      canceled_actors_.emplace_back(actor_id);
      TryNotify();
    }
  }

  void TryNotify() {
    if (canceled_actors_.size() + succeed_actors_.size() + failed_actors_.size() ==
        actor_count_) {
      promise_.set_value(true);
    }
  }

  bool ElementsEqual(const std::vector<std::shared_ptr<gcs::GcsActor>> &expected,
                     const std::vector<std::shared_ptr<gcs::GcsActor>> &target) const {
    if (expected.size() != target.size()) {
      return false;
    }
    for (auto &actor : expected) {
      if (std::find(target.begin(), target.end(), actor) == target.end()) {
        return false;
      }
    }
    return true;
  }

 protected:
  const size_t actor_count_ = 100;
  size_t node_count_ = 1;

  boost::asio::io_service io_service_;
  std::shared_ptr<gcs::RedisGcsClient> redis_gcs_client_;
  std::shared_ptr<gcs::GcsNodeManager> gcs_node_manager_;
  std::shared_ptr<Mocker::MockedGcsActorScheduler> gcs_actor_scheduler_;
  std::unique_ptr<std::thread> io_thread_;
  // Timeout waiting for gcs server reply, default is 5s
  const uint64_t timeout_ms_ = 2000;

  std::vector<std::shared_ptr<gcs::GcsActor>> to_be_scheduled_actors_;
  std::vector<std::shared_ptr<gcs::GcsActor>> succeed_actors_;
  std::vector<std::shared_ptr<gcs::GcsActor>> failed_actors_;
  std::vector<ActorID> canceled_actors_;
  std::promise<bool> promise_;
};

class ZeroNodeTest : public GcsActorSchedulerTest {
 public:
  ZeroNodeTest() : GcsActorSchedulerTest(0) {}
};

class OneNodeTest : public GcsActorSchedulerTest {
 public:
  OneNodeTest() : GcsActorSchedulerTest(1) {}
};

class TwoNodeTest : public GcsActorSchedulerTest {
 public:
  TwoNodeTest() : GcsActorSchedulerTest(2) {}
};

TEST_F(ZeroNodeTest, GcsActorSchedulerTest) {
  RunAndWait();

  ASSERT_EQ(0, succeed_actors_.size());
  ASSERT_TRUE(ElementsEqual(to_be_scheduled_actors_, failed_actors_));
}

TEST_F(OneNodeTest, TestLeaseWorkerSuccess) {
  Mocker::Reset();

  Mocker::mock_lease_worker = [](std::shared_ptr<gcs::GcsActor> actor,
                                 std::shared_ptr<rpc::GcsNodeInfo> node,
                                 rpc::RequestWorkerLeaseReply *reply) {
    reply->mutable_worker_address()->set_raylet_id(node->node_id());
    reply->mutable_worker_address()->set_worker_id(WorkerID::FromRandom().Binary());
    return true;
  };

  Mocker::mock_create_actor = Mocker::default_actor_creator;

  RunAndWait();

  ASSERT_EQ(0, failed_actors_.size());
  ASSERT_TRUE(ElementsEqual(to_be_scheduled_actors_, succeed_actors_));

  std::unordered_set<ClientID> nodes;
  for (auto &actor : succeed_actors_) {
    nodes.emplace(actor->GetNodeID());
  }
  ASSERT_EQ(1, nodes.size());
}

TEST_F(TwoNodeTest, TestLeaseWorkerSuccess) {
  Mocker::Reset();

  Mocker::mock_lease_worker = [](std::shared_ptr<gcs::GcsActor> actor,
                                 std::shared_ptr<rpc::GcsNodeInfo> node,
                                 rpc::RequestWorkerLeaseReply *reply) {
    reply->mutable_worker_address()->set_raylet_id(node->node_id());
    reply->mutable_worker_address()->set_worker_id(WorkerID::FromRandom().Binary());
    return true;
  };

  Mocker::mock_create_actor = Mocker::default_actor_creator;

  RunAndWait();

  ASSERT_EQ(0, failed_actors_.size());
  ASSERT_TRUE(ElementsEqual(to_be_scheduled_actors_, succeed_actors_));

  std::unordered_set<ClientID> nodes;
  for (auto &actor : succeed_actors_) {
    nodes.emplace(actor->GetNodeID());
  }
  // Check all actors are created on the two nodes.
  ASSERT_EQ(2, nodes.size());
}

TEST_F(TwoNodeTest, TestLeaseWorkerSpillbackToOneNode) {
  Mocker::Reset();

  auto &nodes = gcs_node_manager_->GetAllAliveNodes();
  ASSERT_EQ(2, nodes.size());

  auto iter = nodes.begin();
  auto under_resourced_node_id = iter->first.Binary();
  auto well_resourced_node_id = (++iter)->first.Binary();

  Mocker::mock_lease_worker = [under_resourced_node_id, well_resourced_node_id](
                                  std::shared_ptr<gcs::GcsActor> actor,
                                  std::shared_ptr<rpc::GcsNodeInfo> node,
                                  rpc::RequestWorkerLeaseReply *reply) {
    if (node->node_id() == under_resourced_node_id) {
      reply->mutable_retry_at_raylet_address()->set_raylet_id(well_resourced_node_id);
    } else {
      reply->mutable_worker_address()->set_raylet_id(node->node_id());
      reply->mutable_worker_address()->set_worker_id(WorkerID::FromRandom().Binary());
    }
    return true;
  };

  Mocker::mock_create_actor = Mocker::default_actor_creator;

  RunAndWait();

  ASSERT_EQ(0, failed_actors_.size());
  ASSERT_TRUE(ElementsEqual(to_be_scheduled_actors_, succeed_actors_));

  for (auto &actor : succeed_actors_) {
    ASSERT_EQ(well_resourced_node_id, actor->GetNodeID().Binary());
  }
}

TEST_F(TwoNodeTest, TestFailedToLeaseWorkerFromAllNodes) {
  Mocker::Reset();

  Mocker::mock_lease_worker = [](std::shared_ptr<gcs::GcsActor> actor,
                                 std::shared_ptr<rpc::GcsNodeInfo> node,
                                 rpc::RequestWorkerLeaseReply *reply) { return false; };

  Mocker::mock_create_actor = Mocker::default_actor_creator;

  RunAndWait();

  ASSERT_EQ(0, succeed_actors_.size());
  ASSERT_EQ(0, failed_actors_.size());
}

TEST_F(TwoNodeTest, TestOneNodeFailed) {
  Mocker::Reset();

  auto &nodes = gcs_node_manager_->GetAllAliveNodes();
  ASSERT_EQ(2, nodes.size());

  auto iter = nodes.begin();
  auto failed_node_id = iter->first;

  Mocker::mock_lease_worker = [failed_node_id](std::shared_ptr<gcs::GcsActor> actor,
                                               std::shared_ptr<rpc::GcsNodeInfo> node,
                                               rpc::RequestWorkerLeaseReply *reply) {
    auto node_id = ClientID::FromBinary(node->node_id());
    if (node_id == failed_node_id) {
      return false;
    }
    reply->mutable_worker_address()->set_raylet_id(node->node_id());
    reply->mutable_worker_address()->set_worker_id(WorkerID::FromRandom().Binary());
    return true;
  };

  Mocker::mock_create_actor = Mocker::default_actor_creator;

  Run();

  execute_after(io_service_,
                [this, failed_node_id] {
                  gcs_node_manager_->RemoveNode(failed_node_id);
                  CancelOnNode(failed_node_id);
                },
                500);

  Wait();

  ASSERT_EQ(0, failed_actors_.size());
  ASSERT_NE(0, succeed_actors_.size());
  ASSERT_NE(0, canceled_actors_.size());
  ASSERT_EQ(actor_count_, canceled_actors_.size() + succeed_actors_.size());
}

TEST_F(TwoNodeTest, TestTwoNodeFailed) {
  Mocker::Reset();

  Mocker::mock_lease_worker = [](std::shared_ptr<gcs::GcsActor> actor,
                                 std::shared_ptr<rpc::GcsNodeInfo> node,
                                 rpc::RequestWorkerLeaseReply *reply) { return false; };

  Mocker::mock_create_actor = Mocker::default_actor_creator;

  Run();

  execute_after(io_service_,
                [this] {
                  auto nodes = gcs_node_manager_->GetAllAliveNodes();
                  for (auto &node : nodes) {
                    gcs_node_manager_->RemoveNode(node.first);
                    CancelOnNode(node.first);
                  }
                },
                500);

  Wait();

  ASSERT_EQ(0, failed_actors_.size());
  ASSERT_EQ(0, succeed_actors_.size());
  ASSERT_EQ(actor_count_, canceled_actors_.size());
}

TEST_F(TwoNodeTest, TestWorkerFailed) {
  Mocker::Reset();

  Mocker::mock_lease_worker = [](std::shared_ptr<gcs::GcsActor> actor,
                                 std::shared_ptr<rpc::GcsNodeInfo> node,
                                 rpc::RequestWorkerLeaseReply *reply) {
    auto worker_id = WorkerID::FromRandom();
    reply->mutable_worker_address()->set_raylet_id(node->node_id());
    reply->mutable_worker_address()->set_worker_id(worker_id.Binary());
    return true;
  };

  Mocker::mock_create_actor = [](const TaskSpecification &actor_creation_task,
                                 const std::function<void()> &on_done) {
    if (actor_creation_task.ActorCreationId().Hash() % 2 == 0) {
      return;
    }
    on_done();
  };

  Run();

  execute_after(io_service_,
                [this] {
                  auto node_to_workers =
                      gcs_actor_scheduler_->GetNodeToWorkersInPhaseOfCreating();
                  ASSERT_NE(0, node_to_workers.size());
                  for (auto &node_entry : node_to_workers) {
                    for (auto &worker_entry : node_entry.second) {
                      if (worker_entry.second->GetAssignedActorID().Hash() % 2 == 0) {
                        CancelOnWorker(node_entry.first, worker_entry.first);
                      }
                    }
                  }
                },
                500);

  Wait();

  ASSERT_EQ(0, failed_actors_.size());
  ASSERT_NE(0, succeed_actors_.size());
  ASSERT_EQ(actor_count_, canceled_actors_.size() + succeed_actors_.size());
}
}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  RAY_CHECK(argc == 4);
  ray::REDIS_SERVER_EXEC_PATH = argv[1];
  ray::REDIS_CLIENT_EXEC_PATH = argv[2];
  ray::REDIS_MODULE_LIBRARY_PATH = argv[3];
  return RUN_ALL_TESTS();
}
