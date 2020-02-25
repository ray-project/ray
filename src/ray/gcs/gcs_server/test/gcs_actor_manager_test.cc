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
class GcsActorManagerTest : public RedisServiceManagerForTest {
 public:
  explicit GcsActorManagerTest(int node_count = 1) : node_count_(node_count) {}

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
    gcs_actor_manager_ = std::make_shared<gcs::GcsActorManager>(
        redis_gcs_client_, *gcs_node_manager_,
        std::unique_ptr<gcs::GcsActorScheduler>(new Mocker::MockedGcsActorScheduler(
            io_service_, redis_gcs_client_, *gcs_node_manager_)));
    // Init nodes.
    for (size_t i = 0; i < node_count_; ++i) {
      gcs_node_manager_->AddNode(Mocker::GenNodeInfo());
    }
  }

  void Run() {
    FlushAll();

    // Schedule asynchronously.
    io_service_.post([this] {
      JobID job_id = JobID::FromInt(1);
      for (size_t i = 0; i < actor_count_; ++i) {
        gcs_actor_manager_->RegisterActor(Mocker::GenCreateActorRequest(job_id),
                                          [](std::shared_ptr<gcs::GcsActor>) {});
      }
    });
  }

  bool WaitForCondition(std::function<bool()> condition) {
    AsyncCheck(condition, 50);
    return Mocker::WaitForReady(promise_.get_future(), timeout_ms_);
  }

  void AsyncCheck(std::function<bool()> condition, uint32_t interval_ms) {
    if (condition == nullptr) {
      return;
    }
    execute_after(io_service_,
                  [this, condition, interval_ms] {
                    if (condition()) {
                      promise_.set_value(true);
                    } else {
                      AsyncCheck(condition, interval_ms);
                    }
                  },
                  interval_ms);
  }

  bool IsAllRegisteredActorsAlive() {
    auto &actors = gcs_actor_manager_->GetAllRegisteredActors();
    for (auto &entry : actors) {
      if (entry.second->GetState() != rpc::ActorTableData::ALIVE) {
        return false;
      }
    }
    return true;
  }

 protected:
  const size_t actor_count_ = 100;
  size_t node_count_ = 1;

  // Timeout waiting for gcs server reply, default is 5s
  const uint64_t timeout_ms_ = 2000;
  std::promise<bool> promise_;

  boost::asio::io_service io_service_;
  std::shared_ptr<gcs::RedisGcsClient> redis_gcs_client_;
  std::shared_ptr<gcs::GcsNodeManager> gcs_node_manager_;
  std::shared_ptr<gcs::GcsActorManager> gcs_actor_manager_;
  std::unique_ptr<std::thread> io_thread_;
};

class ZeroNodeTest : public GcsActorManagerTest {
 public:
  ZeroNodeTest() : GcsActorManagerTest(0) {}
};

class OneNodeTest : public GcsActorManagerTest {
 public:
  OneNodeTest() : GcsActorManagerTest(1) {}
};

class TwoNodeTest : public GcsActorManagerTest {
 public:
  TwoNodeTest() : GcsActorManagerTest(2) {}
};

TEST_F(ZeroNodeTest, TestSchedule) {
  Run();
  WaitForCondition(nullptr);

  auto &actors = gcs_actor_manager_->GetAllRegisteredActors();
  for (auto &entry : actors) {
    ASSERT_TRUE(entry.second->GetNodeID().IsNil());
    ASSERT_TRUE(entry.second->GetWorkerID().IsNil());
  }
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

  Run();
  ASSERT_TRUE(WaitForCondition([this] { return IsAllRegisteredActorsAlive(); }));

  auto &actors = gcs_actor_manager_->GetAllRegisteredActors();
  for (auto &entry : actors) {
    ASSERT_FALSE(entry.second->GetNodeID().IsNil());
    ASSERT_FALSE(entry.second->GetWorkerID().IsNil());
  }
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

  Run();
  ASSERT_TRUE(WaitForCondition([this] { return IsAllRegisteredActorsAlive(); }));

  std::unordered_set<ClientID> nodes;
  auto &actors = gcs_actor_manager_->GetAllRegisteredActors();
  for (auto &entry : actors) {
    ASSERT_FALSE(entry.second->GetNodeID().IsNil());
    ASSERT_FALSE(entry.second->GetWorkerID().IsNil());
    nodes.emplace(entry.second->GetNodeID());
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

  Run();
  ASSERT_TRUE(WaitForCondition([this] { return IsAllRegisteredActorsAlive(); }));

  auto &actors = gcs_actor_manager_->GetAllRegisteredActors();
  for (auto &entry : actors) {
    ASSERT_EQ(well_resourced_node_id, entry.second->GetNodeID().Binary());
    ASSERT_FALSE(entry.second->GetWorkerID().IsNil());
  }
}

TEST_F(TwoNodeTest, TestFailedToLeaseWorkerFromAllNodes) {
  Mocker::Reset();

  Mocker::mock_lease_worker = [](std::shared_ptr<gcs::GcsActor> actor,
                                 std::shared_ptr<rpc::GcsNodeInfo> node,
                                 rpc::RequestWorkerLeaseReply *reply) { return false; };

  Mocker::mock_create_actor = Mocker::default_actor_creator;

  Run();
  ASSERT_FALSE(WaitForCondition([this] { return IsAllRegisteredActorsAlive(); }));

  auto &actors = gcs_actor_manager_->GetAllRegisteredActors();
  for (auto &entry : actors) {
    ASSERT_NE(rpc::ActorTableData::ALIVE, entry.second->GetState());
    ASSERT_TRUE(entry.second->GetWorkerID().IsNil());
  }
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
                [this, failed_node_id] { gcs_node_manager_->RemoveNode(failed_node_id); },
                500);

  ASSERT_TRUE(WaitForCondition([this] {
    return gcs_node_manager_->GetAllAliveNodes().size() == 1 &&
           IsAllRegisteredActorsAlive();
  }));

  auto &actors = gcs_actor_manager_->GetAllRegisteredActors();
  for (auto &entry : actors) {
    ASSERT_NE(failed_node_id, entry.second->GetNodeID());
  }
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
                  }
                },
                500);

  ASSERT_TRUE(
      WaitForCondition([this] { return gcs_node_manager_->GetAllAliveNodes().empty(); }));

  auto &actors = gcs_actor_manager_->GetAllRegisteredActors();
  for (auto &entry : actors) {
    ASSERT_NE(rpc::ActorTableData::ALIVE, entry.second->GetState());
    ASSERT_TRUE(entry.second->GetNodeID().IsNil());
    ASSERT_TRUE(entry.second->GetWorkerID().IsNil());
  }
}

TEST_F(TwoNodeTest, TestWorkerFailed) {
  Mocker::Reset();

  Mocker::mock_lease_worker = [](std::shared_ptr<gcs::GcsActor> actor,
                                 std::shared_ptr<rpc::GcsNodeInfo> node,
                                 rpc::RequestWorkerLeaseReply *reply) {
    reply->mutable_worker_address()->set_raylet_id(node->node_id());
    reply->mutable_worker_address()->set_worker_id(WorkerID::FromRandom().Binary());
    return true;
  };

  Mocker::mock_create_actor = Mocker::default_actor_creator;

  Run();

  std::vector<std::pair<ClientID, WorkerID>> to_be_killed;
  execute_after(io_service_,
                [this, &to_be_killed] {
                  auto &actors = gcs_actor_manager_->GetAllRegisteredActors();
                  for (auto &entry : actors) {
                    auto node_id = entry.second->GetNodeID();
                    auto worker_id = entry.second->GetWorkerID();
                    ASSERT_EQ(rpc::ActorTableData::ALIVE, entry.second->GetState());
                    ASSERT_FALSE(node_id.IsNil());
                    ASSERT_FALSE(worker_id.IsNil());
                    if (worker_id.Hash() % 2 == 0) {
                      to_be_killed.emplace_back(std::make_pair(node_id, worker_id));
                      gcs_actor_manager_->ReconstructActorOnWorker(node_id, worker_id);
                      ASSERT_EQ(rpc::ActorTableData::RECONSTRUCTING,
                                entry.second->GetState());
                    }
                  }
                },
                1000);

  ASSERT_TRUE(WaitForCondition([this] { return IsAllRegisteredActorsAlive(); }));

  auto &actors = gcs_actor_manager_->GetAllRegisteredActors();
  for (auto &entry : actors) {
    ASSERT_FALSE(entry.second->GetNodeID().IsNil());
    ASSERT_FALSE(entry.second->GetWorkerID().IsNil());
  }
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
