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

#include <memory>
#include "gtest/gtest.h"

namespace ray {
class GcsActorManagerTest : public ::testing::Test {
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
    gcs_node_manager_ = std::make_shared<gcs::GcsNodeManager>(
        io_service_, node_info_accessor_, error_info_accessor_);
    gcs_actor_manager_ = std::make_shared<gcs::GcsActorManager>(
        io_service_, actor_info_accessor_, *gcs_node_manager_,
        [](const rpc::Address &address) {
          return std::make_shared<Mocker::MockedWorkerLeaseImpl>(address);
        },
        [](const rpc::Address &address) {
          return std::make_shared<Mocker::MockedCoreWorkerClientImpl>(address);
        });
    // Init nodes.
    for (size_t i = 0; i < node_count_; ++i) {
      gcs_node_manager_->AddNode(Mocker::GenNodeInfo());
    }
  }

  void Run() {
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
  Mocker::MockedNodeInfoAccessor node_info_accessor_;
  Mocker::MockedErrorInfoAccessor error_info_accessor_;
  Mocker::MockedActorInfoAccessor actor_info_accessor_;
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
  Mocker::Reset();

  Run();
  WaitForCondition(nullptr);

  auto &actors = gcs_actor_manager_->GetAllRegisteredActors();
  for (auto &entry : actors) {
    ASSERT_TRUE(entry.second->GetNodeID().IsNil());
    ASSERT_TRUE(entry.second->GetWorkerID().IsNil());
  }
}

TEST_F(ZeroNodeTest, TestDuplicateRegisteration) {
  std::promise<bool> promise;
  io_service_.post([this, &promise] {
    const auto &actor_to_register_callbacks = gcs_actor_manager_->GetRegisterCallbacks();
    const auto &registerd_actors = gcs_actor_manager_->GetAllRegisteredActors();

    int flushed_count = 0;
    gcs::StatusCallback on_flush_success;
    Mocker::async_update_delegate =
        [&on_flush_success, &flushed_count](
            const ActorID &actor_id, const std::shared_ptr<rpc::ActorTableData> &data_ptr,
            const gcs::StatusCallback &callback) {
          on_flush_success = callback;
          ++flushed_count;
          return ray::Status::OK();
        };

    int callback_invoked_count = 0;
    JobID job_id = JobID::FromInt(1);
    auto create_actor_request = Mocker::GenCreateActorRequest(job_id);
    auto actor_id = ActorID::FromBinary(
        create_actor_request.task_spec().actor_creation_task_spec().actor_id());

    for (int i = 0; i < 10; ++i) {
      gcs_actor_manager_->RegisterActor(
          create_actor_request,
          [&callback_invoked_count](const std::shared_ptr<gcs::GcsActor> &) {
            ++callback_invoked_count;
          });
    }
    ASSERT_EQ(1, flushed_count);
    ASSERT_EQ(0, callback_invoked_count);
    ASSERT_EQ(1, actor_to_register_callbacks.size());
    ASSERT_EQ(10, actor_to_register_callbacks.at(actor_id).size());
    ASSERT_EQ(0, registerd_actors.size());
    ASSERT_NE(nullptr, on_flush_success);

    on_flush_success(ray::Status::OK());
    ASSERT_EQ(1, flushed_count);
    ASSERT_EQ(10, callback_invoked_count);
    ASSERT_EQ(0, actor_to_register_callbacks.size());
    ASSERT_EQ(1, registerd_actors.size());

    gcs_actor_manager_->RegisterActor(
        create_actor_request,
        [&callback_invoked_count](const std::shared_ptr<gcs::GcsActor> &) {
          ++callback_invoked_count;
        });
    ASSERT_EQ(1, flushed_count);
    ASSERT_EQ(11, callback_invoked_count);
    ASSERT_EQ(0, actor_to_register_callbacks.size());
    ASSERT_EQ(1, gcs_actor_manager_->GetAllRegisteredActors().size());

    promise.set_value(true);
  });
  Mocker::WaitForReady(promise.get_future(), 3000);
}

TEST_F(OneNodeTest, TestLeaseWorkerSuccess) {
  Mocker::Reset();

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

  Mocker::worker_lease_delegate = [under_resourced_node_id, well_resourced_node_id](
                                      const rpc::Address &node_address,
                                      const TaskSpecification &task_spec,
                                      rpc::RequestWorkerLeaseReply *reply) {
    if (node_address.raylet_id() == under_resourced_node_id) {
      reply->mutable_retry_at_raylet_address()->set_raylet_id(well_resourced_node_id);
    } else {
      reply->mutable_worker_address()->set_raylet_id(well_resourced_node_id);
      reply->mutable_worker_address()->set_worker_id(WorkerID::FromRandom().Binary());
    }
    return Status::OK();
  };

  Run();
  ASSERT_TRUE(WaitForCondition([this] { return IsAllRegisteredActorsAlive(); }));

  auto &actors = gcs_actor_manager_->GetAllRegisteredActors();
  for (auto &entry : actors) {
    ASSERT_EQ(well_resourced_node_id, entry.second->GetNodeID().Binary());
    ASSERT_FALSE(entry.second->GetWorkerID().IsNil());
  }
}

TEST_F(TwoNodeTest, TestNetworkErrorOnAllNodes) {
  Mocker::Reset();

  Mocker::worker_lease_delegate =
      [](const rpc::Address &node_address, const TaskSpecification &task_spec,
         rpc::RequestWorkerLeaseReply *reply) { return Status::IOError("IO Error!"); };

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

  Mocker::worker_lease_delegate = [failed_node_id](const rpc::Address &node_address,
                                                   const TaskSpecification &task_spec,
                                                   rpc::RequestWorkerLeaseReply *reply) {
    auto node_id = ClientID::FromBinary(node_address.raylet_id());
    if (node_id == failed_node_id) {
      return Status::IOError("IO Error!");
    }
    reply->mutable_worker_address()->set_raylet_id(node_address.raylet_id());
    reply->mutable_worker_address()->set_worker_id(WorkerID::FromRandom().Binary());
    return Status::OK();
  };

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

  Mocker::worker_lease_delegate =
      [](const rpc::Address &node_address, const TaskSpecification &task_spec,
         rpc::RequestWorkerLeaseReply *reply) { return Status::IOError("IO Error!"); };

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

TEST_F(TwoNodeTest, TestPartialWorkerFailed) {
  Mocker::Reset();
  Run();

  std::vector<std::pair<ClientID, WorkerID>> to_be_killed;
  execute_after(io_service_,
                [this, &to_be_killed] {
                  auto &actors = gcs_actor_manager_->GetAllRegisteredActors();
                  for (auto &entry : actors) {
                    ASSERT_EQ(rpc::ActorTableData::ALIVE, entry.second->GetState());
                    auto node_id = entry.second->GetNodeID();
                    auto worker_id = entry.second->GetWorkerID();
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
  return RUN_ALL_TESTS();
}
