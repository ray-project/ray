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
class GcsActorSchedulerTest : public ::testing::Test {
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
    gcs_node_manager_ = std::make_shared<gcs::GcsNodeManager>(
        io_service_, node_info_accessor_, error_info_accessor_);

    gcs_actor_scheduler_ = std::make_shared<gcs::GcsActorScheduler>(
        io_service_, actor_info_accessor_, *gcs_node_manager_,
        [this](std::shared_ptr<gcs::GcsActor> actor) {
          actor->UpdateAddress(rpc::Address());
          actor->UpdateState(rpc::ActorTableData::RECONSTRUCTING);
          failed_actors_.emplace_back(std::move(actor));
          TryNotify();
        },
        [this](std::shared_ptr<gcs::GcsActor> actor) {
          actor->UpdateState(rpc::ActorTableData::ALIVE);
          succeed_actors_.emplace_back(std::move(actor));
          TryNotify();
        },
        [](const rpc::Address &address) {
          return std::make_shared<Mocker::MockedWorkerLeaseImpl>(address);
        },
        [](const rpc::Address &address) {
          return std::make_shared<Mocker::MockedCoreWorkerClientImpl>(address);
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
  Mocker::MockedNodeInfoAccessor node_info_accessor_;
  Mocker::MockedErrorInfoAccessor error_info_accessor_;
  Mocker::MockedActorInfoAccessor actor_info_accessor_;
  std::shared_ptr<gcs::GcsNodeManager> gcs_node_manager_;
  std::shared_ptr<gcs::GcsActorScheduler> gcs_actor_scheduler_;
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
  Mocker::Reset();
  RunAndWait();

  ASSERT_EQ(0, succeed_actors_.size());
  ASSERT_TRUE(ElementsEqual(to_be_scheduled_actors_, failed_actors_));
}

TEST_F(OneNodeTest, TestLeaseWorkerSuccess) {
  Mocker::Reset();
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

  RunAndWait();

  ASSERT_EQ(0, failed_actors_.size());
  ASSERT_TRUE(ElementsEqual(to_be_scheduled_actors_, succeed_actors_));

  for (auto &actor : succeed_actors_) {
    ASSERT_EQ(well_resourced_node_id, actor->GetNodeID().Binary());
  }
}

TEST_F(TwoNodeTest, TestNetworkErrorOnAllNodes) {
  Mocker::Reset();

  Mocker::worker_lease_delegate =
      [](const rpc::Address &node_address, const TaskSpecification &task_spec,
         rpc::RequestWorkerLeaseReply *reply) { return Status::IOError("IO Error!"); };

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
                [this, failed_node_id] {
                  ASSERT_NE(0, succeed_actors_.size());
                  ASSERT_NE(actor_count_, succeed_actors_.size());
                  ASSERT_EQ(0, failed_actors_.size());
                  ASSERT_EQ(0, canceled_actors_.size());
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

  Mocker::worker_lease_delegate =
      [](const rpc::Address &node_address, const TaskSpecification &task_spec,
         rpc::RequestWorkerLeaseReply *reply) { return Status::IOError("IO Error!"); };

  Run();

  execute_after(io_service_,
                [this] {
                  ASSERT_EQ(0, succeed_actors_.size());
                  ASSERT_EQ(0, failed_actors_.size());
                  ASSERT_EQ(0, canceled_actors_.size());
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

TEST_F(TwoNodeTest, TestPartialWorkerFailed) {
  Mocker::Reset();

  Mocker::push_normal_task_delegate = [](const rpc::Address &worker_address,
                                         std::unique_ptr<rpc::PushTaskRequest> request,
                                         rpc::PushTaskReply *reply) {
    auto actor_id =
        ActorID::FromBinary(request->task_spec().actor_creation_task_spec().actor_id());
    if (actor_id.Hash() % 2 == 0) {
      return Status::IOError("IO Error!");
    }
    return Status::OK();
  };

  Run();

  execute_after(io_service_,
                [this] {
                  auto node_to_workers =
                      gcs_actor_scheduler_->GetWorkersInPhaseOfCreating();
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
  return RUN_ALL_TESTS();
}
