#include "ray/gcs/gcs_client.h"
#include <atomic>
#include <chrono>
#include <string>
#include <vector>
#include "gtest/gtest.h"

namespace ray {

namespace gcs {

class GcsClientTest : public ::testing::Test {
 public:
  GcsClientTest() {}

  virtual void SetUp() {
    InitClientOption();
    InitClientInfo();
    GenTestData();

    gcs_client_.reset(new GcsClient(option_, info_));
    RAY_CHECK_OK(gcs_client_->Connect());
  }

  virtual void TearDown() {
    gcs_client_->Disconnect();
    gcs_client_.reset();

    ClearTestData();
  }

 protected:
  void InitClientOption() {
    option_.server_list_.emplace_back(std::make_pair("127.0.0.1", 6379));
    option_.test_mode_ = true;
  }

  void InitClientInfo() {
    info_.type_ = ClientInfo::ClientType::kClientTypeRayletMonitor;
    info_.id_ = ClientID::FromRandom();
  }

  void GenTestData() {
    GenActorData();
    GenTaskData();
    GenNodeData();
  }

  void GenActorData() {
    for (size_t i = 0; i < 2; ++i) {
      std::shared_ptr<ActorTableData> actor = std::make_shared<ActorTableData>();
      ActorID actor_id = ActorID::FromRandom();
      actor->set_actor_id(actor_id.Binary());
      JobID job_id = JobID::FromRandom();
      actor->set_job_id(job_id.Binary());
      actor->set_state(ActorTableData::ALIVE);
      actor_datas_[actor_id] = actor;
    }
  }

  void GenTaskData() {
    for (size_t i = 0; i < 2; ++i) {
      std::shared_ptr<TaskTableData> task = std::make_shared<TaskTableData>();
      task->set_task("task" + std::to_string(i));
      TaskID task_id = TaskID::FromRandom();
      task_datas_[task_id] = task;
    }
  }

  void GenNodeData() {
    std::shared_ptr<ClientTableData> node = std::make_shared<ClientTableData>();
    node->set_client_id(info_.id_.Binary());
    node->set_node_manager_address("127.0.0.1:20000");
    node_datas_[info_.id_] = node;
  }

  void ClearTestData() {
    actor_datas_.clear();
    task_datas_.clear();
    node_datas_.clear();
  }

  void WaitPendingDone(std::chrono::milliseconds timeout) {
    WaitPendingDone(pending_count_, timeout);
  }

  void WaitPendingDone(std::atomic<int> &pending_count,
                       std::chrono::milliseconds timeout) {
    while (pending_count != 0 && timeout.count() > 0) {
      std::chrono::milliseconds interval(10);
      std::this_thread::sleep_for(interval);
      timeout -= interval;
    }
    EXPECT_EQ(pending_count, 0);
  }

 protected:
  ClientOption option_;
  ClientInfo info_;

  std::unique_ptr<GcsClient> gcs_client_;

  std::unordered_map<ActorID, std::shared_ptr<ActorTableData>> actor_datas_;
  std::unordered_map<TaskID, std::shared_ptr<TaskTableData>> task_datas_;
  std::unordered_map<ClientID, std::shared_ptr<ClientTableData>> node_datas_;

  std::atomic<int> pending_count_{0};
};

TEST_F(GcsClientTest, ActorStateAccessor) {
  ActorStateAccessor &actor_accessor = gcs_client_->Actors();
  size_t log_length = 0;
  // add
  for (const auto &elem : actor_datas_) {
    const auto &actor = elem.second;
    JobID job_id = JobID::FromBinary(actor->job_id());
    ++pending_count_;
    actor_accessor.AsyncAdd(job_id, elem.first, actor, log_length, [this](Status status) {
      RAY_CHECK_OK(status);
      --pending_count_;
    });
  }

  std::chrono::milliseconds timeout(10000);
  WaitPendingDone(timeout);

  // get
  for (const auto &elem : actor_datas_) {
    const auto &actor = elem.second;
    JobID job_id = JobID::FromBinary(actor->job_id());
    ++pending_count_;
    actor_accessor.AsyncGet(job_id, elem.first,
                            [this](Status status, std::vector<ActorTableData> datas) {
                              ASSERT_EQ(datas.size(), 1U);
                              ActorID actor_id = ActorID::FromBinary(datas[0].actor_id());
                              auto it = actor_datas_.find(actor_id);
                              ASSERT_TRUE(it != actor_datas_.end());
                              --pending_count_;
                            });
  }

  WaitPendingDone(timeout);
}

TEST_F(GcsClientTest, DISABLED_TaskStateAccessor) {
  // Task data is not stable yet(use both fbs and pb)
  TaskStateAccessor &task_accessor = gcs_client_->Tasks();
  // add
  for (const auto &elem : task_datas_) {
    const auto &task = elem.second;
    JobID job_id = JobID::FromRandom();
    ++pending_count_;
    task_accessor.AsyncAdd(job_id, elem.first, task, [this](Status status) {
      RAY_CHECK_OK(status);
      --pending_count_;
    });
  }

  std::chrono::milliseconds timeout(2000);
  WaitPendingDone(timeout);
}

TEST_F(GcsClientTest, NodeStateAccessor) {
  NodeStateAccessor &node_accessor = gcs_client_->Nodes();
  for (const auto &elem : node_datas_) {
    const auto &node = elem.second;
    RAY_CHECK_OK(node_accessor.Register(*node));
  }

  std::chrono::milliseconds wait_time(500);
  std::this_thread::sleep_for(wait_time);

  std::unordered_map<ClientID, ClientTableData> all_nodes;
  RAY_CHECK_OK(node_accessor.GetAll(&all_nodes));
  ASSERT_GE(all_nodes.size(), node_datas_.size());
  for (const auto &elem : node_datas_) {
    const auto it = all_nodes.find(elem.first);
    if (it == all_nodes.end()) {
      RAY_LOG(ERROR) << "GCS response timeout!";
    }
  }
}

TEST_F(GcsClientTest, ActorStateAccessor_Subscribe) {
  ActorStateAccessor &actor_accessor = gcs_client_->Actors();
  std::chrono::milliseconds timeout(10000);
  // sub
  std::atomic<int> sub_pending_count(0);
  std::atomic<int> do_sub_pending_count(0);
  auto subscribe = [this, &sub_pending_count](const ActorID &actor_id,
                                              std::vector<ActorTableData> datas) {
    const auto it = actor_datas_.find(actor_id);
    ASSERT_TRUE(it != actor_datas_.end());
    --sub_pending_count;
  };
  auto done = [&do_sub_pending_count](Status status) {
    RAY_CHECK_OK(status);
    --do_sub_pending_count;
  };

  ++do_sub_pending_count;
  actor_accessor.AsyncSubscribe(JobID::Nil(), ClientID::Nil(), subscribe, done);
  // wait do sub done
  WaitPendingDone(do_sub_pending_count, timeout);

  // add
  std::atomic<int> add_pending_count(0);
  size_t log_length = 0;
  for (const auto &elem : actor_datas_) {
    const auto &actor = elem.second;
    JobID job_id = JobID::FromBinary(actor->job_id());
    ++sub_pending_count;
    ++add_pending_count;
    actor_accessor.AsyncAdd(job_id, elem.first, actor, log_length,
                            [&add_pending_count](Status status) {
                              RAY_CHECK_OK(status);
                              --add_pending_count;
                            });
  }
  // wait add done
  WaitPendingDone(add_pending_count, timeout);

  // wait all sub notify
  WaitPendingDone(sub_pending_count, timeout);
}

}  // namespace gcs

}  // namespace ray
