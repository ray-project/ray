#include "ray/gcs/gcs_server/service_based_gcs_client.h"
#include "gtest/gtest.h"
#include "ray/gcs/gcs_server/service_based_accessor.h"
#include "ray/gcs/gcs_server/gcs_server.h"
#include "ray/rpc/gcs_server/gcs_rpc_client.h"
#include "ray/util/test_util.h"

namespace ray {

static std::string redis_server_executable;
static std::string redis_client_executable;
static std::string libray_redis_module_path;

class ServiceBasedGcsGcsClientTest : public RedisServiceManagerForTest {
 public:
  void SetUp() override {
    gcs::GcsServerConfig config;
    config.grpc_server_port = 0;
    config.grpc_server_name = "MockedGcsServer";
    config.grpc_server_thread_num = 1;
    config.redis_address = "127.0.0.1";
    config.is_test = true;
    config.redis_port = REDIS_SERVER_PORT;
    gcs_server_.reset(new gcs::GcsServer(config));

    thread_io_service_.reset(new std::thread([this] {
      std::unique_ptr<boost::asio::io_service::work> work(
          new boost::asio::io_service::work(io_service_));
      io_service_.run();
    }));

    thread_gcs_server_.reset(new std::thread([this] { gcs_server_->Start(); }));

    // Wait until server starts listening.
    while (gcs_server_->GetPort() == 0) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    // Create gcs client
    gcs::GcsClientOptions options(config.redis_address, config.redis_port,
                                  config.redis_password, config.is_test);
    gcs_client_.reset(new gcs::ServiceBasedGcsClient(options));
    RAY_CHECK_OK(gcs_client_->Connect(io_service_));
  }

  void TearDown() override {
    gcs_server_->Stop();
    io_service_.stop();
    thread_io_service_->join();
    thread_gcs_server_->join();
    gcs_client_->Disconnect();
  }

  bool AddJob(const std::shared_ptr<rpc::JobTableData> &job_table_data) {
    std::promise<bool> promise;
    RAY_CHECK_OK(gcs_client_->Jobs().AsyncAdd(job_table_data, [&promise](Status status) {
      RAY_CHECK_OK(status);
      promise.set_value(true);
    }));
    return WaitReady(promise.get_future(), timeout_ms_);
  }

  bool MarkJobFinished(const JobID &job_id) {
    std::promise<bool> promise;
    RAY_CHECK_OK(gcs_client_->Jobs().AsyncMarkFinished(job_id, [&promise](Status status) {
      RAY_CHECK_OK(status);
      promise.set_value(true);
    }));
    return WaitReady(promise.get_future(), timeout_ms_);
  }

  bool RegisterActor(const std::shared_ptr<rpc::ActorTableData> &actor_table_data) {
    std::promise<bool> promise;
    RAY_CHECK_OK(
        gcs_client_->Actors().AsyncRegister(actor_table_data, [&promise](Status status) {
          RAY_CHECK_OK(status);
          promise.set_value(true);
        }));
    return WaitReady(promise.get_future(), timeout_ms_);
  }

  bool UpdateActor(const ActorID &actor_id,
                   const std::shared_ptr<rpc::ActorTableData> &actor_table_data) {
    std::promise<bool> promise;
    RAY_CHECK_OK(gcs_client_->Actors().AsyncUpdate(actor_id, actor_table_data,
                                                   [&promise](Status status) {
                                                     RAY_CHECK_OK(status);
                                                     promise.set_value(true);
                                                   }));
    return WaitReady(promise.get_future(), timeout_ms_);
  }

  rpc::ActorTableData GetActor(const ActorID &actor_id) {
    std::promise<bool> promise;
    rpc::ActorTableData actor_table_data;
    RAY_CHECK_OK(gcs_client_->Actors().AsyncGet(
        actor_id, [&actor_table_data, &promise](
                      Status status, const boost::optional<rpc::ActorTableData> &result) {
          RAY_CHECK_OK(status);
          assert(result);
          actor_table_data.CopyFrom(*result);
          promise.set_value(true);
        }));
    EXPECT_TRUE(WaitReady(promise.get_future(), timeout_ms_));
    return actor_table_data;
  }

  bool RegisterSelf(const rpc::GcsNodeInfo &local_node_info) {
    Status status = gcs_client_->Nodes().RegisterSelf(local_node_info);
    return status.ok();
  }

  bool UnregisterSelf() {
    Status status = gcs_client_->Nodes().UnregisterSelf();
    return status.ok();
  }

  bool RegisterNode(const rpc::GcsNodeInfo &node_info) {
    Status status = gcs_client_->Nodes().Register(node_info);
    return status.ok();
  }

  std::vector<rpc::GcsNodeInfo> GetNodeInfoList() {
    std::promise<bool> promise;
    std::vector<rpc::GcsNodeInfo> nodes;
    RAY_CHECK_OK(gcs_client_->Nodes().AsyncGetAll(
        [&nodes, &promise](Status status, const std::vector<rpc::GcsNodeInfo> &result) {
          RAY_CHECK_OK(status);
          assert(result);
          nodes.assign(result.begin(), result.end());
          promise.set_value(true);
        }));
    EXPECT_TRUE(WaitReady(promise.get_future(), timeout_ms_));
    return nodes;
  }

  bool UnregisterNode(const ClientID &node_id) {
    std::promise<bool> promise;
    RAY_CHECK_OK(gcs_client_->Nodes().AsyncUnregister(node_id, [&promise](Status status) {
      RAY_CHECK_OK(status);
      promise.set_value(true);
    }));
    return WaitReady(promise.get_future(), timeout_ms_);
  }

 protected:
  bool WaitReady(const std::future<bool> &future,
                 const std::chrono::milliseconds &timeout_ms) {
    auto status = future.wait_for(timeout_ms);
    return status == std::future_status::ready;
  }

  std::shared_ptr<rpc::JobTableData> GenJobTableData(JobID job_id) {
    auto job_table_data = std::make_shared<rpc::JobTableData>();
    job_table_data->set_job_id(job_id.Binary());
    job_table_data->set_is_dead(false);
    job_table_data->set_timestamp(std::time(nullptr));
    job_table_data->set_node_manager_address("127.0.0.1");
    job_table_data->set_driver_pid(5667L);
    return job_table_data;
  }

  std::shared_ptr<rpc::ActorTableData> GenActorTableData(const JobID &job_id) {
    auto actor_table_data = std::make_shared<rpc::ActorTableData>();
    ActorID actor_id = ActorID::Of(job_id, RandomTaskId(), 0);
    actor_table_data->set_actor_id(actor_id.Binary());
    actor_table_data->set_job_id(job_id.Binary());
    actor_table_data->set_state(
        rpc::ActorTableData_ActorState::ActorTableData_ActorState_ALIVE);
    actor_table_data->set_max_reconstructions(1);
    actor_table_data->set_remaining_reconstructions(1);
    return actor_table_data;
  }

  rpc::GcsNodeInfo GenGcsNodeInfo(const std::string &node_id) {
    rpc::GcsNodeInfo gcs_node_info;
    gcs_node_info.set_node_id(node_id);
    gcs_node_info.set_state(rpc::GcsNodeInfo_GcsNodeState_ALIVE);
    return gcs_node_info;
  }

  // Gcs server
  std::unique_ptr<gcs::GcsServer> gcs_server_;
  std::unique_ptr<std::thread> thread_io_service_;
  std::unique_ptr<std::thread> thread_gcs_server_;
  boost::asio::io_service io_service_;

  // Gcs client
  std::unique_ptr<gcs::GcsClient> gcs_client_;

  // Timeout waiting for gcs server reply, default is 2s
  const std::chrono::milliseconds timeout_ms_{2000};
};

TEST_F(ServiceBasedGcsGcsClientTest, TestJobInfo) {
  // Create job_table_data
  JobID add_job_id = JobID::FromInt(1);
  auto job_table_data = GenJobTableData(add_job_id);

  std::promise<bool> promise;
  auto on_subscribe = [&promise, add_job_id](const JobID &job_id,
                                             const gcs::JobTableData &data) {
    ASSERT_TRUE(add_job_id == job_id);
    promise.set_value(true);
  };
  RAY_CHECK_OK(gcs_client_->Jobs().AsyncSubscribeToFinishedJobs(
      on_subscribe, [](Status status) { RAY_CHECK_OK(status); }));

  ASSERT_TRUE(AddJob(job_table_data));
  ASSERT_TRUE(MarkJobFinished(add_job_id));
  ASSERT_TRUE(WaitReady(promise.get_future(), timeout_ms_));
}

TEST_F(ServiceBasedGcsGcsClientTest, TestActorInfo) {
  // Create actor_table_data
  JobID job_id = JobID::FromInt(1);
  auto actor_table_data = GenActorTableData(job_id);
  ActorID actor_id = ActorID::FromBinary(actor_table_data->actor_id());

  // Subscribe
  std::promise<bool> promise_subscribe;
  std::atomic<int> subscribe_callback_count(0);
  auto on_subscribe = [&subscribe_callback_count](const ActorID &actor_id,
                                                  const gcs::ActorTableData &data) {
    ++subscribe_callback_count;
  };
  RAY_CHECK_OK(gcs_client_->Actors().AsyncSubscribe(actor_id, on_subscribe,
                                                    [&promise_subscribe](Status status) {
                                                      RAY_CHECK_OK(status);
                                                      promise_subscribe.set_value(true);
                                                    }));

  // Register actor
  ASSERT_TRUE(RegisterActor(actor_table_data));
  ASSERT_TRUE(GetActor(actor_id).state() ==
              rpc::ActorTableData_ActorState::ActorTableData_ActorState_ALIVE);

  // Unsubscribe
  std::promise<bool> promise_unsubscribe;
  RAY_CHECK_OK(gcs_client_->Actors().AsyncUnsubscribe(
      actor_id, [&promise_unsubscribe](Status status) {
        RAY_CHECK_OK(status);
        promise_unsubscribe.set_value(true);
      }));
  ASSERT_TRUE(WaitReady(promise_unsubscribe.get_future(), timeout_ms_));

  // Update actor
  actor_table_data->set_state(
      rpc::ActorTableData_ActorState::ActorTableData_ActorState_DEAD);
  ASSERT_TRUE(UpdateActor(actor_id, actor_table_data));
  ASSERT_TRUE(GetActor(actor_id).state() ==
              rpc::ActorTableData_ActorState::ActorTableData_ActorState_DEAD);
  ASSERT_TRUE(WaitReady(promise_subscribe.get_future(), timeout_ms_));
  auto condition = [&subscribe_callback_count]() {
    return 1 == subscribe_callback_count;
  };
  EXPECT_TRUE(WaitForCondition(condition, timeout_ms_.count()));
}

TEST_F(ServiceBasedGcsGcsClientTest, TestActorSubscribeAll) {
  // Create actor_table_data
  JobID job_id = JobID::FromInt(1);
  auto actor_table_data1 = GenActorTableData(job_id);
  auto actor_table_data2 = GenActorTableData(job_id);

  // Subscribe all
  std::promise<bool> promise_subscribe_all;
  std::atomic<int> subscribe_all_callback_count(0);
  auto on_subscribe_all = [&subscribe_all_callback_count](
                              const ActorID &actor_id, const gcs::ActorTableData &data) {
    ++subscribe_all_callback_count;
  };
  RAY_CHECK_OK(gcs_client_->Actors().AsyncSubscribeAll(
      on_subscribe_all, [&promise_subscribe_all](Status status) {
        RAY_CHECK_OK(status);
        promise_subscribe_all.set_value(true);
      }));
  ASSERT_TRUE(WaitReady(promise_subscribe_all.get_future(), timeout_ms_));

  // Register actor
  ASSERT_TRUE(RegisterActor(actor_table_data1));
  ASSERT_TRUE(RegisterActor(actor_table_data2));
  auto condition = [&subscribe_all_callback_count]() {
    return 2 == subscribe_all_callback_count;
  };
  EXPECT_TRUE(WaitForCondition(condition, timeout_ms_.count()));
}

TEST_F(ServiceBasedGcsGcsClientTest, TestNodeInfo) {
  // Create gcs node info
  ClientID node1_id = ClientID::FromRandom();
  auto gcs_node1_info = GenGcsNodeInfo(node1_id.Binary());

  int register_count = 0;
  int unregister_count = 0;
  RAY_CHECK_OK(gcs_client_->Nodes().AsyncSubscribeToNodeChange(
      [&register_count, &unregister_count](const ClientID &node_id,
                                           const rpc::GcsNodeInfo &data) {
        if (data.state() == rpc::GcsNodeInfo::ALIVE) {
          ++register_count;
        } else if (data.state() == rpc::GcsNodeInfo::DEAD) {
          ++unregister_count;
        }
      },
      nullptr));

  // Register self
  ASSERT_TRUE(RegisterSelf(gcs_node1_info));
  EXPECT_EQ(gcs_client_->Nodes().GetSelfId(), node1_id);
  EXPECT_EQ(gcs_client_->Nodes().GetSelfInfo().node_id(), gcs_node1_info.node_id());
  EXPECT_EQ(gcs_client_->Nodes().GetSelfInfo().state(), gcs_node1_info.state());

  // Register node
  ClientID node2_id = ClientID::FromRandom();
  auto gcs_node2_info = GenGcsNodeInfo(node2_id.Binary());
  ASSERT_TRUE(RegisterNode(gcs_node2_info));

  // Get node list
  std::vector<rpc::GcsNodeInfo> node_list = GetNodeInfoList();
  EXPECT_EQ(node_list.size(), 2);
  EXPECT_EQ(register_count, 2);
  ASSERT_TRUE(gcs_client_->Nodes().Get(node1_id));
  EXPECT_EQ(gcs_client_->Nodes().GetAll().size(), 2);

  // Unregister self
  ASSERT_TRUE(UnregisterSelf());

  // Unregister node
  ASSERT_TRUE(UnregisterNode(node2_id));
  node_list = GetNodeInfoList();
  EXPECT_EQ(node_list.size(), 2);
  EXPECT_EQ(node_list[0].state(),
            rpc::GcsNodeInfo_GcsNodeState::GcsNodeInfo_GcsNodeState_DEAD);
  EXPECT_EQ(node_list[1].state(),
            rpc::GcsNodeInfo_GcsNodeState::GcsNodeInfo_GcsNodeState_DEAD);
  EXPECT_EQ(unregister_count, 2);
  ASSERT_TRUE(gcs_client_->Nodes().IsRemoved(node2_id));
}  // namespace ray

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  RAY_CHECK(argc == 4);
  ray::REDIS_SERVER_EXEC_PATH = argv[1];
  ray::REDIS_CLIENT_EXEC_PATH = argv[2];
  ray::REDIS_MODULE_LIBRARY_PATH = argv[3];
  return RUN_ALL_TESTS();
}
