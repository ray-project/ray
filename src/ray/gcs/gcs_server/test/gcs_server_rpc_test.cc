#include "gtest/gtest.h"
#include "ray/gcs/gcs_server/actor_info_handler_impl.h"
#include "ray/gcs/gcs_server/gcs_server.h"
#include "ray/gcs/gcs_server/job_info_handler_impl.h"
#include "ray/rpc/gcs_server/gcs_rpc_client.h"
#include "ray/util/test_util.h"

namespace ray {

static std::string redis_server_executable;
static std::string redis_client_executable;
static std::string libray_redis_module_path;

class GcsServerTest : public RedisServiceManagerForTest {
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

    // Create gcs rpc client
    client_call_manager_.reset(new rpc::ClientCallManager(io_service_));
    client_.reset(
        new rpc::GcsRpcClient("0.0.0.0", gcs_server_->GetPort(), *client_call_manager_));
  }

  void TearDown() override {
    gcs_server_->Stop();
    io_service_.stop();
    thread_io_service_->join();
    thread_gcs_server_->join();
  }

  bool AddJob(const rpc::AddJobRequest &request) {
    std::promise<bool> promise;
    client_->AddJob(request,
                    [&promise](const Status &status, const rpc::AddJobReply &reply) {
                      RAY_CHECK_OK(status);
                      promise.set_value(true);
                    });
    return WaitReady(promise.get_future(), timeout_ms_);
  }

  bool MarkJobFinished(const rpc::MarkJobFinishedRequest &request) {
    std::promise<bool> promise;
    client_->MarkJobFinished(request, [&promise](const Status &status,
                                                 const rpc::MarkJobFinishedReply &reply) {
      RAY_CHECK_OK(status);
      promise.set_value(true);
    });
    return WaitReady(promise.get_future(), timeout_ms_);
  }

  bool RegisterActorInfo(const rpc::RegisterActorInfoRequest &request) {
    std::promise<bool> promise;
    client_->RegisterActorInfo(
        request,
        [&promise](const Status &status, const rpc::RegisterActorInfoReply &reply) {
          RAY_CHECK_OK(status);
          promise.set_value(true);
        });
    return WaitReady(promise.get_future(), timeout_ms_);
  }

  bool UpdateActorInfo(const rpc::UpdateActorInfoRequest &request) {
    std::promise<bool> promise;
    client_->UpdateActorInfo(request, [&promise](const Status &status,
                                                 const rpc::UpdateActorInfoReply &reply) {
      RAY_CHECK_OK(status);
      promise.set_value(true);
    });
    return WaitReady(promise.get_future(), timeout_ms_);
  }

  rpc::ActorTableData GetActorInfo(const std::string &actor_id) {
    rpc::GetActorInfoRequest request;
    request.set_actor_id(actor_id);
    rpc::ActorTableData actor_table_data;
    std::promise<bool> promise;
    client_->GetActorInfo(
        request, [&actor_table_data, &promise](const Status &status,
                                               const rpc::GetActorInfoReply &reply) {
          RAY_CHECK_OK(status);
          actor_table_data.CopyFrom(reply.actor_table_data());
          promise.set_value(true);
        });
    EXPECT_TRUE(WaitReady(promise.get_future(), timeout_ms_));
    return actor_table_data;
  }

  bool RegisterNode(const rpc::RegisterNodeRequest &request) {
    std::promise<bool> promise;
    client_->RegisterNode(
        request, [&promise](const Status &status, const rpc::RegisterNodeReply &reply) {
          RAY_CHECK_OK(status);
          promise.set_value(true);
        });

    return WaitReady(promise.get_future(), timeout_ms_);
  }

  bool UnregisterNode(const rpc::UnregisterNodeRequest &request) {
    std::promise<bool> promise;
    client_->UnregisterNode(
        request, [&promise](const Status &status, const rpc::UnregisterNodeReply &reply) {
          RAY_CHECK_OK(status);
          promise.set_value(true);
        });
    return WaitReady(promise.get_future(), timeout_ms_);
  }

  std::vector<rpc::GcsNodeInfo> GetAllNodeInfo() {
    std::vector<rpc::GcsNodeInfo> node_info_list;
    rpc::GetAllNodeInfoRequest request;
    std::promise<bool> promise;
    client_->GetAllNodeInfo(
        request, [&node_info_list, &promise](const Status &status,
                                             const rpc::GetAllNodeInfoReply &reply) {
          RAY_CHECK_OK(status);
          for (int index = 0; index < reply.node_info_list_size(); ++index) {
            node_info_list.push_back(reply.node_info_list(index));
          }
          promise.set_value(true);
        });
    EXPECT_TRUE(WaitReady(promise.get_future(), timeout_ms_));
    return node_info_list;
  }

  bool WaitReady(const std::future<bool> &future, uint64_t timeout_ms) {
    auto status = future.wait_for(std::chrono::milliseconds(timeout_ms));
    return status == std::future_status::ready;
  }

  rpc::JobTableData GenJobTableData(JobID job_id) {
    rpc::JobTableData job_table_data;
    job_table_data.set_job_id(job_id.Binary());
    job_table_data.set_is_dead(false);
    job_table_data.set_timestamp(std::time(nullptr));
    job_table_data.set_node_manager_address("127.0.0.1");
    job_table_data.set_driver_pid(5667L);
    return job_table_data;
  }

  rpc::ActorTableData GenActorTableData(const JobID &job_id) {
    rpc::ActorTableData actor_table_data;
    ActorID actor_id = ActorID::Of(job_id, RandomTaskId(), 0);
    actor_table_data.set_actor_id(actor_id.Binary());
    actor_table_data.set_job_id(job_id.Binary());
    actor_table_data.set_state(
        rpc::ActorTableData_ActorState::ActorTableData_ActorState_ALIVE);
    actor_table_data.set_max_reconstructions(1);
    actor_table_data.set_remaining_reconstructions(1);
    return actor_table_data;
  }

  rpc::GcsNodeInfo GenGcsNodeInfo(const std::string &node_id) {
    rpc::GcsNodeInfo gcs_node_info;
    gcs_node_info.set_node_id(node_id);
    gcs_node_info.set_state(rpc::GcsNodeInfo_GcsNodeState_ALIVE);
    return gcs_node_info;
  }

 protected:
  // Gcs server
  std::unique_ptr<gcs::GcsServer> gcs_server_;
  std::unique_ptr<std::thread> thread_io_service_;
  std::unique_ptr<std::thread> thread_gcs_server_;
  boost::asio::io_service io_service_;

  // Gcs client
  std::unique_ptr<rpc::GcsRpcClient> client_;
  std::unique_ptr<rpc::ClientCallManager> client_call_manager_;

  // Timeout waiting for gcs server reply, default is 2s
  const uint64_t timeout_ms_ = 2000;
};

TEST_F(GcsServerTest, TestActorInfo) {
  // Create actor_table_data
  JobID job_id = JobID::FromInt(1);
  rpc::ActorTableData actor_table_data = GenActorTableData(job_id);

  // Register actor
  rpc::RegisterActorInfoRequest register_actor_info_request;
  register_actor_info_request.mutable_actor_table_data()->CopyFrom(actor_table_data);
  ASSERT_TRUE(RegisterActorInfo(register_actor_info_request));
  rpc::ActorTableData result = GetActorInfo(actor_table_data.actor_id());
  ASSERT_TRUE(result.state() ==
              rpc::ActorTableData_ActorState::ActorTableData_ActorState_ALIVE);

  // Update actor state
  rpc::UpdateActorInfoRequest update_actor_info_request;
  actor_table_data.set_state(
      rpc::ActorTableData_ActorState::ActorTableData_ActorState_DEAD);
  update_actor_info_request.set_actor_id(actor_table_data.actor_id());
  update_actor_info_request.mutable_actor_table_data()->CopyFrom(actor_table_data);
  ASSERT_TRUE(UpdateActorInfo(update_actor_info_request));
  result = GetActorInfo(actor_table_data.actor_id());
  ASSERT_TRUE(result.state() ==
              rpc::ActorTableData_ActorState::ActorTableData_ActorState_DEAD);
}

TEST_F(GcsServerTest, TestJobInfo) {
  // Create job_table_data
  JobID job_id = JobID::FromInt(1);
  rpc::JobTableData job_table_data = GenJobTableData(job_id);

  // Add job
  rpc::AddJobRequest add_job_request;
  add_job_request.mutable_data()->CopyFrom(job_table_data);
  ASSERT_TRUE(AddJob(add_job_request));

  // Mark job finished
  rpc::MarkJobFinishedRequest mark_job_finished_request;
  mark_job_finished_request.set_job_id(job_table_data.job_id());
  ASSERT_TRUE(MarkJobFinished(mark_job_finished_request));
}

TEST_F(GcsServerTest, TestNodeInfo) {
  // Create gcs node info
  ClientID node_id = ClientID::FromRandom();
  rpc::GcsNodeInfo gcs_node_info = GenGcsNodeInfo(node_id.Binary());

  // Register node info
  rpc::RegisterNodeRequest register_node_info_request;
  register_node_info_request.mutable_node_info()->CopyFrom(gcs_node_info);
  ASSERT_TRUE(RegisterNode(register_node_info_request));
  std::vector<rpc::GcsNodeInfo> node_info_list = GetAllNodeInfo();
  ASSERT_TRUE(node_info_list.size() == 1);
  ASSERT_TRUE(node_info_list[0].state() ==
              rpc::GcsNodeInfo_GcsNodeState::GcsNodeInfo_GcsNodeState_ALIVE);

  // Unregister node info
  rpc::UnregisterNodeRequest unregister_node_info_request;
  unregister_node_info_request.set_node_id(node_id.Binary());
  ASSERT_TRUE(UnregisterNode(unregister_node_info_request));
  node_info_list = GetAllNodeInfo();
  ASSERT_TRUE(node_info_list.size() == 1);
  ASSERT_TRUE(node_info_list[0].state() ==
              rpc::GcsNodeInfo_GcsNodeState::GcsNodeInfo_GcsNodeState_DEAD);
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
