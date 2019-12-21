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

class GcsServerTest : public ::testing::Test {
 public:
  using CallFunction = std::function<void(std::promise<bool> &promise)>;

  static void SetUpTestCase() {
    std::string start_redis_command = redis_server_executable +
                                      " --loglevel warning --loadmodule " +
                                      libray_redis_module_path + " --port 6379 &";
    RAY_LOG(INFO) << "Start redis command is: " << start_redis_command;
    RAY_CHECK(system(start_redis_command.c_str()) == 0);
    usleep(200 * 1000);
  }

  static void TearDownTestCase() {
    std::string stop_redis_command = redis_client_executable + " -p 6379 shutdown";
    RAY_LOG(INFO) << "Stop redis command is: " << stop_redis_command;
    RAY_CHECK(system(stop_redis_command.c_str()) == 0);
  }

  void SetUp() override {
    gcs::GcsServerConfig config;
    config.grpc_server_port = 0;
    config.grpc_server_name = "MockedGcsServer";
    config.grpc_server_thread_num = 1;
    config.redis_address = "127.0.0.1";
    config.is_test = true;
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

  void TestAddJob(const rpc::AddJobRequest &request) {
    auto call_function = [this, request](std::promise<bool> &promise) {
      client_->AddJob(request,
                      [&promise](const Status &status, const rpc::AddJobReply &reply) {
                        RAY_CHECK_OK(status);
                        promise.set_value(true);
                      });
    };
    AsyncCall(call_function, timeout_ms_);
  }

  void TestMarkJobFinished(const rpc::MarkJobFinishedRequest &request) {
    auto call_function = [this, request](std::promise<bool> &promise) {
      client_->MarkJobFinished(
          request,
          [&promise](const Status &status, const rpc::MarkJobFinishedReply &reply) {
            RAY_CHECK_OK(status);
            promise.set_value(true);
          });
    };
    AsyncCall(call_function, timeout_ms_);
  }

  void TestRegisterActorInfo(const rpc::RegisterActorInfoRequest &request) {
    auto call_function = [this, request](std::promise<bool> &promise) {
      client_->RegisterActorInfo(
          request,
          [&promise](const Status &status, const rpc::RegisterActorInfoReply &reply) {
            RAY_CHECK_OK(status);
            promise.set_value(true);
          });
    };
    AsyncCall(call_function, timeout_ms_);
  }

  void TestUpdateActorInfo(const rpc::UpdateActorInfoRequest &request) {
    auto call_function = [this, request](std::promise<bool> &promise) {
      client_->UpdateActorInfo(
          request,
          [&promise](const Status &status, const rpc::UpdateActorInfoReply &reply) {
            RAY_CHECK_OK(status);
            promise.set_value(true);
          });
    };
    AsyncCall(call_function, timeout_ms_);
  }

  void TestGetActorInfo(const rpc::ActorTableData &expected) {
    rpc::GetActorInfoRequest request;
    request.set_actor_id(expected.actor_id());
    auto call_function = [this, request, expected](std::promise<bool> &promise) {
      client_->GetActorInfo(
          request, [&promise, &expected](const Status &status,
                                         const rpc::GetActorInfoReply &reply) {
            RAY_CHECK_OK(status);
            promise.set_value(true);
            ASSERT_TRUE(reply.actor_table_data().state() == expected.state());
          });
    };
    AsyncCall(call_function, timeout_ms_);
  }

  void AsyncCall(const CallFunction &function, uint64_t timeout_ms) {
    std::promise<bool> promise_;
    auto future = promise_.get_future();
    function(promise_);
    auto status = future.wait_for(std::chrono::milliseconds(timeout_ms));
    ASSERT_EQ(status, std::future_status::ready);
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
  TestRegisterActorInfo(register_actor_info_request);
  TestGetActorInfo(actor_table_data);

  // Update actor state
  rpc::UpdateActorInfoRequest update_actor_info_request;
  actor_table_data.set_state(
      rpc::ActorTableData_ActorState::ActorTableData_ActorState_DEAD);
  update_actor_info_request.set_actor_id(actor_table_data.actor_id());
  update_actor_info_request.mutable_actor_table_data()->CopyFrom(actor_table_data);
  TestUpdateActorInfo(update_actor_info_request);
  TestGetActorInfo(actor_table_data);
}

TEST_F(GcsServerTest, TestJobInfo) {
  // Create job_table_data
  JobID job_id = JobID::FromInt(1);
  rpc::JobTableData job_table_data = GenJobTableData(job_id);

  // Add job
  rpc::AddJobRequest add_job_request;
  add_job_request.mutable_data()->CopyFrom(job_table_data);
  TestAddJob(add_job_request);

  // Mark job finished
  rpc::MarkJobFinishedRequest mark_job_finished_request;
  mark_job_finished_request.set_job_id(job_table_data.job_id());
  TestMarkJobFinished(mark_job_finished_request);
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  RAY_CHECK(argc == 4);
  ray::redis_server_executable = argv[1];
  ray::redis_client_executable = argv[2];
  ray::libray_redis_module_path = argv[3];
  return RUN_ALL_TESTS();
}
