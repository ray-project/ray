#include "gtest/gtest.h"
#include "ray/gcs/gcs_server/actor_info_handler_impl.h"
#include "ray/gcs/gcs_server/gcs_server.h"
#include "ray/gcs/gcs_server/job_info_handler_impl.h"
#include "ray/rpc/gcs_server/gcs_rpc_client.h"
#include "ray/util/test_util.h"

namespace ray {
class GcsServerTest : public ::testing::Test {
 public:
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

    // create gcs rpc client
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
    std::promise<rpc::AddJobReply> reply_promise;
    auto reply_future = reply_promise.get_future();
    client_->AddJob(
        request, [&reply_promise](const Status &status, const rpc::AddJobReply &reply) {
          if (status.ok()) {
            reply_promise.set_value(reply);
          }
        });
    auto future_status = reply_future.wait_for(std::chrono::milliseconds(200));
    ASSERT_EQ(future_status, std::future_status::ready);
  }

  void TestMarkJobFinished(const rpc::MarkJobFinishedRequest &request) {
    std::promise<rpc::MarkJobFinishedReply> reply_promise;
    auto reply_future = reply_promise.get_future();
    client_->MarkJobFinished(
        request,
        [&reply_promise](const Status &status, const rpc::MarkJobFinishedReply &reply) {
          if (status.ok()) {
            reply_promise.set_value(reply);
          }
        });
    auto future_status = reply_future.wait_for(std::chrono::milliseconds(200));
    ASSERT_EQ(future_status, std::future_status::ready);
  }

  void TestRegisterActor(const rpc::RegisterActorRequest &request) {
    std::promise<rpc::RegisterActorReply> reply_promise;
    auto reply_future = reply_promise.get_future();
    client_->RegisterActor(
        request,
        [&reply_promise](const Status &status, const rpc::RegisterActorReply &reply) {
          if (status.ok()) {
            reply_promise.set_value(reply);
          }
        });
    auto future_status = reply_future.wait_for(std::chrono::milliseconds(200));
    ASSERT_EQ(future_status, std::future_status::ready);
  }

  void TestUpdateActor(const rpc::UpdateActorRequest &request) {
    std::promise<rpc::UpdateActorReply> reply_promise;
    auto reply_future = reply_promise.get_future();
    client_->UpdateActor(request, [&reply_promise](const Status &status,
                                                   const rpc::UpdateActorReply &reply) {
      if (status.ok()) {
        reply_promise.set_value(reply);
      }
    });
    auto future_status = reply_future.wait_for(std::chrono::milliseconds(200));
    ASSERT_EQ(future_status, std::future_status::ready);
  }

  void TestGetActor(const rpc::ActorTableData &expected) {
    rpc::GetActorRequest request;
    request.set_actor_id(expected.actor_id());
    std::promise<rpc::GetActorReply> reply_promise;
    auto reply_future = reply_promise.get_future();
    client_->GetActor(
        request, [expected, &reply_promise](const Status &status,
                                            const rpc::GetActorReply &reply) {
          if (status.ok()) {
            reply_promise.set_value(reply);
            ASSERT_TRUE(reply.actor_table_data().state() == expected.state());
          }
        });
    auto future_status = reply_future.wait_for(std::chrono::milliseconds(200));
    ASSERT_EQ(future_status, std::future_status::ready);
  }

  rpc::JobTableData genJobTableData(JobID job_id) {
    rpc::JobTableData job_table_data;
    job_table_data.set_job_id(job_id.Binary());
    job_table_data.set_is_dead(false);
    job_table_data.set_timestamp(std::time(nullptr));
    job_table_data.set_node_manager_address("127.0.0.1");
    job_table_data.set_driver_pid(5667L);
    return job_table_data;
  }

  rpc::ActorTableData genActorTableData(const JobID &job_id) {
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
  // gcs server
  std::unique_ptr<gcs::GcsServer> gcs_server_;
  std::unique_ptr<std::thread> thread_io_service_;
  std::unique_ptr<std::thread> thread_gcs_server_;
  boost::asio::io_service io_service_;

  // gcs client
  std::unique_ptr<rpc::GcsRpcClient> client_;
  std::unique_ptr<rpc::ClientCallManager> client_call_manager_;
};

TEST_F(GcsServerTest, TestActorInfo) {
  // Create actor_table_data
  JobID job_id = JobID::FromInt(1);
  rpc::ActorTableData actor_table_data = genActorTableData(job_id);

  // Register actor
  rpc::RegisterActorRequest register_actor_request;
  register_actor_request.mutable_actor_table_data()->CopyFrom(actor_table_data);
  TestRegisterActor(register_actor_request);
  TestGetActor(actor_table_data);

  // Update actor state
  rpc::UpdateActorRequest update_actor_request;
  actor_table_data.set_state(
      rpc::ActorTableData_ActorState::ActorTableData_ActorState_DEAD);
  update_actor_request.set_actor_id(actor_table_data.actor_id());
  update_actor_request.mutable_actor_table_data()->CopyFrom(actor_table_data);
  TestUpdateActor(update_actor_request);

  // Get actor and check actor state
  TestGetActor(actor_table_data);
}

TEST_F(GcsServerTest, TestJobInfo) {
  // Create job_table_data
  JobID job_id = JobID::FromInt(1);
  rpc::JobTableData job_table_data = genJobTableData(job_id);

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
  return RUN_ALL_TESTS();
}
