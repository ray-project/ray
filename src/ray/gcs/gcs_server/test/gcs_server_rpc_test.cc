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

  void TestAsyncRegister(const rpc::ActorAsyncRegisterRequest &request) {
    std::promise<rpc::ActorAsyncRegisterReply> register_actor_reply_promise;
    auto register_actor_reply_future = register_actor_reply_promise.get_future();
    client_->AsyncRegister(
        request, [&register_actor_reply_promise](
                     const Status &status, const rpc::ActorAsyncRegisterReply &reply) {
          if (status.ok()) {
            register_actor_reply_promise.set_value(reply);
          }
        });
    auto future_status =
        register_actor_reply_future.wait_for(std::chrono::milliseconds(200));
    ASSERT_EQ(future_status, std::future_status::ready);

    // Async get actor
    rpc::ActorAsyncGetRequest asyncGetRequest;
    asyncGetRequest.set_actor_id(request.actor_table_data().actor_id());
    std::promise<rpc::ActorAsyncGetReply> get_actor_reply_promise;
    auto get_actor_reply_future = get_actor_reply_promise.get_future();
    client_->AsyncGet(asyncGetRequest,
                      [request, &get_actor_reply_promise](
                          const Status &status, const rpc::ActorAsyncGetReply &reply) {
                        if (status.ok()) {
                          get_actor_reply_promise.set_value(reply);
                          ASSERT_TRUE(reply.actor_table_data().state() ==
                                      request.actor_table_data().state());
                        }
                      });
    future_status = get_actor_reply_future.wait_for(std::chrono::milliseconds(200));
    ASSERT_EQ(future_status, std::future_status::ready);
  }

  void TestAsyncUpdateAndGet(const rpc::ActorAsyncUpdateRequest &request) {
    std::promise<rpc::ActorAsyncUpdateReply> update_actor_reply_promise;
    auto update_actor_reply_future = update_actor_reply_promise.get_future();
    client_->AsyncUpdate(
        request, [&update_actor_reply_promise](const Status &status,
                                               const rpc::ActorAsyncUpdateReply &reply) {
          if (status.ok()) {
            update_actor_reply_promise.set_value(reply);
          }
        });
    auto future_status =
        update_actor_reply_future.wait_for(std::chrono::milliseconds(200));
    ASSERT_EQ(future_status, std::future_status::ready);

    // Async get actor
    rpc::ActorAsyncGetRequest asyncGetRequest;
    asyncGetRequest.set_actor_id(request.actor_table_data().actor_id());
    std::promise<rpc::ActorAsyncGetReply> get_actor_reply_promise;
    auto get_actor_reply_future = get_actor_reply_promise.get_future();
    client_->AsyncGet(asyncGetRequest,
                      [request, &get_actor_reply_promise](
                          const Status &status, const rpc::ActorAsyncGetReply &reply) {
                        if (status.ok()) {
                          get_actor_reply_promise.set_value(reply);
                          ASSERT_TRUE(reply.actor_table_data().state() ==
                                      request.actor_table_data().state());
                        }
                      });
    future_status = get_actor_reply_future.wait_for(std::chrono::milliseconds(200));
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
  // create actor_table_data
  JobID job_id = JobID::FromInt(1);
  rpc::ActorTableData actor_table_data = genActorTableData(job_id);

  // Async register actor
  rpc::ActorAsyncRegisterRequest asyncRegisterRequest;
  asyncRegisterRequest.mutable_actor_table_data()->CopyFrom(actor_table_data);
  TestAsyncRegister(asyncRegisterRequest);

  // Async update actor state and get
  rpc::ActorAsyncUpdateRequest asyncUpdateRequest;
  actor_table_data.set_state(
      rpc::ActorTableData_ActorState::ActorTableData_ActorState_DEAD);
  asyncUpdateRequest.set_actor_id(actor_table_data.actor_id());
  asyncUpdateRequest.mutable_actor_table_data()->CopyFrom(actor_table_data);
  TestAsyncUpdateAndGet(asyncUpdateRequest);
}

TEST_F(GcsServerTest, TestJobInfo) {
  // create job_table_data
  JobID job_id = JobID::FromInt(1);
  rpc::JobTableData job_table_data = genJobTableData(job_id);

  rpc::AddJobRequest add_job_request;
  add_job_request.mutable_data()->CopyFrom(job_table_data);
  std::promise<rpc::AddJobReply> add_job_reply_promise;
  auto add_job_reply_future = add_job_reply_promise.get_future();
  client_->AddJob(
      add_job_request,
      [&add_job_reply_promise](const Status &status, const rpc::AddJobReply &reply) {
        if (status.ok()) {
          add_job_reply_promise.set_value(reply);
        }
      });
  auto future_status = add_job_reply_future.wait_for(std::chrono::milliseconds(200));
  ASSERT_EQ(future_status, std::future_status::ready);

  rpc::MarkJobFinishedRequest mark_job_finished_request;
  mark_job_finished_request.set_job_id(job_table_data.job_id());
  std::promise<rpc::MarkJobFinishedReply> mark_job_finished_reply_promise;
  auto mark_job_finished_reply_future = mark_job_finished_reply_promise.get_future();
  client_->MarkJobFinished(
      mark_job_finished_request,
      [&mark_job_finished_reply_promise](const Status &status,
                                         const rpc::MarkJobFinishedReply &reply) {
        if (status.ok()) {
          mark_job_finished_reply_promise.set_value(reply);
        }
      });
  future_status = mark_job_finished_reply_future.wait_for(std::chrono::milliseconds(200));
  ASSERT_EQ(future_status, std::future_status::ready);
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
