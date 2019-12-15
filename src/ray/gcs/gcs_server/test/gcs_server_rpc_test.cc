#include "gtest/gtest.h"
#include "ray/gcs/gcs_server/actor_info_handler_impl.h"
#include "ray/gcs/gcs_server/gcs_server.h"
#include "ray/gcs/gcs_server/job_info_handler_impl.h"
#include "ray/rpc/gcs_server/gcs_rpc_client.h"

namespace ray {
class GcsServerTest : public ::testing::Test {
 public:
  GcsServerTest() : options_("127.0.0.1", 6379, "", true) {}

  void SetUp() override {
    thread_.reset(new std::thread([this] {
      std::unique_ptr<boost::asio::io_service::work> work(
          new boost::asio::io_service::work(io_service_));
      io_service_.run();
    }));

    // create redis gcs client
    redis_gcs_client_ = std::make_shared<gcs::RedisGcsClient>(options_);
    RAY_CHECK_OK(redis_gcs_client_->Connect(io_service_));

    // create gcs rpc server
    server_.reset(new rpc::GrpcServer("gcs_server", 0));
    job_info_handler_.reset(new rpc::DefaultJobInfoHandler(*redis_gcs_client_));
    job_info_service_.reset(new rpc::JobInfoGrpcService(io_service_, *job_info_handler_));
    actor_info_handler_.reset(new rpc::DefaultActorInfoHandler(*redis_gcs_client_));
    actor_info_service_.reset(
        new rpc::ActorInfoGrpcService(io_service_, *actor_info_handler_));
    server_->RegisterService(*job_info_service_);
    server_->RegisterService(*actor_info_service_);
    server_->Run();

    // create gcs rpc client
    client_call_manager_.reset(new rpc::ClientCallManager(io_service_));
    client_.reset(
        new rpc::GcsRpcClient("0.0.0.0", server_->GetPort(), *client_call_manager_));
  }

  void TearDown() override {
    redis_gcs_client_->Disconnect();

    io_service_.stop();
    thread_->join();

    server_->Shutdown();
  }

  void TestAsyncRegister(const rpc::ActorAsyncRegisterRequest &request) {
    uint64_t count = actor_info_handler_->GetAsyncRegisterCount();
    client_->AsyncRegister(
        request, [](const Status &status, const rpc::ActorAsyncRegisterReply &reply) {
          RAY_LOG(INFO) << "Received GcsServer AsyncRegister response.";
        });
    while (actor_info_handler_->GetAsyncRegisterCount() <= count) {
      sleep(1);
    }

    // Async get actor
    count = actor_info_handler_->GetAsyncGetCount();
    rpc::ActorAsyncGetRequest asyncGetRequest;
    asyncGetRequest.set_actor_id(request.actor_table_data().actor_id());
    client_->AsyncGet(asyncGetRequest, [request](const Status &status,
                                                 const rpc::ActorAsyncGetReply &reply) {
      RAY_LOG(INFO) << "Received GcsServer AsyncGet response.";
      const rpc::ActorTableData &reply_actor_data = reply.actor_table_data();
      RAY_LOG(INFO) << "Actor state is:" << reply_actor_data.state();
      ASSERT_TRUE(reply_actor_data.state() == request.actor_table_data().state());
    });
    while (actor_info_handler_->GetAsyncGetCount() <= count) {
      sleep(1);
    }
  }

  void TestAsyncUpdateAndGet(const rpc::ActorAsyncUpdateRequest &request) {
    uint64_t count = actor_info_handler_->GetAsyncUpdateCount();
    client_->AsyncUpdate(request, [](const Status &status,
                                     const rpc::ActorAsyncUpdateReply &reply) {
      RAY_LOG(INFO) << "Received GcsServer AsyncUpdate response." << reply.DebugString();
    });
    while (actor_info_handler_->GetAsyncUpdateCount() <= count) {
      sleep(1);
    }

    // Async get actor
    count = actor_info_handler_->GetAsyncGetCount();
    rpc::ActorAsyncGetRequest asyncGetRequest;
    asyncGetRequest.set_actor_id(request.actor_table_data().actor_id());
    client_->AsyncGet(asyncGetRequest, [request](const Status &status,
                                                 const rpc::ActorAsyncGetReply &reply) {
      RAY_LOG(INFO) << "Received GcsServer AsyncGet response." << reply.DebugString();
      const rpc::ActorTableData &reply_actor_data = reply.actor_table_data();
      RAY_LOG(INFO) << "Actor state is:" << reply_actor_data.state();
      ASSERT_TRUE(reply_actor_data.state() == request.actor_table_data().state());
    });
    while (actor_info_handler_->GetAsyncGetCount() <= count) {
      sleep(1);
    }
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
    TaskID task_id = TaskID::ForDriverTask(job_id);
    ActorID actor_id = ActorID::Of(job_id, task_id, 0);
    actor_table_data.set_actor_id(actor_id.Binary());
    actor_table_data.set_job_id(job_id.Binary());
    actor_table_data.set_state(
        rpc::ActorTableData_ActorState::ActorTableData_ActorState_ALIVE);
    actor_table_data.set_max_reconstructions(2);
    actor_table_data.set_remaining_reconstructions(2);
    return actor_table_data;
  }

 protected:
  // gcs server
  std::unique_ptr<std::thread> thread_;
  std::unique_ptr<rpc::GrpcServer> server_;
  std::unique_ptr<rpc::JobInfoGrpcService> job_info_service_;
  std::unique_ptr<rpc::DefaultJobInfoHandler> job_info_handler_;
  std::unique_ptr<rpc::ActorInfoGrpcService> actor_info_service_;
  std::unique_ptr<rpc::DefaultActorInfoHandler> actor_info_handler_;
  gcs::GcsClientOptions options_;
  std::shared_ptr<gcs::RedisGcsClient> redis_gcs_client_;
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
  uint64_t count = job_info_handler_->GetAddJobCount();
  client_->AddJob(
      add_job_request, [](const Status &status, const rpc::AddJobReply &reply) {
        RAY_LOG(INFO) << "Received GcsServer AddJob response." << reply.DebugString();
        ASSERT_TRUE(reply.success());
      });
  while (job_info_handler_->GetAddJobCount() <= count) {
    sleep(1);
  }

  count = job_info_handler_->GetMarkJobFinishedCount();
  rpc::MarkJobFinishedRequest mark_job_finished_request;
  mark_job_finished_request.set_job_id(job_table_data.job_id());
  client_->MarkJobFinished(
      mark_job_finished_request,
      [](const Status &status, const rpc::MarkJobFinishedReply &reply) {
        RAY_LOG(INFO) << "Received GcsServer MarkJobFinished response."
                      << reply.DebugString();
        ASSERT_TRUE(reply.success());
      });
  while (job_info_handler_->GetMarkJobFinishedCount() <= count) {
    sleep(1);
  }
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
