#include "ray/gcs/gcs_server/gcs_server.h"
#include "gtest/gtest.h"
#include "ray/gcs/gcs_server/actor_info_handler_impl.h"
#include "ray/rpc/gcs_server/gcs_rpc_client.h"

namespace ray {
class GcsServerTest : public ::testing::Test {
 public:
  GcsServerTest() : options_("127.0.0.1", 6379, "", true) {}

  void SetUp() override {
    work_thread_.reset(new std::thread([this] {
      std::unique_ptr<boost::asio::io_service::work> work(
          new boost::asio::io_service::work(io_service_));
      io_service_.run();
    }));

    redis_gcs_client_ = std::make_shared<gcs::RedisGcsClient>(options_);
    RAY_CHECK_OK(redis_gcs_client_->Connect(io_service_));

    rpc_server_.reset(new rpc::GrpcServer(name_, port_));
    actor_info_handler_.reset(new rpc::DefaultActorInfoHandler(*redis_gcs_client_));
    actor_info_service_.reset(
        new rpc::ActorInfoGrpcService(io_service_, *actor_info_handler_));

    rpc_server_->RegisterService(*actor_info_service_);
    rpc_server_->Run();

    thread_.reset(new std::thread([this] { io_context_.run(); }));
  }

  void TearDown() override {
    redis_gcs_client_->Disconnect();

    io_service_.stop();
    work_thread_->join();
    work_thread_.reset();

    redis_gcs_client_.reset();

    io_context_.stop();
    thread_->join();
    rpc_server_->Shutdown();
  }

  void TestAsyncRegister(rpc::GcsRpcClient &client,
                         const rpc::ActorAsyncRegisterRequest &request) {
    uint64_t count = actor_info_handler_->GetAsyncRegisterCount();
    client.AsyncRegister(
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
    client.AsyncGet(asyncGetRequest, [request](const Status &status,
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

  void TestAsyncUpdateAndGet(rpc::GcsRpcClient &client,
                             const rpc::ActorAsyncUpdateRequest &request) {
    uint64_t count = actor_info_handler_->GetAsyncUpdateCount();
    client.AsyncUpdate(request, [](const Status &status,
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
    client.AsyncGet(asyncGetRequest, [request](const Status &status,
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

  rpc::ActorTableData genActorTableData(JobID job_id) {
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

  boost::asio::io_context io_context_;

 protected:
  std::string name_ = "gcs_server_test";
  const uint32_t port_ = 5566;

  std::unique_ptr<std::thread> thread_;
  std::unique_ptr<rpc::GrpcServer> rpc_server_;
  std::unique_ptr<rpc::ActorInfoGrpcService> actor_info_service_;
  std::unique_ptr<rpc::DefaultActorInfoHandler> actor_info_handler_;

  gcs::GcsClientOptions options_;
  std::shared_ptr<gcs::RedisGcsClient> redis_gcs_client_;
  boost::asio::io_service io_service_;
  std::unique_ptr<std::thread> work_thread_;
};

TEST_F(GcsServerTest, TestActorInfo) {
  boost::asio::io_context client_io_context;
  boost::asio::io_context::work worker(client_io_context);
  rpc::ClientCallManager client_call_manager(client_io_context);
  rpc::GcsRpcClient client("0.0.0.0", 5566, client_call_manager);
  std::thread([&client_io_context] { client_io_context.run(); }).detach();

  // create actor_table_data
  JobID job_id = JobID::FromInt(1);
  rpc::ActorTableData actor_table_data = genActorTableData(job_id);

  // Async register actor
  rpc::ActorAsyncRegisterRequest asyncRegisterRequest;
  asyncRegisterRequest.mutable_actor_table_data()->CopyFrom(actor_table_data);
  TestAsyncRegister(client, asyncRegisterRequest);

  // Async update actor state and get
  rpc::ActorAsyncUpdateRequest asyncUpdateRequest;
  asyncUpdateRequest.set_actor_id(actor_table_data.actor_id());
  asyncUpdateRequest.mutable_actor_table_data()->CopyFrom(actor_table_data);
  TestAsyncUpdateAndGet(client, asyncUpdateRequest);
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
