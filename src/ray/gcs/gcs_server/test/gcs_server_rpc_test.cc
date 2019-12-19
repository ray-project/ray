#include <boost/asio/basic_socket.hpp>
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "ray/gcs/gcs_server/gcs_server.h"
#include "ray/rpc/gcs_server/gcs_rpc_client.h"

namespace ray {
class GcsServerRpcTest : public ::testing::Test {
 protected:
  template <typename T>
  class MockedGcsServer : public gcs::GcsServer {
   public:
    explicit MockedGcsServer(const gcs::GcsServerConfig &config)
        : gcs::GcsServer(config) {}

   protected:
    void InitBackendClient() override {}

    std::unique_ptr<rpc::JobInfoHandler> InitJobInfoHandler() override {
      return std::unique_ptr<rpc::JobInfoHandler>(new T);
    }
  };
};

TEST_F(GcsServerRpcTest, JobInfo) {
  class MockedJobInfoHandler : public rpc::JobInfoHandler {
   public:
    void HandleAddJob(const rpc::AddJobRequest &request, rpc::AddJobReply *reply,
                      rpc::SendReplyCallback send_reply_callback) override {
      send_reply_callback(Status::OK(), nullptr, nullptr);
    }

    void HandleMarkJobFinished(const rpc::MarkJobFinishedRequest &request,
                               rpc::MarkJobFinishedReply *reply,
                               rpc::SendReplyCallback send_reply_callback) override {
      send_reply_callback(Status::OK(), nullptr, nullptr);
    }
  };

  gcs::GcsServerConfig config;
  config.grpc_server_port = 0;
  config.grpc_server_name = "MockedGcsServer";
  config.grpc_server_thread_num = 1;
  MockedGcsServer<MockedJobInfoHandler> server(config);
  std::thread([&server] { server.Start(); }).detach();

  // Wait until server starts listening.
  while (server.GetPort() == 0) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  boost::asio::io_context client_io_context;
  boost::asio::io_context::work worker(client_io_context);
  rpc::ClientCallManager client_call_manager(client_io_context);
  rpc::GcsRpcClient client("0.0.0.0", server.GetPort(), client_call_manager);
  std::thread([&client_io_context] { client_io_context.run(); }).detach();

  rpc::AddJobRequest add_job_request;
  std::promise<rpc::AddJobReply> add_job_reply_promise;
  auto add_job_reply_future = add_job_reply_promise.get_future();
  client.AddJob(add_job_request, [&add_job_reply_promise](const Status &status,
                                                          const rpc::AddJobReply &reply) {
    if (status.ok()) {
      add_job_reply_promise.set_value(reply);
    }
  });
  auto future_status = add_job_reply_future.wait_for(std::chrono::milliseconds(200));
  ASSERT_EQ(future_status, std::future_status::ready);

  rpc::MarkJobFinishedRequest mark_job_finished_request;
  std::promise<rpc::MarkJobFinishedReply> mark_job_finished_reply_promise;
  auto mark_job_finished_reply_future = mark_job_finished_reply_promise.get_future();
  client.MarkJobFinished(
      mark_job_finished_request,
      [&mark_job_finished_reply_promise](const Status &status,
                                         const rpc::MarkJobFinishedReply &reply) {
        if (status.ok()) {
          mark_job_finished_reply_promise.set_value(reply);
        }
      });
  future_status = mark_job_finished_reply_future.wait_for(std::chrono::milliseconds(200));
  ASSERT_EQ(future_status, std::future_status::ready);

  client_io_context.stop();
  server.Stop();
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
