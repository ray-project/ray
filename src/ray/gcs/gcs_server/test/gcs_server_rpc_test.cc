#include <boost/asio/basic_socket.hpp>
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "ray/gcs/gcs_server/gcs_server.h"
#include "ray/rpc/gcs_server/job_info_access_client.h"

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

    std::unique_ptr<rpc::JobInfoAccessHandler> InitJobInfoAccessHandler() override {
      return std::unique_ptr<rpc::JobInfoAccessHandler>(new T);
    }
  };
};

TEST_F(GcsServerRpcTest, JobInfo) {
  class MockedJobInfoAccessHandler : public rpc::JobInfoAccessHandler {
   public:
    void HandleAddJob(const rpc::GcsJobInfo &request, rpc::AddJobReply *reply,
                      rpc::SendReplyCallback send_reply_callback) override {
      send_reply_callback(Status::OK(), nullptr, nullptr);
    }

    void HandleMarkJobFinished(const rpc::FinishedJob &request,
                               rpc::MarkJobFinishedReply *reply,
                               rpc::SendReplyCallback send_reply_callback) override {
      send_reply_callback(Status::OK(), nullptr, nullptr);
    }
  };

  gcs::GcsServerConfig config;
  config.server_port = 0;
  config.server_name = "MockedGcsServer";
  config.server_thread_num = 1;
  MockedGcsServer<MockedJobInfoAccessHandler> server(config);
  std::thread([&server] { server.Start(); }).detach();

  // Wait until server starts listening.
  while (server.GetPort() == 0) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  boost::asio::io_context client_io_context;
  boost::asio::io_context::work worker(client_io_context);
  rpc::ClientCallManager client_call_manager(client_io_context);
  rpc::JobInfoAccessClient client("0.0.0.0", server.GetPort(), client_call_manager);
  std::thread([&client_io_context] { client_io_context.run(); }).detach();

  rpc::GcsJobInfo job_info;
  std::promise<rpc::AddJobReply> add_job_reply_promise;
  auto add_job_reply_future = add_job_reply_promise.get_future();
  client.AddJob(job_info, [&add_job_reply_promise](const Status &status,
                                                   const rpc::AddJobReply &reply) {
    if (status.ok()) {
      add_job_reply_promise.set_value(reply);
    }
  });
  auto future_status = add_job_reply_future.wait_for(std::chrono::milliseconds(200));
  ASSERT_EQ(future_status, std::future_status::ready);

  rpc::FinishedJob job;
  std::promise<rpc::MarkJobFinishedReply> mark_job_finished_reply_promise;
  auto mark_job_finished_reply_future = mark_job_finished_reply_promise.get_future();
  client.MarkJobFinished(
      job, [&mark_job_finished_reply_promise](const Status &status,
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
