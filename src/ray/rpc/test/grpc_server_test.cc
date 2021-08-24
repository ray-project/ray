#include "ray/rpc/grpc_server.h"
#include <chrono>
#include "gtest/gtest.h"
#include "ray/rpc/grpc_client.h"
#include "src/ray/protobuf/gcs_service.grpc.pb.h"

namespace ray {
namespace rpc {
class TestServiceHandler {
 public:
  void HandleSleep(const SleepRequest &request, SleepReply *reply,
                   SendReplyCallback send_reply_callback) {
    RAY_LOG(INFO) << "Got sleep request, time=" << request.sleep_time_ms() << "ms";
    std::this_thread::sleep_for(std::chrono::milliseconds(request.sleep_time_ms()));
    send_reply_callback(ray::Status::OK(), nullptr, nullptr);
  }
};

class TestGrpcService : public GrpcService {
 public:
  /// Constructor.
  ///
  /// \param[in] handler The service handler that actually handle the requests.
  explicit TestGrpcService(instrumented_io_context &handler_io_service_,
                           TestServiceHandler &handler)
      : GrpcService(handler_io_service_), service_handler_(handler){};

 protected:
  grpc::Service &GetGrpcService() override { return service_; }

  void InitServerCallFactories(
      const std::unique_ptr<grpc::ServerCompletionQueue> &cq,
      std::vector<std::unique_ptr<ServerCallFactory>> *server_call_factories) override {
    RPC_SERVICE_HANDLER(TestService, Sleep, 10);
  }

 private:
  /// The grpc async service object.
  TestService::AsyncService service_;
  /// The service handler that actually handle the requests.
  TestServiceHandler &service_handler_;
};

class TestGrpcServerFixture : public ::testing::Test {
 public:
  void SetUp() {
    // Prepare and start test server.
    thread_ = std::make_unique<std::thread>([this]() {
      /// The asio work to keep handler_io_service__ alive.
      boost::asio::io_service::work handler_io_service__work_(
          handler_io_service_);
      handler_io_service_.run();
    });
    test_service_.reset(new TestGrpcService(handler_io_service_, test_service_handler_));
    grpc_server_.reset(new GrpcServer("test", 123321));
    grpc_server_->RegisterService(*test_service_);
    grpc_server_->Run();

    // Prepare a client
    channel_ =
        grpc::CreateChannel("localhost:123321", grpc::InsecureChannelCredentials());
    test_service_stub_ = TestService::NewStub(channel_);
  }

  void TearDown() {
    // Cleanup stuffs.
    grpc_server_->Shutdown();
    handler_io_service_.stop();
    if (thread_->joinable()) {
      thread_->join();
    }
  }

 protected:
  // Server
  TestServiceHandler test_service_handler_;
  instrumented_io_context handler_io_service_;
  std::unique_ptr<std::thread> thread_;
  std::unique_ptr<TestGrpcService> test_service_;
  std::unique_ptr<GrpcServer> grpc_server_;
  grpc::CompletionQueue cq_;
  std::shared_ptr<grpc::Channel> channel_;
  std::unique_ptr<TestService::Stub> test_service_stub_;
  // Client
  instrumented_io_context client_io_service_;
  std::unique_ptr<ClientCallManager> client_call_manager_;
};

TEST_F(TestGrpcServerFixture, TestBasic) {
  // Send request
  SleepRequest request;
  SleepReply reply;
  grpc::ClientContext context;
  request.set_sleep_time_ms(1000);
  std::unique_ptr<grpc::ClientAsyncResponseReader<SleepReply>> rpc(
      test_service_stub_->AsyncSleep(&context, request, &cq_));
  grpc::Status status;
  rpc->Finish(&reply, &status, (void *)1);
  // Wait for reply
  void *got_tag;
  bool ok = false;
  cq_.Next(&got_tag, &ok);
  if (ok && got_tag == (void *)1) {
    RAY_LOG(INFO) << ok;
  }
}

TEST_F(TestGrpcServerFixture, TestMaxActiveRpcs) {
  // Send request
  SleepRequest request;
  SleepReply reply;
  grpc::ClientContext context;
  request.set_sleep_time_ms(1000);
  std::unique_ptr<grpc::ClientAsyncResponseReader<SleepReply>> rpc(
      test_service_stub_->AsyncSleep(&context, request, &cq_));
  grpc::Status status;
  rpc->Finish(&reply, &status, (void *)1);
  // Wait for reply
  void *got_tag;
  bool ok = false;
  cq_.Next(&got_tag, &ok);
  if (ok && got_tag == (void *)1) {
    RAY_LOG(INFO) << ok;
  }
}
}  // namespace rpc
}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
