#include "ray/rpc/grpc_server.h"
#include <chrono>
#include "gtest/gtest.h"
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
  explicit TestGrpcService(instrumented_io_context &io_service,
                           TestServiceHandler &handler)
      : GrpcService(io_service), service_handler_(handler){};

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
    thread = std::make_unique<std::thread>([this]() {
      /// The asio work to keep io_service_ alive.
      boost::asio::io_service::work io_service_work_(io_service);
      io_service.run();
    });
    test_service.reset(new TestGrpcService(io_service, test_service_handler));
    grpc_server.reset(new GrpcServer("test", 123321));
    grpc_server->RegisterService(*test_service);
    grpc_server->Run();

    // Prepare a client
    channel = grpc::CreateChannel("localhost:123321", grpc::InsecureChannelCredentials());
    test_service_stub = TestService::NewStub(channel);
  }

  void TearDown() {
    // Cleanup stuffs.
    grpc_server->Shutdown();
    io_service.stop();
    if (thread->joinable()) {
      thread->join();
    }
  }

 protected:
  TestServiceHandler test_service_handler;
  instrumented_io_context io_service;
  std::unique_ptr<std::thread> thread;
  std::unique_ptr<TestGrpcService> test_service;
  std::unique_ptr<GrpcServer> grpc_server;
  grpc::CompletionQueue cq;
  std::shared_ptr<grpc::Channel> channel;
  std::unique_ptr<TestService::Stub> test_service_stub;
};

TEST_F(TestGrpcServerFixture, TestBasic) {
  // Send request
  SleepRequest request;
  SleepReply reply;
  grpc::ClientContext context;
  request.set_sleep_time_ms(1000);
  std::unique_ptr<grpc::ClientAsyncResponseReader<SleepReply>> rpc(
      test_service_stub->AsyncSleep(&context, request, &cq));
  grpc::Status status;
  rpc->Finish(&reply, &status, (void *)1);
  // Wait for reply
  void *got_tag;
  bool ok = false;
  cq.Next(&got_tag, &ok);
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
