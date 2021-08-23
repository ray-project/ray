#include "src/ray/protobuf/gcs_service.grpc.pb.h"
#include "ray/rpc/grpc_server.h"
#include "gtest/gtest.h"

namespace ray {
namespace rpc {
class TestServiceHandler {
 public:
  void HandleSleep(const TestRequest &request, TestReply *reply,
                   SendReplyCallback send_reply_callback) {
    std::this_thread::sleep_for(chrono::milliseconds(request.sleep_time_ms()));
  }
};

class TestGrpcService : public GrpcService {
 public:
  /// Constructor.
  ///
  /// \param[in] handler The service handler that actually handle the requests.
  explicit TestGrpcService(instrumented_io_context &io_service,
                           TestGcsServiceHandler &handler)
      : GrpcService(io_service), service_handler_(handler){};

 protected:
  grpc::Service &GetGrpcService() override { return service_; }

  void InitServerCallFactories(
      const std::unique_ptr<grpc::ServerCompletionQueue> &cq,
      std::vector<std::unique_ptr<ServerCallFactory>> *server_call_factories) override {
    RPC_SERVICE_HANDLER(TestGrpcService, Sleep, 10);
  }

 private:
  /// The grpc async service object.
  TestService::AsyncService service_;
  /// The service handler that actually handle the requests.
  TestGcsServiceHandler &service_handler_;
};

TEST_F(TestGcsServer, TestClientDieBeforeReply) {
  // Prepare and start test server.
  TestServiceHandler test_service_handler;
  instrumented_io_context io_service;
  auto thread = std::thread([&io_service]() {
    SetThreadName("TestServer");
    /// The asio work to keep io_service_ alive.
    boost::asio::io_service::work io_service_work_(heartbeat_io_service_);
    heartbeat_io_service_.run();
  });
  TestGrpcService test_service(io_service, test_service_handler);
  GrpcServer grpc_server("test", "123321");
  grpc_server.RegisterService(test_service);
  grpc_server.Run();

  // Prepare a client
  grpc::ClientContext context;
  auto channel =
      grpc::CreateChannel("localhost:123321", grpc::InsecureChannelCredentials());
  std::unique_ptr<TestService::Stub> test_service_stub(TestService::NewStub(channel));
  CompletionQueue cq;

  // Send request
  SleepRequest request;
  request.set_sleep_time_ms(1000);
  std::unique_ptr<ClientAsyncResponseReader<SleepReply>> rpc(
      test_service_stub->AsyncSleep(&context, request, &cq));
  Status status;
  rpc->Finish(&reply, &status, (void *)1);

  // Wait for reply
  void *got_tag;
  bool ok = false;
  cq.Next(&got_tag, &ok);
  if (ok && got_tag == (void *)1) {
    RAY_LOG(INFO) << ok;
  }

  // Cleanup stuffs.
  grpc_server.Shutdown();
  io_service.stop();
  if (thread.joinable()) {
    thread.join();
  }
}
}  // namespace rpc
}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
