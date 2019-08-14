#include <grpcpp/grpcpp.h>
#include <gtest/gtest.h>
#include <boost/asio.hpp>

#include "ray/common/status.h"
#include "ray/rpc/client_call.h"
#include "ray/rpc/grpc_client.h"
#include "ray/rpc/grpc_server.h"
#include "src/ray/protobuf/test.grpc.pb.h"

namespace ray {
namespace rpc {

using ray::Status;

class TestClient : public GrpcClient<TestService> {
 public:
  /// Constructor.
  ///
  /// \param[in] address Address of the test server.
  /// \param[in] port Port of the test server.
  /// \param[in] client_call_manager The `ClientCallManager` used for managing requests.
  TestClient(const std::string &address, const int port,
             ClientCallManager &client_call_manager, bool keep_request_order)
      : GrpcClient(keep_request_order), client_call_manager_(client_call_manager) {
    std::shared_ptr<grpc::Channel> channel = grpc::CreateChannel(
        address + ":" + std::to_string(port), grpc::InsecureChannelCredentials());
    stub_ = TestService::NewStub(channel);
  };

  /// Send an echo request.
  ///
  /// \param[in] request The request message.
  /// \param[in] callback The callback function that handles reply.
  Status Echo(const EchoRequest &request, const ClientCallback<EchoReply> &callback) {
    auto call = client_call_manager_.CreateCall<TestService, EchoRequest, EchoReply>(
        *stub_, &TestService::Stub::PrepareAsyncEcho, request, callback,
        GetMetaForNextRequest());
    return call->GetStatus();
  }

 private:
  /// The `ClientCallManager` used for managing requests.
  ClientCallManager &client_call_manager_;
};

class TestServiceHandler {
 public:
  /// Handle a `Echo` request.
  virtual void HandleEcho(const EchoRequest &request, EchoReply *reply,
                          SendReplyCallback send_reply_callback) = 0;
  virtual ~TestServiceHandler() = 0;
};

/// The `GrpcService` for `TestService`.
class TestGrpcService : public GrpcService {
 public:
  /// Constructor.
  ///
  /// \param[in] io_service See super class.
  /// \param[in] handler The service handler that actually handle the requests.
  TestGrpcService(boost::asio::io_service &io_service,
                  TestServiceHandler &service_handler)
      : GrpcService(io_service), service_handler_(service_handler){};

 protected:
  grpc::Service &GetGrpcService() override { return service_; }

  void InitServerCallFactories(
      const std::unique_ptr<grpc::ServerCompletionQueue> &cq,
      std::vector<std::pair<std::unique_ptr<ServerCallFactory>, int>>
          *server_call_factories_and_concurrencies) override {
    // Initialize the factory for `Echo` requests.
    std::unique_ptr<ServerCallFactory> echo_call_factory(
        new ServerCallFactoryImpl<TestService, TestServiceHandler, EchoRequest,
                                  EchoReply>(
            service_, &TestService::AsyncService::RequestEcho, service_handler_,
            &TestServiceHandler::HandleEcho, cq, main_service_));
    server_call_factories_and_concurrencies->emplace_back(std::move(echo_call_factory),
                                                          10);
  }

 private:
  /// The grpc async service object.
  TestService::AsyncService service_;

  /// The service handler that actually handle the requests.
  TestServiceHandler &service_handler_;
};

class TestServer : public TestServiceHandler {
 public:
  explicit TestServer(boost::asio::io_service &io_service)
      : test_server_("TestServer", 0), test_grpc_service_(io_service, *this) {
    test_server_.RegisterService(test_grpc_service_);
    test_server_.Run();
  }

  void HandleEcho(const EchoRequest &request, EchoReply *reply,
                  SendReplyCallback send_reply_callback) override {
    RAY_LOG(DEBUG) << "Server received a request from the client, msg: "
                   << request.request_message();
    reply->set_reply_message(request.request_message());
    auto success_callback = [] { RAY_LOG(DEBUG) << "Reply sent to the client."; };
    auto failure_callback = [] {
      RAY_LOG(FATAL) << "Failed to send reply to the client.";
    };
    send_reply_callback(Status::OK(), success_callback, failure_callback);
  }

  int GetPort() { return test_server_.GetPort(); }

 private:
  GrpcServer test_server_;
  TestGrpcService test_grpc_service_;
};

class RpcTest : public ::testing::Test {
 public:
  RpcTest()
      : client_work_(client_service_),
        server_work_(server_service_),
        client_call_manager_(new ClientCallManager(client_service_)) {}

  void SetUp() override {
    client_thread_.reset(new std::thread([this] { client_service_.run(); }));
    server_thread_.reset(new std::thread([this] { server_service_.run(); }));
  }

  void TearDown() override {
//    RAY_LOG(INFO) << "Tear down";
//    client_service_.stop();
//    client_thread_->join();
//    client_thread_.reset();
//    RAY_LOG(INFO) << "Tear down 1";
//
//    server_service_.stop();
//    server_thread_->join();
//    server_thread_.reset();
//    RAY_LOG(INFO) << "Tear down 2";
  }

  std::unique_ptr<TestServer> CreateServer() {
    return std::unique_ptr<TestServer>(new TestServer(server_service_));
  }

  std::unique_ptr<TestClient> CreateClient(int port, bool keep_request_order) {
    return std::unique_ptr<TestClient>(
        new TestClient("127.0.0.1", port, *client_call_manager_, keep_request_order));
  }

// protected:
  boost::asio::io_service client_service_;
  boost::asio::io_service::work client_work_;
  std::unique_ptr<std::thread> client_thread_;

  boost::asio::io_service server_service_;
  boost::asio::io_service::work server_work_;
  std::unique_ptr<std::thread> server_thread_;

  std::unique_ptr<ClientCallManager> client_call_manager_;
};

void TestUnaryRequests(RpcTest &rpc_test, int num_servers, int num_clients,
                       int num_requests) {
  std::vector<std::unique_ptr<TestServer>> servers;
  std::vector<std::unique_ptr<TestClient>> clients;
  for (int i = 0; i < num_servers; i++) {
    servers.emplace_back(rpc_test.CreateServer());
    for (int j = 0; j < num_clients; j++) {
      clients.emplace_back(rpc_test.CreateClient(servers.back()->GetPort(), false));
    }
  }

//  int num_replies = 0;

//  for (const auto &client : clients) {
//    for (int i = 0; i < num_requests; i++) {
//      EchoRequest request;
//      request.set_request_message(std::to_string(i));
//      auto status = client->Echo(
//          request, [&num_replies, i](const Status &status, const EchoReply &reply) {
//            RAY_CHECK_OK(status);
//            num_replies++;
//            ASSERT_EQ(reply.reply_message(), std::to_string(i));
//          });
//      RAY_CHECK_OK(status);
//    }
//  }
//  std::this_thread::sleep_for(std::chrono::microseconds(2000));
  RAY_LOG(INFO) << "Tear down";
  rpc_test.client_service_.stop();
  rpc_test.client_thread_->join();
  rpc_test.client_thread_.reset();
  RAY_LOG(INFO) << "Tear down 1";

  rpc_test.server_service_.stop();
  rpc_test.server_thread_->join();
  rpc_test.server_thread_.reset();
  RAY_LOG(INFO) << "Tear down 2";
  rpc_test.client_call_manager_.reset(nullptr);
  RAY_LOG(INFO) << "END";
}

TEST_F(RpcTest, TestUnaryRequests) { TestUnaryRequests(*this, 2, 1, 100); }

}  // namespace rpc
}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
