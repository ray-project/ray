#include <grpcpp/grpcpp.h>
#include <gtest/gtest.h>
#include <boost/asio.hpp>

#include "ray/common/status.h"
#include "ray/rpc/client_call.h"
#include "ray/rpc/grpc_client.h"
#include "ray/rpc/grpc_server.h"
#include "ray/util/test_util.h"
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
  /// \param[in] strict_request_order Whether the server should handle requests from this
  /// client in the same order as they were sent.
  TestClient(const std::string &address, const int port,
             ClientCallManager &client_call_manager, bool strict_request_order)
      : GrpcClient(strict_request_order), client_call_manager_(client_call_manager) {
    std::shared_ptr<grpc::Channel> channel = grpc::CreateChannel(
        address + ":" + std::to_string(port), grpc::InsecureChannelCredentials());
    stub_ = TestService::NewStub(channel);
  };

  /// Send a Hello request.
  ///
  /// \param[in] request The request message.
  /// \param[in] callback The callback function that handles reply.
  Status Hello(const HelloRequest &request, const ClientCallback<HelloReply> &callback) {
    auto call = client_call_manager_.CreateCall<TestService, HelloRequest, HelloReply>(
        *stub_, &TestService::Stub::PrepareAsyncHello, request, callback,
        GetMetaForNextRequest());
    return call->GetStatus();
  }

 private:
  /// The `ClientCallManager` used for managing requests.
  ClientCallManager &client_call_manager_;
};

class TestServiceHandler {
 public:
  /// Handle a `Hello` request.
  virtual void HandleHello(const HelloRequest &request, HelloReply *reply,
                           SendReplyCallback send_reply_callback) = 0;
  virtual ~TestServiceHandler() = default;
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
    // Initialize the factory for `Hello` requests.
    std::unique_ptr<ServerCallFactory> Hello_call_factory(
        new ServerCallFactoryImpl<TestService, TestServiceHandler, HelloRequest,
                                  HelloReply>(
            service_, &TestService::AsyncService::RequestHello, service_handler_,
            &TestServiceHandler::HandleHello, cq, main_service_));
    server_call_factories_and_concurrencies->emplace_back(std::move(Hello_call_factory),
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
  /// Constructor.
  ///
  /// \param io_service `io_service` used for the `GrpcServer`.
  /// \param strict_request_order Whether the server should handle the requests in the
  /// same order as they were sent from the client.
  explicit TestServer(boost::asio::io_service &io_service, bool strict_request_order)
      : test_server_("TestServer", 0),
        test_grpc_service_(io_service, *this),
        strict_request_order_(strict_request_order) {
    test_server_.RegisterService(test_grpc_service_);
    test_server_.Run();
  }

  void HandleHello(const HelloRequest &request, HelloReply *reply,
                   SendReplyCallback send_reply_callback) override {
    const auto &client = request.who();
    reply->set_message("Hello, " + client);

    if (strict_request_order_) {
      auto index = request.index();
      ASSERT_TRUE(index == last_received_request_index[client] + 1);
      last_received_request_index[client] += 1;
    }

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
  bool strict_request_order_;
  // Index of the last received request per client.
  std::unordered_map<std::string, int64_t> last_received_request_index;
};

class RpcTest : public ::testing::Test {
 public:
  RpcTest()
      : client_work_(client_service_),
        server_work_(server_service_),
        client_call_manager_(client_service_) {}

  void SetUp() override {
    client_thread_.reset(new std::thread([this] { client_service_.run(); }));
    server_thread_.reset(new std::thread([this] { server_service_.run(); }));
  }

  void TearDown() override {
    client_service_.stop();
    client_thread_->join();
    client_thread_.reset();

    server_service_.stop();
    server_thread_->join();
    server_thread_.reset();
  }

  /// Create a server.
  ///
  /// \param strict_request_order Whether the server should handle the requests in the
  /// same order as they were sent from the client.
  std::unique_ptr<TestServer> CreateServer(bool strict_request_order) {
    return std::unique_ptr<TestServer>(
        new TestServer(server_service_, strict_request_order));
  }

  /// Create a client.
  ///
  /// \param[in] server_port Port of the server.
  /// \param[in] strict_request_order Whether the server should handle requests from this
  /// client in the same order as they were sent.
  std::unique_ptr<TestClient> CreateClient(int server_port, bool strict_request_order) {
    return std::unique_ptr<TestClient>(new TestClient(
        "127.0.0.1", server_port, client_call_manager_, strict_request_order));
  }

 protected:
  // `io_service` for the clients.
  boost::asio::io_service client_service_;
  // A `io_service::work` to keep `client_service_` running.
  boost::asio::io_service::work client_work_;
  // The thread in which `client_service_` is running.
  std::unique_ptr<std::thread> client_thread_;

  // `io_service` for the servers.
  boost::asio::io_service server_service_;
  // A `io_service::work` to keep `server_service_` running.
  boost::asio::io_service::work server_work_;
  // The thread in which `server_service_` is running.
  std::unique_ptr<std::thread> server_thread_;

  // The `ClientCallManager` shared by all clients.
  ClientCallManager client_call_manager_;
};

/// Test sending unary requests.
///
/// \param[in] rpc_test The `RpcTest` object.
/// \param[in] num_servers Number of servers to create.
/// \param[in] num_clients Number of clients per server to create.
/// \param[in] num_requests Number of requests to send per client.
/// \param[in] strict_request_order Whether the server should handle requests from this
/// client in the same order as they were sent.
void TestUnaryRequests(RpcTest &rpc_test, int num_servers, int num_clients,
                       int num_requests, bool strict_request_order) {
  std::vector<std::unique_ptr<TestServer>> servers;
  std::vector<std::unique_ptr<TestClient>> clients;

  // Create servers and clients.
  for (int i = 0; i < num_servers; i++) {
    servers.emplace_back(rpc_test.CreateServer(strict_request_order));
    for (int j = 0; j < num_clients; j++) {
      clients.emplace_back(rpc_test.CreateClient(servers.back()->GetPort(), strict_request_order));
    }
  }

  // Number of pending replies.
  std::atomic<int> pending_replies(num_servers * num_clients * num_requests);

  for (int i = 0; i < clients.size(); i++) {
    const auto &client = clients[i];
    std::string client_name = "client_" + std::to_string(i);
    for (int j = 0; j < num_requests; j++) {
      HelloRequest request;
      request.set_who(client_name);
      request.set_index(j);
      auto status = client->Hello(
          request, [&pending_replies, client_name](const Status &status, const HelloReply &reply) {
            ASSERT_TRUE(status.ok());
            pending_replies -= 1;
            ASSERT_EQ(reply.message(), "Hello, " + client_name);
          });
      ASSERT_TRUE(status.ok());
    }
  }
  // Wait until all replies are received.
  WaitForCondition([&pending_replies]() { return pending_replies == 0; }, 5000);
}

// Test sending unary requests from one client to one server.
TEST_F(RpcTest, TestUnaryRequests) { TestUnaryRequests(*this, 1, 1, 100, false); }

// Test sending unary requests from multiple clients to multiple servers.
TEST_F(RpcTest, TestUnaryRequestsWithManyClientsAndServers) {
  TestUnaryRequests(*this, 5, 5, 100, false);
}

// Test the `strict_request_order` option in `GrpcClient`.
TEST_F(RpcTest, TestStrictRequestOrder) {
  TestUnaryRequests(*this, 1, 10, 100, true);
}

}  // namespace rpc
}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
