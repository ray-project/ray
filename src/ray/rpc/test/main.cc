#include "gtest/gtest.h"
#include "ray/rpc/test/test_client.h"
#include "ray/rpc/test/test_server.h"

#include <boost/asio.hpp>
#include <boost/asio/error.hpp>
#include <boost/bind.hpp>

#include <string>
#include <thread>
#include <vector>

using std::cout;
using std::endl;
using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::vector;

string GenerateMessage(const string &hdr, int idx) {
  return hdr + "-idx-" + std::to_string(idx);
}

bool VerifyMessage(const string &msg, const string &hdr, int idx) {
  if (msg == GenerateMessage(hdr, idx)) {
    return true;
  }
  return false;
}

int GetIndex(const string &str) {
  int idx = -1;
  int pos = str.find("-idx-");
  if (pos != string::npos) {
    string idx_str;
    idx_str.assign(str.begin() + pos + 5, str.end());
    idx = std::stoi(idx_str);
  }
  return idx;
}

namespace ray {
namespace rpc {

// Server handlers for test service.
class ServiceHandlers : public TestServiceHandler {
 public:
  void HandleDebugEcho(const DebugEchoRequest &request, DebugEchoReply *reply,
                       SendReplyCallback send_reply_callback) override {
    RAY_LOG(DEBUG) << "Server received request from the client, msg: "
                   << request.request_message();
    reply->set_reply_message(
        GenerateMessage("ReplyMessage", GetIndex(request.request_message())));
    send_reply_callback(Status::OK(), nullptr, nullptr);
  }

  void HandleDebugStreamEcho(
      const DebugEchoRequest &request,
      StreamReplyWriter<DebugEchoRequest, DebugEchoReply> &stream_reply_writer) override {
    const string &str = request.request_message();
    int idx = GetIndex(str);
    if (idx % 2 == 0) {
      std::unique_ptr<DebugEchoReply> reply(new DebugEchoReply);
      reply->set_reply_message(GenerateMessage("StreamReplyMessage", idx));
      stream_reply_writer.Write(std::move(reply));
    }
    RAY_LOG(DEBUG) << "Received stream request from client, request: "
                   << request.request_message();
  }
};

class GrpcTest : public ::testing::Test {
 public:
  GrpcTest()
      : client_work_(client_service_),
        server_work_(server_service_),
        client_call_manager_(client_service_),
        service_(server_service_, service_handlers_) {
    // Setup grpc server.
    server_.reset(new GrpcServer("DebugTestServer", 12345));
    server_->RegisterService(service_);
    server_->Run();
  }

  ~GrpcTest() {
    // Wait all requests are finished before stop the server.
    usleep(10 * 1000);
    RAY_LOG(INFO) << "Begin to SHUTDOWN the server.";
    server_->Shutdown();
  }

  void SetUp() {
    client_thread_.reset(new std::thread([this]() { client_service_.run(); }));
    server_thread_.reset(new std::thread([this]() { server_service_.run(); }));
  }

  void TearDown() {
    client_service_.stop();
    client_thread_->join();

    server_service_.stop();
    server_thread_->join();

    client_thread_.reset();
    server_thread_.reset();
  }

 protected:
  // The thread for client to handle reply from the server.
  boost::asio::io_service client_service_;
  boost::asio::io_service::work client_work_;
  std::unique_ptr<std::thread> client_thread_;

  // The thread for server to handle request from the client.
  boost::asio::io_service server_service_;
  boost::asio::io_service::work server_work_;
  std::unique_ptr<std::thread> server_thread_;

  unique_ptr<GrpcServer> server_;
  ClientCallManager client_call_manager_;
  ServiceHandlers service_handlers_;
  TestService service_;
};

TEST_F(GrpcTest, AsyncCallTest) {
  int num_requests = 1000;
  int requests_counter = 0;
  DebugTestClient client("127.0.0.1", 12345, client_call_manager_);
  std::promise<void> p;
  std::future<void> f(p.get_future());

  auto callback = [&p, &requests_counter, num_requests](const Status &status,
                                                        const DebugEchoReply &reply) {
    if (!status.ok()) {
      RAY_LOG(INFO) << "Failed to send DebugEchoReply request, msg: " << status.message();
    }
    RAY_LOG(DEBUG) << "Received reply from server, msg: " << reply.reply_message();
    requests_counter++;
    if (requests_counter == num_requests) {
      p.set_value();
    }
  };

  auto start_time = std::chrono::system_clock::now().time_since_epoch();
  for (int i = 0; i < num_requests; i++) {
    DebugEchoRequest request;
    request.set_request_message(GenerateMessage("Reuqest", i + 1));
    RAY_CHECK_OK(client.DebugEcho(request, callback));
  }
  // Wait for the last reply from the server.
  f.get();
  std::chrono::duration<double> cost =
      std::chrono::system_clock::now().time_since_epoch() - start_time;
  RAY_LOG(INFO) << "Spend " << cost.count() << " seconds to finish " << num_requests
                << " requests.";
}

TEST_F(GrpcTest, StreamRequestTest) {
  int num_messages = 1000;
  DebugTestClient client("127.0.0.1", 12345, client_call_manager_);

  std::promise<void> p;
  std::future<void> f(p.get_future());

  client.StartStreamEcho(
      [&p, num_messages](const Status &status, const rpc::DebugEchoReply &reply) {
        RAY_LOG(DEBUG) << "Stream client received reply from server, reply: "
                       << reply.reply_message();
        auto idx = GetIndex(reply.reply_message());
        ASSERT_TRUE(idx % 2 == 0);
        // Wait for the last reply.
        if (idx == num_messages) {
          p.set_value();
        }
      },
      1000);

  auto start_time = std::chrono::system_clock::now().time_since_epoch();
  for (int i = 0; i < num_messages; i++) {
    DebugEchoRequest request;
    request.set_request_message(GenerateMessage("StreamRequest", i + 1));
    client.DebugStreamEcho(request);
  }
  // Wait to receive the last reply from the server.
  f.get();
  std::chrono::duration<double> cost =
      std::chrono::system_clock::now().time_since_epoch() - start_time;
  RAY_LOG(INFO) << "Spend " << cost.count() << " seconds to finish " << num_messages
                << " requests.";
  client.CloseStreamEcho();
  // Wait for the server to receive writes done message.
  usleep(10 * 1000);
}

}  // namespace rpc
}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}