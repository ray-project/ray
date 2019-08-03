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
    RAY_LOG(INFO) << "Server received request in DebugEcho, msg "
                  << request.request_message();
    reply->set_reply_message("Reply for DebugEcho.");
    send_reply_callback(Status::OK(), nullptr, nullptr);
  }

  void HandleDebugStreamEcho(
      const DebugEchoRequest &request,
      StreamReplyWriter<DebugEchoRequest, DebugEchoReply> &stream_reply_writer) override {
    RAY_LOG(INFO) << "Start server callback.";
    const string &str = request.request_message();
    int idx = GetIndex(str);
    if (idx % 2 == 0) {
      std::unique_ptr<DebugEchoReply> reply(new DebugEchoReply);
      reply->set_reply_message(GenerateMessage("StreamReplyMessage", idx));
      stream_reply_writer.Write(std::move(reply));
    }
    RAY_LOG(INFO) << "Received stream request from client, request: "
                  << request.request_message();
  }
};

class GrpcTest : public ::testing::Test {
 public:
  GrpcTest()
      : work_(io_service_),
        client_call_manager_(io_service_),
        service_(io_service_, service_handlers_) {
    // Setup grpc server.
    server_.reset(new GrpcServer("DebugTestServer", 12345));
    server_->RegisterService(service_);
    server_->Run();
  }

  ~GrpcTest() {
    // Wait all requests are finished before stop the server.
    usleep(200 * 1000);
    RAY_LOG(INFO) << "Begin to SHUTDOWN the server.";
    server_->Shutdown();
  }

  void SetUp() {
    handle_thread_.reset(new std::thread([this]() { io_service_.run(); }));
  }

  void TearDown() {
    io_service_.stop();
    handle_thread_->join();
    handle_thread_.reset();
  }

 protected:
  boost::asio::io_service io_service_;
  boost::asio::io_service::work work_;
  std::unique_ptr<std::thread> handle_thread_;
  unique_ptr<GrpcServer> server_;
  ClientCallManager client_call_manager_;
  ServiceHandlers service_handlers_;
  TestService service_;
};

/*
TEST_F(GrpcTest, AsyncCallTest) {
  DebugTestClient client("127.0.0.1", 12345, client_call_manager_);
}
*/

// TEST_F(GrpcTest, UnixDomainSocketTest) {}

// TEST_F(GrpcTest, ThreadSafeClientTest) {}

TEST_F(GrpcTest, StreamRequestTest) {
  int num_messages = 30;
  DebugTestClient client("127.0.0.1", 12345, client_call_manager_);
  client.StartEchoStream(
      [](const Status &status, const rpc::DebugEchoReply &reply) {
        RAY_LOG(INFO) << "Stream client received reply from server, reply: "
                      << reply.reply_message();
        auto idx = GetIndex(reply.reply_message());
        ASSERT_TRUE(idx % 2 == 0);
      },
      100);

  for (int i = 0; i < num_messages; i++) {
    DebugEchoRequest request;
    request.set_request_message(GenerateMessage("StreamRequest", i + 1));
    client.DebugStreamEcho(request);
  }
  // Wait client to send some requests.
  usleep(10 * 1000);
  RAY_LOG(INFO) << "Try to close echo stream.";
  client.CloseEchoStream();
}

}  // namespace rpc
}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}