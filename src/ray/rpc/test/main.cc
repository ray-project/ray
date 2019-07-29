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
    cout << "Received request in DebugEcho, msg " << request.request_message() << endl;
    reply->set_reply_message("Reply for DebugEcho.");
    send_reply_callback(Status::OK(), nullptr, nullptr);
  }

  void HandleDebugStreamEcho(
      const DebugEchoRequest &request,
      StreamReplyWriter<DebugEchoRequest, DebugEchoReply> &stream_writer) override {
    const string &str = request.request_message();
    int idx = GetIndex(str);
    if (idx % 2 == 0) {
      DebugEchoReply reply;
      reply.set_reply_message(GenerateMessage("StreamReplyMessage", idx));
      stream_writer.Write(reply);
    }
    cout << "Received request in DebugStreamEcho, msg " << request.request_message()
         << endl;
  }
};

class GrpcTest : public ::testing::Test {
 public:
  GrpcTest()
      : work_(io_service_),
        client_call_manager_(io_service_),
        service_(io_service_, service_handlers_) {}

  ~GrpcTest() {}

  void SetUp() {
    server_thread_.reset(new std::unique_ptr<std::thread>>
                         ([this]() { io_service_.run(); }));
    server_.reset(new GrpcServer("DebugTestServer", 12345));
    server_->RegisterService(service_);
    server_->Run();
  }

  void TearDown() {
    server_.reset();
    io_service_.stop();
    server_thread_->join();
    server_thread_.reset();
  }

 protected:
  boost::asio::io_service io_service_;
  boost::asio::io_serivce::work work_;
  std::unique_ptr<std::thread> server_thread_;
  unique_ptr<GrpcServer> server_;
  ClientCallManager client_call_manager_;
  ServiceHandlers service_handlers_;
  TestService service_;
};

TEST_F(GrpcTest, MultiClientsTest) {}

TEST_F(GrpcTest, UnixDomainSocketTest) {}

TEST_F(GrpcTest, ThreadSafeClientTest) {}

TEST_F(GrpcTest, StreamRequestTest) {
  int num_messages = 10;
  DebugTestClient client("127.0.0.1", 12345, client_call_manager_);
  client.StartEchoStream([](const Status &status, const rpc::DebugEchoReply &reply) {
    cout << "Received reply from server, reply: " << reply.reply_message();
  });
  for (int i = 0; i < num_messages; i++) {
    DebugEchoRequest request;
    request.set_request_message(GenerateMessage("StreamRequest", i + 1));
    client.DebugStreamEcho(request);
  }
  client.CloseEchoStream();
}

}  // namespace rpc
}  // namespace ray

int main() { return 0; }