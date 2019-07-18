#include "gtest/gtest.h"

#include <boost/asio.hpp>
#include <boost/asio/error.hpp>
#include <boost/bind.hpp>

#include <string>
#include <thread>
#include <vector>

using std::string;
using std::vector;
const string hdr = "RequestMessage";

vector<string> GenerateMessages(int n) {
  vector<string> result;
  for (int i = 1; i <= n; i++) {
    result.emplace_back(hdr + std::to_string(i));
  }
  return result;
}

bool VerifyMessages(vector<string> &messages) {
  for (size_t i = 1; i <= messages.size(); i++) {
    if (messages[i] != (hdr + std::to_string(i))) {
      return false;
    }
  }
  return true;
}

class GrpcTest : public ::testing::Test {
 public:
  GrpcTest() {}

  ~GrpcTest() {}
};

TEST_F(GrpcTest, MultiClientsTest) {}

TEST_F(GrpcTest, UnixDomainSocketTest) {

}

TEST_F(GrpcTest, ThreadSafeClientTest) {}

TEST_F(GrpcTest, StreamRequestTest) {}

int main() { return 0; }