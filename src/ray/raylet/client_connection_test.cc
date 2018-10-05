#include <list>
#include <memory>

#include <boost/asio.hpp>
#include <boost/asio/error.hpp>
#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "ray/common/client_connection.h"

namespace ray {
namespace raylet {

class ClientConnectionTest : public ::testing::Test {
 public:
  ClientConnectionTest() : io_service_(), in_(io_service_), out_(io_service_) {
    boost::asio::local::connect_pair(in_, out_);
  }

 protected:
  boost::asio::io_service io_service_;
  boost::asio::local::stream_protocol::socket in_;
  boost::asio::local::stream_protocol::socket out_;
};

TEST_F(ClientConnectionTest, SimpleSyncWrite) {
  const uint8_t arr[5] = {1, 2, 3, 4, 5};
  int num_messages = 0;

  ClientHandler<boost::asio::local::stream_protocol> client_handler =
      [](LocalClientConnection &client) {};

  MessageHandler<boost::asio::local::stream_protocol> message_handler =
      [&arr, &num_messages](std::shared_ptr<LocalClientConnection> client,
                            int64_t message_type, const uint8_t *message) {
        ASSERT_TRUE(!std::memcmp(arr, message, 5));
        num_messages += 1;
      };

  auto conn1 = LocalClientConnection::Create(client_handler, message_handler,
                                             std::move(in_), "conn1");

  auto conn2 = LocalClientConnection::Create(client_handler, message_handler,
                                             std::move(out_), "conn2");

  RAY_CHECK_OK(conn1->WriteMessage(0, 5, arr));
  RAY_CHECK_OK(conn2->WriteMessage(0, 5, arr));
  conn1->ProcessMessages();
  conn2->ProcessMessages();
  io_service_.run();
  ASSERT_EQ(num_messages, 2);
}

TEST_F(ClientConnectionTest, SimpleAsyncWrite) {
  const uint8_t msg1[5] = {1, 2, 3, 4, 5};
  const uint8_t msg2[5] = {4, 4, 4, 4, 4};
  const uint8_t msg3[5] = {8, 8, 8, 8, 8};
  int num_messages = 0;

  ClientHandler<boost::asio::local::stream_protocol> client_handler =
      [](LocalClientConnection &client) {};

  MessageHandler<boost::asio::local::stream_protocol> noop_handler = [](
      std::shared_ptr<LocalClientConnection> client, int64_t message_type,
      const uint8_t *message) {};

  std::shared_ptr<LocalClientConnection> reader = NULL;

  MessageHandler<boost::asio::local::stream_protocol> message_handler =
      [&msg1, &msg2, &msg3, &num_messages, &reader](
          std::shared_ptr<LocalClientConnection> client, int64_t message_type,
          const uint8_t *message) {
        if (num_messages == 0) {
          ASSERT_TRUE(!std::memcmp(msg1, message, 5));
        } else if (num_messages == 1) {
          ASSERT_TRUE(!std::memcmp(msg2, message, 5));
        } else {
          ASSERT_TRUE(!std::memcmp(msg3, message, 5));
        }
        num_messages += 1;
        if (num_messages < 3) {
          reader->ProcessMessages();
        }
      };

  auto writer = LocalClientConnection::Create(client_handler, noop_handler,
                                              std::move(in_), "writer");

  reader = LocalClientConnection::Create(client_handler, message_handler, std::move(out_),
                                         "reader");

  std::function<void(const ray::Status &)> callback = [this](const ray::Status &status) {
    RAY_CHECK_OK(status);
  };

  writer->WriteMessageAsync(0, 5, msg1, callback);
  writer->WriteMessageAsync(0, 5, msg2, callback);
  writer->WriteMessageAsync(0, 5, msg3, callback);
  reader->ProcessMessages();
  io_service_.run();
  ASSERT_EQ(num_messages, 3);
}

TEST_F(ClientConnectionTest, SimpleAsyncError) {
  const uint8_t msg1[5] = {1, 2, 3, 4, 5};

  ClientHandler<boost::asio::local::stream_protocol> client_handler =
      [](LocalClientConnection &client) {};

  MessageHandler<boost::asio::local::stream_protocol> noop_handler = [](
      std::shared_ptr<LocalClientConnection> client, int64_t message_type,
      const uint8_t *message) {};

  auto writer = LocalClientConnection::Create(client_handler, noop_handler,
                                              std::move(in_), "writer");

  std::function<void(const ray::Status &)> callback = [this](const ray::Status &status) {
    ASSERT_TRUE(!status.ok());
  };

  writer->Close();
  writer->WriteMessageAsync(0, 5, msg1, callback);
  io_service_.run();
}

}  // namespace raylet

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
