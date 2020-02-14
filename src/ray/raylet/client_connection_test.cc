#include <list>
#include <memory>

#include <boost/asio.hpp>
#include <boost/asio/error.hpp>
#include "gmock/gmock.h"
#include "gtest/gtest.h"

#if !defined(BOOST_ASIO_HAS_LOCAL_SOCKETS)
#include <sys/socket.h>
#include <sys/types.h>
#endif

#include "ray/common/client_connection.h"

namespace ray {
namespace raylet {

class ClientConnectionTest : public ::testing::Test {
 public:
  ClientConnectionTest()
      : io_service_(), in_(io_service_), out_(io_service_), error_message_type_(1) {
#if defined(BOOST_ASIO_HAS_LOCAL_SOCKETS)
    boost::asio::local::connect_pair(in_, out_);
#else
    int pair[2] = {};  // TODO(mehrdadn): This should be type SOCKET for Windows
    RAY_CHECK(socketpair(AF_INET, SOCK_STREAM, 0, pair) == 0);
    in_.assign(local_stream_protocol::v4(), pair[0]);
    out_.assign(local_stream_protocol::v4(), pair[1]);
#endif
  }

  ray::Status WriteBadMessage(std::shared_ptr<ray::LocalClientConnection> conn,
                              int64_t type, int64_t length, const uint8_t *message) {
    std::vector<boost::asio::const_buffer> message_buffers;
    auto write_cookie = 123456;  // incorrect version.
    message_buffers.push_back(boost::asio::buffer(&write_cookie, sizeof(write_cookie)));
    message_buffers.push_back(boost::asio::buffer(&type, sizeof(type)));
    message_buffers.push_back(boost::asio::buffer(&length, sizeof(length)));
    message_buffers.push_back(boost::asio::buffer(message, length));
    return conn->WriteBuffer(message_buffers);
  }

 protected:
  boost::asio::io_service io_service_;
  local_stream_protocol::socket in_;
  local_stream_protocol::socket out_;
  int64_t error_message_type_;
};

TEST_F(ClientConnectionTest, SimpleSyncWrite) {
  const uint8_t arr[5] = {1, 2, 3, 4, 5};
  int num_messages = 0;

  ClientHandler<local_stream_protocol> client_handler =
      [](LocalClientConnection &client) {};

  MessageHandler<local_stream_protocol> message_handler =
      [&arr, &num_messages](std::shared_ptr<LocalClientConnection> client,
                            int64_t message_type, const uint8_t *message) {
        ASSERT_TRUE(!std::memcmp(arr, message, 5));
        num_messages += 1;
      };

  auto conn1 = LocalClientConnection::Create(
      client_handler, message_handler, std::move(in_), "conn1", {}, error_message_type_);

  auto conn2 = LocalClientConnection::Create(
      client_handler, message_handler, std::move(out_), "conn2", {}, error_message_type_);

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

  ClientHandler<local_stream_protocol> client_handler =
      [](LocalClientConnection &client) {};

  MessageHandler<local_stream_protocol> noop_handler =
      [](std::shared_ptr<LocalClientConnection> client, int64_t message_type,
         const uint8_t *message) {};

  std::shared_ptr<LocalClientConnection> reader = NULL;

  MessageHandler<local_stream_protocol> message_handler =
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

  auto writer = LocalClientConnection::Create(
      client_handler, noop_handler, std::move(in_), "writer", {}, error_message_type_);

  reader = LocalClientConnection::Create(client_handler, message_handler, std::move(out_),
                                         "reader", {}, error_message_type_);

  std::function<void(const ray::Status &)> callback = [](const ray::Status &status) {
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

  ClientHandler<local_stream_protocol> client_handler =
      [](LocalClientConnection &client) {};

  MessageHandler<local_stream_protocol> noop_handler =
      [](std::shared_ptr<LocalClientConnection> client, int64_t message_type,
         const uint8_t *message) {};

  auto writer = LocalClientConnection::Create(
      client_handler, noop_handler, std::move(in_), "writer", {}, error_message_type_);

  std::function<void(const ray::Status &)> callback = [](const ray::Status &status) {
    ASSERT_TRUE(!status.ok());
  };

  writer->Close();
  writer->WriteMessageAsync(0, 5, msg1, callback);
  io_service_.run();
}

TEST_F(ClientConnectionTest, CallbackWithSharedRefDoesNotLeakConnection) {
  const uint8_t msg1[5] = {1, 2, 3, 4, 5};

  ClientHandler<local_stream_protocol> client_handler =
      [](LocalClientConnection &client) {};

  MessageHandler<local_stream_protocol> noop_handler =
      [](std::shared_ptr<LocalClientConnection> client, int64_t message_type,
         const uint8_t *message) {};

  auto writer = LocalClientConnection::Create(
      client_handler, noop_handler, std::move(in_), "writer", {}, error_message_type_);

  std::function<void(const ray::Status &)> callback =
      [writer](const ray::Status &status) {
        static_cast<void>(writer);
        ASSERT_TRUE(status.ok());
      };
  writer->WriteMessageAsync(0, 5, msg1, callback);
  io_service_.run();
}

TEST_F(ClientConnectionTest, ProcessBadMessage) {
  const uint8_t arr[5] = {1, 2, 3, 4, 5};
  int num_messages = 0;

  ClientHandler<local_stream_protocol> client_handler =
      [](LocalClientConnection &client) {};

  MessageHandler<local_stream_protocol> message_handler =
      [&arr, &num_messages](std::shared_ptr<LocalClientConnection> client,
                            int64_t message_type, const uint8_t *message) {
        ASSERT_TRUE(!std::memcmp(arr, message, 5));
        num_messages += 1;
      };

  auto writer = LocalClientConnection::Create(
      client_handler, message_handler, std::move(in_), "writer", {}, error_message_type_);

  auto reader =
      LocalClientConnection::Create(client_handler, message_handler, std::move(out_),
                                    "reader", {}, error_message_type_);

  // If client ID is set, bad message would crash the test.
  // reader->SetClientID(UniqueID::FromRandom());

  // Intentionally write a message with incorrect cookie.
  // Verify it won't crash as long as client ID is not set.
  RAY_CHECK_OK(WriteBadMessage(writer, 0, 5, arr));
  reader->ProcessMessages();
  io_service_.run();
  ASSERT_EQ(num_messages, 0);
}

}  // namespace raylet

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
