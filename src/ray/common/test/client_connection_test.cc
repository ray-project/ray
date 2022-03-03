// Copyright 2017 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "ray/common/client_connection.h"

#include <boost/asio.hpp>
#include <boost/asio/error.hpp>
#include <list>
#include <memory>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "ray/common/asio/instrumented_io_context.h"

namespace ray {
namespace raylet {

class ClientConnectionTest : public ::testing::Test {
 public:
  ClientConnectionTest()
      : io_service_(), in_(io_service_), out_(io_service_), error_message_type_(1) {
#if defined(BOOST_ASIO_HAS_LOCAL_SOCKETS)
    boost::asio::local::stream_protocol::socket input(io_service_), output(io_service_);
    boost::asio::local::connect_pair(input, output);
    in_ = std::move(input);
    out_ = std::move(output);
#else
    // Choose a free port.
    auto endpoint = ParseUrlEndpoint("tcp://127.0.0.1:65437");
    boost::asio::basic_socket_acceptor<local_stream_protocol> acceptor(io_service_,
                                                                       endpoint);
    out_.connect(endpoint);
    acceptor.accept(in_);
#endif
  }

  ray::Status WriteBadMessage(std::shared_ptr<ray::ClientConnection> conn,
                              int64_t type,
                              int64_t length,
                              const uint8_t *message) {
    std::vector<boost::asio::const_buffer> message_buffers;
    auto write_cookie = 123456;  // incorrect version.
    message_buffers.push_back(boost::asio::buffer(&write_cookie, sizeof(write_cookie)));
    message_buffers.push_back(boost::asio::buffer(&type, sizeof(type)));
    message_buffers.push_back(boost::asio::buffer(&length, sizeof(length)));
    message_buffers.push_back(boost::asio::buffer(message, length));
    return conn->WriteBuffer(message_buffers);
  }

 protected:
  instrumented_io_context io_service_;
  local_stream_socket in_;
  local_stream_socket out_;
  int64_t error_message_type_;
};

TEST_F(ClientConnectionTest, SimpleSyncWrite) {
  const uint8_t arr[5] = {1, 2, 3, 4, 5};
  int num_messages = 0;

  ClientHandler client_handler = [](ClientConnection &client) {};

  MessageHandler message_handler = [&arr, &num_messages](
                                       std::shared_ptr<ClientConnection> client,
                                       int64_t message_type,
                                       const std::vector<uint8_t> &message) {
    ASSERT_TRUE(!std::memcmp(arr, message.data(), 5));
    num_messages += 1;
  };

  auto conn1 = ClientConnection::Create(
      client_handler, message_handler, std::move(in_), "conn1", {}, error_message_type_);

  auto conn2 = ClientConnection::Create(
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

  ClientHandler client_handler = [](ClientConnection &client) {};

  MessageHandler noop_handler = [](std::shared_ptr<ClientConnection> client,
                                   int64_t message_type,
                                   const std::vector<uint8_t> &message) {};

  std::shared_ptr<ClientConnection> reader = NULL;

  MessageHandler message_handler = [&msg1, &msg2, &msg3, &num_messages, &reader](
                                       std::shared_ptr<ClientConnection> client,
                                       int64_t message_type,
                                       const std::vector<uint8_t> &message) {
    if (num_messages == 0) {
      ASSERT_TRUE(!std::memcmp(msg1, message.data(), 5));
    } else if (num_messages == 1) {
      ASSERT_TRUE(!std::memcmp(msg2, message.data(), 5));
    } else {
      ASSERT_TRUE(!std::memcmp(msg3, message.data(), 5));
    }
    num_messages += 1;
    if (num_messages < 3) {
      reader->ProcessMessages();
    }
  };

  auto writer = ClientConnection::Create(
      client_handler, noop_handler, std::move(in_), "writer", {}, error_message_type_);

  reader = ClientConnection::Create(client_handler,
                                    message_handler,
                                    std::move(out_),
                                    "reader",
                                    {},
                                    error_message_type_);

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

TEST_F(ClientConnectionTest, SimpleSyncReadWriteMessage) {
  auto writer = ServerConnection::Create(std::move(in_));
  auto reader = ServerConnection::Create(std::move(out_));

  const std::vector<uint8_t> write_buffer = {1, 2, 3, 4, 5};
  std::vector<uint8_t> read_buffer;

  RAY_CHECK_OK(writer->WriteMessage(42, write_buffer.size(), write_buffer.data()));
  RAY_CHECK_OK(reader->ReadMessage(42, &read_buffer));
  RAY_CHECK(write_buffer == read_buffer);
}

TEST_F(ClientConnectionTest, SimpleAsyncReadWriteBuffers) {
  auto writer = ServerConnection::Create(std::move(in_));
  auto reader = ServerConnection::Create(std::move(out_));

  const std::vector<uint8_t> write_buffer = {1, 2, 3, 4, 5};
  std::vector<uint8_t> read_buffer = {0, 0, 0, 0, 0};

  writer->WriteBufferAsync({boost::asio::buffer(write_buffer)},
                           [](const ray::Status &status) { RAY_CHECK_OK(status); });

  reader->ReadBufferAsync({boost::asio::buffer(read_buffer)},
                          [&write_buffer, &read_buffer](const ray::Status &status) {
                            RAY_CHECK_OK(status);
                            RAY_CHECK(write_buffer == read_buffer);
                          });
  io_service_.run();
}

TEST_F(ClientConnectionTest, SimpleAsyncError) {
  const uint8_t msg1[5] = {1, 2, 3, 4, 5};

  ClientHandler client_handler = [](ClientConnection &client) {};

  MessageHandler noop_handler = [](std::shared_ptr<ClientConnection> client,
                                   int64_t message_type,
                                   const std::vector<uint8_t> &message) {};

  auto writer = ClientConnection::Create(
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

  ClientHandler client_handler = [](ClientConnection &client) {};

  MessageHandler noop_handler = [](std::shared_ptr<ClientConnection> client,
                                   int64_t message_type,
                                   const std::vector<uint8_t> &message) {};

  auto writer = ClientConnection::Create(
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

  ClientHandler client_handler = [](ClientConnection &client) {};

  MessageHandler message_handler = [&arr, &num_messages](
                                       std::shared_ptr<ClientConnection> client,
                                       int64_t message_type,
                                       const std::vector<uint8_t> &message) {
    ASSERT_TRUE(!std::memcmp(arr, message.data(), 5));
    num_messages += 1;
  };

  auto writer = ClientConnection::Create(
      client_handler, message_handler, std::move(in_), "writer", {}, error_message_type_);

  auto reader = ClientConnection::Create(client_handler,
                                         message_handler,
                                         std::move(out_),
                                         "reader",
                                         {},
                                         error_message_type_);

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
