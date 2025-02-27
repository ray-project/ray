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
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "ray/common/asio/instrumented_io_context.h"

namespace ray {
namespace raylet {

class ClientConnectionTest : public ::testing::Test {
 public:
  ClientConnectionTest()
      : io_service_() {}

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


std::pair<std::shared_ptr<ClientConnection>, std::shared_ptr<ClientConnection>> CreateConnectionPair(
    MessageHandler reader_message_handler, MessageHandler writer_message_handler) { 
#if defined(BOOST_ASIO_HAS_LOCAL_SOCKETS) && !defined(_WIN32)
  boost::asio::local::stream_protocol::socket in(io_service_), out(io_service_);
  boost::asio::local::connect_pair(in, out);
#else
  local_stream_socket in(io_service_);
  local_stream_socket out(io_service_);
  auto endpoint = ParseUrlEndpoint("tcp://127.0.0.1", /*default_port=*/0);
  boost::asio::basic_socket_acceptor<local_stream_protocol> acceptor(io_service_,
                                     endpoint);
  out.connect(endpoint);
  acceptor.accept(in);
#endif

  ClientHandler noop_client_handler = [](ClientConnection &client) {};

  auto reader = ClientConnection::Create(
      noop_client_handler, reader_message_handler, std::move(in), "reader", {}, /*error_message_type=*/1);

  auto writer = ClientConnection::Create(
      noop_client_handler, writer_message_handler, std::move(out), "writer", {}, /*error_message_type=*/1);

  return std::make_pair(std::move(reader), std::move(writer));
}

 protected:
  instrumented_io_context io_service_;
};

TEST_F(ClientConnectionTest, UnidirectionalSyncWrite) {
  const uint8_t arr[5] = {1, 2, 3, 4, 5};
  int num_messages = 0;

  MessageHandler reader_message_handler = [&arr, &num_messages](
                                       std::shared_ptr<ClientConnection> client,
                                       int64_t message_type,
                                       const std::vector<uint8_t> &message) {
    ASSERT_TRUE(!std::memcmp(arr, message.data(), 5));
    num_messages += 1;
  };


  MessageHandler writer_message_handler = [](
                                       std::shared_ptr<ClientConnection> client,
                                       int64_t message_type,
                                       const std::vector<uint8_t> &message) {};

  auto [reader, writer] = CreateConnectionPair(reader_message_handler, writer_message_handler);

  ASSERT_EQ(num_messages, 0);

  // Write first message.
  RAY_CHECK_OK(writer->WriteMessage(0, 5, arr));
  reader->ProcessMessages();
  io_service_.run();
  ASSERT_EQ(num_messages, 1);

  io_service_.restart();

  // Write second message.
  RAY_CHECK_OK(writer->WriteMessage(0, 5, arr));
  reader->ProcessMessages();
  io_service_.run();
  ASSERT_EQ(num_messages, 2);
}

TEST_F(ClientConnectionTest, BidirectionalSyncWrite) {
  const uint8_t arr[5] = {1, 2, 3, 4, 5};
  int reader_num_messages = 0;
  int writer_num_messages = 0;

  MessageHandler reader_message_handler = [&arr, &reader_num_messages](
                                       std::shared_ptr<ClientConnection> client,
                                       int64_t message_type,
                                       const std::vector<uint8_t> &message) {
    ASSERT_TRUE(!std::memcmp(arr, message.data(), 5));
    reader_num_messages += 1;
  };

  MessageHandler writer_message_handler = [&arr, &writer_num_messages](
                                       std::shared_ptr<ClientConnection> client,
                                       int64_t message_type,
                                       const std::vector<uint8_t> &message) {
    ASSERT_TRUE(!std::memcmp(arr, message.data(), 5));
    writer_num_messages += 1;
  };

  auto [reader, writer] = CreateConnectionPair(reader_message_handler, writer_message_handler);

  ASSERT_EQ(reader_num_messages, 0);
  ASSERT_EQ(writer_num_messages, 0);

  // Write first message.
  RAY_CHECK_OK(writer->WriteMessage(0, 5, arr));
  reader->ProcessMessages();
  io_service_.run();
  ASSERT_EQ(reader_num_messages, 1);
  ASSERT_EQ(writer_num_messages, 0);

  io_service_.restart();

  // Write second message.
  RAY_CHECK_OK(reader->WriteMessage(0, 5, arr));
  writer->ProcessMessages();
  io_service_.run();
  ASSERT_EQ(reader_num_messages, 1);
  ASSERT_EQ(writer_num_messages, 1);
}

/*
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

TEST_F(ClientConnectionTest, CheckForClientDisconnects) {
#if !defined(_WIN32)
  ClientHandler client_handler = [](ClientConnection &client) {};

  MessageHandler message_handler = [](std::shared_ptr<ClientConnection> client,
                                      int64_t message_type,
                                      const std::vector<uint8_t> &message) {};

  auto conn = ClientConnection::Create(
      client_handler, message_handler, std::move(in_), "conn", {}, error_message_type_);

  // Connection should be open.
  {
    std::vector<bool> disconnects = CheckForClientDisconnects({conn});
    ASSERT_EQ(disconnects.size(), 1);
    ASSERT_FALSE(disconnects[0]);
  }

  // Shut down the connection, check it returns disconnected.
  shutdown(conn->GetNativeHandle(), SHUT_RDWR);
  {
    std::vector<bool> disconnects = CheckForClientDisconnects({conn});
    ASSERT_EQ(disconnects.size(), 1);
    ASSERT_TRUE(disconnects[0]);
  }

  // Check that multiple calls return the same output.
  {
    std::vector<bool> disconnects = CheckForClientDisconnects({conn});
    ASSERT_EQ(disconnects.size(), 1);
    ASSERT_TRUE(disconnects[0]);
  }
#endif
}
*/

}  // namespace raylet

}  // namespace ray
