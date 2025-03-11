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
  ClientConnectionTest() : io_service_() {}

  std::pair<std::shared_ptr<ClientConnection>, std::shared_ptr<ClientConnection>>
  CreateConnectionPair(std::optional<MessageHandler> client_message_handler,
                       std::optional<MessageHandler> server_message_handler) {
    return CreateConnectionPair(
        client_message_handler, server_message_handler, std::nullopt, std::nullopt);
  }

  std::pair<std::shared_ptr<ClientConnection>, std::shared_ptr<ClientConnection>>
  CreateConnectionPair(std::optional<MessageHandler> client_message_handler,
                       std::optional<MessageHandler> server_message_handler,
                       std::optional<ConnectionErrorHandler> client_error_handler,
                       std::optional<ConnectionErrorHandler> server_error_handler) {
#if defined(BOOST_ASIO_HAS_LOCAL_SOCKETS) && !defined(_WIN32)
    boost::asio::local::stream_protocol::socket in(io_service_), out(io_service_);
    boost::asio::local::connect_pair(in, out);
#else
    local_stream_socket in(io_service_);
    local_stream_socket out(io_service_);
    auto endpoint = ParseUrlEndpoint("tcp://127.0.0.1:65437");
    boost::asio::basic_socket_acceptor<local_stream_protocol> acceptor(io_service_,
                                                                       endpoint);
    out.connect(endpoint);
    acceptor.accept(in);
#endif

    MessageHandler noop_message_handler = [](std::shared_ptr<ClientConnection> client,
                                             int64_t message_type,
                                             const std::vector<uint8_t> &message) {};
    ConnectionErrorHandler check_no_error = [](std::shared_ptr<ClientConnection> client,
                                               const boost::system::error_code &error) {
      RAY_CHECK(false) << "Unexpected connection error: " << error.message();
    };

    if (!client_message_handler) {
      client_message_handler = noop_message_handler;
    }
    if (!client_error_handler) {
      client_error_handler = check_no_error;
    }
    auto client = ClientConnection::Create(client_message_handler.value(),
                                           client_error_handler.value(),
                                           std::move(out),
                                           "client",
                                           {});

    if (!server_message_handler) {
      server_message_handler = noop_message_handler;
    }
    if (!server_error_handler) {
      server_error_handler = check_no_error;
    }
    auto server = ClientConnection::Create(server_message_handler.value(),
                                           server_error_handler.value(),
                                           std::move(in),
                                           "server",
                                           {});

    return std::make_pair(std::move(client), std::move(server));
  }

 protected:
  instrumented_io_context io_service_;
};

/*
Check that the ConnectionErrorHandler is called when an unexpected connection
error occurs.
*/
TEST_F(ClientConnectionTest, UnexpectedConnectionError) {
  const uint8_t arr[5] = {1, 2, 3, 4, 5};
  int num_messages = 0;
  MessageHandler server_message_handler = [&arr, &num_messages](
                                              std::shared_ptr<ClientConnection> client,
                                              int64_t message_type,
                                              const std::vector<uint8_t> &message) {
    ASSERT_TRUE(!std::memcmp(arr, message.data(), 5));
    num_messages += 1;
  };

  bool err_handler_called = false;
  ConnectionErrorHandler server_error_handler =
      [&err_handler_called](std::shared_ptr<ClientConnection> client,
                            const boost::system::error_code &error) {
        err_handler_called = true;
      };

  auto [client, server] = CreateConnectionPair(
      std::nullopt, server_message_handler, std::nullopt, server_error_handler);

  RAY_CHECK_OK(client->WriteMessage(0, 5, arr));
  server->ProcessMessages();
  io_service_.run();
  ASSERT_EQ(num_messages, 1);
  ASSERT_FALSE(err_handler_called);

  io_service_.restart();

  client->Close();
  server->ProcessMessages();
  io_service_.run();
  ASSERT_EQ(num_messages, 1);
  ASSERT_TRUE(err_handler_called);
}

TEST_F(ClientConnectionTest, UnidirectionalWriteSync) {
  const uint8_t arr[5] = {1, 2, 3, 4, 5};
  int server_num_messages = 0;

  MessageHandler server_message_handler = [&arr, &server_num_messages](
                                              std::shared_ptr<ClientConnection> client,
                                              int64_t message_type,
                                              const std::vector<uint8_t> &message) {
    ASSERT_FALSE(std::memcmp(arr, message.data(), 5));
    server_num_messages += 1;
  };

  auto [client, server] = CreateConnectionPair(std::nullopt, server_message_handler);

  ASSERT_EQ(server_num_messages, 0);

  // Write first message from client->server.
  RAY_CHECK_OK(client->WriteMessage(0, 5, arr));
  server->ProcessMessages();
  io_service_.run();
  ASSERT_EQ(server_num_messages, 1);

  io_service_.restart();

  // Write second message from client->server.
  RAY_CHECK_OK(client->WriteMessage(0, 5, arr));
  server->ProcessMessages();
  io_service_.run();
  ASSERT_EQ(server_num_messages, 2);
}

TEST_F(ClientConnectionTest, BidirectionalWriteSync) {
  const uint8_t arr[5] = {1, 2, 3, 4, 5};
  int client_num_messages = 0;
  int server_num_messages = 0;

  MessageHandler client_message_handler = [&arr, &client_num_messages](
                                              std::shared_ptr<ClientConnection> client,
                                              int64_t message_type,
                                              const std::vector<uint8_t> &message) {
    ASSERT_FALSE(std::memcmp(arr, message.data(), 5));
    client_num_messages += 1;
  };

  MessageHandler server_message_handler = [&arr, &server_num_messages](
                                              std::shared_ptr<ClientConnection> client,
                                              int64_t message_type,
                                              const std::vector<uint8_t> &message) {
    ASSERT_FALSE(std::memcmp(arr, message.data(), 5));
    server_num_messages += 1;
    RAY_CHECK_OK(client->WriteMessage(0, 5, arr));
  };

  auto [client, server] =
      CreateConnectionPair(client_message_handler, server_message_handler);

  ASSERT_EQ(client_num_messages, 0);
  ASSERT_EQ(server_num_messages, 0);

  // Write a message from client->server, the server handler writes a message to the
  // client.
  RAY_CHECK_OK(client->WriteMessage(0, 5, arr));
  server->ProcessMessages();
  client->ProcessMessages();
  io_service_.run();
  ASSERT_EQ(server_num_messages, 1);
  ASSERT_EQ(client_num_messages, 1);
}

TEST_F(ClientConnectionTest, UnidirectionalWriteAsync) {
  const uint8_t msg1[5] = {1, 2, 3, 4, 5};
  const uint8_t msg2[5] = {4, 4, 4, 4, 4};
  const uint8_t msg3[5] = {8, 8, 8, 8, 8};
  int server_num_messages = 0;

  MessageHandler server_message_handler = [&msg1, &msg2, &msg3, &server_num_messages](
                                              std::shared_ptr<ClientConnection> client,
                                              int64_t message_type,
                                              const std::vector<uint8_t> &message) {
    if (server_num_messages == 0) {
      ASSERT_FALSE(std::memcmp(msg1, message.data(), 5));
    } else if (server_num_messages == 1) {
      ASSERT_FALSE(std::memcmp(msg2, message.data(), 5));
    } else {
      ASSERT_FALSE(std::memcmp(msg3, message.data(), 5));
    }
    server_num_messages += 1;
  };

  auto [client, server] = CreateConnectionPair(std::nullopt, server_message_handler);

  std::function<void(const ray::Status &)> check_ok = [](const ray::Status &status) {
    RAY_CHECK_OK(status);
  };

  // Write three async messages, check they're processed on the server.
  client->WriteMessageAsync(0, 5, msg1, check_ok);
  client->WriteMessageAsync(0, 5, msg2, check_ok);
  client->WriteMessageAsync(0, 5, msg3, check_ok);

  server->ProcessMessages();
  io_service_.run();
  ASSERT_EQ(server_num_messages, 1);

  server->ProcessMessages();
  io_service_.restart();
  io_service_.run();
  ASSERT_EQ(server_num_messages, 2);

  server->ProcessMessages();
  io_service_.restart();
  io_service_.run();
  ASSERT_EQ(server_num_messages, 3);
}

TEST_F(ClientConnectionTest, SimpleAsyncError) {
  const uint8_t msg1[5] = {1, 2, 3, 4, 5};

  auto [client, server] = CreateConnectionPair(std::nullopt, std::nullopt);

  std::function<void(const ray::Status &)> check_not_ok = [](const ray::Status &status) {
    ASSERT_FALSE(status.ok());
  };

  // Close the client before writing to trigger an error.
  client->Close();
  client->WriteMessageAsync(0, 5, msg1, check_not_ok);
  io_service_.run();
}

TEST_F(ClientConnectionTest, CallbackWithSharedPtrDoesNotLeakConnection) {
  const uint8_t msg1[5] = {1, 2, 3, 4, 5};

  auto client_server_pair = CreateConnectionPair(std::nullopt, std::nullopt);
  std::shared_ptr<ClientConnection> client = client_server_pair.first;

  std::function<void(const ray::Status &)> check_ok =
      [client](const ray::Status &status) {
        static_cast<void>(client);
        ASSERT_TRUE(status.ok());
      };
  client->WriteMessageAsync(0, 5, msg1, check_ok);
  io_service_.run();
}

TEST_F(ClientConnectionTest, ProcessBadMessage) {
  const uint8_t arr[5] = {1, 2, 3, 4, 5};
  int server_num_messages = 0;

  MessageHandler server_message_handler = [&arr, &server_num_messages](
                                              std::shared_ptr<ClientConnection> client,
                                              int64_t message_type,
                                              const std::vector<uint8_t> &message) {
    ASSERT_FALSE(std::memcmp(arr, message.data(), 5));
    server_num_messages += 1;
  };

  auto [client, server] = CreateConnectionPair(std::nullopt, server_message_handler);

  // When an invalid cookie is received, the message should be ignored if the client
  // is not registered or crash if it is. Uncommenting the following line causes the
  // crash.
  // server->Register();

  // Intentionally write a message with incorrect cookie and check the message isn't
  // processed.
  int64_t type = 0;
  int64_t length = 5;
  int64_t write_cookie = 123456;  // incorrect version.
  std::vector<boost::asio::const_buffer> message_buffers;
  message_buffers.push_back(boost::asio::buffer(&write_cookie, sizeof(write_cookie)));
  message_buffers.push_back(boost::asio::buffer(&type, sizeof(type)));
  message_buffers.push_back(boost::asio::buffer(&length, sizeof(length)));
  message_buffers.push_back(boost::asio::buffer(arr, length));
  RAY_CHECK_OK(client->WriteBuffer(message_buffers));
  server->ProcessMessages();
  io_service_.run();
  ASSERT_EQ(server_num_messages, 0);
}

#if !defined(_WIN32)
TEST_F(ClientConnectionTest, CheckForClientDisconnects) {
  auto [client0, server0] = CreateConnectionPair(std::nullopt, std::nullopt);
  auto [client1, server1] = CreateConnectionPair(std::nullopt, std::nullopt);
  auto [client2, server2] = CreateConnectionPair(std::nullopt, std::nullopt);

  // No disconnects.
  {
    std::vector<bool> disconnects =
        CheckForClientDisconnects({server0, server1, server2});
    ASSERT_EQ(disconnects.size(), 3);
    ASSERT_FALSE(disconnects[0]);
    ASSERT_FALSE(disconnects[1]);
    ASSERT_FALSE(disconnects[2]);
  }

  // Close one client connection, check it is marked disconnected but not others.
  shutdown(client1->GetNativeHandle(), SHUT_RDWR);
  {
    std::vector<bool> disconnects =
        CheckForClientDisconnects({server0, server1, server2});
    ASSERT_EQ(disconnects.size(), 3);
    ASSERT_FALSE(disconnects[0]);
    ASSERT_TRUE(disconnects[1]);
    ASSERT_FALSE(disconnects[2]);
  }

  // Check that multiple calls return the same output.
  for (int i = 0; i < 10; i++) {
    std::vector<bool> disconnects =
        CheckForClientDisconnects({server0, server1, server2});
    ASSERT_EQ(disconnects.size(), 3);
    ASSERT_FALSE(disconnects[0]);
    ASSERT_TRUE(disconnects[1]);
    ASSERT_FALSE(disconnects[2]);
  }

  // Close the other client connections, check they are all marked disconnected.
  shutdown(client0->GetNativeHandle(), SHUT_RDWR);
  shutdown(client2->GetNativeHandle(), SHUT_RDWR);
  {
    std::vector<bool> disconnects =
        CheckForClientDisconnects({server0, server1, server2});
    ASSERT_EQ(disconnects.size(), 3);
    ASSERT_TRUE(disconnects[0]);
    ASSERT_TRUE(disconnects[1]);
    ASSERT_TRUE(disconnects[2]);
  }
}
#endif

class ServerConnectionTest : public ::testing::Test {
 public:
  ServerConnectionTest() : io_service_() {}

  std::pair<std::shared_ptr<ServerConnection>, std::shared_ptr<ServerConnection>>
  CreateConnectionPair() {
#if defined(BOOST_ASIO_HAS_LOCAL_SOCKETS) && !defined(_WIN32)
    boost::asio::local::stream_protocol::socket in(io_service_), out(io_service_);
    boost::asio::local::connect_pair(in, out);
#else
    local_stream_socket in(io_service_);
    local_stream_socket out(io_service_);
    auto endpoint = ParseUrlEndpoint("tcp://127.0.0.1:65437");
    boost::asio::basic_socket_acceptor<local_stream_protocol> acceptor(io_service_,
                                                                       endpoint);
    out.connect(endpoint);
    acceptor.accept(in);
#endif

    return std::make_pair(ServerConnection::Create(std::move(out)),
                          ServerConnection::Create(std::move(in)));
  }

 protected:
  instrumented_io_context io_service_;
};

TEST_F(ServerConnectionTest, SimpleSyncReadWriteMessage) {
  auto [client, server] = CreateConnectionPair();

  const std::vector<uint8_t> write_buffer = {1, 2, 3, 4, 5};
  std::vector<uint8_t> read_buffer;

  RAY_CHECK_OK(client->WriteMessage(42, write_buffer.size(), write_buffer.data()));
  RAY_CHECK_OK(server->ReadMessage(42, &read_buffer));
  RAY_CHECK(write_buffer == read_buffer);
}

TEST_F(ServerConnectionTest, SimpleAsyncReadWriteBuffers) {
  auto [client, server] = CreateConnectionPair();

  const std::vector<uint8_t> write_buffer = {1, 2, 3, 4, 5};
  std::vector<uint8_t> read_buffer = {0, 0, 0, 0, 0};

  client->WriteBufferAsync({boost::asio::buffer(write_buffer)},
                           [](const ray::Status &status) { RAY_CHECK_OK(status); });

  server->ReadBufferAsync({boost::asio::buffer(read_buffer)},
                          [&write_buffer, &read_buffer](const ray::Status &status) {
                            RAY_CHECK_OK(status);
                            RAY_CHECK(write_buffer == read_buffer);
                          });
  io_service_.run();
}

}  // namespace raylet

}  // namespace ray
