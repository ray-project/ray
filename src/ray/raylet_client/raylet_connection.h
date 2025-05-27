// Copyright 2024 The Ray Authors.
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

#pragma once

#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/client_connection.h"

namespace ray::raylet {

/// `RayletConnection` is a wrapper around a connection with raylet, which is responsible
/// for sending request to raylet.
class RayletConnection {
 public:
  /// Connect to the raylet.
  ///
  /// \param raylet_socket The name of the socket to use to connect to the raylet.
  /// \param worker_id A unique ID to represent the worker.
  /// \param is_worker Whether this client is a worker. If it is a worker, an
  ///        additional message will be sent to register as one.
  /// \param job_id The ID of the driver. This is non-nil if the client is a
  ///        driver.
  /// \return The connection information.
  RayletConnection(instrumented_io_context &io_service,
                   const std::string &raylet_socket,
                   int num_retries,
                   int64_t timeout);

  /// Send request to raylet without waiting for response.
  ray::Status WriteMessage(ray::protocol::MessageType type,
                           flatbuffers::FlatBufferBuilder *fbb = nullptr);

  /// Send request to raylet and blockingly wait for response.
  ray::Status AtomicRequestReply(ray::protocol::MessageType request_type,
                                 ray::protocol::MessageType reply_type,
                                 std::vector<uint8_t> *reply_message,
                                 flatbuffers::FlatBufferBuilder *fbb = nullptr);

 private:
  /// Shutdown the raylet if the local connection is disconnected.
  void ShutdownIfLocalRayletDisconnected(const Status &status);
  /// The connection to raylet.
  std::shared_ptr<ServerConnection> conn_;
  /// A mutex to protect stateful operations of the raylet client.
  std::mutex mutex_;
  /// A mutex to protect write operations of the raylet client.
  std::mutex write_mutex_;
};

}  // namespace ray::raylet
