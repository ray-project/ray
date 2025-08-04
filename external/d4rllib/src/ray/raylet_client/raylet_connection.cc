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

#include "ray/raylet_client/raylet_connection.h"

#include <string>
#include <utility>
#include <vector>

using MessageType = ray::protocol::MessageType;

namespace ray::raylet {

RayletConnection::RayletConnection(instrumented_io_context &io_service,
                                   const std::string &raylet_socket,
                                   int num_retries,
                                   int64_t timeout) {
  local_stream_socket socket(io_service);
  Status s = ConnectSocketRetry(socket, raylet_socket, num_retries, timeout);
  // If we could not connect to the socket, exit.
  if (!s.ok()) {
    RAY_LOG(FATAL) << "Could not connect to socket " << raylet_socket;
  }
  conn_ = ServerConnection::Create(std::move(socket));
}

Status RayletConnection::WriteMessage(MessageType type,
                                      flatbuffers::FlatBufferBuilder *fbb) {
  std::unique_lock<std::mutex> guard(write_mutex_);
  int64_t length = fbb ? fbb->GetSize() : 0;
  uint8_t *bytes = fbb ? fbb->GetBufferPointer() : nullptr;
  auto status = conn_->WriteMessage(static_cast<int64_t>(type), length, bytes);
  ShutdownIfLocalRayletDisconnected(status);
  return status;
}

Status RayletConnection::AtomicRequestReply(MessageType request_type,
                                            MessageType reply_type,
                                            std::vector<uint8_t> *reply_message,
                                            flatbuffers::FlatBufferBuilder *fbb) {
  std::unique_lock<std::mutex> guard(mutex_);
  RAY_RETURN_NOT_OK(WriteMessage(request_type, fbb));
  auto status = conn_->ReadMessage(static_cast<int64_t>(reply_type), reply_message);
  ShutdownIfLocalRayletDisconnected(status);
  return status;
}

void RayletConnection::ShutdownIfLocalRayletDisconnected(const Status &status) {
  if (!status.ok() && IsRayletFailed(RayConfig::instance().RAYLET_PID())) {
    RAY_LOG(WARNING) << "The connection is failed because the local raylet has been "
                        "dead. Terminate the process. Status: "
                     << status;
    QuickExit();
    RAY_LOG(FATAL) << "Unreachable.";
  }
}

}  // namespace ray::raylet
