// Copyright 2025 The Ray Authors.
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

#include "ray/object_manager/plasma/connection.h"

#ifndef _WIN32
#include "ray/object_manager/plasma/fling.h"
#endif
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "ray/object_manager/plasma/plasma_generated.h"
#include "ray/object_manager/plasma/protocol.h"
#include "ray/util/logging.h"
#include "ray/util/process.h"

namespace plasma {

using ray::Status;

std::ostream &operator<<(std::ostream &os, const std::shared_ptr<Client> &client) {
  os << std::to_string(client->GetNativeHandle());
  return os;
}

std::ostream &operator<<(std::ostream &os, const std::shared_ptr<StoreConn> &store_conn) {
  os << std::to_string(store_conn->GetNativeHandle());
  return os;
}

namespace {

const std::vector<std::string> GenerateEnumNames(const char *const *enum_names_ptr,
                                                 int start_index,
                                                 int end_index) {
  std::vector<std::string> enum_names;
  for (int i = 0; i < start_index; ++i) {
    enum_names.push_back("EmptyMessageType");
  }
  size_t i = 0;
  while (true) {
    const char *name = enum_names_ptr[i];
    if (name == nullptr) {
      break;
    }
    enum_names.push_back(name);
    i++;
  }
  RAY_CHECK(static_cast<size_t>(end_index) == enum_names.size() - 1)
      << "Message Type mismatch!";
  return enum_names;
}

static const std::vector<std::string> object_store_message_enum =
    GenerateEnumNames(flatbuf::EnumNamesMessageType(),
                      static_cast<int>(MessageType::MIN),
                      static_cast<int>(MessageType::MAX));
}  // namespace

Client::Client(PrivateTag,
               ray::MessageHandler message_handler,
               ray::ConnectionErrorHandler connection_error_handler,
               ray::local_stream_socket &&socket)
    : ray::ClientConnection(std::move(message_handler),
                            std::move(connection_error_handler),
                            std::move(socket),
                            "worker",
                            object_store_message_enum) {}

std::shared_ptr<Client> Client::Create(
    PlasmaStoreMessageHandler message_handler,
    PlasmaStoreConnectionErrorHandler connection_error_handler,
    ray::local_stream_socket &&socket) {
  ray::MessageHandler ray_message_handler =
      [message_handler](const std::shared_ptr<ray::ClientConnection> &client,
                        int64_t message_type,
                        const std::vector<uint8_t> &message) {
        Status s = message_handler(std::static_pointer_cast<Client>(client),
                                   static_cast<MessageType>(message_type),
                                   message);
        if (!s.ok()) {
          if (!s.IsDisconnected()) {
            RAY_LOG(ERROR) << "Fail to process client message. " << s.ToString();
          }
          client->Close();
        } else {
          client->ProcessMessages();
        }
      };

  ray::ConnectionErrorHandler ray_connection_error_handler =
      [connection_error_handler](const std::shared_ptr<ray::ClientConnection> &client,
                                 const boost::system::error_code &error) {
        connection_error_handler(std::static_pointer_cast<Client>(client), error);
      };

  return std::make_shared<Client>(
      PrivateTag{}, ray_message_handler, ray_connection_error_handler, std::move(socket));
}

Status Client::SendFd(MEMFD_TYPE fd) {
  // Only send the file descriptor if it hasn't been sent (see analogous
  // logic in GetStoreFd in client.cc).
  if (used_fds_.find(fd) == used_fds_.end()) {
#ifdef _WIN32
    DWORD target_pid;
    RAY_RETURN_NOT_OK(ReadBuffer({boost::asio::buffer(&target_pid, sizeof(target_pid))}));
    if (!target_pid) {
      return Status::Invalid("Received invalid PID");
    }
    /* This is a regular handle... fit it into the same struct */
    HANDLE target_process = OpenProcess(PROCESS_DUP_HANDLE, FALSE, target_pid);
    if (!target_process) {
      return Status::Invalid("Cannot open PID = " + std::to_string(target_pid));
    }
    HANDLE target_handle = NULL;
    bool success = DuplicateHandle(GetCurrentProcess(),
                                   fd.first,
                                   target_process,
                                   &target_handle,
                                   0,
                                   TRUE,
                                   DUPLICATE_SAME_ACCESS);
    if (!success) {
      // TODO(suquark): Define better error type.
      return Status::IOError("Fail to duplicate handle to PID = " +
                             std::to_string(target_pid));
    }
    Status s = WriteBuffer({boost::asio::buffer(&target_handle, sizeof(target_handle))});
    if (!s.ok()) {
      /* we failed to send the handle, and it needs cleaning up! */
      HANDLE duplicated_back = NULL;
      if (DuplicateHandle(target_process,
                          fd.first,
                          GetCurrentProcess(),
                          &duplicated_back,
                          0,
                          FALSE,
                          DUPLICATE_CLOSE_SOURCE)) {
        CloseHandle(duplicated_back);
      }
      CloseHandle(target_process);
      return s;
    }
    CloseHandle(target_process);
#else
    auto ec = send_fd(GetNativeHandle(), fd.first);
    if (ec <= 0) {
      if (ec == 0) {
        return Status::IOError("Encountered unexpected EOF");
      } else {
        return Status::IOError("Unknown I/O Error");
      }
    }
#endif
    used_fds_.insert(fd);  // Succeed, record the fd.
  }
  return Status::OK();
}

StoreConn::StoreConn(ray::local_stream_socket &&socket)
    : ray::ServerConnection(std::move(socket)), exit_on_connection_failure_(false) {}

StoreConn::StoreConn(ray::local_stream_socket &&socket, bool exit_on_connection_failure)
    : ray::ServerConnection(std::move(socket)),
      exit_on_connection_failure_(exit_on_connection_failure) {}

Status StoreConn::RecvFd(MEMFD_TYPE_NON_UNIQUE *fd) {
#ifdef _WIN32
  DWORD pid = GetCurrentProcessId();
  Status s = WriteBuffer({boost::asio::buffer(&pid, sizeof(pid))});
  if (!s.ok()) {
    return Status::IOError("Failed to send PID.");
  }
  s = ReadBuffer({boost::asio::buffer(fd, sizeof(*fd))});
  if (!s.ok()) {
    return Status::IOError("Failed to receive the handle.");
  }
#else
  *fd = recv_fd(GetNativeHandle());
  if (*fd < 0) {
    return Status::IOError("Failed to receive the fd.");
  }
#endif
  return Status::OK();
}

ray::Status StoreConn::WriteBuffer(const std::vector<boost::asio::const_buffer> &buffer) {
  auto status = ray::ServerConnection::WriteBuffer(buffer);
  ExitIfErrorStatus(status);
  return status;
}

ray::Status StoreConn::ReadBuffer(
    const std::vector<boost::asio::mutable_buffer> &buffer) {
  auto status = ray::ServerConnection::ReadBuffer(buffer);
  ExitIfErrorStatus(status);
  return status;
}

void StoreConn::ExitIfErrorStatus(const ray::Status &status) {
  if (!status.ok() && exit_on_connection_failure_) {
    RAY_LOG(WARNING) << "The connection to the plasma store is failed. Terminate the "
                     << "process. Status: " << status;
    ray::QuickExit();
    RAY_LOG(FATAL)
        << "Accessing unreachable code. This line should never be reached "
        << "after quick process exit due to plasma store connection failure. Please "
           "create a github issue at https://github.com/ray-project/ray.";
  }
}
}  // namespace plasma
