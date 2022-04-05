#include "ray/object_manager/plasma/connection.h"

#ifndef _WIN32
#include "ray/object_manager/plasma/fling.h"
#endif
#include "ray/object_manager/plasma/plasma_generated.h"
#include "ray/object_manager/plasma/protocol.h"
#include "ray/util/logging.h"

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

Client::Client(ray::MessageHandler &message_handler, ray::local_stream_socket &&socket)
    : ray::ClientConnection(message_handler,
                            std::move(socket),
                            "worker",
                            object_store_message_enum,
                            static_cast<int64_t>(MessageType::PlasmaDisconnectClient)) {}

std::shared_ptr<Client> Client::Create(PlasmaStoreMessageHandler message_handler,
                                       ray::local_stream_socket &&socket) {
  ray::MessageHandler ray_message_handler =
      [message_handler](std::shared_ptr<ray::ClientConnection> client,
                        int64_t message_type,
                        const std::vector<uint8_t> &message) {
        Status s = message_handler(
            std::static_pointer_cast<Client>(client->shared_ClientConnection_from_this()),
            (MessageType)message_type,
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
  std::shared_ptr<Client> self(new Client(ray_message_handler, std::move(socket)));
  // Let our manager process our new connection.
  self->ProcessMessages();
  return self;
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
    : ray::ServerConnection(std::move(socket)) {}

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

}  // namespace plasma
