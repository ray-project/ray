// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "plasma/io.h"

#include <cstdint>
#include <memory>
#include <sstream>

#include "arrow/status.h"
#include "arrow/util/logging.h"

#include "plasma/common.h"
#include "plasma/plasma_generated.h"
#ifndef _WIN32
#include <arpa/inet.h>
#include <netinet/in.h>
#endif

using arrow::Status;

/// Number of times we try connecting to a socket.
constexpr int64_t kNumConnectAttempts = 80;
/// Time to wait between connection attempts to a socket.
constexpr int64_t kConnectTimeoutMs = 100;

namespace plasma {

using flatbuf::MessageType;

Status WriteBytes(int fd, uint8_t* cursor, size_t length) {
  ssize_t nbytes = 0;
  size_t bytesleft = length;
  size_t offset = 0;
  while (bytesleft > 0) {
    // While we haven't written the whole message, write to the file descriptor,
    // advance the cursor, and decrease the amount left to write.
    nbytes = write(fd, cursor + offset, bytesleft);
    if (nbytes < 0) {
      if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) {
        continue;
      }
      return Status::IOError(strerror(errno));
    } else if (nbytes == 0) {
      return Status::IOError("Encountered unexpected EOF");
    }
    ARROW_CHECK(nbytes > 0);
    bytesleft -= nbytes;
    offset += nbytes;
  }

  return Status::OK();
}

Status WriteMessage(int fd, MessageType type, int64_t length, uint8_t* bytes) {
  int64_t version = kPlasmaProtocolVersion;
  RETURN_NOT_OK(WriteBytes(fd, reinterpret_cast<uint8_t*>(&version), sizeof(version)));
  RETURN_NOT_OK(WriteBytes(fd, reinterpret_cast<uint8_t*>(&type), sizeof(type)));
  RETURN_NOT_OK(WriteBytes(fd, reinterpret_cast<uint8_t*>(&length), sizeof(length)));
  return WriteBytes(fd, bytes, length * sizeof(char));
}

Status ReadBytes(int fd, uint8_t* cursor, size_t length) {
  ssize_t nbytes = 0;
  // Termination condition: EOF or read 'length' bytes total.
  size_t bytesleft = length;
  size_t offset = 0;
  while (bytesleft > 0) {
    nbytes = read(fd, cursor + offset, bytesleft);
    if (nbytes < 0) {
      if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) {
        continue;
      }
      return Status::IOError(strerror(errno));
    } else if (0 == nbytes) {
      return Status::IOError("Encountered unexpected EOF");
    }
    ARROW_CHECK(nbytes > 0);
    bytesleft -= nbytes;
    offset += nbytes;
  }

  return Status::OK();
}

Status ReadMessage(int fd, MessageType* type, std::vector<uint8_t>* buffer) {
  int64_t version;
  RETURN_NOT_OK_ELSE(ReadBytes(fd, reinterpret_cast<uint8_t*>(&version), sizeof(version)),
                     *type = MessageType::PlasmaDisconnectClient);
  ARROW_CHECK(version == kPlasmaProtocolVersion) << "version = " << version;
  RETURN_NOT_OK_ELSE(ReadBytes(fd, reinterpret_cast<uint8_t*>(type), sizeof(*type)),
                     *type = MessageType::PlasmaDisconnectClient);
  int64_t length_temp;
  RETURN_NOT_OK_ELSE(
      ReadBytes(fd, reinterpret_cast<uint8_t*>(&length_temp), sizeof(length_temp)),
      *type = MessageType::PlasmaDisconnectClient);
  // The length must be read as an int64_t, but it should be used as a size_t.
  size_t length = static_cast<size_t>(length_temp);
  if (length > buffer->size()) {
    buffer->resize(length);
  }
  RETURN_NOT_OK_ELSE(ReadBytes(fd, buffer->data(), length),
                     *type = MessageType::PlasmaDisconnectClient);
  return Status::OK();
}

int ConnectOrListenIpcSock(const std::string& pathname, bool shall_listen) {
  union {
    struct sockaddr addr;
    struct sockaddr_un un;
    struct sockaddr_in in;
  } socket_address;
  int addrlen;
  memset(&socket_address, 0, sizeof(socket_address));
  if (pathname.find("tcp://") == 0) {
    addrlen = sizeof(socket_address.in);
    socket_address.in.sin_family = AF_INET;
    std::string addr = pathname.substr(pathname.find('/') + 2);
    size_t i = addr.rfind(':'), j;
    if (i >= addr.size()) {
      j = i = addr.size();
    } else {
      j = i + 1;
    }
    socket_address.in.sin_addr.s_addr = inet_addr(addr.substr(0, i).c_str());
    socket_address.in.sin_port = htons(static_cast<short>(atoi(addr.substr(j).c_str())));
    if (socket_address.in.sin_addr.s_addr == INADDR_NONE) {
      ARROW_LOG(ERROR) << "Socket address is not a valid IPv4 address: " << pathname;
      return -1;
    }
    if (socket_address.in.sin_port == htons(0)) {
      ARROW_LOG(ERROR) << "Socket address is missing a valid port: " << pathname;
      return -1;
    }
  } else {
    addrlen = sizeof(socket_address.un);
    socket_address.un.sun_family = AF_UNIX;
    if (pathname.size() + 1 > sizeof(socket_address.un.sun_path)) {
      ARROW_LOG(ERROR) << "Socket pathname is too long.";
      return -1;
    }
    strncpy(socket_address.un.sun_path, pathname.c_str(), pathname.size() + 1);
  }

  int socket_fd = socket(socket_address.addr.sa_family, SOCK_STREAM, 0);
  if (socket_fd < 0) {
    ARROW_LOG(ERROR) << "socket() failed for pathname " << pathname;
    return -1;
  }
  if (shall_listen) {
    // Tell the system to allow the port to be reused.
    int on = 1;
    if (setsockopt(socket_fd, SOL_SOCKET, SO_REUSEADDR, reinterpret_cast<char*>(&on),
                   sizeof(on)) < 0) {
      ARROW_LOG(ERROR) << "setsockopt failed for pathname " << pathname;
      close(socket_fd);
      return -1;
    }

    if (socket_address.addr.sa_family == AF_UNIX) {
#ifdef _WIN32
      _unlink(pathname.c_str());
#else
      unlink(pathname.c_str());
#endif
    }
    if (bind(socket_fd, &socket_address.addr, addrlen) != 0) {
      ARROW_LOG(ERROR) << "Bind failed for pathname " << pathname;
      close(socket_fd);
      return -1;
    }

    if (listen(socket_fd, 128) == -1) {
      ARROW_LOG(ERROR) << "Could not listen to socket " << pathname;
      close(socket_fd);
      return -1;
    }
  } else {
    if (connect(socket_fd, &socket_address.addr, addrlen) != 0) {
      close(socket_fd);
      return -1;
    }
  }
  return socket_fd;
}

Status ConnectIpcSocketRetry(const std::string& pathname, int num_retries,
                             int64_t timeout, int* fd) {
  // Pick the default values if the user did not specify.
  if (num_retries < 0) {
    num_retries = kNumConnectAttempts;
  }
  if (timeout < 0) {
    timeout = kConnectTimeoutMs;
  }
  *fd = ConnectOrListenIpcSock(pathname, false);
  while (*fd < 0 && num_retries > 0) {
    ARROW_LOG(ERROR) << "Connection to IPC socket failed for pathname " << pathname
                     << ", retrying " << num_retries << " more times";
    // Sleep for timeout milliseconds.
    usleep(static_cast<int>(timeout * 1000));
    *fd = ConnectOrListenIpcSock(pathname, false);
    --num_retries;
  }

  // If we could not connect to the socket, exit.
  if (*fd == -1) {
    return Status::IOError("Could not connect to socket ", pathname);
  }

  return Status::OK();
}

int AcceptClient(int socket_fd) {
  int client_fd = accept(socket_fd, NULL, NULL);
  if (client_fd < 0) {
    ARROW_LOG(ERROR) << "Error reading from socket.";
    return -1;
  }
  return client_fd;
}

std::unique_ptr<uint8_t[]> ReadMessageAsync(int sock) {
  int64_t size;
  Status s = ReadBytes(sock, reinterpret_cast<uint8_t*>(&size), sizeof(int64_t));
  if (!s.ok()) {
    // The other side has closed the socket.
    ARROW_LOG(DEBUG) << "Socket has been closed, or some other error has occurred.";
    close(sock);
    return NULL;
  }
  auto message = std::unique_ptr<uint8_t[]>(new uint8_t[size]);
  s = ReadBytes(sock, message.get(), size);
  if (!s.ok()) {
    // The other side has closed the socket.
    ARROW_LOG(DEBUG) << "Socket has been closed, or some other error has occurred.";
    close(sock);
    return NULL;
  }
  return message;
}

}  // namespace plasma
