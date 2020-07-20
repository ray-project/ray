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

#include "ray/object_manager/plasma/plasma.h"

#ifndef _WIN32
#include <sys/socket.h>
#endif
#include <sys/types.h>
#include <unistd.h>

#include "ray/object_manager/format/object_manager_generated.h"
#include "ray/object_manager/plasma/common.h"
#include "ray/object_manager/plasma/protocol.h"

namespace fb = ray::object_manager::protocol;

namespace plasma {

Client::~Client() { close(fd); }

std::ostream &operator<<(std::ostream &os, const std::shared_ptr<Client> &client) {
  os << client->fd;
  return os;
}

StoreConn::~StoreConn() { close(fd); }

std::ostream &operator<<(std::ostream &os, const std::shared_ptr<StoreConn> &store_conn) {
  os << store_conn->fd;
  return os;
}

ObjectTableEntry::ObjectTableEntry() : pointer(nullptr), ref_count(0) {}

ObjectTableEntry::~ObjectTableEntry() { pointer = nullptr; }

int WarnIfSigpipe(int status, int client_sock) {
  if (status >= 0) {
    return 0;
  }
  if (errno == EPIPE || errno == EBADF || errno == ECONNRESET) {
    RAY_LOG(WARNING) << "Received SIGPIPE, BAD FILE DESCRIPTOR, or ECONNRESET when "
                          "sending a message to client on fd "
                       << client_sock
                       << ". The client on the other end may "
                          "have hung up.";
    return errno;
  }
  RAY_LOG(FATAL) << "Failed to write message to client on fd " << client_sock << ".";
  return -1;  // This is never reached.
}

ObjectTableEntry* GetObjectTableEntry(PlasmaStoreInfo* store_info,
                                      const ObjectID& object_id) {
  auto it = store_info->objects.find(object_id);
  if (it == store_info->objects.end()) {
    return NULL;
  }
  return it->second.get();
}

}  // namespace plasma
