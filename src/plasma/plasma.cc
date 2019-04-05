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

#include "plasma/plasma.h"

#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include "plasma/common.h"
#include "plasma/common_generated.h"
#include "plasma/protocol.h"

namespace fb = plasma::flatbuf;

namespace plasma {

ObjectTableEntry::ObjectTableEntry() : pointer(nullptr), ref_count(0) {}

ObjectTableEntry::~ObjectTableEntry() { pointer = nullptr; }

int WarnIfSigpipe(int status, int client_sock) {
  if (status >= 0) {
    return 0;
  }
  if (errno == EPIPE || errno == EBADF || errno == ECONNRESET) {
    ARROW_LOG(WARNING) << "Received SIGPIPE, BAD FILE DESCRIPTOR, or ECONNRESET when "
                          "sending a message to client on fd "
                       << client_sock
                       << ". The client on the other end may "
                          "have hung up.";
    return errno;
  }
  ARROW_LOG(FATAL) << "Failed to write message to client on fd " << client_sock << ".";
  return -1;  // This is never reached.
}

/**
 * This will create a new ObjectInfo buffer. The first sizeof(int64_t) bytes
 * of this buffer are the length of the remaining message and the
 * remaining message is a serialized version of the object info.
 *
 * @param object_info The object info to be serialized
 * @return The object info buffer. It is the caller's responsibility to free
 *         this buffer with "delete" after it has been used.
 */
std::unique_ptr<uint8_t[]> CreateObjectInfoBuffer(fb::ObjectInfoT* object_info) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = fb::CreateObjectInfo(fbb, object_info);
  fbb.Finish(message);
  auto notification =
      std::unique_ptr<uint8_t[]>(new uint8_t[sizeof(int64_t) + fbb.GetSize()]);
  *(reinterpret_cast<int64_t*>(notification.get())) = fbb.GetSize();
  memcpy(notification.get() + sizeof(int64_t), fbb.GetBufferPointer(), fbb.GetSize());
  return notification;
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
