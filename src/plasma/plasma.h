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

#ifndef PLASMA_PLASMA_H
#define PLASMA_PLASMA_H

#include <errno.h>
#include <inttypes.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>  // pid_t

#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include "plasma/compat.h"

#include "arrow/status.h"
#include "arrow/util/logging.h"
#include "arrow/util/macros.h"
#include "plasma/common.h"

#ifdef PLASMA_CUDA
using arrow::cuda::CudaIpcMemHandle;
#endif

namespace plasma {

namespace flatbuf {
struct ObjectInfoT;
}  // namespace flatbuf

#define HANDLE_SIGPIPE(s, fd_)                                              \
  do {                                                                      \
    Status _s = (s);                                                        \
    if (!_s.ok()) {                                                         \
      if (errno == EPIPE || errno == EBADF || errno == ECONNRESET) {        \
        ARROW_LOG(WARNING)                                                  \
            << "Received SIGPIPE, BAD FILE DESCRIPTOR, or ECONNRESET when " \
               "sending a message to client on fd "                         \
            << fd_                                                          \
            << ". "                                                         \
               "The client on the other end may have hung up.";             \
      } else {                                                              \
        return _s;                                                          \
      }                                                                     \
    }                                                                       \
  } while (0);

/// Allocation granularity used in plasma for object allocation.
constexpr int64_t kBlockSize = 64;

struct Client;

// TODO(pcm): Replace this by the flatbuffers message PlasmaObjectSpec.
struct PlasmaObject {
#ifdef PLASMA_CUDA
  // IPC handle for Cuda.
  std::shared_ptr<CudaIpcMemHandle> ipc_handle;
#endif
  /// The file descriptor of the memory mapped file in the store. It is used as
  /// a unique identifier of the file in the client to look up the corresponding
  /// file descriptor on the client's side.
  int store_fd;
  /// The offset in bytes in the memory mapped file of the data.
  ptrdiff_t data_offset;
  /// The offset in bytes in the memory mapped file of the metadata.
  ptrdiff_t metadata_offset;
  /// The size in bytes of the data.
  int64_t data_size;
  /// The size in bytes of the metadata.
  int64_t metadata_size;
  /// Device number object is on.
  int device_num;
};

enum class ObjectStatus : int {
  /// The object was not found.
  OBJECT_NOT_FOUND = 0,
  /// The object was found.
  OBJECT_FOUND = 1
};

/// The plasma store information that is exposed to the eviction policy.
struct PlasmaStoreInfo {
  /// Objects that are in the Plasma store.
  ObjectTable objects;
  /// Boolean flag indicating whether to start the object store with hugepages
  /// support enabled. Huge pages are substantially larger than normal memory
  /// pages (e.g. 2MB or 1GB instead of 4KB) and using them can reduce
  /// bookkeeping overhead from the OS.
  bool hugepages_enabled;
  /// A (platform-dependent) directory where to create the memory-backed file.
  std::string directory;
};

/// Get an entry from the object table and return NULL if the object_id
/// is not present.
///
/// @param store_info The PlasmaStoreInfo that contains the object table.
/// @param object_id The object_id of the entry we are looking for.
/// @return The entry associated with the object_id or NULL if the object_id
///         is not present.
ObjectTableEntry* GetObjectTableEntry(PlasmaStoreInfo* store_info,
                                      const ObjectID& object_id);

/// Print a warning if the status is less than zero. This should be used to check
/// the success of messages sent to plasma clients. We print a warning instead of
/// failing because the plasma clients are allowed to die. This is used to handle
/// situations where the store writes to a client file descriptor, and the client
/// may already have disconnected. If we have processed the disconnection and
/// closed the file descriptor, we should get a BAD FILE DESCRIPTOR error. If we
/// have not, then we should get a SIGPIPE. If we write to a TCP socket that
/// isn't connected yet, then we should get an ECONNRESET.
///
/// @param status The status to check. If it is less less than zero, we will
///        print a warning.
/// @param client_sock The client socket. This is just used to print some extra
///        information.
/// @return The errno set.
int WarnIfSigpipe(int status, int client_sock);

std::unique_ptr<uint8_t[]> CreateObjectInfoBuffer(flatbuf::ObjectInfoT* object_info);

}  // namespace plasma

#endif  // PLASMA_PLASMA_H
