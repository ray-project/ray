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

#pragma once

#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "ray/common/buffer.h"
#include "ray/common/ray_object.h"
#include "ray/common/status.h"
#include "ray/object_manager/plasma/client.h"
#include "ray/object_manager/plasma/common.h"
#include "ray/object_manager/plasma/plasma.h"
#include "ray/util/visibility.h"
#include "src/ray/protobuf/common.pb.h"

namespace ray {

class ExperimentalChannelManager {
 public:
  ExperimentalChannelManager() {}

  Status RegisterWriterChannel(const ObjectID &object_id,
                               std::unique_ptr<plasma::MutableObject> mutable_object);

  Status WriteAcquire(const ObjectID &object_id,
                      int64_t data_size,
                      const uint8_t *metadata,
                      int64_t metadata_size,
                      int64_t num_readers,
                      std::shared_ptr<Buffer> *data);

  Status WriteRelease(const ObjectID &object_id);

  Status RegisterReaderChannel(const ObjectID &object_id,
                               std::unique_ptr<plasma::MutableObject> mutable_object);

  bool ReaderChannelRegistered(const ObjectID &object_id) const;

  Status ReadAcquire(const ObjectID &object_id, std::shared_ptr<RayObject> *result);

  Status ReadRelease(const ObjectID &object_id);

  Status SetError(const ObjectID &object_id);

 private:
  struct WriterChannel {
    WriterChannel(std::unique_ptr<plasma::MutableObject> mutable_object_ptr)
        : mutable_object(std::move(mutable_object_ptr)) {}

    bool is_sealed = true;
    std::unique_ptr<plasma::MutableObject> mutable_object;
  };

  struct ReaderChannel {
    ReaderChannel(std::unique_ptr<plasma::MutableObject> mutable_object_ptr)
        : mutable_object(std::move(mutable_object_ptr)) {}

    /// The last version that we read. To read again, we must pass a newer
    /// version than this.
    int64_t next_version_to_read = 1;
    /// Whether we currently have a read lock on the object. If this is true,
    /// then it is safe to read the value of the object. For immutable objects,
    /// this will always be true once the object has been sealed. For mutable
    /// objects, ReadRelease resets this to false, and ReadAcquire resets to
    /// true.
    bool read_acquired = false;
    std::unique_ptr<plasma::MutableObject> mutable_object;
  };

  Status EnsureGetAcquired(ReaderChannel &channel);

  absl::flat_hash_map<ObjectID, WriterChannel> writer_channels_;
  absl::flat_hash_map<ObjectID, ReaderChannel> reader_channels_;
};

}  // namespace ray
