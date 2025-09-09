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

#pragma once

#include <memory>
#include <string>
#include <vector>

// A simple fake implementation of PlasmaClientInterface for use in unit tests.
//
// This base fake does nothing (returns OK for most methods, empty results for Get).
// Extend it in test files to add behavior (recording batches, timeouts, missing objects).

#include "ray/common/buffer.h"
#include "ray/common/id.h"
#include "ray/common/status.h"
#include "ray/object_manager/plasma/client.h"

namespace plasma {

class FakePlasmaClient : public PlasmaClientInterface {
 public:
  FakePlasmaClient(std::vector<std::vector<ObjectID>> *observed_batches = nullptr)
      : observed_batches_(observed_batches) {}

  Status Connect(const std::string &, const std::string &, int) override {
    return Status::OK();
  }

  Status Release(const ObjectID &) override { return Status::OK(); }

  Status Contains(const ObjectID &, bool *) override { return Status::OK(); }

  Status Disconnect() override { return Status::OK(); }

  Status Get(const std::vector<ObjectID> &object_ids,
             int64_t /*timeout_ms*/,
             std::vector<ObjectBuffer> *object_buffers) override {
    if (observed_batches_ != nullptr) {
      observed_batches_->push_back(object_ids);
    }
    // Return non-null buffers to simulate presence for tests.
    object_buffers->resize(object_ids.size());
    for (size_t i = 0; i < object_ids.size(); i++) {
      uint8_t byte = 0;
      auto parent =
          std::make_shared<ray::LocalMemoryBuffer>(&byte, 1, /*copy_data=*/true);
      (*object_buffers)[i].data = SharedMemoryBuffer::Slice(parent, 0, 1);
      (*object_buffers)[i].metadata = SharedMemoryBuffer::Slice(parent, 0, 1);
    }
    return Status::OK();
  }

  Status GetExperimentalMutableObject(const ObjectID &,
                                      std::unique_ptr<MutableObject> *) override {
    return Status::OK();
  }

  Status Seal(const ObjectID &) override { return Status::OK(); }

  Status Abort(const ObjectID &) override { return Status::OK(); }

  Status CreateAndSpillIfNeeded(const ObjectID &,
                                const ray::rpc::Address &,
                                bool,
                                int64_t,
                                const uint8_t *,
                                int64_t,
                                std::shared_ptr<Buffer> *,
                                flatbuf::ObjectSource,
                                int) override {
    return Status::OK();
  }

  Status TryCreateImmediately(const ObjectID &,
                              const ray::rpc::Address &,
                              int64_t,
                              const uint8_t *,
                              int64_t,
                              std::shared_ptr<Buffer> *,
                              flatbuf::ObjectSource,
                              int) override {
    return Status::OK();
  }

  Status Delete(const std::vector<ObjectID> &) override { return Status::OK(); }

  StatusOr<std::string> GetMemoryUsage() override { return std::string("fake"); }

 private:
  std::vector<std::vector<ObjectID>> *observed_batches_;
};

}  // namespace plasma
