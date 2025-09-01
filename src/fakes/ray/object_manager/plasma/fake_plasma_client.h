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

namespace ray {
namespace fakes {

class FakePlasmaClient : public plasma::PlasmaClientInterface {
 public:
  FakePlasmaClient() = default;

  Status Connect(const std::string &, const std::string &, int) override {
    return Status::OK();
  }

  Status Release(const ObjectID &) override { return Status::OK(); }

  Status Contains(const ObjectID &, bool *) override { return Status::OK(); }

  Status Disconnect() override { return Status::OK(); }

  Status Get(const std::vector<ObjectID> &object_ids,
             int64_t /*timeout_ms*/,
             std::vector<plasma::ObjectBuffer> *object_buffers) override {
    object_buffers->assign(object_ids.size(), plasma::ObjectBuffer{});
    return Status::OK();
  }

  Status GetExperimentalMutableObject(const ObjectID &,
                                      std::unique_ptr<plasma::MutableObject> *) override {
    return Status::OK();
  }

  Status Seal(const ObjectID &) override { return Status::OK(); }

  Status Abort(const ObjectID &) override { return Status::OK(); }

  Status CreateAndSpillIfNeeded(const ObjectID &,
                                const rpc::Address &,
                                bool,
                                int64_t,
                                const uint8_t *,
                                int64_t,
                                std::shared_ptr<Buffer> *,
                                plasma::flatbuf::ObjectSource,
                                int) override {
    return Status::OK();
  }

  Status TryCreateImmediately(const ObjectID &,
                              const rpc::Address &,
                              int64_t,
                              const uint8_t *,
                              int64_t,
                              std::shared_ptr<Buffer> *,
                              plasma::flatbuf::ObjectSource,
                              int) override {
    return Status::OK();
  }

  Status Delete(const std::vector<ObjectID> &) override { return Status::OK(); }
};

}  // namespace fakes
}  // namespace ray
