// Copyright 2017 The Ray Authors.
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

#include "ray/common/gcs_callback_types.h"
#include "ray/common/id.h"
#include "src/ray/protobuf/gcs.pb.h"
#include "src/ray/protobuf/logging.pb.h"

namespace ray {
namespace gcs {

/// \class PublisherAccessorInterface
/// Interface for Publisher operations.
class PublisherAccessorInterface {
 public:
  virtual ~PublisherAccessorInterface() = default;

  virtual Status PublishError(std::string key_id,
                              rpc::ErrorTableData data,
                              int64_t timeout_ms) = 0;

  virtual Status PublishLogs(std::string key_id,
                             rpc::LogBatch data,
                             int64_t timeout_ms) = 0;

  virtual void AsyncPublishNodeResourceUsage(std::string key_id,
                                             std::string node_resource_usage_json,
                                             const StatusCallback &done) = 0;
};

}  // namespace gcs
}  // namespace ray
