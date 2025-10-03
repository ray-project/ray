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

#include "ray/gcs_rpc_client/accessors/publisher_accessor_interface.h"
#include "ray/gcs_rpc_client/gcs_client_context.h"
#include "src/ray/protobuf/gcs.pb.h"

namespace ray {
namespace gcs {

/// \class PublisherAccessor
/// Implementation of PublisherAccessorInterface.
class PublisherAccessor : public PublisherAccessorInterface {
 public:
  PublisherAccessor() = default;
  explicit PublisherAccessor(GcsClientContext *context);
  virtual ~PublisherAccessor() = default;

  virtual Status PublishError(std::string key_id,
                              rpc::ErrorTableData data,
                              int64_t timeout_ms) override;

  virtual Status PublishLogs(std::string key_id,
                             rpc::LogBatch data,
                             int64_t timeout_ms) override;

  virtual void AsyncPublishNodeResourceUsage(std::string key_id,
                                             std::string node_resource_usage_json,
                                             const StatusCallback &done) override;

 private:
  // GCS client implementation.
  GcsClientContext *context_ = nullptr;
};

}  // namespace gcs
}  // namespace ray
