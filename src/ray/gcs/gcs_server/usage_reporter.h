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

#include "ray/common/asio/instrumented_io_context.h"
#include "ray/gcs/gcs_server/gcs_kv_manager.h"
#include "src/ray/protobuf/usage.pb.h"

namespace ray {
namespace gcs {

/// Helper class collects custom usage data.
/// Note: this class could only be used on Gcs.
class GcsUsageReporter {
 public:
  GcsUsageReporter(instrumented_io_context &service, InternalKVInterface &kv);

  // Report a custom usage key/value pairs. If the key
  // already exists, the value will be overwritten.
  void ReportValue(usage::TagKey key, std::string value);

  // Report a monotonically increasing counter.
  void ReportCounter(usage::TagKey key, int64_t counter);

 private:
  instrumented_io_context &service_;
  InternalKVInterface &kv_;
};

}  // namespace gcs
}  // namespace ray
