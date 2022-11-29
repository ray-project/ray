// Copyright 2022 The Ray Authors.
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

#include "ray/gcs/gcs_client/usage_stats_client.h"

namespace ray {
namespace gcs {
UsageStatsClient::UsageStatsClient(const std::string &gcs_address,
                                   instrumented_io_context &io_service) {
  GcsClientOptions options(gcs_address);
  gcs_client_ = std::make_unique<GcsClient>(options);
  RAY_CHECK_OK(gcs_client_->Connect(io_service));
}

void UsageStatsClient::RecordExtraUsageTag(const std::string &key,
                                           const std::string &value) {
  RAY_CHECK_OK(gcs_client_->InternalKV().AsyncInternalKVPut(
      kUsageStatsNamespace,
      kExtraUsageTagPrefix + key,
      value,
      /*overwrite=*/true,
      [](Status status, boost::optional<int> added_num) {
        if (!status.ok()) {
          RAY_LOG(DEBUG) << "Failed to put extra usage tag, status = " << status;
        }
      }));
}
}  // namespace gcs
}  // namespace ray
