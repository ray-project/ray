// Copyright 2024 The Ray Authors.
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

#include "ray/gcs/usage_stats_client.h"

#include <string>

namespace ray {
namespace gcs {
UsageStatsClient::UsageStatsClient(InternalKVInterface &internal_kv,
                                   instrumented_io_context &io_context)
    : internal_kv_(internal_kv), io_context_(io_context) {}

void UsageStatsClient::RecordExtraUsageTag(usage::TagKey key, const std::string &value) {
  internal_kv_.Put(kUsageStatsNamespace,
                   kExtraUsageTagPrefix + absl::AsciiStrToLower(usage::TagKey_Name(key)),
                   value,
                   /*overwrite=*/true,
                   {[](bool added) {
                      if (!added) {
                        RAY_LOG(DEBUG)
                            << "Did not add new extra usage tag, maybe overwritten";
                      }
                    },
                    io_context_});
}

void UsageStatsClient::RecordExtraUsageCounter(usage::TagKey key, int64_t counter) {
  RecordExtraUsageTag(key, std::to_string(counter));
}
}  // namespace gcs
}  // namespace ray
