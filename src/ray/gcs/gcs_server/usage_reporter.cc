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

#include "ray/gcs/gcs_server/usage_reporter.h"

#include <string>

namespace ray {
namespace gcs {

namespace {
const std::string kUsageStatsNamespace{"usage_stats"};
}

GcsUsageReporter::GcsUsageReporter(InternalKVInterface &kv) : kv_(kv) {}

void GcsUsageReporter::ReportValue(usage::TagKey key, std::string value) {
  kv_.Put(kUsageStatsNamespace,
          usage::TagKey_Name(key),
          value,
          /*overwrite*/ true,
          /*callback*/ [](bool /*newly_added*/) {});
}

void GcsUsageReporter::ReportCounter(usage::TagKey key, int64_t counter) {
  ReportValue(key, std::to_string(counter));
}

}  // namespace gcs
}  // namespace ray
