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

namespace ray {
namespace gcs {

enum SchedulingType {
  SPREAD = 0,
  STRICT_SPREAD = 1,
  PACK = 2,
  STRICT_PACK = 3,
  SchedulingType_MAX = 4,
};

class SchedulingPolicy {
 public:
  SchedulingPolicy(const SchedulingType &type) : type_(type) {}

  const SchedulingType type_;
};

/// Gcs resource scheduler implementation.
/// Non-thread safe.
class GcsResourceScheduler {
 public:
  ResourceScheduler(GcsResourceManager &gcs_resource_manager)
      : gcs_resources_manager_(gcs_resources_manager) {}

  virtual ~GcsResourceScheduler() = default;

  std::vector<NodeID> Schedule(std::vector<ResourceSet> required_resources,
                               SchedulingPolicy policy);

 private:
  /// Reference of GcsResourceManager.
  GcsResourceManager &gcs_resource_manager_;
};

}  // namespace gcs
}  // namespace ray
