// Copyright 2026 The Ray Authors.
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

#include "ray/raylet/worker_resource_limits.h"

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <string>

#include "absl/strings/str_cat.h"
#include "nlohmann/json.hpp"
#include "ray/common/ray_config.h"
#include "ray/common/scheduling/placement_group_util.h"
#include "ray/common/scheduling/scheduling_ids.h"

namespace ray {
namespace raylet {
namespace {

constexpr int64_t kDefaultCpuPeriodUs = 100000;
constexpr int64_t kMaxCpuPeriodUs = 1000000;
constexpr int64_t kMinCpuQuotaUs = 1000;
constexpr double kMinRepresentableCpus =
    static_cast<double>(kMinCpuQuotaUs) / kMaxCpuPeriodUs;

bool IsImageUriRuntimeEnv(const std::string &serialized_runtime_env) {
  try {
    const auto runtime_env = nlohmann::json::parse(serialized_runtime_env);
    const auto image_uri = runtime_env.find("image_uri");
    return image_uri != runtime_env.end() && image_uri->is_string() &&
           !image_uri->get_ref<const std::string &>().empty();
  } catch (const nlohmann::json::exception &) {
    return false;
  }
}

double GetNormalizedResourceQuantity(const ResourceSet &resources,
                                     const scheduling::ResourceID &target) {
  double quantity = 0;
  for (const auto &resource_id : resources.ResourceIds()) {
    std::string resource_name = resource_id.Binary();
    const auto placement_group_resource =
        ParsePgFormattedResource(resource_name,
                                 /*for_wildcard_resource=*/true,
                                 /*for_indexed_resource=*/true);
    if (placement_group_resource.has_value()) {
      resource_name = placement_group_resource->original_resource;
    }
    if (resource_name == target.Binary()) {
      // Placement group allocations contain both wildcard and indexed aliases for the
      // same allocation. Taking the maximum preserves the actual quantity once.
      quantity = std::max(quantity, resources.Get(resource_id).Double());
    }
  }
  return quantity;
}

}  // namespace

rpc::WorkerResourceLimits BuildWorkerResourceLimits(
    const std::string &serialized_runtime_env, const ResourceSet &resources) {
  rpc::WorkerResourceLimits limits;
  if (!RayConfig::instance().worker_resource_limits_enabled() ||
      !IsImageUriRuntimeEnv(serialized_runtime_env)) {
    return limits;
  }

  const double cpus =
      GetNormalizedResourceQuantity(resources, scheduling::ResourceID::CPU());
  if (cpus > 0) {
    if (cpus < kMinRepresentableCpus) {
      limits.set_validation_error(
          absl::StrCat("Per-worker container CPU limits below ",
                       kMinRepresentableCpus,
                       " CPU cannot be represented by Linux CFS. Requested: ",
                       cpus,
                       " CPU."));
    } else {
      // Linux CFS requires quota >= 1 ms and period <= 1 second. Use a longer
      // period for small fractional requests so their quota remains legal without
      // changing the requested CPU fraction.
      const int64_t period_us =
          cpus < static_cast<double>(kMinCpuQuotaUs) / kDefaultCpuPeriodUs
              ? kMaxCpuPeriodUs
              : kDefaultCpuPeriodUs;
      limits.set_cpu_period_us(period_us);
      limits.set_cpu_quota_us(std::llround(cpus * period_us));
    }
  }

  const double memory =
      GetNormalizedResourceQuantity(resources, scheduling::ResourceID::Memory());
  if (memory > 0) {
    limits.set_memory_bytes(static_cast<uint64_t>(std::llround(memory)));
  }
  return limits;
}

bool WorkerResourceLimitsEqual(const rpc::WorkerResourceLimits &left,
                               const rpc::WorkerResourceLimits &right) {
  return left.cpu_period_us() == right.cpu_period_us() &&
         left.cpu_quota_us() == right.cpu_quota_us() &&
         left.memory_bytes() == right.memory_bytes() &&
         left.validation_error() == right.validation_error();
}

bool HasWorkerResourceLimits(const rpc::WorkerResourceLimits &limits) {
  return limits.cpu_period_us() > 0 || limits.cpu_quota_us() > 0 ||
         limits.memory_bytes() > 0 || !limits.validation_error().empty();
}

}  // namespace raylet
}  // namespace ray
