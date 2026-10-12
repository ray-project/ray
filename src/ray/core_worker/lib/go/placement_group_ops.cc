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

#include "ray/core_worker/lib/go/placement_group_ops.h"

#include <cstring>
#include <memory>
#include <nlohmann/json.hpp>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "ray/core_worker/core_worker.h"
#include "ray/core_worker/lib/go/cgo_error.h"
#include "ray/util/logging.h"

namespace ray {
namespace go {

namespace {

// Static CoreWorker operations - defaults to the production adapter over the
// CoreWorkerProcess singleton. Tests inject a GoogleMock ICoreWorkerOps via
// SetCoreWorkerOps to unit-test the placement group path without constructing
// a real CoreWorker.
std::shared_ptr<ICoreWorkerOps> g_core_worker_ops = std::make_shared<CoreWorkerAdapter>();

}  // namespace

PlacementGroupOperations &PlacementGroupOperations::GetInstance() {
  static PlacementGroupOperations instance;
  return instance;
}

void PlacementGroupOperations::SetCoreWorkerOps(std::shared_ptr<ICoreWorkerOps> ops) {
  g_core_worker_ops = ops ? std::move(ops) : std::make_shared<CoreWorkerAdapter>();
}

ICoreWorkerOps &PlacementGroupOperations::GetCoreWorkerOps() {
  return *g_core_worker_ops;
}

ray::Status PlacementGroupOperations::CreatePlacementGroup(
    const ray::core::PlacementGroupCreationOptions &options,
    PlacementGroupID *placement_group_id) {
  RAY_CHECK(placement_group_id != nullptr);
  return GetCoreWorkerOps().CreatePlacementGroup(options, placement_group_id);
}

ray::Status PlacementGroupOperations::RemovePlacementGroup(
    const PlacementGroupID &placement_group_id) {
  return GetCoreWorkerOps().RemovePlacementGroup(placement_group_id);
}

ray::Status PlacementGroupOperations::WaitPlacementGroupReady(
    const PlacementGroupID &placement_group_id, int64_t timeout_seconds) {
  return GetCoreWorkerOps().WaitPlacementGroupReady(placement_group_id, timeout_seconds);
}

bool PlacementGroupOperations::ParseBundlesJson(
    const std::string &json,
    std::vector<std::unordered_map<std::string, double>> *bundles) {
  try {
    auto arr = nlohmann::json::parse(json);
    if (!arr.is_array()) {
      return false;
    }
    for (const auto &item : arr) {
      if (!item.is_object()) {
        return false;
      }
      std::unordered_map<std::string, double> bundle;
      for (auto it = item.begin(); it != item.end(); ++it) {
        if (!it.value().is_number()) {
          return false;
        }
        bundle[it.key()] = it.value().get<double>();
      }
      bundles->push_back(std::move(bundle));
    }
    return true;
  } catch (const std::exception &) {
    return false;
  }
}

}  // namespace go
}  // namespace ray

extern "C" {

int ray_runtime_create_placement_group(const char *name,
                                       const char *bundles_json,
                                       int strategy,
                                       char **pg_id_hex,
                                       char **error) {
  if (pg_id_hex) {
    *pg_id_hex = nullptr;
  }
  if (error) {
    *error = nullptr;
  }
  try {
    std::vector<std::unordered_map<std::string, double>> bundles;
    if (!ray::go::PlacementGroupOperations::ParseBundlesJson(
            bundles_json ? bundles_json : "", &bundles)) {
      SetBoundaryError(error, "failed to parse bundles json");
      return 0;
    }
    ray::core::PlacementGroupCreationOptions options(
        /*name=*/name ? name : "",
        /*strategy=*/static_cast<ray::core::PlacementStrategy>(strategy),
        /*bundles=*/std::move(bundles),
        /*is_detached=*/false);
    ray::PlacementGroupID pg_id;
    auto status = ray::go::PlacementGroupOperations::GetInstance().CreatePlacementGroup(
        options, &pg_id);
    if (!status.ok()) {
      SetBoundaryError(error, status.message().c_str());
      return 0;
    }
    if (pg_id_hex) {
      *pg_id_hex = strdup(pg_id.Hex().c_str());
    }
    return 1;
  } catch (const std::exception &e) {
    SetBoundaryError(error, e.what());
    return 0;
  }
}

int ray_runtime_remove_placement_group(const char *pg_id_hex, char **error) {
  if (error) {
    *error = nullptr;
  }
  try {
    auto pg_id = ray::PlacementGroupID::FromHex(pg_id_hex ? pg_id_hex : "");
    // FromHex does not throw on malformed input; it logs and returns Nil. Treat
    // a nil id as an explicit boundary error instead of silently forwarding it
    // to the seam.
    if (pg_id.IsNil()) {
      SetBoundaryError(error, "invalid placement group id");
      return 0;
    }
    auto status =
        ray::go::PlacementGroupOperations::GetInstance().RemovePlacementGroup(pg_id);
    if (!status.ok()) {
      SetBoundaryError(error, status.message().c_str());
      return 0;
    }
    return 1;
  } catch (const std::exception &e) {
    SetBoundaryError(error, e.what());
    return 0;
  }
}

int ray_runtime_wait_placement_group_ready(const char *pg_id_hex,
                                           int timeout_seconds,
                                           int *ready,
                                           char **error) {
  if (ready) {
    *ready = 0;
  }
  if (error) {
    *error = nullptr;
  }
  try {
    auto pg_id = ray::PlacementGroupID::FromHex(pg_id_hex ? pg_id_hex : "");
    if (pg_id.IsNil()) {
      SetBoundaryError(error, "invalid placement group id");
      return 0;
    }
    auto status =
        ray::go::PlacementGroupOperations::GetInstance().WaitPlacementGroupReady(
            pg_id, static_cast<int64_t>(timeout_seconds));
    if (!status.ok()) {
      // A timeout means the call succeeded but the group was not ready within
      // the deadline: report not-ready via the flag, not as an error. All other
      // non-OK statuses (NotFound etc.) are real failures.
      if (status.IsTimedOut()) {
        if (ready) {
          *ready = 0;
        }
        return 1;
      }
      SetBoundaryError(error, status.message().c_str());
      return 0;
    }
    if (ready) {
      *ready = 1;
    }
    return 1;
  } catch (const std::exception &e) {
    SetBoundaryError(error, e.what());
    return 0;
  }
}
}
