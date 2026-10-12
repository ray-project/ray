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

#ifndef SRC_RAY_CORE_WORKER_LIB_GO_PLACEMENT_GROUP_OPS_H_
#define SRC_RAY_CORE_WORKER_LIB_GO_PLACEMENT_GROUP_OPS_H_

/**
 * @file placement_group_ops.h
 * @brief Business logic layer for placement group operations.
 *
 * Pure C++ business logic for placement group creation / removal / readiness
 * waiting, separated from CGO boundary concerns. It depends on the injectable
 * ICoreWorkerOps seam (core_worker_provider.h) so the CoreWorker-dependent
 * methods can be unit-tested with a GoogleMock fake. The C boundary functions
 * below are the Go-facing contract, callable via cgo.
 */

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "ray/common/id.h"
#include "ray/core_worker/common.h"
#include "ray/core_worker/lib/go/core_worker_provider.h"

namespace ray {
namespace go {

/**
 * @brief Business logic operations for placement groups.
 *
 * Thread-safe singleton; the underlying CoreWorker operations are injected via
 * SetCoreWorkerOps (default: CoreWorkerAdapter over the CoreWorkerProcess
 * singleton) so unit tests can substitute a GoogleMock ICoreWorkerOps.
 */
class PlacementGroupOperations {
 public:
  /**
   * @brief Get the singleton instance.
   */
  static PlacementGroupOperations &GetInstance();

  /**
   * @brief Set the CoreWorker operations injected for the placement group path.
   *
   * @param ops Shared pointer to ICoreWorkerOps (may be null to restore the
   * default adapter).
   */
  static void SetCoreWorkerOps(std::shared_ptr<ICoreWorkerOps> ops);

  /**
   * @brief Get the current CoreWorker operations.
   */
  static ICoreWorkerOps &GetCoreWorkerOps();

  /**
   * @brief Create a placement group.
   *
   * @param options Creation options (name / strategy / bundles / detached).
   * @param placement_group_id Output parameter for the created id.
   * @return Status of the creation (OK on success).
   */
  ray::Status CreatePlacementGroup(
      const ray::core::PlacementGroupCreationOptions &options,
      PlacementGroupID *placement_group_id);

  /**
   * @brief Remove a placement group by id (synchronous).
   *
   * @param placement_group_id Id of the placement group to remove.
   * @return Status of the removal (OK on success).
   */
  ray::Status RemovePlacementGroup(const PlacementGroupID &placement_group_id);

  /**
   * @brief Block until the placement group is ready or the timeout expires.
   *
   * @param placement_group_id Id of the placement group.
   * @param timeout_seconds Timeout in seconds.
   * @return Status OK if the placement group became ready; TimedOut if the
   * timeout expired without becoming ready; other non-OK statuses for real
   * failures.
   */
  ray::Status WaitPlacementGroupReady(const PlacementGroupID &placement_group_id,
                                      int64_t timeout_seconds);

  /**
   * @brief Parse a JSON array of resource maps into per-bundle resource maps.
   *
   * Pure data transformation (no CoreWorker access), so it is directly
   * unit-testable. Returns false on malformed input.
   *
   * @param json JSON text like `[{"CPU":1,"memory":128}]`.
   * @param bundles Output resource maps (one per bundle).
   */
  static bool ParseBundlesJson(
      const std::string &json,
      std::vector<std::unordered_map<std::string, double>> *bundles);

 private:
  PlacementGroupOperations() = default;
};

}  // namespace go
}  // namespace ray

extern "C" {
// Create a placement group. On success returns 1 and sets pg_id_hex (hex string,
// caller frees with free()); on failure returns 0 and sets error (also freed by
// the caller with free()).
int ray_runtime_create_placement_group(const char *name,
                                       const char *bundles_json,
                                       int strategy,
                                       char **pg_id_hex,
                                       char **error);

// Remove a placement group by hex id. Returns 1 on success, 0 on failure
// (error set, caller frees with free()).
int ray_runtime_remove_placement_group(const char *pg_id_hex, char **error);

// Wait until the placement group is ready or the timeout expires. Returns 1 on
// success (ready flag indicates readiness), 0 on failure (error set, caller
// frees with free()).
int ray_runtime_wait_placement_group_ready(const char *pg_id_hex,
                                           int timeout_seconds,
                                           int *ready,
                                           char **error);
}

#endif  // SRC_RAY_CORE_WORKER_LIB_GO_PLACEMENT_GROUP_OPS_H_
