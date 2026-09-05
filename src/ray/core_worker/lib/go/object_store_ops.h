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

#ifndef RAY_CORE_WORKER_LIB_GO_OBJECT_STORE_OPS_H_
#define RAY_CORE_WORKER_LIB_GO_OBJECT_STORE_OPS_H_

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "object_store_provider.h"
#include "ray/common/id.h"
#include "ray/common/ray_object.h"
#include "ray/core_worker/core_worker.h"
#include "ray/core_worker/core_worker_options.h"
#include "ray/core_worker/core_worker_process.h"
#include "ray/util/logging.h"

namespace ray {
namespace go {

/**
 * ObjectStoreOperations - Business logic layer for ObjectStore operations.
 *
 * This class encapsulates all ObjectStore business logic, independent of CGO.
 * It uses dependency injection via IObjectStoreProvider for testability.
 *
 * Design principles:
 * - Pure C++ implementation (no C types)
 * - Throws exceptions on errors (no error handling)
 * - Single responsibility: business logic only
 * - Testable via MockObjectStoreProvider
 */
class ObjectStoreOperations {
 public:
  /**
   * Get singleton instance.
   * @return Reference to singleton instance
   */
  static ObjectStoreOperations &GetInstance();

  /**
   * Set the CoreWorker provider for dependency injection.
   * @param provider Shared pointer to IObjectStoreProvider
   */
  static void SetCoreWorkerProvider(std::shared_ptr<IObjectStoreProvider> provider);

  /**
   * Get the current CoreWorker provider.
   * @return Reference to IObjectStoreProvider
   */
  static IObjectStoreProvider &GetCoreWorkerProvider();

  // ============================================================================
  // Business Logic Methods
  // ============================================================================

  /**
   * Put an object into the object store.
   * @param object The RayObject to store
   * @return ObjectID of the stored object
   * @throws std::exception on failure
   */
  ray::ObjectID Put(const std::shared_ptr<ray::RayObject> &object);

  /**
   * Put an object into the object store with a specific ID.
   * @param object_id The ObjectID to use
   * @param object The RayObject to store
   * @throws std::exception on failure
   */
  void PutWithID(const ray::ObjectID &object_id,
                 const std::shared_ptr<ray::RayObject> &object);

  /**
   * Get objects from the object store.
   * @param ids Vector of ObjectIDs to retrieve
   * @param timeout_ms Timeout in milliseconds (-1 for infinite)
   * @return Vector of RayObjects
   * @throws std::exception on failure
   */
  std::vector<std::shared_ptr<ray::RayObject>> Get(const std::vector<ray::ObjectID> &ids,
                                                   int64_t timeout_ms);

  /**
   * Wait for objects to be ready.
   * @param ids Vector of ObjectIDs to wait for
   * @param num_objects Number of objects to wait for
   * @param timeout_ms Timeout in milliseconds (-1 for infinite)
   * @param fetch_local Whether to fetch locally
   * @return Vector of booleans indicating readiness
   * @throws std::exception on failure
   */
  std::vector<bool> Wait(const std::vector<ray::ObjectID> &ids,
                         int num_objects,
                         int64_t timeout_ms,
                         bool fetch_local);

  /**
   * Delete objects from the object store.
   * @param ids Vector of ObjectIDs to delete
   * @param local_only Whether to delete only locally
   * @throws std::exception on failure
   */
  void Delete(const std::vector<ray::ObjectID> &ids, bool local_only);

  /**
   * Add a local reference to an object.
   * @param object_id The ObjectID to reference
   * @throws std::exception on failure
   */
  void AddLocalReference(const ray::ObjectID &object_id);

  /**
   * Remove a local reference from an object.
   * @param object_id The ObjectID to dereference
   * @throws std::exception on failure
   */
  void RemoveLocalReference(const ray::ObjectID &object_id);

  /**
   * Get all reference counts.
   * @return Map of ObjectID to (local_count, submitted_count)
   * @throws std::exception on failure
   */
  std::unordered_map<ray::ObjectID, std::pair<size_t, size_t>> GetAllReferenceCounts();

  /**
   * Get the owner address for an object.
   * @param object_id The ObjectID
   * @return Owner address
   * @throws std::exception on failure
   */
  ray::rpc::Address GetOwnerAddress(const ray::ObjectID &object_id);

  /**
   * Get ownership information for an object.
   * @param object_id The ObjectID
   * @return Serialized ownership metadata
   * @throws std::exception on failure
   */
  std::string GetOwnershipInfo(const ray::ObjectID &object_id);

  /**
   * Register ownership information and resolve a future.
   * @param object_id The ObjectID
   * @param outer_object_id The outer ObjectID
   * @param owner_address The owner address
   * @throws std::exception on failure
   */
  void RegisterOwnershipInfoAndResolveFuture(const ray::ObjectID &object_id,
                                             const ray::ObjectID &outer_object_id,
                                             const ray::rpc::Address &owner_address);

 private:
  ObjectStoreOperations() = default;

  /**
   * Get CoreWorker instance from provider.
   * @return Reference to CoreWorker
   * @throws std::runtime_error if not initialized
   */
  core::CoreWorker &GetCoreWorker() const {
    return GetCoreWorkerProvider().GetCoreWorker();
  }
};

}  // namespace go
}  // namespace ray

#endif  // RAY_CORE_WORKER_LIB_GO_OBJECT_STORE_OPS_H_
