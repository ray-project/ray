#ifndef RAY_CORE_WORKER_PLASMA_STORE_PROVIDER_H
#define RAY_CORE_WORKER_PLASMA_STORE_PROVIDER_H

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "plasma/client.h"
#include "ray/common/buffer.h"
#include "ray/common/id.h"
#include "ray/common/status.h"
#include "ray/core_worker/common.h"
#include "ray/raylet/raylet_client.h"

namespace ray {

class CoreWorker;

/// The class provides implementations for accessing plasma store, which includes both
/// local and remote stores. Local access goes is done via a
/// CoreWorkerLocalPlasmaStoreProvider and remote access goes through the raylet.
/// See `CoreWorkerStoreProvider` for the semantics of public methods.
class CoreWorkerPlasmaStoreProvider {
 public:
  CoreWorkerPlasmaStoreProvider(const std::string &store_socket,
                                const std::unique_ptr<RayletClient> &raylet_client,
                                std::function<Status()> check_signals);

  ~CoreWorkerPlasmaStoreProvider();

  Status SetClientOptions(std::string name, int64_t limit_bytes);

  Status Put(const RayObject &object, const ObjectID &object_id);

  Status Create(const std::shared_ptr<Buffer> &metadata, const size_t data_size,
                const ObjectID &object_id, std::shared_ptr<Buffer> *data);

  Status Seal(const ObjectID &object_id);

  Status Get(const absl::flat_hash_set<ObjectID> &object_ids, int64_t timeout_ms,
             const TaskID &task_id,
             absl::flat_hash_map<ObjectID, std::shared_ptr<RayObject>> *results,
             bool *got_exception);

  Status Contains(const ObjectID &object_id, bool *has_object);

  Status Wait(const absl::flat_hash_set<ObjectID> &object_ids, int num_objects,
              int64_t timeout_ms, const TaskID &task_id,
              absl::flat_hash_set<ObjectID> *ready);

  Status Delete(const absl::flat_hash_set<ObjectID> &object_ids, bool local_only,
                bool delete_creating_tasks);

  std::string MemoryUsageString();

 private:
  /// Ask the raylet to fetch a set of objects and then attempt to get them
  /// from the local plasma store. Successfully fetched objects will be removed
  /// from the input set of remaining IDs and added to the results map.
  ///
  /// \param[in/out] remaining IDs of the remaining objects to get.
  /// \param[in] batch_ids IDs of the objects to get.
  /// \param[in] timeout_ms Timeout in milliseconds.
  /// \param[in] fetch_only Whether the raylet should only fetch or also attempt to
  /// reconstruct objects.
  /// \param[in] task_id The current TaskID.
  /// \param[out] results Map of objects to write results into. This method will only
  /// add to this map, not clear or remove from it, so the caller can pass in a non-empty
  /// map.
  /// \param[out] got_exception Set to true if any of the fetched objects contained an
  /// exception.
  /// \return Status.
  Status FetchAndGetFromPlasmaStore(
      absl::flat_hash_set<ObjectID> &remaining, const std::vector<ObjectID> &batch_ids,
      int64_t timeout_ms, bool fetch_only, const TaskID &task_id,
      absl::flat_hash_map<ObjectID, std::shared_ptr<RayObject>> *results,
      bool *got_exception);

  /// Print a warning if we've attempted too many times, but some objects are still
  /// unavailable. Only the keys in the 'remaining' map are used.
  ///
  /// \param[in] num_attemps The number of attempted times.
  /// \param[in] remaining The remaining objects.
  static void WarnIfAttemptedTooManyTimes(int num_attempts,
                                          const absl::flat_hash_set<ObjectID> &remaining);

  const std::unique_ptr<RayletClient> &raylet_client_;
  plasma::PlasmaClient store_client_;
  std::mutex store_client_mutex_;
  std::function<Status()> check_signals_;
};

}  // namespace ray

#endif  // RAY_CORE_WORKER_PLASMA_STORE_PROVIDER_H
