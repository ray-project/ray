#ifndef RAY_CORE_WORKER_PLASMA_STORE_PROVIDER_H
#define RAY_CORE_WORKER_PLASMA_STORE_PROVIDER_H

#include "plasma/client.h"
#include "ray/common/buffer.h"
#include "ray/common/id.h"
#include "ray/common/status.h"
#include "ray/core_worker/common.h"
#include "ray/core_worker/store_provider/store_provider.h"
#include "ray/raylet/raylet_client.h"

namespace ray {

class CoreWorker;

/// The class provides implementations for accessing plasma store, which includes both
/// local and remote store, remote access is done via raylet.
class CoreWorkerPlasmaStoreProvider : public CoreWorkerStoreProvider {
 public:
  CoreWorkerPlasmaStoreProvider(const std::string &store_socket,
                                std::unique_ptr<RayletClient> &raylet_client);

  /// See `CoreWorkerStoreProvider::Put` for semantics.
  Status Put(const RayObject &object, const ObjectID &object_id) override;

  /// See `CoreWorkerStoreProvider::Get` for semantics.
  Status Get(const std::unordered_set<ObjectID> &object_ids, int64_t timeout_ms,
             const TaskID &task_id,
             std::unordered_map<ObjectID, std::shared_ptr<RayObject>> *results) override;

  /// See `CoreWorkerStoreProvider::Wait` for semantics.
  Status Wait(const std::unordered_set<ObjectID> &object_ids, int num_objects,
              int64_t timeout_ms, const TaskID &task_id,
              std::unordered_set<ObjectID> *ready) override;

  /// See `CoreWorkerStoreProvider::Delete` for semantics.
  Status Delete(const std::vector<ObjectID> &object_ids, bool local_only = true,
                bool delete_creating_tasks = false) override;

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
  /// \param[out] got_exception Whether any of the fetched objects contained an
  /// exception.
  /// \return Status.
  Status FetchAndGetFromPlasmaStore(
      std::unordered_set<ObjectID> &remaining, const std::vector<ObjectID> &batch_ids,
      int64_t timeout_ms, bool fetch_only, const TaskID &task_id,
      std::unordered_map<ObjectID, std::shared_ptr<RayObject>> *results,
      bool *got_exception);

  /// Whether the buffer represents an exception object.
  ///
  /// \param[in] object Object data.
  /// \return Whether it represents an exception object.
  static bool IsException(const RayObject &object);

  /// Print a warning if we've attempted too many times, but some objects are still
  /// unavailable.
  ///
  /// \param[in] num_attemps The number of attempted times.
  /// \param[in] remaining The remaining objects.
  static void WarnIfAttemptedTooManyTimes(int num_attempts,
                                          const std::unordered_set<ObjectID> &remaining);

  std::unique_ptr<RayletClient> &raylet_client_;
  plasma::PlasmaClient store_client_;
  std::mutex store_client_mutex_;
};

}  // namespace ray

#endif  // RAY_CORE_WORKER_PLASMA_STORE_PROVIDER_H
