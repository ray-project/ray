#ifndef RAY_CORE_WORKER_PLASMA_STORE_PROVIDER_H
#define RAY_CORE_WORKER_PLASMA_STORE_PROVIDER_H

#include "plasma/client.h"
#include "ray/common/buffer.h"
#include "ray/common/id.h"
#include "ray/common/status.h"
#include "ray/core_worker/common.h"
#include "ray/core_worker/context.h"
#include "ray/core_worker/store_provider/local_plasma_provider.h"
#include "ray/core_worker/store_provider/store_provider.h"
#include "ray/rpc/raylet/raylet_client.h"

namespace ray {

using rpc::RayletClient;

class CoreWorker;

/// The class provides implementations for accessing plasma store, which includes both
/// local and remote store, remote access is done via raylet.
class CoreWorkerPlasmaStoreProvider : public CoreWorkerStoreProvider {
 public:
  CoreWorkerPlasmaStoreProvider(const WorkerContext &worker_context,
                                const std::string &store_socket,
                                std::unique_ptr<RayletClient> &raylet_client);

  /// See `CoreWorkerStoreProvider::Put` for semantics.
  Status Put(const RayObject &object, const ObjectID &object_id) override;

  /// See `CoreWorkerStoreProvider::Get` for semantics.
  Status Get(const std::vector<ObjectID> &ids, int64_t timeout_ms,
             std::vector<std::shared_ptr<RayObject>> *results) override;

  /// See `CoreWorkerStoreProvider::Wait` for semantics.
  Status Wait(const std::vector<ObjectID> &object_ids, int num_objects,
              int64_t timeout_ms, std::vector<bool> *results) override;

/// See `CoreWorkerStoreProvider::Delete` for semantics.
  Status Delete(const std::vector<ObjectID> &object_ids, bool local_only = true,
                bool delete_creating_tasks = false) override;

 private:
  /// Whether the buffer represents an exception object.
  ///
  /// \param[in] object Object data.
  /// \return Whether it represents an exception object.
  static bool IsException(const RayObject &object);

  /// Print a warning if we've attempted too many times, but some objects are still
  /// unavailable.
  ///
  /// \param[in] num_attemps The number of attempted times.
  /// \param[in] unready The unready objects.
  static void WarnIfAttemptedTooManyTimes(
      int num_attempts, const std::unordered_map<ObjectID, int> &unready);

  /// Reference to `WorkerContext`.
  const WorkerContext &worker_context_;

  /// local plasma store provider.
  CoreWorkerLocalPlasmaStoreProvider local_store_provider_;

  /// Reference to raylet client.
  std::unique_ptr<RayletClient> &raylet_client_;
};

}  // namespace ray

#endif  // RAY_CORE_WORKER_PLASMA_STORE_PROVIDER_H
