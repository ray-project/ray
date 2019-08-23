#ifndef RAY_CORE_WORKER_PLASMA_STORE_PROVIDER_H
#define RAY_CORE_WORKER_PLASMA_STORE_PROVIDER_H

#include "plasma/client.h"
#include "ray/common/buffer.h"
#include "ray/common/id.h"
#include "ray/common/status.h"
#include "ray/core_worker/common.h"
#include "ray/core_worker/store_provider/local_plasma_provider.h"
#include "ray/core_worker/store_provider/store_provider.h"
#include "ray/raylet/raylet_client.h"

namespace ray {

class CoreWorker;

/// The class provides implementations for accessing plasma store, which includes both
/// local and remote stores. Local access goes is done via a
/// CoreWorkerLocalPlasmaStoreProvider and remote access goes through the raylet.
/// See `CoreWorkerStoreProvider` for the semantics of each method.
class CoreWorkerPlasmaStoreProvider : public CoreWorkerStoreProvider {
 public:
  CoreWorkerPlasmaStoreProvider(const std::string &store_socket,
                                std::unique_ptr<RayletClient> &raylet_client);

  Status Put(const RayObject &object, const ObjectID &object_id) override;

  Status Create(const std::shared_ptr<Buffer> &metadata, const size_t data_size,
                const ObjectID &object_id, std::shared_ptr<Buffer> *data) override;

  Status Seal(const ObjectID &object_id) override;

  Status Get(const std::vector<ObjectID> &ids, int64_t timeout_ms, const TaskID &task_id,
             std::vector<std::shared_ptr<RayObject>> *results) override;

  Status Wait(const std::vector<ObjectID> &object_ids, int num_objects,
              int64_t timeout_ms, const TaskID &task_id,
              std::vector<bool> *results) override;

  Status Delete(const std::vector<ObjectID> &object_ids, bool local_only = true,
                bool delete_creating_tasks = false) override;

 private:
  /// Whether the buffer represents an exception object.
  ///
  /// \param[in] object Object data.
  /// \return Whether it represents an exception object.
  static bool IsException(const RayObject &object);

  /// Print a warning if we've attempted too many times, but some objects are still
  /// unavailable. Only the keys in the 'remaining' map are used.
  ///
  /// \param[in] num_attemps The number of attempted times.
  /// \param[in] remaining The remaining objects.
  static void WarnIfAttemptedTooManyTimes(
      int num_attempts, const std::unordered_map<ObjectID, int> &remaining);

  CoreWorkerLocalPlasmaStoreProvider local_store_provider_;
  std::unique_ptr<RayletClient> &raylet_client_;
};

}  // namespace ray

#endif  // RAY_CORE_WORKER_PLASMA_STORE_PROVIDER_H
