#ifndef RAY_CORE_WORKER_LOCAL_PLASMA_STORE_PROVIDER_H
#define RAY_CORE_WORKER_LOCAL_PLASMA_STORE_PROVIDER_H

#include "plasma/client.h"
#include "ray/common/buffer.h"
#include "ray/common/id.h"
#include "ray/common/status.h"
#include "ray/core_worker/common.h"
#include "ray/core_worker/store_provider/store_provider.h"
#include "ray/rpc/raylet/raylet_client.h"

namespace ray {

class CoreWorker;

/// The class provides a store provider implementation for accessing the local plasma
/// store. See `CoreWorkerStoreProvider` for the semantics of each method.
class CoreWorkerLocalPlasmaStoreProvider : public CoreWorkerStoreProvider {
 public:
  CoreWorkerLocalPlasmaStoreProvider(const std::string &store_socket);

  Status Put(const RayObject &object, const ObjectID &object_id) override;

  Status Create(const std::shared_ptr<Buffer> &metadata, const size_t data_size,
                const ObjectID &object_id, std::shared_ptr<Buffer> *data) override;

  Status Seal(const ObjectID &object_id) override;

  Status Get(const std::vector<ObjectID> &ids, int64_t timeout_ms, const TaskID &task_id,
             std::vector<std::shared_ptr<RayObject>> *results) override;

  /// Note that `num_objects` must equal to number of items in `object_ids`.
  Status Wait(const std::vector<ObjectID> &object_ids, int num_objects,
              int64_t timeout_ms, const TaskID &task_id,
              std::vector<bool> *results) override;

  /// Note that `local_only` msut be true, and `delete_creating_tasks` must be false here.
  Status Free(const std::vector<ObjectID> &object_ids, bool local_only = true,
              bool delete_creating_tasks = false) override;

 private:
  plasma::PlasmaClient store_client_;
  std::mutex store_client_mutex_;
};

}  // namespace ray

#endif  // RAY_CORE_WORKER_LOCAL_PLASMA_STORE_PROVIDER_H
