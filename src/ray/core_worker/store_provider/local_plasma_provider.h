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

/// The class provides implementations for accessing local plasma store.
class CoreWorkerLocalPlasmaStoreProvider : public CoreWorkerStoreProvider {
 public:
  CoreWorkerLocalPlasmaStoreProvider(const std::string &store_socket);

  /// See `CoreWorkerStoreProvider::Put` for semantics.
  Status Put(const RayObject &object, const ObjectID &object_id) override;

  /// See `CoreWorkerStoreProvider::Get` for semantics.
  Status Get(const std::vector<ObjectID> &ids, int64_t timeout_ms,
             std::vector<std::shared_ptr<RayObject>> *results) override;

  /// See `CoreWorkerStoreProvider::Wait` for semantics.
  Status Wait(const std::vector<ObjectID> &object_ids,
              int64_t timeout_ms, std::vector<bool> *results) override;

  /// See `CoreWorkerStoreProvider::Delete` for semantics.
  Status Delete(const std::vector<ObjectID> &object_ids) override;

 private:
  /// Plasma store client.
  plasma::PlasmaClient store_client_;

  /// Mutex to protect store_client_.
  std::mutex store_client_mutex_;
};

}  // namespace ray

#endif  // RAY_CORE_WORKER_LOCAL_PLASMA_STORE_PROVIDER_H
