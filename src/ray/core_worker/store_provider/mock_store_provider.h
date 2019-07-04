#ifndef RAY_CORE_WORKER_MOCK_STORE_PROVIDER_H
#define RAY_CORE_WORKER_MOCK_STORE_PROVIDER_H

#include "ray/common/buffer.h"
#include "ray/common/id.h"
#include "ray/common/status.h"
#include "ray/core_worker/common.h"
#include "ray/core_worker/store_provider/store_provider.h"
#include "ray/core_worker/transport/mock_transport.h"

namespace ray {

class CoreWorkerMockTaskPool;

/// The class provides implementations for local memory store, which is used for single
/// process mode.
class CoreWorkerMockStoreProvider : public CoreWorkerStoreProvider {
 public:
  /// Put an object with specified ID into object store.
  ///
  /// \param[in] object The ray object.
  /// \param[in] object_id Object ID specified by user.
  /// \return Status.
  Status Put(const RayObject &object, const ObjectID &object_id) override;

  /// Get a list of objects from the object store.
  ///
  /// \param[in] ids IDs of the objects to get.
  /// \param[in] timeout_ms Timeout in milliseconds, wait infinitely if it's negative.
  /// \param[in] task_id ID for the current task.
  /// \param[out] results Result list of objects data.
  /// \return Status.
  Status Get(const std::vector<ObjectID> &ids, int64_t timeout_ms, const TaskID &task_id,
             std::vector<std::shared_ptr<RayObject>> *results) override;

  /// Wait for a list of objects to appear in the object store.
  ///
  /// \param[in] IDs of the objects to wait for.
  /// \param[in] num_returns Number of objects that should appear.
  /// \param[in] timeout_ms Timeout in milliseconds, wait infinitely if it's negative.
  /// \param[in] task_id ID for the current task.
  /// \param[out] results A bitset that indicates each object has appeared or not.
  /// \return Status.
  Status Wait(const std::vector<ObjectID> &object_ids, int num_objects,
              int64_t timeout_ms, const TaskID &task_id,
              std::vector<bool> *results) override;

  /// Delete a list of objects from the object store.
  ///
  /// \param[in] object_ids IDs of the objects to delete.
  /// \param[in] local_only Whether only delete the objects in local node, or all nodes in
  /// the cluster.
  /// \param[in] delete_creating_tasks Whether also delete the tasks that
  /// created these objects. \return Status.
  Status Delete(const std::vector<ObjectID> &object_ids, bool local_only,
                bool delete_creating_tasks) override;

  bool IsObjectReady(const ObjectID &object_id);

  void SetMockTaskPool(std::shared_ptr<CoreWorkerMockTaskPool> mock_task_pool);

 private:
  std::unordered_map<ObjectID, RayObject> pool_;

  std::mutex mutex_;

  const int64_t get_check_interval_ms_ = 100;

  std::shared_ptr<CoreWorkerMockTaskPool> mock_task_pool_;
};

}  // namespace ray

#endif  // RAY_CORE_WORKER_MOCK_STORE_PROVIDER_H
