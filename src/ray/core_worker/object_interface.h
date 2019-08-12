#ifndef RAY_CORE_WORKER_OBJECT_INTERFACE_H
#define RAY_CORE_WORKER_OBJECT_INTERFACE_H

#include "plasma/client.h"
#include "ray/common/buffer.h"
#include "ray/common/id.h"
#include "ray/common/status.h"
#include "ray/core_worker/common.h"
#include "ray/core_worker/context.h"
#include "ray/core_worker/store_provider/store_provider_layer.h"
#include "ray/core_worker/transport/transport_layer.h"

namespace ray {

using rpc::RayletClient;

class CoreWorker;
class CoreWorkerStoreProvider;

/// The interface that contains all `CoreWorker` methods that are related to object store.
class CoreWorkerObjectInterface {
 public:
  CoreWorkerObjectInterface(WorkerContext &worker_context,
                            std::unique_ptr<RayletClient> &raylet_client,
                            CoreWorkerStoreProviderLayer &store_provider_layer,
                            CoreWorkerTaskSubmitterLayer &task_submitter_layer);

  /// Put an object into object store.
  ///
  /// \param[in] object The ray object.
  /// \param[out] object_id Generated ID of the object.
  /// \return Status.
  Status Put(const RayObject &object, ObjectID *object_id);

  /// Put an object with specified ID into object store.
  ///
  /// \param[in] object The ray object.
  /// \param[in] object_id Object ID specified by user.
  /// \return Status.
  Status Put(const RayObject &object, const ObjectID &object_id);

  /// Get a list of objects from the object store.
  ///
  /// \param[in] ids IDs of the objects to get.
  /// \param[in] timeout_ms Timeout in milliseconds, wait infinitely if it's negative.
  /// \param[out] results Result list of objects data.
  /// \return Status.
  Status Get(const std::vector<ObjectID> &ids, int64_t timeout_ms,
             std::vector<std::shared_ptr<RayObject>> *results);

  /// Wait for a list of objects to appear in the object store.
  ///
  /// \param[in] IDs of the objects to wait for.
  /// \param[in] num_returns Number of objects that should appear.
  /// \param[in] timeout_ms Timeout in milliseconds, wait infinitely if it's negative.
  /// \param[out] results A bitset that indicates each object has appeared or not.
  /// \return Status.
  Status Wait(const std::vector<ObjectID> &object_ids, int num_objects,
              int64_t timeout_ms, std::vector<bool> *results);

  /// Delete a list of objects from the object store.
  ///
  /// \param[in] object_ids IDs of the objects to delete.
  /// \param[in] local_only Whether only delete the objects in local node, or all nodes in
  /// the cluster.
  /// \param[in] delete_creating_tasks Whether also delete the tasks that
  /// created these objects.
  /// \return Status.
  Status Delete(const std::vector<ObjectID> &object_ids, bool local_only,
                bool delete_creating_tasks);

 private:
  /// Helper function to get a list of objects from a specific store provider.
  ///
  /// \param[in] type The type of store provider to use.
  /// \param[in] object_ids IDs of the objects to get.
  /// \param[in] timeout_ms Timeout in milliseconds, wait infinitely if it's -1.
  /// \param[out] results Result list of objects data.
  /// \return Status.
  Status Get(StoreProviderType type, TaskTransportType transport_type,
             const std::unordered_set<ObjectID> &object_ids,
             int64_t timeout_ms,
             std::unordered_map<ObjectID, std::shared_ptr<RayObject>> *results);

  /// Helper function to check whether to wait for a list of objects to appear,
  /// e.g. wait for the tasks which create these objects to finish.
  ///
  /// \param[in] transport_type The type of the transport to check status with.
  /// \param[in] object_ids A list of object ids.
  bool ShouldWaitObjects(TaskTransportType transport_type, const std::vector<ObjectID> &object_ids);

  /// Reference to the parent CoreWorker's context.
  WorkerContext &worker_context_;
  /// Reference to the parent CoreWorker's raylet client.
  std::unique_ptr<RayletClient> &raylet_client_;
  /// Reference to store provider layer.
  CoreWorkerStoreProviderLayer &store_provider_layer_;
  /// Reference to task submitter layer.
  CoreWorkerTaskSubmitterLayer &task_submitter_layer_;
};

}  // namespace ray

#endif  // RAY_CORE_WORKER_OBJECT_INTERFACE_H
