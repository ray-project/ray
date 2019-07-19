#ifndef RAY_CORE_WORKER_STORE_PROVIDER_H
#define RAY_CORE_WORKER_STORE_PROVIDER_H

#include "ray/common/buffer.h"
#include "ray/common/id.h"
#include "ray/common/status.h"
#include "ray/core_worker/common.h"

namespace ray {

/// Binary representation of ray object.
class RayObject {
 public:
  /// Create a ray object instance.
  ///
  /// \param[in] data Data of the ray object.
  /// \param[in] metadata Metadata of the ray object.
  RayObject(const std::shared_ptr<Buffer> &data, const std::shared_ptr<Buffer> &metadata)
      : data_(data), metadata_(metadata) {}

  /// Return the data of the ray object.
  const std::shared_ptr<Buffer> &GetData() const { return data_; };

  /// Return the metadata of the ray object.
  const std::shared_ptr<Buffer> &GetMetadata() const { return metadata_; };

 private:
  /// Data of the ray object.
  const std::shared_ptr<Buffer> data_;
  /// Metadata of the ray object.
  const std::shared_ptr<Buffer> metadata_;
};

/// Provider interface for store access. Store provider should inherit from this class and
/// provide implementions for the methods. The actual store provider may use a plasma
/// store or local memory store in worker process, or possibly other types of storage.

class CoreWorkerStoreProvider {
 public:
  CoreWorkerStoreProvider() {}

  virtual ~CoreWorkerStoreProvider() {}

  /// Put an object with specified ID into object store.
  ///
  /// \param[in] object The ray object.
  /// \param[in] object_id Object ID specified by user.
  /// \return Status.
  virtual Status Put(const RayObject &object, const ObjectID &object_id) = 0;

  /// Get a list of objects from the object store.
  ///
  /// \param[in] ids IDs of the objects to get.
  /// \param[in] timeout_ms Timeout in milliseconds, wait infinitely if it's negative.
  /// \param[in] task_id ID for the current task.
  /// \param[out] results Result list of objects data.
  /// \return Status.
  virtual Status Get(const std::vector<ObjectID> &ids, int64_t timeout_ms,
                     const TaskID &task_id,
                     std::vector<std::shared_ptr<RayObject>> *results) = 0;

  /// Wait for a list of objects to appear in the object store.
  ///
  /// \param[in] IDs of the objects to wait for.
  /// \param[in] num_returns Number of objects that should appear.
  /// \param[in] timeout_ms Timeout in milliseconds, wait infinitely if it's negative.
  /// \param[in] task_id ID for the current task.
  /// \param[out] results A bitset that indicates each object has appeared or not.
  /// \return Status.
  virtual Status Wait(const std::vector<ObjectID> &object_ids, int num_objects,
                      int64_t timeout_ms, const TaskID &task_id,
                      std::vector<bool> *results) = 0;

  /// Delete a list of objects from the object store.
  ///
  /// \param[in] object_ids IDs of the objects to delete.
  /// \param[in] local_only Whether only delete the objects in local node, or all nodes in
  /// the cluster.
  /// \param[in] delete_creating_tasks Whether also delete the tasks that
  /// created these objects.
  /// \return Status.
  virtual Status Delete(const std::vector<ObjectID> &object_ids, bool local_only = true,
                        bool delete_creating_tasks = false) = 0;
};

}  // namespace ray

#endif  // RAY_CORE_WORKER_STORE_PROVIDER_H
