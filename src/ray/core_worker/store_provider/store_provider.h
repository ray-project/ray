#ifndef RAY_CORE_WORKER_STORE_PROVIDER_H
#define RAY_CORE_WORKER_STORE_PROVIDER_H

#include "ray/common/buffer.h"
#include "ray/common/id.h"
#include "ray/common/status.h"
#include "ray/core_worker/common.h"

namespace arrow {
namespace py {
struct SerializedPyObject;
}
}

namespace ray {

/// RayObjects consist of data and metadata. Metadata is always passed
/// in as a buffer because it is small, but data can be handled
/// differently by implementations in order to avoid copying large data.
class RayObject {
 public:
  RayObject(const std::shared_ptr<Buffer> &metadata)
      : metadata_(metadata) {}

  virtual ~RayObject() {}

  const std::shared_ptr<Buffer> &Metadata() const { return metadata_; };

  virtual size_t DataSize() = 0;

  virtual Status WriteDataTo(std::shared_ptr<Buffer> buffer) = 0;

 private:
  const std::shared_ptr<Buffer> metadata_;
}

/// BufferedRayObject is a RayObject whose data is held in a buffer.
class BufferedRayObject : RayObject {
 public:
  BufferedRayObject(const std::shared_ptr<Buffer> &metadata,
		const std::shared_ptr<Buffer> &data)
      : RayObject(metadata), data_(data) {}

  const size_t DataSize() const { return data_.Size() };

  Status WriteDataTo(std::shared_ptr<Buffer> buffer);

 private:
  const std::shared_ptr<Buffer> data_;
};

/// PyArrowRayObject is a RayObject whose data is a SerializedPyObject passed
/// in from an Arrow serialization. This is not placed into a buffer because
/// it consists of a collection of pointers directly into Python memory. The
/// WriteDataTo method will directly write from this memory.
class PyArrowRayObject : RayObject {
 public:
  PyArrowRayObject(const std::shared_ptr<Buffer> &metadata,
		const SerializedPyObject &object)
      : RayObject(metadata), object_(object) {}

  const size_t DataSize() const { return object_.total_bytes };

  Status WriteDataTo(std::shared_ptr<Buffer> buffer);

 private:
  const std::shared_ptr<arrow::py::SerializedPyObject> object_;
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
  virtual Status Delete(const std::vector<ObjectID> &object_ids, bool local_only,
                        bool delete_creating_tasks) = 0;
};

}  // namespace ray

#endif  // RAY_CORE_WORKER_STORE_PROVIDER_H
