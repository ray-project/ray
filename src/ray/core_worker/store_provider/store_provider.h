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
  /// \param[in] copy_data Whether this class should hold a copy of data.
  RayObject(const std::shared_ptr<Buffer> &data, const std::shared_ptr<Buffer> &metadata,
            bool copy_data = false)
      : data_(data), metadata_(metadata), has_data_copy_(copy_data) {
    if (has_data_copy_) {
      // If this object is required to hold a copy of the data,
      // make a copy if the passed in buffers don't already have a copy.
      if (data_ && !data_->OwnsData()) {
        data_ = std::make_shared<LocalMemoryBuffer>(data_->Data(), data_->Size(), true);
      }

      if (metadata_ && !metadata_->OwnsData()) {
        metadata_ = std::make_shared<LocalMemoryBuffer>(metadata_->Data(),
                                                        metadata_->Size(), true);
      }
    }
  }

  /// Return the data of the ray object.
  const std::shared_ptr<Buffer> &GetData() const { return data_; };

  /// Return the metadata of the ray object.
  const std::shared_ptr<Buffer> &GetMetadata() const { return metadata_; };

  uint64_t GetSize() const {
    uint64_t size = 0;
    size += (data_ != nullptr) ? data_->Size() : 0;
    size += (metadata_ != nullptr) ? metadata_->Size() : 0;
    return size;
  }

  /// Whether this object has metadata.
  bool HasMetadata() const { return metadata_ != nullptr && metadata_->Size() > 0; }

  /// Whether this object represents an exception object.
  bool IsException() const {
    if (!HasMetadata()) {
      return false;
    }

    // TODO (kfstorm): metadata should be structured.
    const std::string metadata(reinterpret_cast<const char *>(GetMetadata()->Data()),
                               GetMetadata()->Size());
    const auto error_type_descriptor = ray::rpc::ErrorType_descriptor();
    for (int i = 0; i < error_type_descriptor->value_count(); i++) {
      const auto error_type_number = error_type_descriptor->value(i)->number();
      if (metadata == std::to_string(error_type_number)) {
        return true;
      }
    }
    return false;
  }

 private:
  /// Data of the ray object.
  std::shared_ptr<Buffer> data_;
  /// Metadata of the ray object.
  std::shared_ptr<Buffer> metadata_;
  /// Whether this class holds a data copy.
  bool has_data_copy_;
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
  /// \param[out] results Result list of objects data.
  /// \return Status.
  virtual Status Get(const std::vector<ObjectID> &ids, int64_t timeout_ms,
             std::vector<std::shared_ptr<RayObject>> *results) = 0;

  /// Wait for a list of objects to appear in the object store.
  ///
  /// \param[in] IDs of the objects to wait for.
  /// \param[in] num_returns Number of objects that should appear.
  /// \param[in] timeout_ms Timeout in milliseconds, wait infinitely if it's negative.
  /// \param[out] results A bitset that indicates each object has appeared or not.
  /// \return Status.
  virtual Status Wait(const std::vector<ObjectID> &object_ids, int num_objects,
              int64_t timeout_ms, std::vector<bool> *results) = 0;

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

    /// Print a warning if we've attempted too many times, but some objects are still
    /// unavailable.
    ///
    /// \param[in] num_attemps The number of attempted times.
    /// \param[in] unready The unready objects.
    static void WarnIfAttemptedTooManyTimes(
        int num_attempts, const std::unordered_set<ObjectID> &unready) {
        if (num_attempts % RayConfig::instance().object_store_get_warn_per_num_attempts() ==
            0) {
            std::ostringstream oss;
            size_t printed = 0;
            for (auto &entry : unready) {
            if (printed >=
                RayConfig::instance().object_store_get_max_ids_to_print_in_warning()) {
                break;
            }
            if (printed > 0) {
                oss << ", ";
            }
            oss << entry.Hex();
            }
            if (printed < unready.size()) {
            oss << ", etc";
            }
            RAY_LOG(WARNING)
                << "Attempted " << num_attempts << " times to reconstruct objects, but "
                << "some objects are still unavailable. If this message continues to print,"
                << " it may indicate that object's creating task is hanging, or something wrong"
                << " happened in raylet backend. " << unready.size()
                << " object(s) pending: " << oss.str() << ".";
        }
    }
};

}  // namespace ray

#endif  // RAY_CORE_WORKER_STORE_PROVIDER_H
