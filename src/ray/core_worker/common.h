#ifndef RAY_CORE_WORKER_COMMON_H
#define RAY_CORE_WORKER_COMMON_H

#include <string>

#include "ray/common/buffer.h"
#include "ray/common/id.h"
#include "ray/common/task/task_spec.h"
#include "ray/raylet/raylet_client.h"
#include "ray/util/util.h"

namespace ray {
using WorkerType = rpc::WorkerType;

/// Information about a remote function.
struct RayFunction {
  /// Language of the remote function.
  const Language language;
  /// Function descriptor of the remote function.
  const std::vector<std::string> function_descriptor;
};

/// Binary representation of ray object.
class RayObject {
 public:
  /// Create a ray object instance.
  ///
  /// \param[in] data Data of the ray object.
  /// \param[in] metadata Metadata of the ray object.
  /// \param[in] copy_data Whether this class should hold a copy of data.
  RayObject(const std::shared_ptr<Buffer> &data,
            const std::shared_ptr<Buffer> &metadata = nullptr, bool copy_data = false)
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

    RAY_CHECK((data_ && data_->Size()) || (metadata_ && metadata_->Size()))
        << "Data and metadat cannot be both empty.";
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
  bool HasMetadata() const { return metadata_ != nullptr; }

 private:
  // TODO (kfstorm): Currently both a null pointer and a pointer points to a buffer with
  // zero size means empty data/metadata. We'd better pick one and treat the other as
  // invalid.

  /// Data of the ray object.
  std::shared_ptr<Buffer> data_;
  /// Metadata of the ray object.
  std::shared_ptr<Buffer> metadata_;
  /// Whether this class holds a data copy.
  bool has_data_copy_;
};

/// Argument of a task.
class TaskArg {
 public:
  /// Create a pass-by-reference task argument.
  ///
  /// \param[in] object_id Id of the argument.
  /// \return The task argument.
  static TaskArg PassByReference(const ObjectID &object_id) {
    return TaskArg(std::make_shared<ObjectID>(object_id), nullptr);
  }

  /// Create a pass-by-value task argument.
  ///
  /// \param[in] value Value of the argument.
  /// \return The task argument.
  static TaskArg PassByValue(const RayObject &value) {
    return TaskArg(nullptr, std::make_shared<RayObject>(value));
  }

  /// Create a pass-by-value task argument.
  ///
  /// \param[in] value Value of the argument.
  /// \return The task argument.
  static TaskArg PassByValue(const std::shared_ptr<RayObject> &value) {
    RAY_CHECK(value) << "Value can't be null.";
    return TaskArg(nullptr, value);
  }

  /// Return true if this argument is passed by reference, false if passed by value.
  bool IsPassedByReference() const { return id_ != nullptr; }

  /// Get the reference object ID.
  const ObjectID &GetReference() const {
    RAY_CHECK(id_ != nullptr) << "This argument isn't passed by reference.";
    return *id_;
  }

  /// Get the value.
  std::shared_ptr<RayObject> GetValue() const {
    RAY_CHECK(value_ != nullptr) << "This argument isn't passed by value.";
    return value_;
  }

 private:
  TaskArg(const std::shared_ptr<ObjectID> id, const std::shared_ptr<RayObject> value)
      : id_(id), value_(value) {}

  /// Id of the argument, if passed by reference, otherwise nullptr.
  const std::shared_ptr<ObjectID> id_;
  /// Value of the argument, if passed by value, otherwise nullptr.
  const std::shared_ptr<RayObject> value_;
};

enum class StoreProviderType { LOCAL_PLASMA, PLASMA, MEMORY };

enum class TaskTransportType { RAYLET, DIRECT_ACTOR };

}  // namespace ray

#endif  // RAY_CORE_WORKER_COMMON_H
