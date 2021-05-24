// Copyright 2017 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once
#include <memory>
#include <string>
#include "ray/core.h"

namespace ray {
namespace api {

enum ErrorType : int {
  WORKER_DIED = 0,
  ACTOR_DIED = 1,
  OBJECT_UNRECONSTRUCTABLE = 2,
  TASK_EXECUTION_EXCEPTION = 3,
  OBJECT_IN_PLASMA = 4,
  TASK_CANCELLED = 5,
  ACTOR_CREATION_FAILED = 6,
};

enum WorkerType : int {
  WORKER = 0,
  DRIVER = 1,
  SPILL_WORKER = 2,
  RESTORE_WORKER = 3,
};

enum Language : int {
  PYTHON = 0,
  JAVA = 1,
  CPP = 2,
};

enum TaskType : int {
  NORMAL_TASK = 0,
  ACTOR_CREATION_TASK = 1,
  ACTOR_TASK = 2,
  DRIVER_TASK = 3,
};

class Buffer {
 public:
  /// Pointer to the data.
  virtual uint8_t *Data() const = 0;

  /// Size of this buffer.
  virtual size_t Size() const = 0;

  /// Whether this buffer owns the data.
  virtual bool OwnsData() const = 0;

  virtual bool IsPlasmaBuffer() const = 0;

  virtual ~Buffer(){};

  bool operator==(const Buffer &rhs) const {
    if (this->Size() != rhs.Size()) {
      return false;
    }

    return this->Size() == 0 || memcmp(Data(), rhs.Data(), Size()) == 0;
  }
};

class LocalMemoryBuffer : public Buffer {
 public:
  LocalMemoryBuffer(uint8_t *data, size_t size, bool copy_data = false)
      : has_data_copy_(copy_data) {
    if (copy_data) {
      RAY_CHECK(data != nullptr);
      buffer_ = reinterpret_cast<uint8_t *>(aligned_malloc(size, BUFFER_ALIGNMENT));
      std::copy(data, data + size, buffer_);
      data_ = buffer_;
      size_ = size;
    } else {
      data_ = data;
      size_ = size;
    }
  }

  /// Construct a LocalMemoryBuffer of all zeros of the given size.
  LocalMemoryBuffer(size_t size) : has_data_copy_(true) {
    buffer_ = reinterpret_cast<uint8_t *>(aligned_malloc(size, BUFFER_ALIGNMENT));
    data_ = buffer_;
    size_ = size;
  }

  uint8_t *Data() const override { return data_; }

  size_t Size() const override { return size_; }

  bool OwnsData() const override { return has_data_copy_; }

  bool IsPlasmaBuffer() const override { return false; }

  ~LocalMemoryBuffer() {
    size_ = 0;
    if (buffer_ != NULL) {
      aligned_free(buffer_);
    }
  }

 private:
  /// Disable copy constructor and assignment, as default copy will
  /// cause invalid data_.
  LocalMemoryBuffer &operator=(const LocalMemoryBuffer &) = delete;
  LocalMemoryBuffer(const LocalMemoryBuffer &) = delete;

  /// Pointer to the data.
  uint8_t *data_;
  /// Size of the buffer.
  size_t size_;
  /// Whether this buffer holds a copy of data.
  bool has_data_copy_;
  /// This is only valid when `should_copy` is true.
  uint8_t *buffer_ = NULL;
};

class RayObject {
 public:
  RayObject(const std::shared_ptr<Buffer> &data, const std::shared_ptr<Buffer> &metadata,
            const std::vector<ObjectID> &nested_ids, bool copy_data = false)
      : data_(data), metadata_(metadata), nested_ids_(nested_ids) {
    if (copy_data) {
      // If this object is required to hold a copy of the data,
      // make a copy if the passed in buffers don't already have a copy.
      if (data_ && !data_->OwnsData()) {
        data_ = std::make_shared<LocalMemoryBuffer>(data_->Data(), data_->Size(),
                                                    /*copy_data=*/true);
      }

      if (metadata_ && !metadata_->OwnsData()) {
        metadata_ = std::make_shared<LocalMemoryBuffer>(
            metadata_->Data(), metadata_->Size(), /*copy_data=*/true);
      }
    }

    RAY_CHECK(data_ || metadata_) << "Data and metadata cannot both be empty.";
  }

  /// Return the data of the ray object.
  const std::shared_ptr<Buffer> &GetData() const { return data_; }

  /// Return the metadata of the ray object.
  const std::shared_ptr<Buffer> &GetMetadata() const { return metadata_; }

  /// Return the object IDs that were serialized in data.
  const std::vector<ObjectID> &GetNestedIds() const { return nested_ids_; }

  uint64_t GetSize() const {
    uint64_t size = 0;
    size += (data_ != nullptr) ? data_->Size() : 0;
    size += (metadata_ != nullptr) ? metadata_->Size() : 0;
    return size;
  }

 private:
  std::shared_ptr<Buffer> data_;
  std::shared_ptr<Buffer> metadata_;
  const std::vector<ObjectID> nested_ids_;
};

struct Address {
  std::string raylet_id;
  std::string ip_address;
  int32_t port;
  // Optional unique id for the worker.
  std::string worker_id;
};

enum class ArgType { ArgByRef, ArgByValue };

/// Argument of a task.
class TaskArg {
 public:
  TaskArg() = default;
  TaskArg(ArgType arg_type) : arg_type_(arg_type) {}
  virtual ~TaskArg(){};
  ArgType GetArgType() { return arg_type_; }

  virtual const ObjectID GetObjectID() { return {}; }
  virtual const Address GetAddress() { return {}; }
  virtual std::shared_ptr<RayObject> GetValue() { return nullptr; }

 protected:
  ArgType arg_type_;
};

class TaskArgByReference : public TaskArg {
 public:
  /// Create a pass-by-reference task argument.
  ///
  /// \param[in] object_id Id of the argument.
  /// \return The task argument.
  TaskArgByReference(const ObjectID &object_id, const Address &owner_address)
      : id_(object_id), owner_address_(owner_address) {
    arg_type_ = ArgType::ArgByRef;
  }
  const ObjectID GetObjectID() { return id_; }
  const Address GetAddress() { return owner_address_; }

 private:
  /// Id of the argument if passed by reference, otherwise nullptr.
  const ObjectID id_;
  const Address owner_address_;
};

class TaskArgByValue : public TaskArg {
 public:
  /// Create a pass-by-value task argument.
  ///
  /// \param[in] value Value of the argument.
  /// \return The task argument.
  explicit TaskArgByValue(const std::shared_ptr<RayObject> &value) : value_(value) {
    // RAY_CHECK(value) << "Value can't be null.";
    arg_type_ = ArgType::ArgByValue;
  }

  std::shared_ptr<RayObject> GetValue() { return value_; }

 private:
  /// Value of the argument.
  const std::shared_ptr<RayObject> value_;
};

}  // namespace api
}  // namespace ray