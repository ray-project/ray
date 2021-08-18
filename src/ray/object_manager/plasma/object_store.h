// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include "absl/container/flat_hash_map.h"
#include "ray/object_manager/plasma/allocator.h"
#include "ray/object_manager/plasma/common.h"
#include "ray/object_manager/plasma/plasma.h"

namespace plasma {

// IObjectStore stores objects with unique object id.
// It's not thread safe.
class IObjectStore {
 public:
  virtual ~IObjectStore() = default;

  /// Create a new object given object's info. Caller need to decide
  /// to use primary allocation or fallback allocation by setting
  /// fallback_allocate flag.
  /// NOTE: It ABORT the program if an object with the same id already exists.
  ///
  /// \param object_info Plasma object info.
  /// \param source From where the object is created.
  /// \param fallback_allocate Whether to use fallback allocation.
  /// \return
  ///   - pointer to created object or nullptr when out of space.
  virtual const LocalObject *CreateObject(const ray::ObjectInfo &object_info,
                                          plasma::flatbuf::ObjectSource source,
                                          bool fallback_allocate) = 0;

  /// Get object by id.
  ///
  /// \param object_id Object ID of the object to be sealed.
  /// \return
  ///   - nullptr if such object doesn't exist.
  ///   - otherwise, pointer to the object.
  virtual const LocalObject *GetObject(const ObjectID &object_id) const = 0;

  /// Seal created object by id.
  ///
  /// \param object_id Object ID of the object to be sealed.
  /// \return
  ///   - nulltpr if such object doesn't exist, or the object has already been sealed.
  ///   - otherise, pointer to the sealed object.
  virtual const LocalObject *SealObject(const ObjectID &object_id) = 0;

  /// Delete an existing object.
  ///
  /// \param object_id Object ID of the object to be sealed.
  /// \return
  ///   - false if such object doesn't exist.
  ///   - true if deleted.
  virtual bool DeleteObject(const ObjectID &object_id) = 0;

  virtual int64_t GetNumBytesCreatedTotal() const = 0;

  virtual int64_t GetNumBytesUnsealed() const = 0;

  virtual int64_t GetNumObjectsUnsealed() const = 0;

  virtual void GetDebugDump(std::stringstream &buffer) const = 0;
};

// ObjectStore implements IObjectStore. It uses IAllocator
// to allocate memory for object creation.
class ObjectStore : public IObjectStore {
 public:
  explicit ObjectStore(IAllocator &allocator);

  const LocalObject *CreateObject(const ray::ObjectInfo &object_info,
                                  plasma::flatbuf::ObjectSource source,
                                  bool fallback_allocate) override;

  const LocalObject *GetObject(const ObjectID &object_id) const override;

  const LocalObject *SealObject(const ObjectID &object_id) override;

  bool DeleteObject(const ObjectID &object_id) override;

  int64_t GetNumBytesCreatedTotal() const override;

  int64_t GetNumBytesUnsealed() const override;

  int64_t GetNumObjectsUnsealed() const override;

  void GetDebugDump(std::stringstream &buffer) const override;

 private:
  friend class ObjectStoreTest;
  FRIEND_TEST(ObjectStoreTest, PassThroughTest);
  FRIEND_TEST(ObjectStoreTest, SourceMetricsTest);

  struct ObjectStoreMetrics {
    /// A running total of the objects that have ever been created on this node.
    int64_t num_bytes_created_total = 0;
    /// Total number of objects allocated to objects that are created but not yet
    /// sealed.
    int64_t num_objects_unsealed = 0;
    int64_t num_bytes_unsealed = 0;
    /// Total number of objects created by core workers.
    size_t num_objects_created_by_worker = 0;
    size_t num_bytes_created_by_worker = 0;
    /// Total number of objects created by object restoration.
    size_t num_objects_restored = 0;
    size_t num_bytes_restored = 0;
    /// Total number of objects created by object pulling.
    size_t num_objects_received = 0;
    size_t num_bytes_received = 0;
    /// Total number of objects that are created as an error object.
    size_t num_objects_errored = 0;
    size_t num_bytes_errored = 0;

    bool operator==(const ObjectStoreMetrics &metrics) const {
      return (num_objects_unsealed == metrics.num_objects_unsealed &&
              num_bytes_unsealed == metrics.num_bytes_unsealed &&
              num_objects_created_by_worker == metrics.num_objects_created_by_worker &&
              num_bytes_created_by_worker == metrics.num_bytes_created_by_worker &&
              num_objects_restored == metrics.num_objects_restored &&
              num_bytes_restored == metrics.num_bytes_restored &&
              num_objects_received == metrics.num_objects_received &&
              num_bytes_received == metrics.num_bytes_received &&
              num_objects_errored == metrics.num_objects_errored &&
              num_bytes_errored == metrics.num_bytes_errored);
    }
  };

  LocalObject *GetMutableObject(const ObjectID &object_id);

  /// Testing only
  ObjectStoreMetrics GetMetrics();

  /// Update metrics when object is newly created.
  void MetricsUpdateOnObjectCreated(LocalObject *obj);

  /// Update metrics when unsealed object is deleted.
  /// e.g., when the object is sealed or unsealed object is deleted.
  void MetricsUpdateOnObjectSealed(LocalObject *obj);

  /// Update metrics when the object is deleted from the object table.
  void MetricsUpdateOnObjectDeleted(LocalObject *obj);

  /// Allocator that allocates memory.
  IAllocator &allocator_;

  /// Mapping from ObjectIDs to information about the object.
  absl::flat_hash_map<ObjectID, std::unique_ptr<LocalObject>> object_table_;

  ObjectStoreMetrics object_store_metrics_;
};
}  // namespace plasma
