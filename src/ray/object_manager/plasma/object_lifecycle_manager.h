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

#include "absl/container/flat_hash_set.h"
#include "gtest/gtest.h"
#include "ray/object_manager/plasma/common.h"
#include "ray/object_manager/plasma/eviction_policy.h"
#include "ray/object_manager/plasma/object_store.h"
#include "ray/object_manager/plasma/plasma_allocator.h"
#include "ray/object_manager/plasma/stats_collector.h"

namespace plasma {

class IObjectLifecycleManager {
 public:
  virtual ~IObjectLifecycleManager() = default;

  /// Create a new object given object's info. Object creation might
  /// fail if runs out of space; or an object with the same id exists.
  ///
  /// \param object_info Plasma object info.
  /// \param source From where the object is created.
  /// \param fallback_allocator Whether to allow fallback allocation.
  /// \return
  ///   - pointer to created object and PlasmaError::OK when succeeds.
  ///   - nullptr and error message, including ObjectExists/OutOfMemory
  /// TODO(scv119): use RAII instead of pointer for returned object.
  virtual std::pair<const LocalObject *, flatbuf::PlasmaError> CreateObject(
      const ray::ObjectInfo &object_info,
      plasma::flatbuf::ObjectSource source,
      bool fallback_allocator) = 0;

  /// Get object by id.
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

  /// Abort object creation by id. It deletes the object regardless of reference
  /// counting.
  ///
  /// \param object_id Object ID of the object to be aborted.
  /// \return One of the following error codes:
  ///  - PlasmaError::OK, if the object was aborted successfully.
  ///  - PlasmaError::ObjectNonexistent, if ths object doesn't exist.
  ///  - PlasmaError::ObjectSealed, if ths object has already been sealed.
  virtual flatbuf::PlasmaError AbortObject(const ObjectID &object_id) = 0;

  /// Delete a specific object by object_id. The object is delete immediately
  /// if it's been sealed and reference counting is zero. Otherwise it will be
  /// asynchronously deleted once there is no usage.
  ///
  /// \param object_id Object ID of the object to be deleted.
  /// \return One of the following error codes:
  ///  - PlasmaError::OK, if the object was delete successfully.
  ///  - PlasmaError::ObjectNonexistent, if ths object doesn't exist.
  ///  - PlasmaError::ObjectNotsealed, if ths object is created but not sealed.
  ///  - PlasmaError::ObjectInUse, if the object is in use; it will be deleted
  ///  once it's no longer used (ref count becomes 0).
  virtual flatbuf::PlasmaError DeleteObject(const ObjectID &object_id) = 0;

  /// Bump up the reference count of the object.
  ///
  /// \return true if object exists, false otherise.
  virtual bool AddReference(const ObjectID &object_id) = 0;

  /// Decrese the reference count of the object. When reference count
  /// drop to zero the object becomes evictable.
  ///
  /// \return true if object exists and reference count is greater than 0, false otherise.
  virtual bool RemoveReference(const ObjectID &object_id) = 0;
};

// ObjectLifecycleManager allocates LocalObjects from the allocator.
// It tracks object’s lifecycle states such as reference count or object states
// (created/sealed). It lazily garbage collects objects when running out of space.
class ObjectLifecycleManager : public IObjectLifecycleManager {
 public:
  ObjectLifecycleManager(IAllocator &allocator,
                         ray::DeleteObjectCallback delete_object_callback);

  std::pair<const LocalObject *, flatbuf::PlasmaError> CreateObject(
      const ray::ObjectInfo &object_info,
      plasma::flatbuf::ObjectSource source,
      bool fallback_allocator) override;

  const LocalObject *GetObject(const ObjectID &object_id) const override;

  const LocalObject *SealObject(const ObjectID &object_id) override;

  flatbuf::PlasmaError AbortObject(const ObjectID &object_id) override;

  flatbuf::PlasmaError DeleteObject(const ObjectID &object_id) override;

  bool AddReference(const ObjectID &object_id) override;

  bool RemoveReference(const ObjectID &object_id) override;

  /// Ask it to evict objects until we have at least size of capacity
  /// available.
  /// TEST ONLY
  ///
  /// \return The number of bytes evicted.
  int64_t RequireSpace(int64_t size);

  std::string EvictionPolicyDebugString() const;

  bool IsObjectSealed(const ObjectID &object_id) const;

  int64_t GetNumBytesInUse() const;

  int64_t GetNumBytesCreatedTotal() const;

  int64_t GetNumBytesUnsealed() const;

  int64_t GetNumObjectsUnsealed() const;

  void RecordMetrics() const;

  void GetDebugDump(std::stringstream &buffer) const;

 private:
  // Test only
  ObjectLifecycleManager(std::unique_ptr<IObjectStore> store,
                         std::unique_ptr<IEvictionPolicy> eviction_policy,
                         ray::DeleteObjectCallback delete_object_callback);

  const LocalObject *CreateObjectInternal(const ray::ObjectInfo &object_info,
                                          plasma::flatbuf::ObjectSource source,
                                          bool allow_fallback_allocation);

  // Evict objects returned by the eviction policy.
  //
  // \param object_ids Object IDs of the objects to be evicted.
  void EvictObjects(const std::vector<ObjectID> &object_ids);

  void DeleteObjectInternal(const ObjectID &object_id);

 private:
  friend struct ObjectLifecycleManagerTest;
  friend struct ObjectStatsCollectorTest;
  FRIEND_TEST(ObjectLifecycleManagerTest, DeleteFailure);
  FRIEND_TEST(ObjectLifecycleManagerTest, RemoveReferenceOneRefEagerlyDeletion);
  friend struct GetRequestQueueTest;
  FRIEND_TEST(GetRequestQueueTest, TestAddRequest);

  std::unique_ptr<IObjectStore> object_store_;
  std::unique_ptr<IEvictionPolicy> eviction_policy_;
  const ray::DeleteObjectCallback delete_object_callback_;

  // list of objects which will be removed immediately
  // once reference count becomes 0.
  absl::flat_hash_set<ObjectID> earger_deletion_objects_;

  ObjectStatsCollector stats_collector_;
};

}  // namespace plasma
