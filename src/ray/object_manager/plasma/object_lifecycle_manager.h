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
#include "absl/container/flat_hash_set.h"
#include "absl/types/optional.h"
#include "gtest/gtest.h"
#include "ray/object_manager/plasma/common.h"
#include "ray/object_manager/plasma/eviction_policy.h"
#include "ray/object_manager/plasma/object_store.h"
#include "ray/object_manager/plasma/plasma_allocator.h"
#include "ray/object_manager/plasma/spill_manager.h"
#include "ray/object_manager/plasma/stats_collector.h"

namespace plasma {

// ObjectLifecycleManager allocates LocalObjects from the allocator.
// It tracks object’s lifecycle states such as reference count or object states
// (created/sealed). It lazily garbage collects objects when running out of space.
class ObjectLifecycleManager {
 public:
  ObjectLifecycleManager(IAllocator &allocator,
                         ray::DeleteObjectCallback delete_object_callback);

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
  std::pair<const LocalObject *, flatbuf::PlasmaError> CreateObject(
      const ray::ObjectInfo &object_info, plasma::flatbuf::ObjectSource source,
      bool fallback_allocator);

  /// Get object by id.
  /// \return
  ///   - nullptr if such object doesn't exist.
  ///   - otherwise, pointer to the object.
  const LocalObject *GetObject(const ObjectID &object_id) const;

  /// Seal created object by id.
  ///
  /// \param object_id Object ID of the object to be sealed.
  /// \return
  ///   - nulltpr if such object doesn't exist, or the object has already been sealed.
  ///   - otherise, pointer to the sealed object.
  const LocalObject *SealObject(const ObjectID &object_id);

  /// Abort object creation by id. It deletes the object regardless of reference
  /// counting.
  ///
  /// \param object_id Object ID of the object to be aborted.
  /// \return One of the following error codes:
  ///  - PlasmaError::OK, if the object was aborted successfully.
  ///  - PlasmaError::ObjectNonexistent, if ths object doesn't exist.
  ///  - PlasmaError::ObjectSealed, if ths object has already been sealed.
  flatbuf::PlasmaError AbortObject(const ObjectID &object_id);

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
  flatbuf::PlasmaError DeleteObject(const ObjectID &object_id);

  /// Bump up the reference count of the object.
  ///
  /// \return true if object exists, false otherise.
  bool AddReference(const ObjectID &object_id);

  /// Decrese the reference count of the object. When reference count
  /// drop to zero the object becomes evictable.
  ///
  /// \return true if object exists and reference count is greater than 0, false otherise.
  bool RemoveReference(const ObjectID &object_id);

  /// Ask it to evict objects until we have at least size of capacity
  /// available.
  /// TEST ONLY
  ///
  /// \return The number of bytes evicted.
  int64_t RequireSpace(int64_t size);

  /// Mark this object as the primary copy.
  ///
  /// Only primary object can be spilled. It's spillable once
  /// its reference count becomes 1.
  /// TODO:(scv119) this should be 0 once we deprecate
  /// pinning logic from local object manager.
  ///
  /// Primary object can't be evicted unless it's spilled.
  bool SetObjectAsPrimaryCopy(const ObjectID &object_id);

  /// Spill objects as much as possible as fast as possible up to
  /// the max throughput.
  ///
  /// TODO:(scv119) the current implementation matches the spilling behavior of
  /// local object manager. In the future we can have precise control on how
  /// much object to spill.
  bool SpillObjectUptoMaxThroughput();

  bool IsSpillingInProgress() const;

  absl::optional<std::string> GetLocalSpilledObjectURL(const ObjectID &object_id) const;

  std::string EvictionPolicyDebugString() const;

  bool IsObjectSealed(const ObjectID &object_id) const;

  int64_t GetNumBytesInUse() const;

  int64_t GetNumBytesCreatedTotal() const;

  int64_t GetNumBytesUnsealed() const;

  int64_t GetNumObjectsUnsealed() const;

  void GetDebugDump(std::stringstream &buffer) const;

 private:
  // Test only
  ObjectLifecycleManager(std::unique_ptr<IObjectStore> store,
                         std::unique_ptr<IEvictionPolicy> eviction_policy,
                         std::unique_ptr<ISpillManager> spill_manager,
                         ray::DeleteObjectCallback delete_object_callback);

  const LocalObject *CreateObjectInternal(const ray::ObjectInfo &object_info,
                                          plasma::flatbuf::ObjectSource source,
                                          bool allow_fallback_allocation);

  // Evict objects returned by the eviction policy.
  //
  // \param object_ids Object IDs of the objects to be evicted.
  void EvictObjects(const std::vector<ObjectID> &object_ids);

  void DeleteObjectInternal(const ObjectID &object_id);

  std::vector<const LocalObject &> FindObjectsForSpilling(int64_t num_bytes_to_spill);

  void OnSpillTaskFinished(ray::Status status,
                           absl::flat_hash_map<ObjectID, std::string> result);

  bool IsObjectSpillable(const ObjectID &object_id) const :

      private : friend struct ObjectLifecycleManagerTest;
  friend struct ObjectStatsCollectorTest;
  FRIEND_TEST(ObjectLifecycleManagerTest, DeleteFailure);
  FRIEND_TEST(ObjectLifecycleManagerTest, RemoveReferenceOneRefEagerlyDeletion);

  const int64_t kMinSpillingSize;
  std::unique_ptr<IObjectStore> object_store_;
  std::unique_ptr<IEvictionPolicy> eviction_policy_;
  std::unique_ptr<ISpillManager> spill_manager_;
  const ray::DeleteObjectCallback delete_object_callback_;

  // list of objects which will be removed immediately
  // once reference count becomes 0.
  absl::flat_hash_set<ObjectID> earger_deletion_objects_;

  // primary/spilled object states.
  absl::flat_hash_set<ObjectID> primary_objects_;
  absl::flat_hash_set<ObjectID> spilling_objects_;
  absl::flat_hash_map<ObjectID, std::string> spilled_objects_;

  // Total bytes of the objects whose references are greater than 0.
  int64_t num_bytes_in_use_;

  ObjectStatsCollector stats_collector_;
};

}  // namespace plasma
