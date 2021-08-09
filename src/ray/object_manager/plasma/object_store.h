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

// ObjectStore stores objects with unique object id. It uses IAllocator
// to allocate memory for object creation.
// ObjectStore is not thread safe.
class ObjectStore {
 public:
  explicit ObjectStore(IAllocator &allocator);

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
  const LocalObject *CreateObject(const ray::ObjectInfo &object_info,
                                  plasma::flatbuf::ObjectSource source,
                                  bool fallback_allocate);

  /// Get object by id.
  ///
  /// \param object_id Object ID of the object to be sealed.
  /// \return
  ///   - nullptr if such object doesn't exist.
  ///   - otherwise, pointer to the object.
  const LocalObject *GetObject(const ObjectID &object_id) const;

  /// Seal created object by id.
  ///
  /// \param object_id Object ID of the object to be sealed.
  /// \return
  ///   - nulltpr if such object doesn't exist, or the object has already been sealed.
  ///   - otherise, pointer to the sealed object..
  const LocalObject *SealObject(const ObjectID &object_id);

  /// Delete an existing object.
  ///
  /// \param object_id Object ID of the object to be sealed.
  /// \return
  ///   - false if such object doesn't exist.
  ///   - true if abort successfuly.
  bool DeleteObject(const ObjectID &object_id);

  int64_t GetNumBytesCreatedTotal() const;

  int64_t GetNumBytesUnsealed() const;

  int64_t GetNumObjectsUnsealed() const;

  void GetDebugDump(std::stringstream &buffer) const;

 private:
  LocalObject *GetMutableObject(const ObjectID &object_id);

  /// Allocator that allocates memory.
  IAllocator &allocator_;

  /// Mapping from ObjectIDs to information about the object.
  absl::flat_hash_map<ObjectID, std::unique_ptr<LocalObject>> object_table_;

  /// Total number of bytes allocated to objects that are created but not yet
  /// sealed.
  int64_t num_bytes_unsealed_ = 0;

  /// Number of objects that are created but not sealed.
  int64_t num_objects_unsealed_ = 0;

  /// A running total of the objects that have ever been created on this node.
  int64_t num_bytes_created_total_ = 0;
};
}  // namespace plasma
