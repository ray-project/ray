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

#include "ray/object_manager/plasma/common.h"
#include "ray/object_manager/plasma/plasma.h"

namespace plasma {

class ObjectStore {
 public:
  ObjectStore();

  LocalObject *CreateObject(Allocation allocation_info,
                            const ray::ObjectInfo &object_info,
                            plasma::flatbuf::ObjectSource source);

  const LocalObject *GetObject(const ObjectID &object_id) const;

  /// Check if the plasma store contains an object:
  ///
  /// \param object_id Object ID that will be checked.
  /// \return OBJECT_FOUND if the object is in the store, OBJECT_NOT_FOUND if
  /// not
  ObjectStatus ContainsObject(const ObjectID &object_id);

  const LocalObject *SealObject(const ObjectID &object_id);

  void DeleteObject(const ObjectID &object_id);

  size_t GetNumBytesCreatedTotal() const;

  size_t GetNumBytesUnsealed() const;

  size_t GetNumObjectsUnsealed() const;

  int64_t GetNumBytesAllocated() const;

  int64_t GetNumBytesCapacity() const;

  void GetDebugDump(std::stringstream &buffer) const;

 private:
  LocalObject *GetObjectInternal(const ObjectID &object_id);

  std::unordered_map<ObjectID, std::unique_ptr<LocalObject>> object_table_;
  /// Total number of bytes allocated to objects that are created but not yet
  /// sealed.
  size_t num_bytes_unsealed_ = 0;

  /// Number of objects that are created but not sealed.
  size_t num_objects_unsealed_ = 0;

  /// A running total of the objects that have ever been created on this node.
  size_t num_bytes_created_total_ = 0;
};

}  // namespace plasma
