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

#include <memory>

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

 private:
  friend struct ObjectStatsCollectorTest;

  LocalObject *GetMutableObject(const ObjectID &object_id);

  /// Allocator that allocates memory.
  IAllocator &allocator_;

  /// Mapping from ObjectIDs to information about the object.
  absl::flat_hash_map<ObjectID, std::unique_ptr<LocalObject>> object_table_;
};
}  // namespace plasma
