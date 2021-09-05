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
#include "ray/object_manager/plasma/lifecycle_meta_store.h"
#include "ray/object_manager/plasma/object_store.h"

namespace plasma {

// IStatsCollector subscribes to plasma store state changes
// and calculate the store statistics.
class IStatsCollector {
 public:
  virtual ~IStatsCollector() = default;

  // Called after a new object is created.
  virtual void OnObjectCreated(const ObjectID &id) = 0;

  // Called after an object is sealed.
  virtual void OnObjectSealed(const ObjectID &id) = 0;

  // Called BEFORE an object is deleted.
  virtual void OnObjectDeleting(const ObjectID &id) = 0;

  // Called after an object's ref count is bumped by 1.
  virtual void OnObjectRefIncreased(const ObjectID &id) = 0;

  // Called after an object's ref count is decreased by 1.
  virtual void OnObjectRefDecreased(const ObjectID &id) = 0;

  // Debug dump the stats.
  virtual void GetDebugDump(std::stringstream &buffer) const = 0;

  virtual int64_t GetNumBytesInUse() const = 0;

  virtual int64_t GetNumBytesCreatedTotal() const = 0;

  virtual int64_t GetNumBytesUnsealed() const = 0;

  virtual int64_t GetNumObjectsUnsealed() const = 0;
};

class ObjectStatsCollector : public IStatsCollector {
 public:
  ObjectStatsCollector(const IObjectStore &object_store,
                       const LifecycleMetadataStore &meta_store);

  void OnObjectCreated(const ObjectID &id) override;

  void OnObjectSealed(const ObjectID &id) override;

  void OnObjectDeleting(const ObjectID &id) override;

  void OnObjectRefIncreased(const ObjectID &id) override;

  void OnObjectRefDecreased(const ObjectID &id) override;

  void GetDebugDump(std::stringstream &buffer) const override;

  int64_t GetNumBytesInUse() const override;

  int64_t GetNumBytesCreatedTotal() const override;

  int64_t GetNumBytesUnsealed() const override;

  int64_t GetNumObjectsUnsealed() const override;

 private:
  const LocalObject &GetObject(const ObjectID &id) const;
  const LifecycleMetadata &GetMetadata(const ObjectID &id) const;

 private:
  friend struct ObjectStatsCollectorTest;

  const IObjectStore &object_store_;
  const LifecycleMetadataStore &meta_store_;

  int64_t num_objects_spillable_ = 0;
  int64_t num_bytes_spillable_ = 0;
  int64_t num_objects_unsealed_ = 0;
  int64_t num_bytes_unsealed_ = 0;
  int64_t num_objects_in_use_ = 0;
  int64_t num_bytes_in_use_ = 0;
  int64_t num_objects_evictable_ = 0;
  int64_t num_bytes_evictable_ = 0;

  int64_t num_objects_created_by_worker_ = 0;
  int64_t num_bytes_created_by_worker_ = 0;
  int64_t num_objects_restored_ = 0;
  int64_t num_bytes_restored_ = 0;
  int64_t num_objects_received_ = 0;
  int64_t num_bytes_received_ = 0;
  int64_t num_objects_errored_ = 0;
  int64_t num_bytes_errored_ = 0;
  int64_t num_bytes_created_total_ = 0;
};

}  // namespace plasma