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

#include <utility>  // std::pair

#include "ray/object_manager/plasma/common.h"
#include "ray/util/counter_map.h"  // CounterMap

namespace plasma {

// ObjectStatsCollector subscribes to plasma store state changes
// and calculate the store statistics.
//
// TODO(scv119): move other stats from PlasmaStore/ObjectStore/
// ObjectLifeCycleManager into this class.
class ObjectStatsCollector {
 public:
  virtual ~ObjectStatsCollector() = default;

  // Called after a new object is created.
  // Marked virtual for test mocking
  virtual void OnObjectCreated(const LocalObject &object);

  // Called after an object is sealed.
  // Marked virtual for test mocking
  virtual void OnObjectSealed(const LocalObject &object);

  // Called BEFORE an object is deleted.
  // Marked virtual for test mocking
  virtual void OnObjectDeleting(const LocalObject &object);

  // Called after an object's ref count is bumped by 1.
  void OnObjectRefIncreased(const LocalObject &object);

  // Called after an object's ref count is decreased by 1.
  void OnObjectRefDecreased(const LocalObject &object);

  /// Record the internal metrics.
  void RecordMetrics() const;

  /// Debug dump the stats.
  void GetDebugDump(std::stringstream &buffer) const;

  int64_t GetNumBytesInUse() const;

  int64_t GetNumBytesCreatedTotal() const;

  int64_t GetNumBytesUnsealed() const;

  int64_t GetNumObjectsUnsealed() const;

 private:
  friend struct ObjectStatsCollectorTest;

  int64_t GetNumBytesCreatedCurrent() const;

  CounterMap<std::pair</* fallback_allocated*/ bool, /*sealed*/ bool>> bytes_by_loc_seal_;
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