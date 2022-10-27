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

#include "ray/object_manager/plasma/stats_collector.h"

#include "ray/stats/metric_defs.h"

namespace plasma {

void ObjectStatsCollector::OnObjectCreated(const LocalObject &obj) {
  const auto kObjectSize = obj.GetObjectInfo().GetObjectSize();
  const auto kSource = obj.GetSource();
  const auto &kAllocation = obj.GetAllocation();

  if (kAllocation.fallback_allocated) {
    bytes_by_loc_seal_.Increment({/*fallback_allocated*/ true, /*sealed*/ false},
                                 kObjectSize);
  } else {
    bytes_by_loc_seal_.Increment({/*fallback_allocated*/ false, /*sealed*/ false},
                                 kObjectSize);
  }

  num_bytes_created_total_ += kObjectSize;

  if (kSource == plasma::flatbuf::ObjectSource::CreatedByWorker) {
    num_objects_created_by_worker_++;
    num_bytes_created_by_worker_ += kObjectSize;
  } else if (kSource == plasma::flatbuf::ObjectSource::RestoredFromStorage) {
    num_objects_restored_++;
    num_bytes_restored_ += kObjectSize;
  } else if (kSource == plasma::flatbuf::ObjectSource::ReceivedFromRemoteRaylet) {
    num_objects_received_++;
    num_bytes_received_ += kObjectSize;
  } else if (kSource == plasma::flatbuf::ObjectSource::ErrorStoredByRaylet) {
    num_objects_errored_++;
    num_bytes_errored_ += kObjectSize;
  }

  RAY_CHECK(!obj.Sealed());
  num_objects_unsealed_++;
  num_bytes_unsealed_ += kObjectSize;
}

void ObjectStatsCollector::OnObjectSealed(const LocalObject &obj) {
  RAY_CHECK(obj.Sealed());
  const auto kObjectSize = obj.GetObjectInfo().GetObjectSize();

  const auto &kAllocation = obj.GetAllocation();
  if (kAllocation.fallback_allocated) {
    bytes_by_loc_seal_.Swap({/*fallback_allocated*/ true, /* sealed */ false},
                            {/*fallback_allocated*/ true, /* sealed */ true},
                            kObjectSize);
  } else {
    bytes_by_loc_seal_.Swap({/*fallback_allocated*/ false, /* sealed */ false},
                            {/*fallback_allocated*/ false, /* sealed */ true},
                            kObjectSize);
  }

  num_objects_unsealed_--;
  num_bytes_unsealed_ -= kObjectSize;

  if (obj.GetRefCount() == 1) {
    if (obj.GetSource() == plasma::flatbuf::ObjectSource::CreatedByWorker) {
      num_objects_spillable_++;
      num_bytes_spillable_ += kObjectSize;
    }
  }

  // though this won't happen in practice but add it here for completeness.
  if (obj.GetRefCount() == 0) {
    num_objects_evictable_++;
    num_bytes_evictable_ += kObjectSize;
  }
}

void ObjectStatsCollector::OnObjectDeleting(const LocalObject &obj) {
  const auto kObjectSize = obj.GetObjectInfo().GetObjectSize();
  const auto kSource = obj.GetSource();
  const auto &kAllocation = obj.GetAllocation();

  auto counter_type = std::make_pair(false, false);
  if (kAllocation.fallback_allocated) {
    counter_type = obj.Sealed()
                       ? std::make_pair(/* fallback_allocated */ true, /* sealed*/ true)
                       : std::make_pair(/* fallback_allocated */ true, /* sealed*/ false);
  } else {
    counter_type =
        obj.Sealed() ? std::make_pair(/* fallback_allocated */ false, /* sealed*/ true)
                     : std::make_pair(/* fallback_allocated */ false, /* sealed*/ false);
  }

  bytes_by_loc_seal_.Decrement(counter_type, kObjectSize);

  if (kSource == plasma::flatbuf::ObjectSource::CreatedByWorker) {
    num_objects_created_by_worker_--;
    num_bytes_created_by_worker_ -= kObjectSize;
  } else if (kSource == plasma::flatbuf::ObjectSource::RestoredFromStorage) {
    num_objects_restored_--;
    num_bytes_restored_ -= kObjectSize;
  } else if (kSource == plasma::flatbuf::ObjectSource::ReceivedFromRemoteRaylet) {
    num_objects_received_--;
    num_bytes_received_ -= kObjectSize;
  } else if (kSource == plasma::flatbuf::ObjectSource::ErrorStoredByRaylet) {
    num_objects_errored_--;
    num_bytes_errored_ -= kObjectSize;
  }

  if (obj.GetRefCount() > 0) {
    num_objects_in_use_--;
    num_bytes_in_use_ -= kObjectSize;
  }

  if (!obj.Sealed()) {
    num_objects_unsealed_--;
    num_bytes_unsealed_ -= kObjectSize;
    return;
  }

  // obj sealed
  if (obj.GetRefCount() == 1 &&
      kSource == plasma::flatbuf::ObjectSource::CreatedByWorker) {
    num_objects_spillable_--;
    num_bytes_spillable_ -= kObjectSize;
  }

  if (obj.GetRefCount() == 0) {
    num_objects_evictable_--;
    num_bytes_evictable_ -= kObjectSize;
  }
}

void ObjectStatsCollector::OnObjectRefIncreased(const LocalObject &obj) {
  const auto kObjectSize = obj.GetObjectInfo().GetObjectSize();
  const auto kSource = obj.GetSource();
  const bool kSealed = obj.Sealed();

  // object ref count bump from 0 to 1
  if (obj.GetRefCount() == 1) {
    num_objects_in_use_++;
    num_bytes_in_use_ += kObjectSize;

    if (kSource == plasma::flatbuf::ObjectSource::CreatedByWorker && kSealed) {
      num_objects_spillable_++;
      num_bytes_spillable_ += kObjectSize;
    }

    if (kSealed) {
      num_objects_evictable_--;
      num_bytes_evictable_ -= kObjectSize;
    }
  }

  // object ref count bump from 1 to 2
  if (obj.GetRefCount() == 2 &&
      kSource == plasma::flatbuf::ObjectSource::CreatedByWorker && kSealed) {
    num_objects_spillable_--;
    num_bytes_spillable_ -= kObjectSize;
  }
}

void ObjectStatsCollector::OnObjectRefDecreased(const LocalObject &obj) {
  const auto kObjectSize = obj.GetObjectInfo().GetObjectSize();
  const auto kSource = obj.GetSource();
  const bool kSealed = obj.Sealed();

  // object ref count decrease from 2 to 1
  if (obj.GetRefCount() == 1) {
    if (kSource == plasma::flatbuf::ObjectSource::CreatedByWorker && kSealed) {
      num_objects_spillable_++;
      num_bytes_spillable_ += kObjectSize;
    }
  }

  // object ref count decrease from 1 to 0
  if (obj.GetRefCount() == 0) {
    num_objects_in_use_--;
    num_bytes_in_use_ -= kObjectSize;

    if (kSource == plasma::flatbuf::ObjectSource::CreatedByWorker && kSealed) {
      num_objects_spillable_--;
      num_bytes_spillable_ -= kObjectSize;
    }

    if (kSealed) {
      num_objects_evictable_++;
      num_bytes_evictable_ += kObjectSize;
    }
  }
}

int64_t ObjectStatsCollector::GetNumBytesCreatedCurrent() const {
  return num_bytes_created_by_worker_ + num_bytes_restored_ + num_bytes_received_ +
         num_bytes_errored_;
}

void ObjectStatsCollector::RecordMetrics() const {
  // Shared memory sealed
  ray::stats::STATS_object_store_memory.Record(
      bytes_by_loc_seal_.Get({/* fallback_allocated */ false, /* sealed */ true}),
      {{ray::stats::LocationKey.name(), ray::stats::kObjectLocMmapShm},
       {ray::stats::ObjectStateKey.name(), ray::stats::kObjectSealed}});

  // Shared memory unsealed
  ray::stats::STATS_object_store_memory.Record(
      bytes_by_loc_seal_.Get({/* fallback_allocated */ false, /* sealed */ false}),
      {{ray::stats::LocationKey.name(), ray::stats::kObjectLocMmapShm},
       {ray::stats::ObjectStateKey.name(), ray::stats::kObjectUnsealed}});

  // Fallback memory sealed
  ray::stats::STATS_object_store_memory.Record(
      bytes_by_loc_seal_.Get({/* fallback_allocated */ true, /* sealed */ true}),
      {{ray::stats::LocationKey.name(), ray::stats::kObjectLocMmapDisk},
       {ray::stats::ObjectStateKey.name(), ray::stats::kObjectSealed}});

  // Fallback memory unsealed
  ray::stats::STATS_object_store_memory.Record(
      bytes_by_loc_seal_.Get({/* fallback_allocated */ true, /* sealed */ false}),
      {{ray::stats::LocationKey.name(), ray::stats::kObjectLocMmapDisk},
       {ray::stats::ObjectStateKey.name(), ray::stats::kObjectUnsealed}});
}

void ObjectStatsCollector::GetDebugDump(std::stringstream &buffer) const {
  buffer << "- objects spillable: " << num_objects_spillable_ << "\n";
  buffer << "- bytes spillable: " << num_bytes_spillable_ << "\n";
  buffer << "- objects unsealed: " << num_objects_unsealed_ << "\n";
  buffer << "- bytes unsealed: " << num_bytes_unsealed_ << "\n";
  buffer << "- objects in use: " << num_objects_in_use_ << "\n";
  buffer << "- bytes in use: " << num_bytes_in_use_ << "\n";
  buffer << "- objects evictable: " << num_objects_evictable_ << "\n";
  buffer << "- bytes evictable: " << num_bytes_evictable_ << "\n";
  buffer << "\n";

  buffer << "- objects created by worker: " << num_objects_created_by_worker_ << "\n";
  buffer << "- bytes created by worker: " << num_bytes_created_by_worker_ << "\n";
  buffer << "- objects restored: " << num_objects_restored_ << "\n";
  buffer << "- bytes restored: " << num_bytes_restored_ << "\n";
  buffer << "- objects received: " << num_objects_received_ << "\n";
  buffer << "- bytes received: " << num_bytes_received_ << "\n";
  buffer << "- objects errored: " << num_objects_errored_ << "\n";
  buffer << "- bytes errored: " << num_bytes_errored_ << "\n";
}

int64_t ObjectStatsCollector::GetNumBytesInUse() const { return num_bytes_in_use_; }

int64_t ObjectStatsCollector::GetNumBytesCreatedTotal() const {
  return num_bytes_created_total_;
}

int64_t ObjectStatsCollector::GetNumBytesUnsealed() const { return num_bytes_unsealed_; }

int64_t ObjectStatsCollector::GetNumObjectsUnsealed() const {
  return num_objects_unsealed_;
}

}  // namespace plasma