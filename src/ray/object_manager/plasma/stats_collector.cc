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

namespace plasma {

ObjectStatsCollector::ObjectStatsCollector(const IObjectStore &object_store,
                                           const LifecycleMetadataStore &meta_store)
    : object_store_(object_store), meta_store_(meta_store) {}

void ObjectStatsCollector::OnObjectCreated(const ObjectID &id) {
  auto &obj = GetObject(id);
  const auto kObjectSize = obj.GetObjectInfo().GetObjectSize();
  const auto kSource = obj.GetSource();

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

void ObjectStatsCollector::OnObjectSealed(const ObjectID &id) {
  auto &obj = GetObject(id);
  auto &metadata = GetMetadata(id);
  RAY_CHECK(obj.Sealed());
  const auto kObjectSize = obj.GetObjectInfo().GetObjectSize();

  num_objects_unsealed_--;
  num_bytes_unsealed_ -= kObjectSize;

  if (metadata.ref_count == 1) {
    if (obj.GetSource() == plasma::flatbuf::ObjectSource::CreatedByWorker) {
      num_objects_spillable_++;
      num_bytes_spillable_ += kObjectSize;
    }
  }

  // though this won't happen in practice but add it here for completeness.
  if (metadata.ref_count == 0) {
    num_objects_evictable_++;
    num_bytes_evictable_ += kObjectSize;
  }
}

void ObjectStatsCollector::OnObjectDeleting(const ObjectID &id) {
  auto &obj = GetObject(id);
  auto &metadata = GetMetadata(id);
  const auto kObjectSize = obj.GetObjectInfo().GetObjectSize();
  const auto kSource = obj.GetSource();

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

  if (metadata.ref_count > 0) {
    num_objects_in_use_--;
    num_bytes_in_use_ -= kObjectSize;
  }

  if (!obj.Sealed()) {
    num_objects_unsealed_--;
    num_bytes_unsealed_ -= kObjectSize;
    return;
  }

  // obj sealed
  if (metadata.ref_count == 1 &&
      kSource == plasma::flatbuf::ObjectSource::CreatedByWorker) {
    num_objects_spillable_--;
    num_bytes_spillable_ -= kObjectSize;
  }

  if (metadata.ref_count == 0) {
    num_objects_evictable_--;
    num_bytes_evictable_ -= kObjectSize;
  }
}

void ObjectStatsCollector::OnObjectRefIncreased(const ObjectID &id) {
  auto &obj = GetObject(id);
  auto &metadata = GetMetadata(id);
  const auto kObjectSize = obj.GetObjectInfo().GetObjectSize();
  const auto kSource = obj.GetSource();
  const bool kSealed = obj.Sealed();

  // object ref count bump from 0 to 1
  if (metadata.ref_count == 1) {
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
  if (metadata.ref_count == 2 &&
      kSource == plasma::flatbuf::ObjectSource::CreatedByWorker && kSealed) {
    num_objects_spillable_--;
    num_bytes_spillable_ -= kObjectSize;
  }
}

void ObjectStatsCollector::OnObjectRefDecreased(const ObjectID &id) {
  auto &obj = GetObject(id);
  auto &metadata = GetMetadata(id);
  const auto kObjectSize = obj.GetObjectInfo().GetObjectSize();
  const auto kSource = obj.GetSource();
  const bool kSealed = obj.Sealed();

  // object ref count decrease from 2 to 1
  if (metadata.ref_count == 1) {
    if (kSource == plasma::flatbuf::ObjectSource::CreatedByWorker && kSealed) {
      num_objects_spillable_++;
      num_bytes_spillable_ += kObjectSize;
    }
  }

  // object ref count decrease from 1 to 0
  if (metadata.ref_count == 0) {
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

const LocalObject &ObjectStatsCollector::GetObject(const ObjectID &id) const {
  auto entry = object_store_.GetObject(id);
  RAY_CHECK(entry) << id << " doesn't exist in object store";
  return *entry;
}

const LifecycleMetadata &ObjectStatsCollector::GetMetadata(const ObjectID &id) const {
  return meta_store_.GetMetadata(id);
}

}  // namespace plasma
