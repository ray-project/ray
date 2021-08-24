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

#include "ray/object_manager/plasma/plasma.h"

#include "ray/object_manager/plasma/common.h"

namespace plasma {

LocalObject::LocalObject(Allocation allocation)
    : allocation(std::move(allocation)), ref_count(0) {}

void ToPlasmaObject(const LocalObject &entry, PlasmaObject *object, bool check_sealed) {
  RAY_DCHECK(object != nullptr);
  if (check_sealed) {
    RAY_DCHECK(entry.Sealed());
  }
  object->store_fd = entry.GetAllocation().fd;
  object->data_offset = entry.GetAllocation().offset;
  object->metadata_offset =
      entry.GetAllocation().offset + entry.GetObjectInfo().data_size;
  object->data_size = entry.GetObjectInfo().data_size;
  object->metadata_size = entry.GetObjectInfo().metadata_size;
  object->device_num = entry.GetAllocation().device_num;
  object->mmap_size = entry.GetAllocation().mmap_size;
}
}  // namespace plasma
