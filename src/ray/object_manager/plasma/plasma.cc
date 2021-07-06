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

LocalObject::LocalObject() : ref_count(0) {}

Allocation::Allocation()
    : address(nullptr),
      size(0),
      fd{INVALID_FD, INVALID_UNIQUE_FD_ID},
      offset(0),
      device_num(0),
      mmap_size(0) {}

PlasmaStoreConfig::PlasmaStoreConfig(bool hugepages_enabled, const std::string &directory,
                                     const std::string &fallback_directory)
    : hugepages_enabled(hugepages_enabled),
      directory(directory),
      fallback_directory(fallback_directory) {}

}  // namespace plasma
