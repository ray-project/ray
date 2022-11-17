// Copyright 2017 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

/// The definitions of tag keys that you can use every where.
/// You can follow these examples to define and register your tag keys.

using TagKeyType = opencensus::tags::TagKey;
using TagsType = std::vector<std::pair<opencensus::tags::TagKey, std::string>>;

extern const TagKeyType ComponentKey;

extern const TagKeyType JobNameKey;

extern const TagKeyType CustomKey;

extern const TagKeyType NodeAddressKey;

extern const TagKeyType VersionKey;

extern const TagKeyType LanguageKey;

extern const TagKeyType WorkerPidKey;

extern const TagKeyType DriverPidKey;

extern const TagKeyType ResourceNameKey;

extern const TagKeyType ActorIdKey;

extern const TagKeyType WorkerIdKey;

extern const TagKeyType JobIdKey;

extern const TagKeyType SessionNameKey;

extern const TagKeyType NameKey;

// Object store memory location tag constants
extern const TagKeyType LocationKey;
constexpr char kObjectLocMmapShm[] = "MMAP_SHM";
constexpr char kObjectLocMmapDisk[] = "MMAP_DISK";
constexpr char kObjectLocSpilled[] = "SPILLED";
constexpr char kObjectLocWorkerHeap[] = "WORKER_HEAP";

// Object store memory sealed/unsealed tag
extern const TagKeyType ObjectStateKey;
constexpr char kObjectSealed[] = "SEALED";
constexpr char kObjectUnsealed[] = "UNSEALED";
