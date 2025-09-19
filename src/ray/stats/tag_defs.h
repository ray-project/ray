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

#include "ray/observability/metric_interface.h"

/// The definitions of tag keys that you can use every where.
/// You can follow these examples to define and register your tag keys.

namespace ray {
namespace stats {

inline const TagKeyType ComponentKey = TagKeyType::Register("Component");

inline const TagKeyType NodeAddressKey = TagKeyType::Register("NodeAddress");

inline const TagKeyType VersionKey = TagKeyType::Register("Version");

inline const TagKeyType LanguageKey = TagKeyType::Register("Language");

// Keep in sync with the WORKER_ID_TAG_KEY in
// python/ray/_private/telemetry/metric_cardinality.py
inline const TagKeyType WorkerIdKey = TagKeyType::Register("WorkerId");

inline const TagKeyType SessionNameKey = TagKeyType::Register("SessionName");

inline const TagKeyType NameKey = TagKeyType::Register("Name");

// Object store memory location tag constants
inline const TagKeyType LocationKey = TagKeyType::Register("Location");

// Object store memory sealed/unsealed tag
inline const TagKeyType ObjectStateKey = TagKeyType::Register("ObjectState");

inline const TagKeyType SourceKey = TagKeyType::Register("Source");

}  // namespace stats
}  // namespace ray
