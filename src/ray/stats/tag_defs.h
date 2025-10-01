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

extern const TagKeyType ComponentKey;

extern const TagKeyType NodeAddressKey;

extern const TagKeyType VersionKey;

extern const TagKeyType LanguageKey;

extern const TagKeyType WorkerIdKey;

extern const TagKeyType SessionNameKey;

extern const TagKeyType NameKey;

extern const TagKeyType SourceKey;

// Object store memory location tag constants
constexpr std::string_view LocationKey = "Location";

// Object store memory sealed/unsealed tag
constexpr std::string_view ObjectStateKey = "ObjectState";

}  // namespace stats
}  // namespace ray
