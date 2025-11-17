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

#include "ray/stats/tag_defs.h"

namespace ray {
namespace stats {
const TagKeyType ComponentKey = TagKeyType::Register("Component");

const TagKeyType NodeAddressKey = TagKeyType::Register("NodeAddress");

const TagKeyType VersionKey = TagKeyType::Register("Version");

const TagKeyType LanguageKey = TagKeyType::Register("Language");

// Keep in sync with the WORKER_ID_TAG_KEY in
// python/ray/_private/telemetry/metric_cardinality.py
const TagKeyType WorkerIdKey = TagKeyType::Register("WorkerId");

const TagKeyType SessionNameKey = TagKeyType::Register("SessionName");

const TagKeyType NameKey = TagKeyType::Register("Name");

const TagKeyType SourceKey = TagKeyType::Register("Source");
}  // namespace stats
}  // namespace ray
