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

#include "ray/stats/metric.h"

namespace ray {
namespace stats {
const TagKeyType ComponentKey = TagKeyType::Register("Component");

const TagKeyType JobNameKey = TagKeyType::Register("JobName");

const TagKeyType NodeAddressKey = TagKeyType::Register("NodeAddress");

const TagKeyType VersionKey = TagKeyType::Register("Version");

const TagKeyType LanguageKey = TagKeyType::Register("Language");

const TagKeyType WorkerPidKey = TagKeyType::Register("WorkerPid");

const TagKeyType DriverPidKey = TagKeyType::Register("DriverPid");

const TagKeyType ActorIdKey = TagKeyType::Register("ActorId");

const TagKeyType WorkerIdKey = TagKeyType::Register("WorkerId");

const TagKeyType JobIdKey = TagKeyType::Register("JobId");

const TagKeyType SessionNameKey = TagKeyType::Register("SessionName");

const TagKeyType NameKey = TagKeyType::Register("Name");

const TagKeyType LocationKey = TagKeyType::Register("Location");

const TagKeyType ObjectStateKey = TagKeyType::Register("ObjectState");

const TagKeyType SourceKey = TagKeyType::Register("Source");
}  // namespace stats
}  // namespace ray
