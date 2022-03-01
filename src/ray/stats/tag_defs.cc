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

const TagKeyType CustomKey = TagKeyType::Register("CustomKey");

const TagKeyType NodeAddressKey = TagKeyType::Register("NodeAddress");

const TagKeyType VersionKey = TagKeyType::Register("Version");

const TagKeyType LanguageKey = TagKeyType::Register("Language");

const TagKeyType WorkerPidKey = TagKeyType::Register("WorkerPid");

const TagKeyType DriverPidKey = TagKeyType::Register("DriverPid");

const TagKeyType ResourceNameKey = TagKeyType::Register("ResourceName");

const TagKeyType ActorIdKey = TagKeyType::Register("ActorId");
}  // namespace stats
}  // namespace ray
