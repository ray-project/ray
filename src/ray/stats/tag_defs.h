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

static const TagKeyType ComponentKey = TagKeyType::Register("Component");

static const TagKeyType JobNameKey = TagKeyType::Register("JobName");

static const TagKeyType CustomKey = TagKeyType::Register("CustomKey");

static const TagKeyType NodeAddressKey = TagKeyType::Register("NodeAddress");

static const TagKeyType VersionKey = TagKeyType::Register("Version");

static const TagKeyType LanguageKey = TagKeyType::Register("Language");

static const TagKeyType WorkerPidKey = TagKeyType::Register("WorkerPid");

static const TagKeyType DriverPidKey = TagKeyType::Register("DriverPid");

static const TagKeyType ResourceNameKey = TagKeyType::Register("ResourceName");

static const TagKeyType ValueTypeKey = TagKeyType::Register("ValueType");

static const TagKeyType ActorIdKey = TagKeyType::Register("ActorId");
