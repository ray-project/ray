// Copyright 2025 The Ray Authors.
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

#include <string>
#include <string_view>
#include <unordered_map>

#include "opencensus/tags/tag_key.h"

namespace ray {

// TODO(can-anyscale): Use stats namespace for backward compatibility. We will remove
// these types soon when opencensus is removed, and then we can remove this namespace.
namespace stats {

using TagKeyType = opencensus::tags::TagKey;
using TagsType = std::vector<std::pair<TagKeyType, std::string>>;

}  // namespace stats

namespace observability {

class MetricInterface {
 public:
  virtual ~MetricInterface() = default;

  virtual void Record(double value) = 0;
  virtual void Record(double value, stats::TagsType tags) = 0;
  virtual void Record(double value,
                      std::vector<std::pair<std::string_view, std::string>> tags) = 0;
};

}  // namespace observability
}  // namespace ray
