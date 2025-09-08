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

#include "ray/observability/metric_interface.h"

namespace ray {
namespace observability {

class FakeMetric : public MetricInterface {
 public:
  FakeMetric() = default;
  ~FakeMetric() = default;

  void Record(double value) override { Record(value, stats::TagsType{}); }

  void Record(double value, stats::TagsType tags) override {
    absl::flat_hash_map<std::string, std::string> tags_map;
    for (const auto &tag : tags) {
      tags_map[tag.first.name()] = tag.second;
    }
    tag_to_value_.emplace(std::move(tags_map), value);
  }

  void Record(double value,
              const std::unordered_map<std::string, std::string> &tags) override {
    stats::TagsType tags_pair_vec;
    tags_pair_vec.reserve(tags.size());
    std::for_each(tags.begin(), tags.end(), [&tags_pair_vec](auto &tag) {
      return tags_pair_vec.emplace_back(stats::TagKeyType::Register(tag.first),
                                        std::move(tag.second));
    });
    Record(value, std::move(tags_pair_vec));
  }

  void Record(double value,
              const std::unordered_map<std::string_view, std::string> &tags) override {
    stats::TagsType tags_pair_vec;
    tags_pair_vec.reserve(tags.size());
    std::for_each(tags.begin(), tags.end(), [&tags_pair_vec](auto &tag) {
      return tags_pair_vec.emplace_back(stats::TagKeyType::Register(tag.first),
                                        std::move(tag.second));
    });
    Record(value, std::move(tags_pair_vec));
  }

  const absl::flat_hash_map<absl::flat_hash_map<std::string, std::string>, double>
      &GetTagToValue() const {
    return tag_to_value_;
  }

 private:
  absl::flat_hash_map<absl::flat_hash_map<std::string, std::string>, double>
      tag_to_value_;
};

}  // namespace observability
}  // namespace ray
