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

#include "absl/container/flat_hash_map.h"
#include "ray/observability/metric_interface.h"

namespace ray {
namespace observability {

class FakeMetric : public MetricInterface {
 public:
  FakeMetric() {}
  ~FakeMetric() = default;

  void Record(double value) override { Record(value, stats::TagsType{}); }

  void Record(double value,
              std::vector<std::pair<std::string_view, std::string>> tags) override {
    stats::TagsType tags_pair_vec;
    tags_pair_vec.reserve(tags.size());
    std::for_each(tags.begin(), tags.end(), [&tags_pair_vec](auto &tag) {
      tags_pair_vec.emplace_back(stats::TagKeyType::Register(tag.first),
                                 std::move(tag.second));
    });
    Record(value, std::move(tags_pair_vec));
  }

  void Record(double value, stats::TagsType tags) override = 0;

  const absl::flat_hash_map<absl::flat_hash_map<std::string, std::string>, double>
      &GetTagToValue() const {
    return tag_to_value_;
  }

 protected:
  absl::flat_hash_map<absl::flat_hash_map<std::string, std::string>, double>
      tag_to_value_;
};

class FakeCounter : public FakeMetric {
 public:
  FakeCounter() {}
  ~FakeCounter() = default;

  void Record(double value, stats::TagsType tags) override {
    absl::flat_hash_map<std::string, std::string> tags_map;
    for (const auto &tag : tags) {
      tags_map[tag.first.name()] = tag.second;
    }
    // accumulate the value of the tag set
    tag_to_value_[std::move(tags_map)] += value;
  }
};

class FakeGauge : public FakeMetric {
 public:
  FakeGauge() {}
  ~FakeGauge() = default;

  void Record(double value, stats::TagsType tags) override {
    absl::flat_hash_map<std::string, std::string> tags_map;
    for (const auto &tag : tags) {
      tags_map[tag.first.name()] = tag.second;
    }
    // record the last value of the tag set
    tag_to_value_.emplace(std::move(tags_map), value);
  }
};

}  // namespace observability
}  // namespace ray
