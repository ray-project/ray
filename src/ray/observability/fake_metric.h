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

  void Record(double value) override {}
  void Record(double value, stats::TagsType tags) override {}
  void Record(double value,
              const std::unordered_map<std::string, std::string> &tags) override {}
  void Record(double value,
              const std::unordered_map<std::string_view, std::string> &tags) override {}
};

}  // namespace observability
}  // namespace ray
