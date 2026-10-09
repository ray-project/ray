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

#include "ray/api/metric.h"

#include "ray/stats/metric.h"
#include "ray/util/logging.h"

namespace ray {

Metric::~Metric() {
  if (metric_ != nullptr) {
    stats::Metric *metric = reinterpret_cast<stats::Metric *>(metric_);
    delete metric;
    metric_ = nullptr;
  }
}

std::string Metric::GetName() const {
  RAY_CHECK(metric_ != nullptr) << "The metric_ must not be nullptr.";
  stats::Metric *metric = reinterpret_cast<stats::Metric *>(metric_);
  return metric->GetName();
}

void Metric::Record(double value,
                    const std::unordered_map<std::string, std::string> &tags) {
  RAY_CHECK(metric_ != nullptr) << "The metric_ must not be nullptr.";
  stats::Metric *metric = reinterpret_cast<stats::Metric *>(metric_);
  std::vector<std::pair<std::string_view, std::string>> tags_pair_vec;
  tags_pair_vec.reserve(tags.size());
  for (const auto &tag : tags) {
    tags_pair_vec.emplace_back(std::string_view(tag.first), tag.second);
  }
  metric->Record(value, std::move(tags_pair_vec));
}

Gauge::Gauge(const std::string &name,
             const std::string &description,
             const std::string &unit,
             const std::vector<std::string> &tag_str_keys) {
  metric_ = new stats::Gauge(name, description, unit, tag_str_keys);
}

void Gauge::Set(double value, const std::unordered_map<std::string, std::string> &tags) {
  Record(value, tags);
}

Histogram::Histogram(const std::string &name,
                     const std::string &description,
                     const std::string &unit,
                     const std::vector<double> boundaries,
                     const std::vector<std::string> &tag_str_keys) {
  metric_ = new stats::Histogram(name, description, unit, boundaries, tag_str_keys);
}

void Histogram::Observe(double value,
                        const std::unordered_map<std::string, std::string> &tags) {
  Record(value, tags);
}

Counter::Counter(const std::string &name,
                 const std::string &description,
                 const std::string &unit,
                 const std::vector<std::string> &tag_str_keys) {
  metric_ = new stats::Count(name, description, unit, tag_str_keys);
}

void Counter::Inc(double value,
                  const std::unordered_map<std::string, std::string> &tags) {
  Record(value, tags);
}

Sum::Sum(const std::string &name,
         const std::string &description,
         const std::string &unit,
         const std::vector<std::string> &tag_str_keys) {
  metric_ = new stats::Sum(name, description, unit, tag_str_keys);
}

}  // namespace ray
