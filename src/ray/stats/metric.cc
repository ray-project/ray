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

#include "opencensus/stats/internal/aggregation_window.h"
#include "opencensus/stats/internal/set_aggregation_window.h"

namespace ray {

namespace stats {

static void RegisterAsView(opencensus::stats::ViewDescriptor view_descriptor,
                           const std::vector<opencensus::tags::TagKey> &keys) {
  // Register global keys.
  for (const auto &tag : ray::stats::StatsConfig::instance().GetGlobalTags()) {
    view_descriptor = view_descriptor.add_column(tag.first);
  }

  // Register custom keys.
  for (const auto &key : keys) {
    view_descriptor = view_descriptor.add_column(key);
  }
  opencensus::stats::View view(view_descriptor);
  view_descriptor.RegisterForExport();
}

///
/// Stats Config
///

StatsConfig &StatsConfig::instance() {
  static StatsConfig instance;
  return instance;
}

void StatsConfig::SetGlobalTags(const TagsType &global_tags) {
  global_tags_ = global_tags;
}

const TagsType &StatsConfig::GetGlobalTags() const { return global_tags_; }

void StatsConfig::SetIsDisableStats(bool disable_stats) {
  is_stats_disabled_ = disable_stats;
}

bool StatsConfig::IsStatsDisabled() const { return is_stats_disabled_; }

void StatsConfig::SetReportInterval(const absl::Duration interval) {
  report_interval_ = interval;
}

const absl::Duration &StatsConfig::GetReportInterval() const { return report_interval_; }

void StatsConfig::SetHarvestInterval(const absl::Duration interval) {
  harvest_interval_ = interval;
}

const absl::Duration &StatsConfig::GetHarvestInterval() const {
  return harvest_interval_;
}

void StatsConfig::SetIsInitialized(bool initialized) { is_initialized_ = initialized; }

bool StatsConfig::IsInitialized() const { return is_initialized_; }

///
/// Metric
///
void Metric::Record(double value, const TagsType &tags) {
  if (StatsConfig::instance().IsStatsDisabled()) {
    return;
  }

  if (measure_ == nullptr) {
    measure_.reset(new opencensus::stats::Measure<double>(
        opencensus::stats::Measure<double>::Register(name_, description_, unit_)));
    RegisterView();
  }

  // Do record.
  TagsType combined_tags(tags);
  combined_tags.insert(std::end(combined_tags),
                       std::begin(StatsConfig::instance().GetGlobalTags()),
                       std::end(StatsConfig::instance().GetGlobalTags()));
  opencensus::stats::Record({{*measure_, value}}, combined_tags);
}

void Metric::Record(double value, std::unordered_map<std::string, std::string> &tags) {
  TagsType tags_pair_vec;
  std::for_each(
      tags.begin(), tags.end(),
      [&tags_pair_vec](std::pair<std::string, std::string> tag) {
        return tags_pair_vec.push_back({TagKeyType::Register(tag.first), tag.second});
      });
  Record(value, tags_pair_vec);
}

void Gauge::RegisterView() {
  opencensus::stats::ViewDescriptor view_descriptor =
      opencensus::stats::ViewDescriptor()
          .set_name(name_)
          .set_description(description_)
          .set_measure(name_)
          .set_aggregation(opencensus::stats::Aggregation::LastValue());
  RegisterAsView(view_descriptor, tag_keys_);
}

void Histogram::RegisterView() {
  opencensus::stats::ViewDescriptor view_descriptor =
      opencensus::stats::ViewDescriptor()
          .set_name(name_)
          .set_description(description_)
          .set_measure(name_)
          .set_aggregation(opencensus::stats::Aggregation::Distribution(
              opencensus::stats::BucketBoundaries::Explicit(boundaries_)));

  RegisterAsView(view_descriptor, tag_keys_);
}

void Count::RegisterView() {
  opencensus::stats::ViewDescriptor view_descriptor =
      opencensus::stats::ViewDescriptor()
          .set_name(name_)
          .set_description(description_)
          .set_measure(name_)
          .set_aggregation(opencensus::stats::Aggregation::Count());

  RegisterAsView(view_descriptor, tag_keys_);
}

void Sum::RegisterView() {
  opencensus::stats::ViewDescriptor view_descriptor =
      opencensus::stats::ViewDescriptor()
          .set_name(name_)
          .set_description(description_)
          .set_measure(name_)
          .set_aggregation(opencensus::stats::Aggregation::Sum());

  RegisterAsView(view_descriptor, tag_keys_);
}

}  // namespace stats

}  // namespace ray
