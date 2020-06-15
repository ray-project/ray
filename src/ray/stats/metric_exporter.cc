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

#include <future>

#include "ray/stats/metric_exporter.h"

namespace ray {
namespace stats {

template <>
void MetricExporter::ExportToPoints(
    const opencensus::stats::ViewData::DataMap<opencensus::stats::Distribution>
        &view_data,
    const std::string &metric_name, std::vector<std::string> &keys,
    MetricPoints &points) {
  if (view_data.size() == 0) {
    return;
  }
  // Note(lingxuan.zlx): No sampling in histgram data, so all points all be filled in.
  std::unordered_map<std::string, std::string> tags;
  for (size_t i = 0; i < view_data.begin()->first.size(); ++i) {
    tags[keys[i]] = view_data.begin()->first[i];
  }
  for (const auto &row : view_data) {
    MetricPoint mean_point{.metric_name = metric_name + ".mean",
                           .timestamp = current_sys_time_ms(),
                           .value = static_cast<double>(row.second.mean()),
                           .tags = tags};
    MetricPoint max_point{.metric_name = metric_name + ".max",
                          .timestamp = current_sys_time_ms(),
                          .value = static_cast<double>(row.second.max()),
                          .tags = tags};
    MetricPoint min_point{.metric_name = metric_name + ".min",
                          .timestamp = current_sys_time_ms(),
                          .value = static_cast<double>(row.second.min()),
                          .tags = tags};
    points.push_back(std::move(mean_point));
    points.push_back(std::move(max_point));
    points.push_back(std::move(min_point));
    RAY_LOG(DEBUG) << "Metric name " << metric_name
                   << ", mean value : " << mean_point.value
                   << " max value : " << max_point.value
                   << "  min value : " << min_point.value;

    if (points.size() >= kMaxBatchSize) {
      RAY_LOG(DEBUG) << "Point size : " << points.size();
      metric_exporter_client_->ReportMetrics(points);
      points.clear();
    }
  }
}

void MetricExporter::ExportViewData(
    const std::vector<std::pair<opencensus::stats::ViewDescriptor,
                                opencensus::stats::ViewData>> &data) {
  MetricPoints points;
  // NOTE(lingxuan.zlx): There is no sampling in view data, so all raw metric
  // data will be processed.
  for (const auto &datum : data) {
    auto &descriptor = datum.first;
    auto &view_data = datum.second;

    std::vector<std::string> keys;
    for (size_t i = 0; i < descriptor.columns().size(); ++i) {
      keys.push_back(descriptor.columns()[i].name());
    }
    auto &metric_name = descriptor.name();
    switch (view_data.type()) {
    case opencensus::stats::ViewData::Type::kDouble:
      ExportToPoints<double>(view_data.double_data(), metric_name, keys, points);
      break;
    case opencensus::stats::ViewData::Type::kInt64:
      ExportToPoints<int64_t>(view_data.int_data(), metric_name, keys, points);
      break;
    case opencensus::stats::ViewData::Type::kDistribution:
      ExportToPoints<opencensus::stats::Distribution>(view_data.distribution_data(),
                                                      metric_name, keys, points);
      break;
    default:
      RAY_LOG(WARNING) << "Unknown view data type.";
      break;
    }
  }
  RAY_LOG(DEBUG) << "Point size : " << points.size();
  metric_exporter_client_->ReportMetrics(points);
}
}  // namespace stats
}  // namespace ray
