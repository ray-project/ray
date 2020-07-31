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
    const opencensus::stats::MeasureDescriptor &measure_descriptor,
    std::vector<std::string> &keys, std::vector<MetricPoint> &points) {
  // Return if no raw data found in view map.
  if (view_data.size() == 0) {
    return;
  }

  const auto &metric_name = measure_descriptor.name();

  // NOTE(lingxuan.zlx): No sampling in histogram data, so all points all be filled in.
  std::unordered_map<std::string, std::string> tags;
  for (size_t i = 0; i < view_data.begin()->first.size(); ++i) {
    tags[keys[i]] = view_data.begin()->first[i];
  }
  // Histogram metric will be append suffix with mean/max/min.
  double hist_mean = 0.0;
  double hist_max = 0.0;
  double hist_min = 0.0;
  bool in_first_hist_data = true;
  for (const auto &row : view_data) {
    if (in_first_hist_data) {
      hist_mean = static_cast<double>(row.second.mean());
      hist_max = static_cast<double>(row.second.max());
      hist_min = static_cast<double>(row.second.min());
      in_first_hist_data = false;
    } else {
      hist_mean += static_cast<double>(row.second.mean());
      hist_max = std::max(hist_max, static_cast<double>(row.second.max()));
      hist_min = std::min(hist_min, static_cast<double>(row.second.min()));
    }
  }
  hist_mean /= view_data.size();
  MetricPoint mean_point = {metric_name + ".mean", current_sys_time_ms(), hist_mean, tags,
                            measure_descriptor};
  MetricPoint max_point = {metric_name + ".max", current_sys_time_ms(), hist_max, tags,
                           measure_descriptor};
  MetricPoint min_point = {metric_name + ".min", current_sys_time_ms(), hist_min, tags,
                           measure_descriptor};
  points.push_back(std::move(mean_point));
  points.push_back(std::move(max_point));
  points.push_back(std::move(min_point));
  RAY_LOG(DEBUG) << "Metric name " << metric_name << ", mean value : " << mean_point.value
                 << " max value : " << max_point.value
                 << "  min value : " << min_point.value;

  if (points.size() >= report_batch_size_) {
    RAY_LOG(DEBUG) << "Point size : " << points.size();
    metric_exporter_client_->ReportMetrics(points);
    points.clear();
  }
}

void MetricExporter::ExportViewData(
    const std::vector<std::pair<opencensus::stats::ViewDescriptor,
                                opencensus::stats::ViewData>> &data) {
  std::vector<MetricPoint> points;
  // NOTE(lingxuan.zlx): There is no sampling in view data, so all raw metric
  // data will be processed.
  for (const auto &datum : data) {
    auto &descriptor = datum.first;
    auto &view_data = datum.second;

    std::vector<std::string> keys;
    for (size_t i = 0; i < descriptor.columns().size(); ++i) {
      keys.push_back(descriptor.columns()[i].name());
    }
    const auto &measure_descriptor = descriptor.measure_descriptor();
    switch (view_data.type()) {
    case opencensus::stats::ViewData::Type::kDouble:
      ExportToPoints<double>(view_data.double_data(), measure_descriptor, keys, points);
      break;
    case opencensus::stats::ViewData::Type::kInt64:
      ExportToPoints<int64_t>(view_data.int_data(), measure_descriptor, keys, points);
      break;
    case opencensus::stats::ViewData::Type::kDistribution:
      ExportToPoints<opencensus::stats::Distribution>(view_data.distribution_data(),
                                                      measure_descriptor, keys, points);
      break;
    default:
      RAY_LOG(FATAL) << "Unknown view data type.";
      break;
    }
  }
  RAY_LOG(DEBUG) << "Point size : " << points.size();
  metric_exporter_client_->ReportMetrics(points);
}

}  // namespace stats
}  // namespace ray
