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

#include "ray/stats/metric_exporter.h"

#include <future>

namespace ray {
namespace stats {

template <>
void MetricPointExporter::ExportToPoints(
    const opencensus::stats::ViewData::DataMap<opencensus::stats::Distribution>
        &view_data,
    const opencensus::stats::MeasureDescriptor &measure_descriptor,
    std::vector<std::string> &keys,
    std::vector<MetricPoint> &points) {
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
  MetricPoint mean_point = {
      metric_name + ".mean", current_sys_time_ms(), hist_mean, tags, measure_descriptor};
  MetricPoint max_point = {
      metric_name + ".max", current_sys_time_ms(), hist_max, tags, measure_descriptor};
  MetricPoint min_point = {
      metric_name + ".min", current_sys_time_ms(), hist_min, tags, measure_descriptor};
  points.push_back(std::move(mean_point));
  points.push_back(std::move(max_point));
  points.push_back(std::move(min_point));

  if (points.size() >= report_batch_size_) {
    metric_exporter_client_->ReportMetrics(points);
    points.clear();
  }
}

void MetricPointExporter::ExportViewData(
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
      ExportToPoints<opencensus::stats::Distribution>(
          view_data.distribution_data(), measure_descriptor, keys, points);
      break;
    default:
      RAY_LOG(FATAL) << "Unknown view data type.";
      break;
    }
  }
  metric_exporter_client_->ReportMetrics(points);
}

OpenCensusProtoExporter::OpenCensusProtoExporter(const int port,
                                                 instrumented_io_context &io_service,
                                                 const std::string address)
    : client_call_manager_(io_service) {
  client_.reset(new rpc::MetricsAgentClient(address, port, client_call_manager_));
};

void OpenCensusProtoExporter::ExportViewData(
    const std::vector<std::pair<opencensus::stats::ViewDescriptor,
                                opencensus::stats::ViewData>> &data) {
  // Start converting opencensus data into their protobuf format.
  // The format can be found here
  // https://github.com/census-instrumentation/opencensus-proto/blob/master/src/opencensus/proto/metrics/v1/metrics.proto
  rpc::ReportOCMetricsRequest request_proto;

  for (const auto &datum : data) {
    // Unpack the fields we need for in memory data structure.
    auto &view_descriptor = datum.first;
    auto &view_data = datum.second;
    auto &measure_descriptor = view_descriptor.measure_descriptor();

    // Create one metric `Point` in protobuf.
    auto request_point_proto = request_proto.add_metrics();

    // Write the `MetricDescriptor`.
    auto metric_descriptor_proto = request_point_proto->mutable_metric_descriptor();
    metric_descriptor_proto->set_name(measure_descriptor.name());
    metric_descriptor_proto->set_description(measure_descriptor.description());
    metric_descriptor_proto->set_unit(measure_descriptor.units());
    for (const auto &tag_key : view_descriptor.columns()) {
      metric_descriptor_proto->add_label_keys()->set_key(tag_key.name());
    };

    // Helpers for writing the actual `TimeSeries`.
    auto start_time = absl::ToUnixSeconds(view_data.start_time());
    auto end_time = absl::ToUnixSeconds(view_data.end_time());
    auto make_new_data_point_proto = [&request_point_proto, start_time, end_time](
                                         const std::vector<std::string> &tag_values) {
      auto metric_timeseries_proto = request_point_proto->add_timeseries();
      metric_timeseries_proto->mutable_start_timestamp()->set_seconds(start_time);

      for (const auto &value : tag_values) {
        metric_timeseries_proto->add_label_values()->set_value(value);
      };

      auto point_proto = metric_timeseries_proto->add_points();
      point_proto->mutable_timestamp()->set_seconds(end_time);
      return point_proto;
    };

    // Write the `TimeSeries` for the given aggregated data type.
    switch (view_data.type()) {
    case opencensus::stats::ViewData::Type::kDouble:
      for (const auto &row : view_data.double_data()) {
        auto point_proto = make_new_data_point_proto(row.first /*tag_values*/);
        point_proto->set_double_value(row.second);
      }
      break;
    case opencensus::stats::ViewData::Type::kInt64:
      for (const auto &row : view_data.int_data()) {
        auto point_proto = make_new_data_point_proto(row.first /*tag_values*/);
        point_proto->set_int64_value(row.second);
      }
      break;
    case opencensus::stats::ViewData::Type::kDistribution:
      for (const auto &row : view_data.distribution_data()) {
        opencensus::stats::Distribution dist_value = row.second;

        auto point_proto = make_new_data_point_proto(row.first /*tag_values*/);

        // Copy in memory data into `DistributionValue` protobuf.
        auto distribution_proto = point_proto->mutable_distribution_value();
        distribution_proto->set_count(dist_value.count());
        distribution_proto->set_sum(dist_value.count() * dist_value.mean());
        distribution_proto->set_sum_of_squared_deviation(
            dist_value.sum_of_squared_deviation());

        // Write the `BucketOption` and `Bucket` data.
        auto bucket_opt_proto =
            distribution_proto->mutable_bucket_options()->mutable_explicit_();
        for (const auto &bound : dist_value.bucket_boundaries().lower_boundaries()) {
          bucket_opt_proto->add_bounds(bound);
        }
        for (const auto &count : dist_value.bucket_counts()) {
          distribution_proto->add_buckets()->set_count(count);
        }
      }
      break;
    default:
      RAY_LOG(FATAL) << "Unknown view data type.";
      break;
    }
  }

  client_->ReportOCMetrics(
      request_proto, [](const Status &status, const rpc::ReportOCMetricsReply &reply) {
        RAY_UNUSED(reply);
        if (!status.ok()) {
          RAY_LOG_EVERY_N(WARNING, 10000)
              << "Export metrics to agent failed: " << status
              << ". This won't affect Ray, but you can lose metrics from the cluster.";
        }
      });
}

}  // namespace stats
}  // namespace ray
