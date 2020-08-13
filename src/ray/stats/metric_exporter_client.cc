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

#include "ray/stats/metric_exporter_client.h"
#include "absl/time/time.h"

#include <algorithm>

namespace ray {
namespace stats {

///
/// Stdout Exporter
///
void StdoutExporterClient::ReportMetrics(const std::vector<MetricPoint> &points) {
  RAY_LOG(DEBUG) << "Metric point size : " << points.size();
}

///
/// Metrics Exporter Decorator
///
MetricExporterDecorator::MetricExporterDecorator(
    std::shared_ptr<MetricExporterClient> exporter)
    : exporter_(exporter) {}

void MetricExporterDecorator::ReportMetrics(const std::vector<MetricPoint> &points) {
  if (exporter_) {
    exporter_->ReportMetrics(points);
  }
}

///
/// Metrics Agent Exporter
///
MetricsAgentExporter::MetricsAgentExporter(std::shared_ptr<MetricExporterClient> exporter,
                                           const int port,
                                           boost::asio::io_service &io_service,
                                           const std::string address)
    : MetricExporterDecorator(exporter), client_call_manager_(io_service) {
  client_.reset(new rpc::MetricsAgentClient(address, port, client_call_manager_));
}

void MetricsAgentExporter::ReportMetrics(const std::vector<MetricPoint> &points) {
  MetricExporterDecorator::ReportMetrics(points);
  rpc::ReportMetricsRequest request;
  for (auto point : points) {
    auto metric_point = request.add_metrics_points();
    metric_point->set_metric_name(point.metric_name);
    metric_point->set_timestamp(point.timestamp);
    metric_point->set_value(point.value);
    auto mutable_tags = metric_point->mutable_tags();
    for (auto &tag : point.tags) {
      (*mutable_tags)[tag.first] = tag.second;
    }
    // If description and units information is requested from
    // the metrics agent, append the information.
    // TODO(sang): It can be inefficient if there are lots of new registered metrics.
    // We should make it more efficient if there's compelling use cases.
    if (should_update_description_) {
      metric_point->set_description(point.measure_descriptor.description());
      metric_point->set_units(point.measure_descriptor.units());
    }
  }
  should_update_description_ = false;

  // TODO(sang): Should retry metrics report if it fails.
  client_->ReportMetrics(
      request, [this](const Status &status, const rpc::ReportMetricsReply &reply) {
        if (!status.ok()) {
          RAY_LOG(WARNING) << "ReportMetrics failed with status " << status;
        }
        should_update_description_ = reply.metrcs_description_required();
      });
}

void MetricsAgentExporter::ReportMetrics(
    const std::vector<std::pair<opencensus::stats::ViewDescriptor,
                                opencensus::stats::ViewData>> &data) {
  rpc::ReportOCMetricsRequest request_proto;
  for (auto datum : data) {
    auto view_descriptor = datum.first;
    auto view_data = datum.second;
    auto measure_descriptor = view_descriptor.measure_descriptor();

    auto request_point_proto = request_proto.add_metrics();

    auto metric_descriptor_proto = request_point_proto->mutable_metric_descriptor();
    metric_descriptor_proto->set_name(measure_descriptor.name());
    metric_descriptor_proto->set_description(measure_descriptor.description());
    metric_descriptor_proto->set_unit(measure_descriptor.units());
    for (auto &tag_key : view_descriptor.columns()) {
      metric_descriptor_proto->add_label_keys()->set_key(tag_key.name());
    };

    auto start_time = absl::ToUnixSeconds(view_data.start_time());
    auto end_time = absl::ToUnixSeconds(view_data.end_time());

    auto make_new_data_point_proto = [request_point_proto, start_time,
                                      end_time](std::vector<std::string> tag_values) {
      auto metric_timeseries_proto = request_point_proto->add_timeseries();
      metric_timeseries_proto->mutable_start_timestamp()->set_seconds(start_time);

      for (auto value : tag_values) {
        metric_timeseries_proto->add_label_values()->set_value(value);
      };

      auto point_proto = metric_timeseries_proto->add_points();
      point_proto->mutable_timestamp()->set_seconds(end_time);
      return point_proto;
    };

    switch (view_data.type()) {
    case opencensus::stats::ViewData::Type::kDouble:
      for (auto &row : view_data.double_data()) {
        std::vector<std::string> tag_values = row.first;
        double point_value = row.second;

        auto point_proto = make_new_data_point_proto(tag_values);
        point_proto->set_double_value(point_value);
      }
      break;
    case opencensus::stats::ViewData::Type::kInt64:
      for (auto &row : view_data.int_data()) {
        auto tag_values = row.first;
        int64_t point_value = row.second;

        auto point_proto = make_new_data_point_proto(tag_values);
        point_proto->set_int64_value(point_value);
      }
      break;
    case opencensus::stats::ViewData::Type::kDistribution:
      for (auto &row : view_data.distribution_data()) {
        auto tag_values = row.first;
        opencensus::stats::Distribution dist_value = row.second;

        auto point_proto = make_new_data_point_proto(tag_values);
        point_proto->mutable_timestamp()->set_seconds(end_time);
        auto distribution_proto = point_proto->mutable_distribution_value();
        distribution_proto->set_count(dist_value.count());
        distribution_proto->set_sum(dist_value.count() * dist_value.mean());
        distribution_proto->set_sum_of_squared_deviation(
            dist_value.sum_of_squared_deviation());

        auto bucket_opt_proto =
            distribution_proto->mutable_bucket_options()->mutable_explicit_();
        for (auto bound : dist_value.bucket_boundaries().lower_boundaries()) {
          bucket_opt_proto->add_bounds(bound);
        }
        for (auto count : dist_value.bucket_counts()) {
          distribution_proto->add_buckets()->set_count(count);
        }
      }
      break;
    default:
      RAY_LOG(FATAL) << "Unknown view data type.";
      break;
    }
  }

  // TODO: do batching if the size is too large
  client_->ReportOCMetrics(
      request_proto, [](const Status &status, const rpc::ReportOCMetricsReply &reply) {
        RAY_UNUSED(reply);
        if (!status.ok()) {
          RAY_LOG(WARNING) << "Export metrics to agent failed: " << status;
        }
      });
}

}  // namespace stats
}  // namespace ray
