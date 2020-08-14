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
                                           const std::string address,
                                           size_t report_batch_size)
    : MetricExporterDecorator(exporter),
      client_call_manager_(io_service),
      report_batch_size_(report_batch_size) {
  client_.reset(new rpc::MetricsAgentClient(address, port, client_call_manager_));
}

void MetricsAgentExporter::ReportMetrics(const std::vector<MetricPoint> &points) {
  MetricExporterDecorator::ReportMetrics(points);
}

void MetricsAgentExporter::ReportMetrics(
    const std::vector<std::pair<opencensus::stats::ViewDescriptor,
                                opencensus::stats::ViewData>> &opencensus_data) {
  // Start converting opencensus data into their protobuf format.
  // The format can be found here
  // https://github.com/census-instrumentation/opencensus-proto/blob/master/src/opencensus/proto/metrics/v1/metrics.proto
  std::vector<rpc::ReportOCMetricsRequest> request_proto_batches;
  size_t batch_counter = 0;

  request_proto_batches.emplace_back();
  auto &current_request_proto = request_proto_batches.at(0);

  for (const auto &datum : opencensus_data) {
    // Unpack the fields we need for in memory data structure.
    auto view_descriptor = datum.first;
    auto view_data = datum.second;
    auto measure_descriptor = view_descriptor.measure_descriptor();

    // Create one metric `Point` in protobuf.
    auto request_point_proto = current_request_proto.add_metrics();

    // Write the `MetricDescriptor`.
    auto metric_descriptor_proto = request_point_proto->mutable_metric_descriptor();
    metric_descriptor_proto->set_name(measure_descriptor.name());
    metric_descriptor_proto->set_description(measure_descriptor.description());
    metric_descriptor_proto->set_unit(measure_descriptor.units());
    for (auto &tag_key : view_descriptor.columns()) {
      metric_descriptor_proto->add_label_keys()->set_key(tag_key.name());
    };

    // Helpers for writing the actual `TimeSeries`.
    auto start_time = absl::ToUnixSeconds(view_data.start_time());
    auto end_time = absl::ToUnixSeconds(view_data.end_time());
    auto make_new_data_point_proto = [request_point_proto, start_time, end_time](
                                         const std::vector<std::string> tag_values) {
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
        std::vector<std::string> tag_values = row.first;
        double point_value = row.second;

        auto point_proto = make_new_data_point_proto(tag_values);
        point_proto->set_double_value(point_value);
      }
      break;
    case opencensus::stats::ViewData::Type::kInt64:
      for (const auto &row : view_data.int_data()) {
        auto tag_values = row.first;
        int64_t point_value = row.second;

        auto point_proto = make_new_data_point_proto(tag_values);
        point_proto->set_int64_value(point_value);
      }
      break;
    case opencensus::stats::ViewData::Type::kDistribution:
      for (const auto &row : view_data.distribution_data()) {
        auto tag_values = row.first;
        opencensus::stats::Distribution dist_value = row.second;

        auto point_proto = make_new_data_point_proto(tag_values);

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

    // Increment the batch counter, and add another request if necessary.
    batch_counter++;
    if (batch_counter == report_batch_size_) {
      batch_counter = 0;
      request_proto_batches.emplace_back();
      current_request_proto = request_proto_batches.at(request_proto_batches.size() - 1);
    }
  }

  for (const auto &request : request_proto_batches) {
    client_->ReportOCMetrics(
        request, [](const Status &status, const rpc::ReportOCMetricsReply &reply) {
          RAY_UNUSED(reply);
          if (!status.ok()) {
            RAY_LOG(WARNING) << "Export metrics to agent failed: " << status;
          }
        });
  }
}

}  // namespace stats
}  // namespace ray
